"""Governed compensating-control execution with factual terminal receipts."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import logging
import math
import os
import threading
import unicodedata
from collections.abc import Collection, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit

import httpx

from remediation_models import (
    ActionableTarget,
    RemediationReceipt,
    RemediationStatus,
)


logger = logging.getLogger("Aftershock-Engine")
_HTTPX_REQUEST_LOG_TEMPLATE = 'HTTP Request: %s %s "%s %d %s"'
_HTTPX_REQUEST_LOG_FILTER_LOCK = threading.Lock()
_ALLOWLIST_ENV = "AFTERSHOCK_REMEDIATION_ALLOWLIST_JSON"
_ALLOWLIST_ERROR = "invalid remediation endpoint allowlist configuration"
_MAX_EXTERNAL_RECEIPT_ID_LENGTH = 256

# Compensating-control response contract v1. A terminal result is proven only
# by a JSON object with these three top-level fields:
#   {"receipt_version": 1, "status": "succeeded", "receipt_id": "..."}
# ``receipt_id`` must be a nonblank, control-free string of at most 256 code
# points. ``failed`` is terminal failure; ``accepted`` and ``pending`` are
# explicitly nonterminal. HTTP 408 and 5xx remain ambiguous unless this
# contract explicitly proves a terminal result. Extra fields are never logged.
_V1_TERMINAL_STATUSES = frozenset({"succeeded", "failed"})
_V1_NONTERMINAL_STATUSES = frozenset({"accepted", "pending"})


class RemediationConfigurationError(RuntimeError):
    """The operator-provided outbound endpoint policy is unusable."""


class _HttpxRequestURLFilter(logging.Filter):
    """Sanitize only HTTPX's stable request-summary URL argument."""

    def filter(self, record: logging.LogRecord) -> bool:
        if (
            record.name == "httpx"
            and record.msg == _HTTPX_REQUEST_LOG_TEMPLATE
            and isinstance(record.args, tuple)
            and len(record.args) == 5
        ):
            arguments = list(record.args)
            arguments[1] = (
                _sanitize_endpoint(str(arguments[1]))
                or "<invalid-remediation-endpoint>"
            )
            record.args = tuple(arguments)
        return True


_HTTPX_REQUEST_URL_FILTER = _HttpxRequestURLFilter()


def _install_httpx_request_log_filter() -> None:
    """Install the URL filter once without altering HTTPX logger policy."""

    httpx_logger = logging.getLogger("httpx")
    with _HTTPX_REQUEST_LOG_FILTER_LOCK:
        if _HTTPX_REQUEST_URL_FILTER not in httpx_logger.filters:
            httpx_logger.addFilter(_HTTPX_REQUEST_URL_FILTER)


def parse_remediation_allowlist_json(raw: str | None) -> frozenset[str]:
    """Parse a nonempty JSON array of exact, independently safe URLs.

    The returned strings are deliberately not broadened or prefix-matched.
    Path and query are part of the authorization decision.
    """

    if not isinstance(raw, str) or not raw.strip():
        raise RemediationConfigurationError(_ALLOWLIST_ERROR)
    try:
        decoded = json.loads(raw)
    except (TypeError, ValueError):
        raise RemediationConfigurationError(_ALLOWLIST_ERROR) from None
    if not isinstance(decoded, list) or not decoded:
        raise RemediationConfigurationError(_ALLOWLIST_ERROR)

    endpoints: set[str] = set()
    for endpoint in decoded:
        if not isinstance(endpoint, str) or not _is_authorizable_endpoint(endpoint):
            raise RemediationConfigurationError(_ALLOWLIST_ERROR)
        endpoints.add(endpoint)
    if not endpoints:
        raise RemediationConfigurationError(_ALLOWLIST_ERROR)
    return frozenset(endpoints)


def build_remediation_allowlist_from_env(
    environ: Mapping[str, str] | None = None,
) -> frozenset[str]:
    """Build the exact endpoint policy from operator environment config."""

    source = os.environ if environ is None else environ
    return parse_remediation_allowlist_json(source.get(_ALLOWLIST_ENV))


class CompensatingActionEngine:
    """Execute governed controls with bounded concurrency and one deadline."""

    def __init__(
        self,
        http_client: httpx.AsyncClient | None = None,
        *,
        allowed_endpoints: Collection[str] | None = None,
        max_concurrency: int = 8,
        workflow_timeout_seconds: float = 30.0,
    ) -> None:
        _install_httpx_request_log_filter()
        if (
            isinstance(max_concurrency, bool)
            or not isinstance(max_concurrency, int)
            or max_concurrency <= 0
        ):
            raise ValueError("max_concurrency must be a positive integer")
        if (
            isinstance(workflow_timeout_seconds, bool)
            or not isinstance(workflow_timeout_seconds, (int, float))
            or not math.isfinite(workflow_timeout_seconds)
            or workflow_timeout_seconds <= 0
        ):
            raise ValueError("workflow_timeout_seconds must be finite and positive")
        if isinstance(allowed_endpoints, (str, bytes)):
            raise RemediationConfigurationError(_ALLOWLIST_ERROR)

        configured_endpoints = tuple(allowed_endpoints or ())
        if any(
            not isinstance(endpoint, str)
            or not _is_authorizable_endpoint(endpoint)
            for endpoint in configured_endpoints
        ):
            raise RemediationConfigurationError(_ALLOWLIST_ERROR)
        governed_endpoints = frozenset(configured_endpoints)

        self.http_client = http_client or httpx.AsyncClient(timeout=10.0)
        self._owns_client = http_client is None
        self.allowed_endpoints = governed_endpoints
        self.max_concurrency = max_concurrency
        self.workflow_timeout_seconds = float(workflow_timeout_seconds)

    async def aclose(self) -> None:
        """Close the HTTP client only when the engine created it."""

        if self._owns_client:
            await self.http_client.aclose()

    async def execute_rollback(
        self, target: ActionableTarget, incident_id: str
    ) -> RemediationReceipt:
        """Attempt one governed control and return secret-safe evidence."""

        missing = _missing_playbook_error(target)
        if missing is not None:
            logger.warning(
                "Skipping remediation control for target %s: incomplete playbook",
                target.urn,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=_sanitize_endpoint(target.remediation_webhook),
                status="skipped",
                error=missing,
            )

        raw_endpoint = target.remediation_webhook
        if not _is_authorizable_endpoint(raw_endpoint):
            logger.error(
                "Remediation control for target %s has an invalid endpoint",
                target.urn,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=None,
                status="failed",
                error="invalid remediation endpoint",
            )

        endpoint = _sanitize_endpoint(raw_endpoint)
        if raw_endpoint not in self.allowed_endpoints:
            logger.warning(
                "Skipping remediation control for target %s: endpoint denied by policy",
                target.urn,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="skipped",
                error="remediation endpoint is not allowlisted",
            )

        payload = {
            "incident_id": incident_id,
            "target_urn": target.urn,
            "action": "REVERT_STATE",
            "business_action": target.business_action,
        }

        try:
            request = self.http_client.build_request(
                "POST",
                raw_endpoint,
                json=payload,
                headers={
                    "Idempotency-Key": _idempotency_key(
                        incident_id, target.urn, target.business_action
                    )
                },
            )
        except Exception:
            logger.error(
                "Remediation request preparation failed for target %s",
                target.urn,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="failed",
                error="remediation request could not be prepared",
            )

        try:
            # The exact configured URL is sent only after exact authorization;
            # its sanitized form is the only form logged or persisted. Once
            # ``send`` begins, a transport error cannot prove non-execution.
            response = await self.http_client.send(
                request,
                follow_redirects=False,
            )
        except httpx.RequestError:
            logger.error(
                "Remediation outcome is unknown after dispatch for target %s",
                target.urn,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="outcome_unknown",
                error="remediation outcome unknown after dispatch",
            )
        except Exception:
            # A transport exception can happen after a peer applied the action.
            # Details may contain URLs, bodies, or credentials, so omit them.
            logger.error(
                "Unexpected remediation outcome after dispatch for target %s",
                target.urn,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="outcome_unknown",
                error="remediation outcome unknown after dispatch",
            )

        return _receipt_from_response(target, incident_id, endpoint, response)

    async def process_blast_radius(
        self, targets: Sequence[ActionableTarget], incident_id: str
    ) -> list[RemediationReceipt]:
        """Execute targets with bounded workers, preserving input ordering."""

        logger.info(
            "Processing %d downstream remediation controls for incident %s",
            len(targets),
            incident_id,
        )
        if not targets:
            return []

        receipts: list[RemediationReceipt | None] = [None] * len(targets)
        started: set[int] = set()
        next_index = 0

        async def worker() -> None:
            nonlocal next_index
            while next_index < len(targets):
                index = next_index
                next_index += 1
                started.add(index)
                receipts[index] = await self.execute_rollback(
                    targets[index], incident_id
                )
                # Even locally skipped targets must not monopolize the event
                # loop and bypass the workflow deadline.
                await asyncio.sleep(0)

        workers = [
            asyncio.create_task(worker())
            for _ in range(min(self.max_concurrency, len(targets)))
        ]
        deadline_expired = False
        try:
            async with asyncio.timeout(self.workflow_timeout_seconds):
                await asyncio.gather(*workers)
        except TimeoutError:
            deadline_expired = True
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
        except BaseException:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise

        if deadline_expired:
            for index, receipt in enumerate(receipts):
                if receipt is not None:
                    continue
                was_dispatched = index in started
                receipts[index] = _receipt(
                    targets[index],
                    incident_id,
                    endpoint=_sanitize_endpoint(targets[index].remediation_webhook),
                    status=("outcome_unknown" if was_dispatched else "skipped"),
                    error=(
                        "workflow deadline expired after dispatch"
                        if was_dispatched
                        else "workflow deadline expired before dispatch"
                    ),
                )

        settled = [receipt for receipt in receipts if receipt is not None]
        if len(settled) != len(targets):  # pragma: no cover - invariant guard
            raise RuntimeError("remediation worker did not settle every target")

        logger.info(
            "Remediation controls settled for incident %s: %s",
            incident_id,
            ", ".join(
                f"{status}={sum(receipt.status == status for receipt in settled)}"
                for status in (
                    "succeeded",
                    "accepted",
                    "failed",
                    "skipped",
                    "outcome_unknown",
                )
            ),
        )
        return settled


def _receipt_from_response(
    target: ActionableTarget,
    incident_id: str,
    endpoint: str,
    response: httpx.Response,
) -> RemediationReceipt:
    status_code = response.status_code
    ambiguous_http_status = status_code == 408 or 500 <= status_code < 600
    if not 200 <= status_code < 300 and not ambiguous_http_status:
        logger.error(
            "Remediation control failed for target %s with HTTP %d",
            target.urn,
            status_code,
        )
        return _receipt(
            target,
            incident_id,
            endpoint=endpoint,
            status="failed",
            http_status=status_code,
            error=f"remediation endpoint returned HTTP {status_code}",
        )

    payload = _response_json_object(response)
    contract = _v1_contract(payload)
    if ambiguous_http_status and (
        contract is None or contract[0] not in _V1_TERMINAL_STATUSES
    ):
        logger.warning(
            "Remediation outcome is unknown for target %s after HTTP %d",
            target.urn,
            status_code,
        )
        return _receipt(
            target,
            incident_id,
            endpoint=endpoint,
            status="outcome_unknown",
            http_status=status_code,
            error="remediation outcome unknown after dispatch",
        )

    if status_code == 202:
        accepted_receipt_id = (
            contract[1]
            if contract is not None
            else _valid_receipt_id(payload.get("receipt_id"))
            if payload is not None
            else None
        )
        return _receipt(
            target,
            incident_id,
            endpoint=endpoint,
            status="accepted",
            http_status=status_code,
            external_receipt_id=accepted_receipt_id,
        )

    if contract is not None:
        contract_status, receipt_id = contract
        if contract_status == "succeeded":
            logger.info(
                "Remediation control returned terminal success for target %s",
                target.urn,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="succeeded",
                http_status=status_code,
                external_receipt_id=receipt_id,
            )
        if contract_status == "failed":
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="failed",
                http_status=status_code,
                external_receipt_id=receipt_id,
                error="remediation endpoint reported terminal failure",
            )
        return _receipt(
            target,
            incident_id,
            endpoint=endpoint,
            status="accepted",
            http_status=status_code,
            external_receipt_id=receipt_id,
        )

    if payload is not None and payload.get("accepted") is True:
        return _receipt(
            target,
            incident_id,
            endpoint=endpoint,
            status="accepted",
            http_status=status_code,
            external_receipt_id=_valid_receipt_id(payload.get("receipt_id")),
        )

    return _receipt(
        target,
        incident_id,
        endpoint=endpoint,
        status="outcome_unknown",
        http_status=status_code,
        error="remediation endpoint returned no valid terminal receipt",
    )


def _response_json_object(response: httpx.Response) -> dict[str, object] | None:
    try:
        payload = response.json()
    except Exception:
        # Decoder details and body excerpts are not receipt evidence.
        return None
    return payload if isinstance(payload, dict) else None


def _v1_contract(
    payload: dict[str, object] | None,
) -> tuple[str, str] | None:
    if payload is None or type(payload.get("receipt_version")) is not int:
        return None
    if payload["receipt_version"] != 1:
        return None
    status = payload.get("status")
    receipt_id = _valid_receipt_id(payload.get("receipt_id"))
    valid_statuses = _V1_TERMINAL_STATUSES | _V1_NONTERMINAL_STATUSES
    if not isinstance(status, str) or status not in valid_statuses or receipt_id is None:
        return None
    return status, receipt_id


def _valid_receipt_id(value: object) -> str | None:
    if not isinstance(value, str) or value != value.strip() or not value:
        return None
    if len(value) > _MAX_EXTERNAL_RECEIPT_ID_LENGTH:
        return None
    if any(unicodedata.category(character).startswith("C") for character in value):
        return None
    return value


def _missing_playbook_error(target: ActionableTarget) -> str | None:
    missing_action = not target.business_action
    missing_webhook = not target.remediation_webhook
    if missing_action and missing_webhook:
        return "missing business action and remediation webhook"
    if missing_action:
        return "missing business action"
    if missing_webhook:
        return "missing remediation webhook"
    return None


def _is_authorizable_endpoint(raw_url: object) -> bool:
    """Validate one exact URL without broadening its authorization scope."""

    if (
        not isinstance(raw_url, str)
        or not raw_url
        or raw_url != raw_url.strip()
        or "#" in raw_url
    ):
        return False
    if any(
        character.isspace()
        or unicodedata.category(character).startswith("C")
        for character in raw_url
    ):
        return False
    try:
        parts = urlsplit(raw_url)
        scheme = parts.scheme.lower()
        hostname = parts.hostname
        _ = parts.port
    except (UnicodeError, ValueError):
        return False
    if scheme not in {"http", "https"} or not hostname:
        return False
    if parts.username is not None or parts.password is not None or parts.fragment:
        return False
    if scheme == "http" and not _is_loopback_host(hostname):
        return False
    return True


def _is_loopback_host(hostname: str) -> bool:
    if hostname.casefold() == "localhost":
        return True
    try:
        return ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        return False


def _sanitize_endpoint(raw_url: str | None) -> str | None:
    """Return only an HTTP(S) endpoint's scheme, host, port, and path."""

    if not isinstance(raw_url, str) or not raw_url:
        return None
    try:
        parts = urlsplit(raw_url)
        scheme = parts.scheme.lower()
        hostname = parts.hostname
        port = parts.port
    except (UnicodeError, ValueError):
        return None

    if scheme not in {"http", "https"} or not hostname:
        return None
    if any(character.isspace() for character in hostname):
        return None

    safe_host = f"[{hostname}]" if ":" in hostname else hostname
    netloc = f"{safe_host}:{port}" if port is not None else safe_host
    return urlunsplit((scheme, netloc, parts.path, "", ""))


def _idempotency_key(
    incident_id: str, target_urn: str, business_action: str
) -> str:
    """Derive a stable opaque key for a downstream service's retry contract."""

    digest = hashlib.sha256()
    for value in (incident_id, target_urn, business_action):
        encoded = value.encode("utf-8", errors="surrogatepass")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return f"aftershock-{digest.hexdigest()}"


def _receipt(
    target: ActionableTarget,
    incident_id: str,
    *,
    endpoint: str | None,
    status: RemediationStatus,
    http_status: int | None = None,
    external_receipt_id: str | None = None,
    error: str | None = None,
) -> RemediationReceipt:
    return RemediationReceipt(
        incident_id=incident_id,
        target_urn=target.urn,
        entity_type=target.entity_type,
        business_action=target.business_action,
        endpoint=endpoint,
        status=status,
        http_status=http_status,
        external_receipt_id=external_receipt_id,
        error=error,
    )
