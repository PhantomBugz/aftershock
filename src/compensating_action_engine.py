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
from dataclasses import dataclass
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
_MAX_RECEIPT_RESPONSE_BYTES = 64 * 1024
_MAX_LOG_VALUE_LENGTH = 256
_CONTROL_KEYS = frozenset(
    {"target_urn", "entity_type", "business_action", "endpoint"}
)

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


@dataclass(frozen=True, slots=True)
class RemediationGrant:
    """One immutable, exact authorization for one compensating control."""

    target_urn: str
    entity_type: str
    business_action: str
    endpoint: str

    def __post_init__(self) -> None:
        if (
            not _is_exact_control_value(self.target_urn)
            or not _is_exact_control_value(self.entity_type)
            or not _is_exact_control_value(self.business_action)
            or not _is_authorizable_endpoint(self.endpoint)
        ):
            raise RemediationConfigurationError(_ALLOWLIST_ERROR)


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


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


def parse_remediation_allowlist_json(
    raw: str | None,
) -> frozenset[RemediationGrant]:
    """Parse exact target/action/endpoint grants from operator JSON."""

    if not isinstance(raw, str) or not raw.strip():
        raise RemediationConfigurationError(_ALLOWLIST_ERROR)
    try:
        decoded = json.loads(raw, object_pairs_hook=_strict_json_object)
    except (TypeError, ValueError):
        raise RemediationConfigurationError(_ALLOWLIST_ERROR) from None
    if not isinstance(decoded, list) or not decoded:
        raise RemediationConfigurationError(_ALLOWLIST_ERROR)

    controls: set[RemediationGrant] = set()
    for item in decoded:
        if not isinstance(item, dict) or set(item) != _CONTROL_KEYS:
            raise RemediationConfigurationError(_ALLOWLIST_ERROR)
        try:
            controls.add(
                RemediationGrant(
                    target_urn=item["target_urn"],
                    entity_type=item["entity_type"],
                    business_action=item["business_action"],
                    endpoint=item["endpoint"],
                )
            )
        except (KeyError, TypeError, RemediationConfigurationError):
            raise RemediationConfigurationError(_ALLOWLIST_ERROR) from None
    if not controls:
        raise RemediationConfigurationError(_ALLOWLIST_ERROR)
    return frozenset(controls)


def build_remediation_allowlist_from_env(
    environ: Mapping[str, str] | None = None,
) -> frozenset[RemediationGrant]:
    """Build exact remediation grants from operator environment config."""

    source = os.environ if environ is None else environ
    return parse_remediation_allowlist_json(source.get(_ALLOWLIST_ENV))


class CompensatingActionEngine:
    """Execute governed controls with bounded concurrency and one deadline."""

    def __init__(
        self,
        http_client: httpx.AsyncClient | None = None,
        *,
        allowed_controls: Collection[RemediationGrant] | None = None,
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
        if isinstance(allowed_controls, (str, bytes, Mapping)):
            raise RemediationConfigurationError(_ALLOWLIST_ERROR)

        configured_controls = tuple(allowed_controls or ())
        if any(
            type(control) is not RemediationGrant
            for control in configured_controls
        ):
            raise RemediationConfigurationError(_ALLOWLIST_ERROR)
        governed_controls = frozenset(configured_controls)

        self.http_client = http_client or httpx.AsyncClient(timeout=10.0)
        self._owns_client = http_client is None
        self.allowed_controls = governed_controls
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

        log_target = _safe_log_value(target.urn)
        missing = _missing_playbook_error(target)
        if missing is not None:
            logger.warning(
                "Skipping remediation control for target %s: incomplete playbook",
                log_target,
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
                log_target,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=None,
                status="failed",
                error="invalid remediation endpoint",
            )

        endpoint = _sanitize_endpoint(raw_endpoint)
        try:
            selected_control = RemediationGrant(
                target_urn=target.urn,
                entity_type=target.entity_type,
                business_action=target.business_action,
                endpoint=raw_endpoint,
            )
        except RemediationConfigurationError:
            selected_control = None
        if selected_control not in self.allowed_controls:
            logger.warning(
                "Skipping remediation control for target %s: control denied by policy",
                log_target,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="skipped",
                error="remediation control is not authorized",
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
                    ),
                    "Accept-Encoding": "identity",
                },
            )
        except Exception:
            logger.error(
                "Remediation request preparation failed for target %s",
                log_target,
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
                stream=True,
            )
        except httpx.RequestError:
            logger.error(
                "Remediation outcome is unknown after dispatch for target %s",
                log_target,
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
                log_target,
            )
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="outcome_unknown",
                error="remediation outcome unknown after dispatch",
            )

        return await _receipt_from_response(
            target, incident_id, endpoint, response
        )

    async def process_blast_radius(
        self, targets: Sequence[ActionableTarget], incident_id: str
    ) -> list[RemediationReceipt]:
        """Execute targets with bounded workers, preserving input ordering."""

        log_incident = _safe_log_value(incident_id)
        logger.info(
            "Processing %d downstream remediation controls for incident %s",
            len(targets),
            log_incident,
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
            log_incident,
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


async def _receipt_from_response(
    target: ActionableTarget,
    incident_id: str,
    endpoint: str,
    response: httpx.Response,
) -> RemediationReceipt:
    status_code = response.status_code
    log_target = _safe_log_value(target.urn)
    body, read_error = await _read_bounded_response(response)
    ambiguous_http_status = status_code == 408 or 500 <= status_code < 600
    clear_http_failure = not 200 <= status_code < 300 and not ambiguous_http_status

    if read_error is not None:
        if clear_http_failure:
            logger.error(
                "Remediation control failed for target %s with HTTP %d",
                log_target,
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
        logger.warning(
            "Remediation response could not prove an outcome for target %s",
            log_target,
        )
        return _receipt(
            target,
            incident_id,
            endpoint=endpoint,
            status="outcome_unknown",
            http_status=status_code,
            error=read_error,
        )

    payload = _response_json_object(body)
    contract = _v1_contract(payload)
    if clear_http_failure:
        if contract is not None and contract[0] == "failed":
            return _receipt(
                target,
                incident_id,
                endpoint=endpoint,
                status="failed",
                http_status=status_code,
                external_receipt_id=contract[1],
                error="remediation endpoint reported terminal failure",
            )
        logger.error(
            "Remediation control failed for target %s with HTTP %d",
            log_target,
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

    if ambiguous_http_status and (
        contract is None or contract[0] not in _V1_TERMINAL_STATUSES
    ):
        logger.warning(
            "Remediation outcome is unknown for target %s after HTTP %d",
            log_target,
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

    if contract is not None:
        contract_status, receipt_id = contract
        if contract_status == "succeeded":
            logger.info(
                "Remediation control returned terminal success for target %s",
                log_target,
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

    if status_code == 202:
        return _receipt(
            target,
            incident_id,
            endpoint=endpoint,
            status="accepted",
            http_status=status_code,
        )

    return _receipt(
        target,
        incident_id,
        endpoint=endpoint,
        status="outcome_unknown",
        http_status=status_code,
        error="remediation endpoint returned no valid terminal receipt",
    )


async def _read_bounded_response(
    response: httpx.Response,
) -> tuple[bytes | None, str | None]:
    """Read at most 64 KiB of raw identity-encoded receipt evidence."""

    body = bytearray()
    read_error: str | None = None
    try:
        content_encoding = response.headers.get("Content-Encoding", "identity")
        if content_encoding.strip().casefold() not in {"", "identity"}:
            read_error = "remediation response used unsupported content encoding"
        elif response.is_stream_consumed:
            loaded_body = response.content
            if len(loaded_body) > _MAX_RECEIPT_RESPONSE_BYTES:
                read_error = (
                    "remediation response exceeded "
                    f"{_MAX_RECEIPT_RESPONSE_BYTES} bytes"
                )
            else:
                body.extend(loaded_body)
        elif not isinstance(response.stream, httpx.AsyncByteStream):
            read_error = "remediation response could not be read"
        else:
            # Iterate the raw stream directly so HTTPX cannot mark the response
            # closed before its own close await completes. Closure is handled
            # exactly once below and is protected from caller cancellation.
            response.is_stream_consumed = True
            async for chunk in response.stream:
                if len(body) + len(chunk) > _MAX_RECEIPT_RESPONSE_BYTES:
                    read_error = (
                        "remediation response exceeded "
                        f"{_MAX_RECEIPT_RESPONSE_BYTES} bytes"
                    )
                    break
                body.extend(chunk)
    except Exception:
        read_error = "remediation response could not be read"
    finally:
        closed = await _close_response_before_cancellation(response)
        if not closed and read_error is None:
            read_error = "remediation response could not be closed"

    if read_error is not None:
        return None, read_error
    return bytes(body), None


async def _close_response_before_cancellation(
    response: httpx.Response,
) -> bool:
    """Finish one response close before propagating caller cancellation."""

    close_task = asyncio.create_task(response.aclose())
    cancellation: asyncio.CancelledError | None = None
    close_failed = False
    while not close_task.done():
        try:
            await asyncio.shield(close_task)
        except asyncio.CancelledError as observed:
            if cancellation is None:
                cancellation = observed
        except Exception:
            close_failed = True
            break

    if close_task.done():
        try:
            close_task.result()
        except asyncio.CancelledError as observed:
            if cancellation is None:
                cancellation = observed
        except Exception:
            close_failed = True

    if cancellation is not None:
        raise cancellation
    return not close_failed


def _response_json_object(body: bytes | None) -> dict[str, object] | None:
    if body is None:
        return None
    try:
        payload = json.loads(
            body.decode("utf-8"), object_pairs_hook=_strict_json_object
        )
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


def _safe_log_value(value: object) -> str:
    """Render one bounded log field without line or terminal controls."""

    try:
        text = str(value)
    except Exception:
        return "<unprintable>"
    safe = "".join(
        " "
        if character.isspace()
        or unicodedata.category(character).startswith("C")
        else character
        for character in text
    )
    collapsed = " ".join(safe.split()) or "<empty>"
    if len(collapsed) > _MAX_LOG_VALUE_LENGTH:
        return f"{collapsed[: _MAX_LOG_VALUE_LENGTH - 3]}..."
    return collapsed


def _is_exact_control_value(value: object) -> bool:
    """Reject values whose authorization meaning depends on normalization."""

    return (
        isinstance(value, str)
        and bool(value)
        and value == value.strip()
        and not any(
            character.isspace()
            or unicodedata.category(character).startswith("C")
            for character in value
        )
    )


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
