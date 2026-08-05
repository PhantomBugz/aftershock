# Aftershock submission checklist

**Submission deadline:** August 10, 2026 at 5:00 PM EDT
([official rules](https://datahub.devpost.com/rules)).

This file separates verified technical evidence from the external actions that
still require a person. An unchecked item is not complete merely because the
repository contains supporting files.

## Verified technical evidence

- [x] On August 4, 2026, ran `python -m pytest -q`: `309 passed, 1 skipped`.
  The skip was the expected opt-in live test.
- [x] On August 5, 2026, reran the hardened release suite: `320 passed, 1
  skipped`. The skip remained the expected opt-in live test.
- [x] On August 4, 2026, enabled and ran the live MCP contract against local
  DataHub OSS Quickstart v1.6.0: `1 passed`.
- [x] On August 4, 2026, completed the live judge demo with the built-in receiver
  and real MCP:
  receiver BEFORE `PO-AFTERSHOCK-001=issued`/`apply_count=0`; ordered
  OBSERVE/DECIDE/ACT/PERSIST; one canonical PO-bound terminal `succeeded`
  receipt; receiver AFTER `PO-AFTERSHOCK-001=canceled`/`apply_count=1`;
  successful DataHub document;
  `search_documents`, `grep_documents`, and both related-asset backlinks
  verified. See the [live proof transcript](../examples/live_demo_proof.txt).
- [x] On August 5, 2026, ran two consecutive real MCP demonstrations after the
  replay fix. Both updated
  `urn:li:document:shared-015c94ed-69c0-40a6-a851-ce76a4920616`, passed the
  three read-back gates, and left the exact-title record count unchanged at
  five. See the
  [replay-hardening proof](../examples/replay_hardening_proof.txt).
- [x] On August 4, 2026, ran `python -m compileall -q src scripts tests` against
  the frozen submission tree.
- [x] On August 4, 2026, ran the fixture dashboard and regenerated the
  deterministic example pair; both SHA-256 hashes remained unchanged.
- [x] Reviewed the final Git diff and ran `detect-secrets` plus a tracked-history
  high-confidence pattern scan. Six scanner candidates were reviewed as
  intentional invalid-credential test fixtures; no release secret was found.
- [x] Visually and statically reviewed every captured example, screenshot, and
  proof artifact; no credential, private remote URL, or unsupported claim was
  found.

## Public repository

- [x] The intended repository is public:
  <https://github.com/PhantomBugz/aftershock> (verified August 4, 2026).
- [x] GitHub detects the repository license as Apache-2.0 (verified August 4,
  2026).
- [x] Integrate the reviewed feature work into the branch that will be submitted.
- [x] Push the reviewed submission code to the public default branch. Remote
  `main` includes replay-safe code commit `8082c4b` and matched the reviewed
  release worktree on August 5, 2026.
- [x] Confirm the public default branch contains all source, configuration,
  examples, screenshot, proof transcript, and setup instructions.
- [x] Open the final repository, license, README image, and every documentation
  link anonymously; all returned HTTP 200 on August 5, 2026.

## Project access and video

- [x] Choose and record the final project/test URL judges can use without paid
  access or private credentials: <https://aftershock.phantombugz.com>.
- [x] Record the functioning live MCP demonstration using the
  [video script](video-script.md).
- [x] Keep the edited video under three minutes and preserve the truthful
  before/action/after sequence. The final source is exactly 168 seconds.
- [x] Upload the video to YouTube, Vimeo, or Youku and make it publicly visible,
  as required by the official rules: <https://youtu.be/yopSGs_kx5s>.
- [x] Verify the uploaded video anonymously. YouTube reported playability `OK`,
  1080p availability, and a 168-second duration; the matching source file had
  already passed full visual and audio QA.
- [x] Confirm no credential, private URL, notification, copyrighted music, or
  third-party footage appears.

## Devpost entry

- [ ] Select **Agents That Do Real Work** as the primary category.
- [ ] Confirm that the submitted project was newly created during the July 6 to
  August 10, 2026 submission period and disclose any pre-existing code or work
  incorporated into it.
- [ ] Paste the final URLs into Devpost: project/test
  <https://aftershock.phantombugz.com>, repository
  <https://github.com/PhantomBugz/aftershock>, and video
  <https://youtu.be/yopSGs_kx5s>.
- [ ] Use the reviewed [Devpost copy](devpost-pitch.md).
- [ ] Include the AI-assistance disclosure from the README.
- [ ] Opt in to the feedback section only if desired, and review the
  [feedback survey draft](FEEDBACK_SURVEY_DRAFT.md) before submitting it.
- [ ] Verify every required field, team/representative detail, and eligibility
  statement personally before the deadline.
- [ ] Submit the entry.
- [ ] Reopen the submitted entry, repository, project URL, and video while signed
  out to confirm judge access.

The project is not fully submitted until every unchecked external gate above is
complete.
