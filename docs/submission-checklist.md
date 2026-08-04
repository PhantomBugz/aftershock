# Aftershock submission checklist

This file records human actions still required before submission. An unchecked
item is not complete merely because the repository contains supporting files.

## Final technical verification

- [ ] Merge or otherwise integrate the reviewed feature branch into the branch
  that will be submitted.
- [ ] Run `python -m pytest -q` and save the fresh result.
- [ ] Run `python -m compileall -q src scripts`.
- [ ] Run the fixture dashboard and confirm it visibly says
  `OFFLINE FIXTURE MODE` and shows external receipt IDs.
- [ ] Confirm examples match a fresh fixture run and contain no credentials,
  private URLs, or unsupported outcome claims.
- [ ] Review the final Git diff and run a secret scan before publishing.
- [ ] If presenting a live result, run the opt-in test against the named
  instance and verify the document in DataHub. A skipped test does not satisfy
  this item.

## Public repository

- [ ] Push the final submission branch to the intended Git hosting account.
- [ ] Make the repository public.
- [ ] Confirm the default branch contains all source, config, examples, and
  setup instructions.
- [ ] Confirm the Apache-2.0 `LICENSE` is detected and visible on the repository
  page while signed out.
- [ ] Open the repository and all documentation links in a signed-out browser.

## Project access and video

- [ ] Provide a project URL judges can use without paid access or private
  credentials. Clear repository instructions may be used where permitted.
- [ ] Record the functioning fixture demo using `docs/video-script.md`.
- [ ] Keep the final video under three minutes.
- [ ] Upload the video to an allowed host and set it public or unlisted so it is
  accessible to judges while signed out.
- [ ] Watch the uploaded video while signed out and confirm no credential,
  private URL, copyrighted music, or third-party footage appears.

## Devpost entry

- [ ] Select **Agents That Do Real Work** as the primary category.
- [ ] Paste the final public repository URL, project/test URL, and accessible
  video URL.
- [ ] Use the reviewed copy in `docs/devpost-pitch.md`.
- [ ] Include the AI-assistance disclosure from the README.
- [ ] Opt in to the feedback section only if desired, and review
  `docs/FEEDBACK_SURVEY_DRAFT.md` before submitting it.
- [ ] Verify every required field, team/representative detail, and eligibility
  statement personally before the deadline.
- [ ] Submit, then reopen the entry while signed out to confirm judge access.

No item in this checklist is asserted complete by this document.
