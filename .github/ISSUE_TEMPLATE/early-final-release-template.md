---
name: Early and final release archive updates
about: Template for publishing early/final release EIA archives.
title: Publish {{ date | date('MMMM Do YYYY') }} archives
labels: archive-update, zenodo
assignees: e-belfer

---

# Summary of results:
See the job run logs and results [here]({{ env.RUN_URL }}).

# Review and publish archives

For each of the following archives, find the run status in the Github archiver run. If validation tests pass, manually review the archive and publish. If no changes detected, delete the draft. If changes are detected, manually review the archive following the guidelines in step 3 of `README.md`, then publish the new version. Then check the box here to confirm publication status, adding a note on the status (e.g., "v1 published", "no changes detected, draft deleted"):

- [ ] eia860
- [ ] eia861
- [ ] eia923

# Validation failures
For each run that failed because of validation test failures (seen in the GHA logs), add it to the tasklist. Download the run summary JSON by going into the "Upload run summaries" tab of the GHA run for each dataset, and follow the link. Investigate the validation failure.

If the validation failure is deemed ok after manual review (e.g., Q2 of 2024 data doubles the size of a file that only had Q1 data previously, but the new data looks as expected), go ahead and approve the archive and leave a note explaining your decision in the task list.

If the validation failure is blocking (e.g., file format incorrect, whole dataset changes size by 200%), make an issue to resolve it.

- [ ] dataset

# Other failures
For each run that failed because of another reason (e.g., underlying data changes, code failures), create an issue describing the failure and take necessary steps to resolve it.

- [ ] dataset
