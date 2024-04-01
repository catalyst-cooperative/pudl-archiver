---
name: Monthly archive update
about: Template for publishing monthly archives.
title: Publish archives for the month of MONTH
labels: automation, zenodo
assignees: ''

---

# Review and publish archives

For each of the following archives, find the run status in the Github archiver run. If approved, manually review the archive and publish. Then check the box here to confirm publication status:

```[tasklist]
- [ ] eia176
- [ ] eia191
- [ ] eia757a
- [ ] eia860
- [ ] eia860m
- [ ] eia861
- [ ] eia923
- [ ] eia930
- [ ] eiaaeo
- [ ] eiawater
- [ ] eia_bulk_elec
- [ ] epacamd_eia
- [ ] ferc1
- [ ] ferc2
- [ ] ferc6
- [ ] ferc60
- [ ] ferc714
- [ ] mshamines
- [ ] nrelatb
- [ ] phmsagas
- [ ] epacems
```

# Validation failures
For each run that failed because of validation test failures (seen in the GHA logs), add it to the tasklist. Download the run summary JSON by going into the "Upload run summaries" tab of the GHA run for each dataset, and follow the link. Investigate the validation failure, and make an issue to resolve it.

```[tasklist]
- [ ]
```

# Other failures
For each run that failed because of another reason (e.g., underlying data changes, code failures), create an issue describing the failure and take necessary steps to resolve it.

```[tasklist]
- [ ]
```

# Relevant logs
[Link to logs from GHA run]( PLEASE FIND THE ACTUAL LINK AND FILL IN HERE )
