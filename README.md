# data.gouv.fr weekly dataset watch

`pipeline.py` downloads data.gouv.fr's own bulk catalog export (refreshed weekly
by data.gouv.fr itself, ~260MB CSV, one row per dataset), classifies every
dataset into one of 14 domain buckets, and builds a digest of what's new or
updated in a trailing window. No API pagination, no scraping — one file
download per run.

Standard library only, no dependencies to install.

## Usage

```
python pipeline.py --download --digest-json digest_latest.json
```

The digest JSON is pushed to the "Veille Data.gouv.fr" Artifact's `digests`
database collection (one document per run, keyed by date) by the scheduled
cloud routine that runs this weekly — see the routine's prompt for the exact
push step.

Dashboard: https://claude.ai/code/artifact/3dfcd0ce-1668-4941-ae23-1c69ba5afdfa
