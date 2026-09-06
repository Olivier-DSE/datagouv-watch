# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A weekly watcher for data.gouv.fr's dataset catalog. `pipeline.py` downloads
data.gouv.fr's own bulk catalog export (~260MB CSV, one row per dataset,
refreshed weekly by data.gouv.fr itself), classifies every dataset into a
domain/subcategory taxonomy, and builds a digest of what's new, updated, or
archived in a trailing window (default 8 days). Standard library only — no
dependencies to install.

The digest feeds a dashboard published as a Claude Artifact ("Veille
Data.gouv.fr", `digests` db collection, one document per run keyed by date):
https://claude.ai/code/artifact/3dfcd0ce-1668-4941-ae23-1c69ba5afdfa

## Commands

```
python pipeline.py --download --digest-json digest_latest.json   # full run: fetch + classify + digest
python pipeline.py --digest-json digest_latest.json              # reuse existing catalog.csv
```

There is no test suite and no linter configured.

## Architecture

Everything lives in `pipeline.py`, run top to bottom as a single-pass script
(no classes, no persisted state):

1. **Download** (`download_catalog`) — fetches the CSV from data.gouv.fr's
   bulk export API to `catalog.csv`.
2. **Parse + classify** (`parse_catalog`) — reads the CSV, normalizes each
   row's tags/title/description (accent-stripped, lowercased), and runs it
   through two taxonomy passes:
   - `TAXONOMY` — 14 top-level domains (Sante, Transport & Mobilite, etc.),
     matched by whole-word/phrase regex against tags first, then free text.
   - `SUBTAXONOMY` — a subcategory taxonomy nested under each domain, same
     matching strategy, scoped so keywords don't need repeating per domain.
   Keywords are matched as `\b...\b` (whole word/phrase) — never raw
   substrings. This is deliberate: short fragments like "eau" or "plu" used
   to false-match inside words like "nouveaux" or "plus" and skewed
   classification badly. Any new taxonomy keyword must stay whole-word safe.
3. **Digest** (`build_digest`) — derives new/updated/archived purely from
   data.gouv.fr's own `created_at`/`last_modified` timestamps filtered to
   the trailing window; needs no state carried over from the previous run.
   Also builds a third classification level: real tags publishers actually
   attached, counted per (domain, subcategory) rather than hand-picked —
   this stays accurate as tag usage shifts and needs no manual upkeep.
   `TAG_STOPWORDS` filters out platform boilerplate tags (e.g.
   "donnees-ouvertes") that carry no distinguishing signal.
4. **Output** — writes a local Markdown report (`reports/report_<date>.md`,
   readability only, not consumed downstream) and, when `--digest-json` is
   given, two JSON files:
   - `digest_latest.json` — the digest minus `subtag_links` (pushed to the
     Artifact's `digests` collection).
   - `tag_links.json` — `subtag_links` split out by domain slug
     (`DOMAIN_SLUGS`) into separate small per-domain documents. This split
     exists because the combined data is 1MB+, well past the Artifact db's
     256 KiB per-document cap; the dashboard fetches a domain's doc lazily
     only when a tag chip in it is clicked.

## Execution model — read before changing the run scripts

This pipeline runs in two disconnected places, and the split is load-bearing,
not incidental:

- **Locally, via `run-weekly.ps1`** (triggered by the Windows Scheduled Task
  `DataGouvWatchWeekly` on this machine, configured to run whether the user
  is logged on or not — fires Fridays at 17:00 local time) — does the actual
  download, classification, and digest build, then commits and pushes
  `digest_latest.json` + `tag_links.json` to GitHub
  (https://github.com/Olivier-DSE/datagouv-watch). This step *must* run
  locally because data.gouv.fr blocks Anthropic's cloud sandbox IPs
  mid-TLS-handshake, so the download cannot happen from a cloud routine.
  `git push` authenticates via Windows Git Credential Manager (a cached
  HTTPS credential); if that ever expires, the push could start failing on
  a machine with nobody watching — the cloud routine below is the backstop
  that catches this.
- **In the cloud, via the scheduled routine `datagouv-watch-weekly`** (fires
  Fridays 15:30 UTC, ~30 min after the local run) — pulls the repo, checks
  that `digest_latest.json`'s `date` field is actually today's date, and if
  so pushes it plus `tag_links.json` into the dashboard Artifact's database
  (`digests` and `tag_links` collections, one doc per date/domain). If the
  digest is stale (local run didn't land) or any step errors out, it sends a
  push + email notification describing the failure instead of silently
  pushing nothing — see the routine's prompt (in the routines list, not this
  repo) for the exact logic.

This directory is a real git repo (`main`, tracking `origin/main` on the
GitHub remote above) — `run-weekly.ps1`'s `git pull`/`git commit`/`git push`
steps work as-is; no further git setup is needed. It lives at
`C:\Users\PERFORM2235\Claude_Projects\datagouv-watch` on this machine, which
`run-weekly.ps1`'s `$repoDir` points at.

### Known quirks

- **The Windows Scheduled Task's action is not derived from anything in this
  repo** — it's a separate object in Task Scheduler holding its own program
  path and arguments, independent of `run-weekly.ps1`'s `$repoDir`. If this
  repo ever moves again, updating the script and this file is not enough:
  the task's action must be updated too, or it'll fail at launch on its next
  fire. Because the task runs "whether logged on or not" (stored-password
  logon), *any* edit to it — via `Set-ScheduledTask`, `schtasks /Change`, or
  even just clicking OK in the Task Scheduler GUI — requires re-entering the
  Windows account password to re-save the credential; there's no way around
  this programmatically. The GUI is also fragile here: editing the task's
  Name field while fixing the Action can silently drop the weekly trigger,
  leaving a task with the right action but no schedule at all (happened
  once). The reliable fix is to delete and recreate the task in one shot:
  ```
  schtasks /Create /TN "DataGouvWatchWeekly" /TR "\"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe\" -NoProfile -ExecutionPolicy Bypass -File \"C:\Users\PERFORM2235\Claude_Projects\datagouv-watch\run-weekly.ps1\"" /SC WEEKLY /D FRI /ST 17:00 /RU PERFORM2235 /RP *
  ```
  `/RP *` makes it prompt interactively for the password instead of needing
  it inline — run this from an interactive terminal, not unattended.
- **Manually re-running the cloud routine is not a reliable health check.**
  Triggering `datagouv-watch-weekly` on demand (via the routines UI or the
  API's `run` action) can hit a permission prompt on its Artifact `write_db`
  call that the actual Friday cron fire does not hit, leaving the run stuck
  in `requires_action` indefinitely with no way to approve it
  programmatically — it just sits there until it gets archived, unresolved.
  This has happened on more than one manual trigger. The scheduled fire
  itself has run clean every week. So when checking whether this routine is
  healthy, trust its last *scheduled* run's status, not a manual test run.

## Gitignored / regenerated files

`catalog.csv`, `reports/`, `run-logs/`, and `tag_links_split/` are
gitignored — they're regenerated by each run and shouldn't be committed.
`digest_latest.json` and `tag_links.json`, by contrast, are the two files
`run-weekly.ps1` explicitly commits and pushes each week.
