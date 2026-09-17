# SpotLake frontend

Next.js static export, deployed from `frontend/out/` to the website S3 bucket by
`.github/workflows/web-sync.yml` after frontend changes merge to `main`.

## Development

```bash
npm ci
npm run dev
npm run build
```

Use `http://localhost:3000`: this origin is allowed by the public-data CloudFront
CORS policy. The Home page reads the existing daily public snapshots, with common
columns ordered consistently across vendors. Columns expand to use available width
and retain a minimum readable width with horizontal scrolling on narrow screens.

## Maintaining research publications

Edit `src/data/publications.yaml` through the normal PR process. Its `ddps` list is
for **DDPS Lab research that actually uses or builds on SpotLake**, not all lab
publications. The original SpotLake paper is already shown in “Paper and code”.

Each entry contains `title`, `authors` (a comma-separated string), `venue`, `year`
(an integer), and optional `links` containing `name` and an HTTP(S) `url`. Quote
YAML titles containing a colon. The page groups entries by year and sorts titles;
no component edits are needed to add or correct a paper. Verify relevance manually
before adding it.

## External citations

An AWS Lambda refreshes the external list monthly from Semantic Scholar, using
Crossref for missing venue metadata. The About page reads its static JSON from the
existing data CloudFront distribution. It does not call scholarly APIs directly.

“Last Updated” next to the Google Scholar button is the timestamp of the displayed
snapshot's last successful citation refresh. An unsuccessful refresh preserves the
previous list and timestamp. Even when no new papers are found, a successful refresh
advances the date. The Scholar button remains a link to its more complete list.

Citation lists are stored only in AWS; no citation snapshot is committed to GitHub
or bundled into the website. While the request is pending, the section shows a
loading message. A failed request shows an error and leaves the Google Scholar
link available; it does not display a fabricated Last Updated value. Lab papers
remain visible independently. Routine updates require no GitHub commits, and
frontend deployment does not write or overwrite the AWS snapshot.

See [monthly updater operations](../utility/monthly_citation_updater/README.md) for
the exact schedule, AWS resource names, deployment, retries, and local validation.
