# Notes for Claude

## Deploying

`./deploy.sh` syncs `web/index.html`, `web/data.json`, and the favicons to
S3 bucket `ppsclosures.info` and invalidates CloudFront distribution
`E37QSWDS20JF8U`. The script is gitignored since it contains infra IDs.

Typical flow after a data or UI change:

```bash
.venv/bin/python scripts/export_web.py   # regen web/data.json if pipeline ran
./deploy.sh                              # push web/ to S3 + invalidate CF
```
