# srajob

[![Netlify Status](https://api.netlify.com/api/v1/badges/e61ec88d-2beb-487a-aafa-3fad121cf882/deploy-status)](https://app.netlify.com/projects/srajob/deploys)

https://srajob.netlify.app/

Utilities for job scraping and application automation.

## Run workers to scrape jobs in temporal 

Use the `-ForceScrapeAll` switch with `start_worker.ps1` to reset active sites and force every scheduled site to run right away (even if not scheduled at the moment). For production convex, use `-UseProd`.

Example:

```powershell
./start_worker.ps1 -ForceScrapeAll -UseProd
```

# deployment - Convex db and UI

- `./job_board_application`
- `pnpm run dev` will deploy convex functions to dev deployment.
- `netlify` is setup to deploy to prod all UI changes and convex changes in code whenever the repo has a push to `main`.
- `npx convex deploy` will deploy immediately to prod without waiting for you to `git push` and build on netlify. However this will only be DB changes - UI will potentially be out of sync with your type changes.

- include migrations: 
- dev `npx convex run convex/migrations.ts:runAll`
- prod `npx convex deploy --cmd 'npm run build' && npx convex run convex/migrations.ts:runAll --prod`
