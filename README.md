# srajob

Utilities for job scraping and application automation.

# deployment - Convex db and UI

- `./job_board_application`
- `pnpm run dev` will deploy convex functions to dev deployment.
- `npx convex deploy` will deploy to prod.
- `netlify` is setup to deploy to prod all UI changes and convex changes in code whenever the repo has a push to `main`.

- include migrations: 
- dev `npx convex run convex/migrations.ts:runAll`
- prod `npx convex deploy --cmd 'npm run build' && npx convex run convex/migrations.ts:runAll --prod`