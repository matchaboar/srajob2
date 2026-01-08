# Queue Refactor: Listing vs Detail Worker Pools

Goal: route listing-page URLs to a dedicated worker pool and lease only 4 listing URLs at a time, while detail URLs stay on the existing job-details flow.

## Implementation Checklist

- [x] Convex queue leasing supports `urlType` filtering (listing vs detail)
- [x] New listing workflow that leases `urlType=listing` with batch size 4
- [x] Job-details workflow leases `urlType=detail`
- [x] New listing task queue + worker role wiring in worker config
- [x] Runtime config adds listing batch size + listing worker count
- [x] Schedules route listing workflow to listing queue
- [x] start_worker script launches listing worker pool
- [x] Tests updated for Convex queue leasing by urlType
- [x] Tests updated for listing workflow + worker queue selection
- [x] Tests updated for enqueue payload urlTypes for listing URLs
- [x] Ruff lint run after Python edits

## Notes
- Use `urlType` on `scrape_url_queue` to partition listing vs detail.
- Listing workers should only lease 4 URLs at a time.
