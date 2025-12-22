- [x] Come up with a strategy to scrape netflix job sites.

- [x] This should be coded under a new site_handler for netflix.

- [x] This should have methods to deal with enque paginated pages.

- [x] we know the netflix site has a button "show more positions", but this may be complicated, so check if we can just query an API or add a querystring for page or pagination instead of clicking that.

- [x] The URL we should add to the schedule sites yaml, and we should run agent_scripts to generate text fixtures, and write unit tests to ensure we extract jobs for:

url: https://explore.jobs.netflix.net/careers?query=engineer&pid=790313345439&Region=ucan&domain=netflix.com&sort_by=date
