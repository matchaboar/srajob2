

In `./job_scrape_application` we have `./workflows`, which has subfolders.

Add a new subfolder, `site-handlers` or whatever appropriate name should be used for the pattern you will use.

One thing to fix now, ensure we are using SDKs as appropriate instead of raw API calls to things like spidercloud.

The goal is that for a specific website or api type, we will create a file with a class. These should all implement an abstract base class.

Here is a rough example (you would add all methods as appropriately used in code already), and then you'd replace those functions or one off config with a call to a specific site class if we know that is what matches the uri or is configured to be used. Add unit tests as well adjusting to the new flow of code usage and the abstract base class usage.



```
class AshbyHq:
    def get_api_uri(uri: str):
        pass
    def get_company_uri(uri: str):
        pass
    def get_links_from_markdown(markdown: str):
        pass
    def get_links_from_raw_html(html: str):
        pass
    def get_links_from_json(json: str):
        pass
    def get_spidercloud_config():
        pass
    def get_firecrawl_config():
        pass
    # other methods as makes sense...
```