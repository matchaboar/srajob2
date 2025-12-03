import os
from pprint import pprint

import requests
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

DOMAIN = "careers.datadoghq.com"
WHITELIST_WORDS = ["*job*", "*detail*"]
WHITELIST = [
    r"^https://careers\.datadoghq\.com/.*(job|detail).*"
    # .*{word}.*" for word in WHITELIST_WORDS
]

URL = "https://careers.datadoghq.com/all-jobs/?s=software%20engineer&location_Americas%5B0%5D=New%20York"
SPIDER_API_KEY = os.getenv("SPIDER_API_KEY")
# WHITELIST = ["job", "detail"]
# headers = {
#     'Authorization': f'Bearer {os.getenv("SPIDER_API_KEY")}',
#     'Content-Type': 'application/json',
# }

# json_data = {"return_format":"markdown","url":URL,"request":"smart_mode","return_json_data":False}

# response = requests.post('https://api.spider.cloud/scrape', 
#   headers=headers, json=json_data)

# print(response.json())

######################


# import requests, os

# headers = {
#     'Authorization': f'Bearer {os.getenv("SPIDER_API_KEY")}',
#     'Content-Type': 'application/json',
# }

# json_data = {
#     "limit":100,
#     "return_format":"commonmark",
#     "url": URL,
#     "proxy":"isp",
#     "request":"chrome",
#     "whitelist": WHITELIST
#     }

# response = requests.post('https://api.spider.cloud/links', 
#   headers=headers, json=json_data)

# Spider.crawl_url(
#     url=URL,
#     api_key=SPIDER_API_KEY,
#     limit=100,
#     whitelist=WHITELIST,
#     return_format="commonmark",
#     proxy="isp",
#     request="chrome"
# )

# pprint(response.json())
URL = "https://careers.datadoghq.com/all-jobs/?s=software%20engineer&location_Americas%5B0%5D=New%20York"
# ensure the URL is valid regex pattern
url_valid = URL.replace(".", r"\.").replace("?", r"\?").replace("=", r"\=").replace("&", r"\&").replace("%", r"\%").replace("-", r"\-").replace("_", r"\_")
# URL = "https://careers.datadoghq.com"
WHITELIST = [
  # r"^https://careers\.datadoghq\.com/.*(job|detail).*"
  # "detail/"
  url_valid,
  "detail",
  "all-jobs",
  # "careers.datadoghq.com"
  # "https://careers.datadoghq.com/detail/*",
  # "https://careers.datadoghq.com/all-jobs/*"
  # .*{word}.*" for word in WHITELIST_WORDS
]

def crawl_page() -> dict:
  headers = {
    'Authorization': f'Bearer {os.getenv("SPIDER_API_KEY")}',
    'Content-Type': 'application/json',
  }
  json_data = {
    "limit":40,
    "depth": 20,
    "return_format":"commonmark",
    "url": URL,
    "proxy":"isp",
    "request":"chrome",
    "block_ads":False,
    "service_worker_enabled":True,
    "filter_output_main_only": True,
    "whitelist": WHITELIST
  }

  response = requests.post('https://api.spider.cloud/crawl', 
    headers=headers, json=json_data)
  return response.json()
    
if __name__ == "__main__":
  res = crawl_page()
  pprint(res)
  with open("sample-crawl-response.json", "w") as f:
      import json
      json.dump(res, f, indent=4)
  with open("sample-crawl-response.json", "r") as f:
      import json
      res = json.load(f)
      print(res)
      # res = json.loads(f.read())
      print([item['url'] for item in res])
      print(f"Crawled {len(res)} pages.")



# should give 97 jobs
URL = "https://job-boards.greenhouse.io/stubhubinc?keyword=engineer"
WHITELIST = [
  # r"^https://careers\.datadoghq\.com/.*(job|detail).*"
  # "detail/"
  url_valid,
  "detail",
  "all-jobs",
  # "careers.datadoghq.com"
  # "https://careers.datadoghq.com/detail/*",
  # "https://careers.datadoghq.com/all-jobs/*"
  # .*{word}.*" for word in WHITELIST_WORDS
]

company_name = "stubhubinc"
GREENHOUSE_API_URL = f"https://boards-api.greenhouse.io/v1/boards/{company_name}/jobs"
DEBUG_URL = "https://boards-api.greenhouse.io/v1/boards/stubhubinc/jobs"
