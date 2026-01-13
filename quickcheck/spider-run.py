import json
import os
import requests
from dotenv import load_dotenv

META_URL = (
    r"https://www.metacareers.com/jobsearch/?"
    r"teams[0]=Software%20Engineering&teams[1]=Research&teams[2]=Enterprise%20Engineering&teams[3]=Design%20%26%20User%20Experience&teams"
    r"[4]=Data%20Center&teams[5]=Data%20%26%20Analytics&teams[6]=Artificial%20Intelligence&teams[7]=AR%2FVR&offices[0]=Seattle%2C%20WA"
    r"&offices[1]=San%20Francisco%2C%20CA&offices[2]=Mesa%2C%20AZ&offices[3]=Chandler%2C%20AZ"
)

print(META_URL)
load_dotenv()
api_key = os.getenv("SPIDER_API_KEY") or os.getenv("SPIDER_KEY")



headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json',
}

spidercloud_config = {
    "return_format": ["raw_html", "commonmark"],
    "request": "chrome",
    "url": META_URL,
    "return_page_links":True
}

response = requests.post('https://api.spider.cloud/scrape', 
  headers=headers, json=spidercloud_config)

# write the output to ./output.json file
with open('./output.json', 'w', encoding='utf-8') as f:
    f.write(response.text)

# pretty print the json in that file

with open('./output.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    print(json.dumps(data, indent=2))

# print(response.json())
