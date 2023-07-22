"""NOTION DataBase API Endpoint"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

NOTION_API = os.environ.get('NOTION_API')
DATABASE_ID = os.environ.get('DATABASE_ID')

# API header
headers = {
    "Authorization": f"Bearer {NOTION_API}",
    "accept": "application/json",
    "Notion-Version": "2022-06-28"
}

# Get database data
def get_pages(num_pages=None):
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"

    page_size = 100 if num_pages is None else num_pages

    payload = {"page_size": page_size}
    resp = requests.post(url, json=payload, headers=headers)
    
    if resp.status_code == 200:

        data = resp.json()['results']

        for attributes in data:
            # print(attributes)
            question = attributes['properties']['Questions']['title'][0]['text']['content']
            tags = attributes['properties']['Tags']['select']['name']
            created_time = attributes['created_time']
            platform = attributes['properties']['Platform']['select']['name']
            company = attributes['properties']['Company']['multi_select']

            ndict = {
                'Question Title': question,
                'Tags': tags,
                'Created Time': created_time,
                'Platform Solved': platform,
                'Company Tag': company
            }

            print(ndict)
            print()

get_pages(100)