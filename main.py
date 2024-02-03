"""Extract and Load layer call"""
from src.notion.notion_api import NotionWrapper

if __name__ == '__main__':
    nw = NotionWrapper()
    data = nw.extract()
    nw.load_data(
        raw_data=data
    )
