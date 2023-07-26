"""Main file"""

from src.notion.notion_api import NSEtl

notion_ob = NSEtl()
notion_ob.elt_process()
