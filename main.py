"""Main file"""

from notion_elt.notion.notion_api import NSEtl

notion_ob = NSEtl()
notion_ob.elt_process()
