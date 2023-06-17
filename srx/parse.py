import requests
from bs4 import BeautifulSoup

from config import Config
from srx.db_queries import store_data_airtable
from srx.tg_reports import send_tg_report, send_updates_file
from srx.utils import (get_last_page_number,
                       gather_projects_links,
                       gather_main_table_data,
                       gather_units_table_data,
                       gather_amenities_table_data,
                       gather_list_for_sale_data,
                       combine_units_data, gather_detail_for_sale_data)


async def parse_srx():
    """Parse, handle & save srx site data."""

    # max_page = get_last_page_number()
    projects_links = await gather_projects_links(16)

    for link in list(set(projects_links)):
        try:
            print(link)
            r = requests.get(link, headers=Config.SRX_HEADERS)
            soup = BeautifulSoup(r.text, 'html.parser')
            main_data = await gather_main_table_data(link, soup)
            units_data = await gather_units_table_data(soup, main_data)
            list_for_sale_data = await gather_list_for_sale_data(soup, main_data)
            detail_for_sale_data = await gather_detail_for_sale_data(main_data)
            units = await combine_units_data(units_data, list_for_sale_data, detail_for_sale_data)

            amenities_data = await gather_amenities_table_data(soup)
            label, new_units, total_units = store_data_airtable(main_data, units, amenities_data)
            await send_tg_report(main_data, label, new_units, total_units)
        except Exception:
            continue
    await send_updates_file()
