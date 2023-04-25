import requests
from bs4 import BeautifulSoup

from config import Config
from newlauncher.utils import (get_detail_page_links,
                               gather_project_details,
                               gather_project_facilities,
                               gather_project_amenities,
                               gather_project_units,
                               gather_project_balances,
                               merge_units_and_balances,
                               merge_gathered_data, gather_floor_plans, merge_units_and_floor_plans)


async def parse_new_launcher():
    """Parse, handle & save new launcher data."""
    links = await get_detail_page_links()

    for link in links:
        r = requests.get(link, headers=Config.NEW_LAUNCHER_HEADERS)
        soup = BeautifulSoup(r.text, "html.parser")

        details = await gather_project_details(soup, link)
        facilities = await gather_project_facilities(soup)
        amenities, attachments = await gather_project_amenities(soup)
        units = await gather_project_units(soup)
        balances, overall = await gather_project_balances(soup)
        plans = await gather_floor_plans(soup)

        units_and_balances = await merge_units_and_balances(units, balances)
        units_and_floor_plans = await merge_units_and_floor_plans(units_and_balances, plans)

        complete_response = await merge_gathered_data(details,
                                                      facilities,
                                                      amenities,
                                                      attachments,
                                                      overall,
                                                      units_and_floor_plans)
