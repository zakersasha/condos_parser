from miami.db_queries import store_data_airtable
from miami.utils import gather_condos_list, parse_page, parse_units_data


def parse_miami():
    condos_links = gather_condos_list()

    for link in condos_links:
        print(link)

        main_data, soup = parse_page(link)
        units_data = parse_units_data(soup, main_data['name'])

        store_data_airtable(main_data, units_data)