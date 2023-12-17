from onthemarket.db_queries import store_data_airtable
from onthemarket.utils import gather_condos_list, parse_page, parse_units_links, gather_unit_data


def parse_uk():
    cities = {'Liverpool': 'https://www.onthemarket.com/new-homes/developers/liverpool/',
              'Manchester': 'https://www.onthemarket.com/new-homes/developers/manchester/',
              'Birmingham': 'https://www.onthemarket.com/new-homes/developers/birmingham/'}

    for c, l in cities.items():
        links = gather_condos_list(l)

        for link in links:
            print(link)
            soup, general_data = parse_page(link, c)
            units_links, overall_available_units = parse_units_links(soup)
            general_data['overall_available_units'] = overall_available_units
            units_data, tenure, overall_min_unit_price, overall_max_unit_price, facilities = gather_unit_data(
                units_links, general_data['name'])
            general_data['tenure'] = tenure
            general_data['overall_min_unit_price'] = overall_min_unit_price
            general_data['overall_max_unit_price'] = overall_max_unit_price
            general_data['facilities'] = facilities

            store_data_airtable(general_data, units_data)

