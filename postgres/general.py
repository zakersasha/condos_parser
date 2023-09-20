from datetime import date

import requests
from bs4 import BeautifulSoup

from config import Config
from postgres.db import connection


def gather_main_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.MAIN_TABLE_ID}',
                                headers=Config.AIR_TABLE_HEADERS, params=params)
        if response.status_code == 200:
            data = response.json()
            records = data['records']
            all_records.extend(records)

            if 'offset' in data:
                params['offset'] = data['offset']
            else:
                break
        else:
            print(f"Failed to retrieve records (status code: {response.status_code}): {response.text}")
            break
    return all_records


def gather_condo_desc(input_str):
    urls = input_str.split(', ')

    for url in urls:
        if 'newlaunches' in url:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                                  'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                                  'scale=3.00; 1170x2532; 463736449) NW/3'}
                r = requests.get(url, headers=headers)
                soup = BeautifulSoup(r.text, 'html.parser')
                desc = ''
                desc_data = soup.find('section', {"id": "project_location"})
                for i, paragraph in enumerate(desc_data.find_all('p')):
                    desc += paragraph.get_text(strip=True) + '\n'
                    if i == 4:
                        break
                return desc
            except Exception:
                return None
    return None


def save_main_data(data):
    cursor = connection.cursor()

    insert_sql = """
    INSERT INTO general (
        name, address, district, type, units_number, units_size,
        previewing_start_date, date_of_completion, tenure,
        architect, developer, link_to_condo, brochure, facilities,
        site_plans_attachments, overall_available_units,
        overall_min_unit_size, overall_max_unit_size,
        overall_min_unit_psf, overall_max_unit_psf,
        overall_min_unit_price, overall_max_unit_price,
        location_map_attachments, units, amenities,
        floor_plans_urls, site_plans_urls,
        "Condo ID", latest_update, description
    )
    VALUES (
        %(name)s, %(address)s, %(district)s, %(type)s, %(units_number)s, %(units_size)s,
        %(previewing_start_date)s, %(date_of_completion)s, %(tenure)s,
        %(architect)s, %(developer)s, %(link_to_condo)s, %(brochure)s, %(facilities)s,
        %(site_plans_attachments)s, %(overall_available_units)s,
        %(overall_min_unit_size)s, %(overall_max_unit_size)s,
        %(overall_min_unit_psf)s, %(overall_max_unit_psf)s,
        %(overall_min_unit_price)s, %(overall_max_unit_price)s,
        %(location_map_attachments)s, %(units)s, %(amenities)s,
        %(floor_plans_urls)s, %(site_plans_urls)s,
        %(Condo ID)s, %(latest_update)s, %(description)s
    );
    """

    for record in data:
        for key in insert_sql.split('%(')[1:]:
            key = key.split(')s')[0]
            if key not in record:
                record[key] = None

        cursor.execute(insert_sql, record)
        connection.commit()

    cursor.close()


def prepare_main_data(main_data):
    main_data_to_save = []
    for item in main_data:
        data = item['fields']
        try:
            data['images'] = [link["url"] for link in data["images"]]
        except KeyError:
            pass
        try:
            data['brochure'] = [link["url"] for link in data["brochure"]]
        except KeyError:
            pass
        try:
            data['site_plans_attachments'] = [link["url"] for link in data["site_plans_attachments"]]
        except KeyError:
            pass
        try:
            data['location_map_attachments'] = [link["url"] for link in data["location_map_attachments"]]
        except KeyError:
            pass
        try:
            del data['images_urls']
        except KeyError:
            pass
        data['latest_update'] = date.today().strftime('%Y-%m-%d')

        try:
            desc = gather_condo_desc(data['link_to_condo'])
            if desc:
                data['description'] = desc
        except KeyError:
            pass
        main_data_to_save.append(data)

    return main_data_to_save


def delete_old_main_data():
    try:
        cursor = connection.cursor()
        cur_date = date.today().strftime('%Y-%m-%d')
        delete_sql = """
                    DELETE FROM general
                    WHERE latest_update != %s;
                """

        cursor.execute(delete_sql, (cur_date,))
        connection.commit()

        cursor.close()

        print("Rows deleted successfully.")
    except Exception as e:
        print(f"Error: {e}")
