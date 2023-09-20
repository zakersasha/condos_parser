from datetime import date

import requests

from config import Config
from postgres.db import connection


def gather_units_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.UNITS_TABLE_ID}',
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


def prepare_units_data(units_data):
    units_data_to_save = []
    for item in units_data:
        data = item['fields']
        data['unit_id'] = item['id']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')

        try:
            del data['General']
        except KeyError:
            pass
        try:
            del data['Book Preview Button']
        except KeyError:
            pass
        try:
            del data['Unit ID']
        except KeyError:
            pass

        try:
            data['Condo Images'] = [link["url"] for link in data["Condo Images"]]
        except KeyError:
            pass

        try:
            data['Address'] = data['Address'][0]
        except (KeyError, IndexError):
            pass

        units_data_to_save.append(data)

    return units_data_to_save


def save_units_data(data):
    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO units (
            unit_id, unit_type, all_units, available_units, price_min, size_min, size_max,
            psf_min, district, date_of_completion, condo_images, address, latest_update
        )
        VALUES (
            %(unit_id)s, %(unit_type)s, %(all_units)s, %(available_units)s, %(price_min)s, %(size_min)s,
            %(size_max)s, %(psf_min)s, %(district)s,
            %(date_of_completion)s, %(Condo Images)s, %(Address)s, %(latest_update)s
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


def delete_old_units_data():
    try:
        cursor = connection.cursor()
        cur_date = date.today().strftime('%Y-%m-%d')
        delete_sql = """
                    DELETE FROM units
                    WHERE latest_update != %s;
                """

        cursor.execute(delete_sql, (cur_date,))
        connection.commit()

        cursor.close()

        print("Rows deleted successfully.")
    except Exception as e:
        print(f"Error: {e}")
