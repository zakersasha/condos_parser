from datetime import date

import requests

from config import Config
from postgres.db import connection


def gather_amenities_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.AMENITIES_TABLE_ID}',
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


def prepare_amenities_data(amenities_data):
    amenities_data_to_save = []
    for item in amenities_data:
        data = item['fields']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')

        data['amenity_id'] = item['id']

        try:
            data['amenities_type'] = data['amenities_type'][0]
        except (KeyError, IndexError):
            pass
        try:
            del data['General']
        except KeyError:
            pass

        amenities_data_to_save.append(data)

    return amenities_data_to_save


def save_amenities_data(data):
    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO amenities (
            amenity_id, amenities_name, amenities_type, distance
        )
        VALUES (
            %(amenity_id)s, %(amenities_name)s, %(amenities_type)s, %(distance)s
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
    connection.close()


def delete_old_amenities_data():
    try:
        cursor = connection.cursor()
        cur_date = date.today().strftime('%Y-%m-%d')
        delete_sql = """
                    DELETE FROM amenities
                    WHERE latest_update != %s;
                """

        cursor.execute(delete_sql, (cur_date,))
        connection.commit()

        cursor.close()

        print("Rows deleted successfully.")
    except Exception as e:
        print(f"Error: {e}")
