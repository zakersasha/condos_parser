from datetime import date

import psycopg2
import requests

from config import Config
from postgres.db import db_params


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


def gather_miami_units_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/app9O58fJIVtHvrHn/tblMYDp7Z3PeOPU8i',
                                headers={
                                    "Authorization": "Bearer " + 'pat01eANVrLAmHO9g.d01b80d2e2b1f45656284ce5ec987e5b06393623f54daf907415b0352cf5a0d7',
                                    "Content-Type": "application/json",
                                    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                                                  'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                                                  'scale=3.00; 1170x2532; 463736449) NW/3'}, params=params)
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


def gather_dubai_units_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/appoHsQ6y9Ff4cWaW/tbl1R07YOuz0bwpBR',
                                headers={
                                    "Authorization": "Bearer " + 'patchZXglSCP5RnWW.26392eeef90ff792693a091fa1e8e882881f0cf3cc9c4a719ba7c6bc91b1db25',
                                    "Content-Type": "application/json",
                                    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                                                  'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                                                  'scale=3.00; 1170x2532; 463736449) NW/3'}, params=params)
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


def gather_uk_units_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/app8DMFTDLafaMcKg/tblLNkWLT640h6Fbb',
                                headers={
                                    "Authorization": "Bearer " + 'patMehsoohn9gsPhO.084730f4e5118c35fcdb70dd3345d4e13e3b15beaed541456cf20ec3140e7795',
                                    "Content-Type": "application/json",
                                    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                                                  'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                                                  'scale=3.00; 1170x2532; 463736449) NW/3'}, params=params)
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


def gather_bali_units_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/app55xAPfpJD3zubt/tblIf5RHvKDoaTXCo',
                                headers={
                                    "Authorization": "Bearer " + 'patZ36V2m2fbzEGCr.3f90bb8375f018885977f3dd3e761da6915e1e5ab9be493ab1bfd9c6437e670c',
                                    "Content-Type": "application/json",
                                    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                                                  'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                                                  'scale=3.00; 1170x2532; 463736449) NW/3'}, params=params)
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


def prepare_units_data(units_data, general_data):
    units_data_to_save = []
    for item in units_data:
        data = item['fields']
        data['unit_id'] = item['id']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        try:
            if data['floor_plan_image_links']:
                lst = data['floor_plan_image_links'].split(',')
                lst = [item.strip() for item in lst]
                data['floor_plan_image_links'] = lst
            else:
                data['floor_plan_image_links'] = None
        except KeyError:
            data['floor_plan_image_links'] = None

        try:
            data['num_bedrooms'] = int(data['Bedrooms'][0])
        except (KeyError, ValueError, IndexError):
            data['num_bedrooms'] = None
        try:
            del data['General']
        except KeyError:
            pass
        try:
            del data['Book Preview Button']
        except KeyError:
            pass
        try:
            data['Unit_ID'] = data['Unit ID']
        except KeyError:
            data['Unit_ID'] = None

        try:
            data['Condo Images'] = [link["url"] for link in data["Condo Images"]]
        except KeyError:
            pass

        data['general_id'] = find_dict_with_string(general_data, item['id'])

        try:
            data['Address'] = data['Address'][0]
        except (KeyError, IndexError):
            pass

        units_data_to_save.append(data)

    return units_data_to_save


def prepare_miami_units_data(units_data, general_data):
    units_data_to_save = []

    for item in units_data:
        data = item['fields']
        data['unit_id'] = item['id']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        data['Unit_ID'] = data["Unit ID"]
        data['general_id'] = find_dict_with_string(general_data, item['id'])
        try:
            if data['floor_plan_image_links']:
                lst = data['floor_plan_image_links'].split(',')
                lst = [item.strip() for item in lst]
                data['floor_plan_image_links'] = lst
            else:
                data['floor_plan_image_links'] = None
        except KeyError:
            data['floor_plan_image_links'] = None
        units_data_to_save.append(data)

    return units_data_to_save


def prepare_dubai_units_data(units_data, general_data):
    units_data_to_save = []
    for item in units_data:
        data = item['fields']
        data['unit_id'] = item['id']
        data['Unit_ID'] = data["Unit ID"]
        data['general_id'] = find_dict_with_string(general_data, item['id'])
        try:
            if data['floor_plan_image_links']:
                lst = data['floor_plan_image_links'].split(',')
                lst = [item.strip() for item in lst]
                data['floor_plan_image_links'] = lst
            else:
                data['floor_plan_image_links'] = None
        except KeyError:
            data['floor_plan_image_links'] = None
        data['latest_update'] = date.today().strftime('%Y-%m-%d')

        units_data_to_save.append(data)

    return units_data_to_save


def prepare_uk_units_data(units_data, general_data):
    units_data_to_save = []
    for item in units_data:
        data = item['fields']
        data['unit_id'] = item['id']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        data['Unit_ID'] = data["Unit ID"]
        data['general_id'] = find_dict_with_string(general_data, item['id'])
        try:
            if data['floor_plan_image_links']:
                lst = data['floor_plan_image_links'].split(',')
                lst = [item.strip() for item in lst]
                data['floor_plan_image_links'] = lst
            else:
                data['floor_plan_image_links'] = None
        except KeyError:
            data['floor_plan_image_links'] = None
        units_data_to_save.append(data)

    return units_data_to_save


def prepare_bali_units_data(units_data, general_data):
    units_data_to_save = []
    for item in units_data:
        data = item['fields']
        data['unit_id'] = item['id']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        data['Unit_ID'] = data["Unit ID"]
        data['general_id'] = find_dict_with_string(general_data, item['id'])
        try:
            data['num_bedrooms'] = int(data['Bedrooms'][0])
        except (KeyError, ValueError, IndexError):
            data['num_bedrooms'] = None
        try:
            if data['floor_plan_image_links']:
                lst = data['floor_plan_image_links'].split(',')
                lst = [item.strip() for item in lst]
                data['floor_plan_image_links'] = lst
            else:
                data['floor_plan_image_links'] = None
        except KeyError:
            data['floor_plan_image_links'] = None
        units_data_to_save.append(data)

    return units_data_to_save


def save_units_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO units (
            unit_id, unit_type, all_units, available_units, price_min, size_min, size_max,
            psf_min, condo_images, latest_update, "Unit ID", general_id, num_bedrooms, floor_plan_image_links
        )
        VALUES (
            %(unit_id)s, %(unit_type)s, %(all_units)s, %(available_units)s, %(price_min)s, %(size_min)s,
            %(size_max)s, %(psf_min)s, %(Condo Images)s, %(latest_update)s, %(Unit_ID)s, %(general_id)s, %(num_bedrooms)s, %(floor_plan_image_links)s
        );
        """

    formatted_data = []
    for record in data:
        formatted_record = {}
        for key in insert_sql.split('%(')[1:]:
            key = key.split(')s')[0]
            if key not in record:
                record[key] = None
            formatted_record[key] = record[key]
        formatted_data.append(formatted_record)

    try:
        cursor = connection.cursor()
        cursor.executemany(insert_sql, formatted_data)
        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def save_dubai_units_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO units (
            unit_id, unit_type, available_units, price_min, size_min, size_max,
            psf_min, latest_update, price_max, psf_max, num_bedrooms,
            floor_plan_image_links, "Unit ID", general_id
        )
        VALUES (
            %(unit_id)s, %(unit_type)s, %(available_units)s, %(price_min)s, %(size_min)s,
            %(size_max)s, %(psf_min)s, %(latest_update)s, %(price_max)s, %(psf_max)s,
            %(num_bedrooms)s, %(floor_plan_image_links)s, %(Unit_ID)s, %(general_id)s
        );
        """

    formatted_data = []
    for record in data:
        formatted_record = {}
        for key in insert_sql.split('%(')[1:]:
            key = key.split(')s')[0]
            if key not in record:
                record[key] = None
            formatted_record[key] = record[key]
        formatted_data.append(formatted_record)

    try:
        cursor = connection.cursor()
        cursor.executemany(insert_sql, formatted_data)
        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def save_miami_units_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO units (
             unit_id, unit_type, size_min, size_max, latest_update, num_bedrooms, floor_plan_image_links, "Unit ID",
             general_id
        )
        VALUES (
            %(unit_id)s, %(unit_type)s, %(size_min)s, %(size_max)s, %(latest_update)s, %(num_bedrooms)s,
            %(floor_plan_image_links)s, %(Unit_ID)s, %(general_id)s
        );
        """

    formatted_data = []
    for record in data:
        formatted_record = {}
        for key in insert_sql.split('%(')[1:]:
            key = key.split(')s')[0]
            if key not in record:
                record[key] = None
            formatted_record[key] = record[key]
        formatted_data.append(formatted_record)

    try:
        cursor = connection.cursor()
        cursor.executemany(insert_sql, formatted_data)
        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def save_uk_units_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO units (
            unit_id, unit_type, latest_update, num_bedrooms, floor_plan_image_links, price_min, price_max,
            available_units, "Unit ID", general_id
        )
        VALUES (
            %(unit_id)s, %(unit_type)s, %(latest_update)s, %(num_bedrooms)s,
            %(floor_plan_image_links)s, %(price_min)s, %(price_max)s, %(available_units)s, %(Unit_ID)s, %(general_id)s
        );
        """

    formatted_data = []
    for record in data:
        formatted_record = {}
        for key in insert_sql.split('%(')[1:]:
            key = key.split(')s')[0]
            if key not in record:
                record[key] = None
            formatted_record[key] = record[key]
        formatted_data.append(formatted_record)

    try:
        cursor = connection.cursor()
        cursor.executemany(insert_sql, formatted_data)
        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def save_bali_units_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
        INSERT INTO units (
            unit_id, unit_type, latest_update, num_bedrooms, floor_plan_image_links, price_min, size_min,
            available_units, "Unit ID", general_id
        )
        VALUES (
            %(unit_id)s, %(unit_type)s, %(latest_update)s, %(num_bedrooms)s,
            %(floor_plan_image_links)s, %(price_min)s, %(size_min)s, %(available_units)s, %(Unit_ID)s, %(general_id)s
        );
        """

    formatted_data = []
    for record in data:
        formatted_record = {}
        for key in insert_sql.split('%(')[1:]:
            key = key.split(')s')[0]
            if key not in record:
                record[key] = None
            formatted_record[key] = record[key]
        formatted_data.append(formatted_record)

    try:
        cursor = connection.cursor()
        cursor.executemany(insert_sql, formatted_data)
        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def delete_old_units_data():
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cur_date = date.today().strftime('%Y-%m-%d')
        delete_sql = """
                    DELETE FROM units
                    WHERE latest_update != %s;
                """

        cursor.execute(delete_sql, (cur_date,))
        connection.commit()

        cursor.close()
        connection.close()

        print("Units table cleared.")
    except Exception as e:
        print(f"Error: {e}")


def delete_units_with_no_general():
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        delete_sql = """
                    DELETE FROM units
                    WHERE general_id IS NULL;
                """

        cursor.execute(delete_sql)
        connection.commit()

        cursor.close()
        connection.close()

        print("Units without condo deleted.")
    except Exception as e:
        print(f"Error: {e}")


def find_dict_with_string(lst, search_string):
    for d in lst:
        if d.get('units', None):
            if search_string in d.get('units', []):
                return d['id']
