import json
from datetime import date, datetime

import psycopg2
import requests
from bs4 import BeautifulSoup

from config import Config
from postgres.db import db_params


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


def gather_dubai_main_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/appoHsQ6y9Ff4cWaW/tbl76GHXJbJGdOanH',
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


def gather_bali_main_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/app55xAPfpJD3zubt/tblOuLrGqrN4cbIoe',
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


def gather_miami_main_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/app9O58fJIVtHvrHn/tblSdjZ6UKZUQ7FU8',
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


def gather_uk_main_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/app8DMFTDLafaMcKg/tblR20wKONeGjoqX1',
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


def gather_oman_main_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/appnWWKxXiSjmDLCR/tbl6laBo8kWKvfZPC',
                                headers={
                                    "Authorization": "Bearer " + 'patozqZihGw0H9iIY.c571db7a2ffbf522ccd1f7970679514f2f003422cdcfa54c9a947803bc93ef49',
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
    connection = psycopg2.connect(**db_params)
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
        "Condo ID", latest_update, description, city, companies, selected
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
        %(Condo ID)s, %(latest_update)s, %(description)s, %(city)s, %(companies)s, %(selected)s
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
        for record in formatted_data:
            try:
                cursor.execute(insert_sql, record)
            except Exception:
                continue

        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def save_dubai_main_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
    INSERT INTO general (
        name, address, district, units_number,
        date_of_completion, link_to_condo, brochure, facilities,
        overall_available_units,
        overall_min_unit_size, overall_max_unit_size,
        overall_min_unit_psf, overall_max_unit_psf,
        overall_min_unit_price, overall_max_unit_price,
        units, site_plans_urls,
        "Condo ID", latest_update, description, city, longitude, latitude, payment_plans, companies, selected
    )
    VALUES (
        %(name)s, %(address)s, %(district)s, %(units_number)s, %(date_of_completion)s,
        %(link_to_condo)s, %(brochure)s, %(facilities)s,
        %(overall_available_units)s,
        %(overall_min_unit_size)s, %(overall_max_unit_size)s,
        %(overall_min_unit_psf)s, %(overall_max_unit_psf)s,
        %(overall_min_unit_price)s, %(overall_max_unit_price)s,
        %(units)s, %(site_plans_urls)s,
        %(Condo ID)s, %(latest_update)s, %(description)s, %(city)s, %(longitude)s, %(latitude)s, %(payment_plans)s, %(companies)s, %(selected)s
    ) RETURNING id;
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
        for record in formatted_data:
            try:
                cursor.execute(insert_sql, record)
            except Exception:
                continue
        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def save_bali_main_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
    INSERT INTO general (
        name, address, district, units_number, link_to_condo, brochure, facilities,
        overall_available_units, units, "Condo ID", latest_update, city, companies
    )
    VALUES (
        %(name)s, %(address)s, %(district)s, %(units_number)s, %(link_to_condo)s,
        %(brochure)s, %(facilities)s, %(overall_available_units)s,
        %(units)s,
        %(Condo ID)s, %(latest_update)s, %(city)s, %(companies)s
    ) RETURNING id;
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
        for record in formatted_data:
            try:
                cursor.execute(insert_sql, record)
            except Exception:
                continue
        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def save_miami_main_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
    INSERT INTO general (
        name, address, district, date_of_completion,
        link_to_condo, facilities, payment_plans,
        overall_min_unit_size, overall_max_unit_size,
        overall_min_unit_psf, overall_min_unit_price,
        units, "Condo ID", latest_update, description, city, features, companies, selected, payment_plans_attached
    )
    VALUES (
        %(name)s, %(address)s, %(district)s, %(date_of_completion)s,
        %(link_to_condo)s, %(facilities)s, %(payment_plans)s,
        %(overall_min_unit_size)s, %(overall_max_unit_size)s,
        %(overall_min_unit_psf)s,%(overall_min_unit_price)s,
        %(units)s, %(condo_id)s, %(latest_update)s, %(description)s, %(city)s, %(features)s, %(companies)s, %(selected)s, %(payment_plans_attached)s
    ) RETURNING id;
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
        for record in formatted_data:
            try:
                cursor.execute(insert_sql, record)
            except Exception:
                continue
        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def save_uk_main_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
    INSERT INTO general (
        name, address,
        link_to_condo, facilities, overall_min_unit_price, overall_max_unit_price,
        units, "Condo ID", latest_update, description, city, brochure, tenure, overall_available_units, companies, district
    )
    VALUES (
        %(name)s, %(address)s,
        %(link_to_condo)s, %(facilities)s,
        %(overall_min_unit_price)s, %(overall_max_unit_price)s,
        %(units)s, %(Condo ID)s, %(latest_update)s, %(description)s, %(city)s, %(brochure)s, %(tenure)s, 
        %(overall_available_units)s, %(companies)s, %(district)s
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
        for record in formatted_data:
            try:
                cursor.execute(insert_sql, record)
            except Exception:
                continue
        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def save_oman_main_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
    INSERT INTO general (
        name, link_to_condo, district, overall_available_units,
        overall_min_unit_price, overall_min_unit_psf, "Condo ID", latest_update, city, companies
    )
    VALUES (
        %(name)s, %(link_to_condo)s,
        %(district)s, %(overall_available_units)s,
        %(overall_min_unit_price)s, %(overall_min_unit_psf)s,
        %(Condo ID)s, %(latest_update)s, %(city)s, %(companies)s
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
        for record in formatted_data:
            try:
                cursor.execute(insert_sql, record)
            except Exception:
                continue
        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


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
            if data['floor_plans_urls']:
                lst = data['floor_plans_urls'].split(',')
                lst = [item.strip() for item in lst]
                data['floor_plans_urls'] = lst
            else:
                data['floor_plans_urls'] = None
        except KeyError:
            data['floor_plans_urls'] = None

        try:
            if data['site_plans_urls']:
                lst = data['site_plans_urls'].split(',')
                lst = [item.strip() for item in lst]
                data['site_plans_urls'] = lst
            else:
                data['site_plans_urls'] = None
        except KeyError:
            data['site_plans_urls'] = None

        try:
            desc = gather_condo_desc(data['link_to_condo'])
            if desc:
                data['description'] = desc
        except KeyError:
            pass
        main_data_to_save.append(data)

    return main_data_to_save


def prepare_dubai_main_data(main_data):
    main_data_to_save = []
    for item in main_data:
        data = item['fields']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        try:
            data['Condo ID'] = str(data['Condo ID'])
        except (ValueError, KeyError):
            pass
        try:
            data['payment_plans'] = str(data['payment_plans'])
        except (ValueError, KeyError):
            pass
        try:
            if data['brochure']:
                data['brochure'] = [data['brochure'][0]['url']]
        except KeyError:
            pass
        main_data_to_save.append(data)

    return main_data_to_save


def prepare_bali_main_data(main_data):
    main_data_to_save = []
    for item in main_data:
        data = item['fields']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        try:
            data['Condo ID'] = str(data['Condo ID'])
        except (ValueError, KeyError):
            pass
        try:
            if data['brochure']:
                data['brochure'] = [data['brochure'][0]['url']]
        except KeyError:
            pass
        main_data_to_save.append(data)

    return main_data_to_save


def prepare_oman_main_data(main_data):
    main_data_to_save = []
    for item in main_data:
        data = item['fields']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        try:
            data['Condo ID'] = str(data['Condo ID'])
        except (ValueError, KeyError):
            pass

        main_data_to_save.append(data)

    return main_data_to_save


def prepare_miami_main_data(main_data):
    main_data_to_save = []
    for item in main_data:
        data = item['fields']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        try:
            if data['payment_plans_attached']:
                data['payment_plans_attached'] = [data['payment_plans_attached'][0]['url']]
        except KeyError:
            pass
        try:
            data['overall_min_unit_price'] = float(data['overall_min_unit_price'])
        except (ValueError, KeyError):
            pass
        try:
            data['overall_max_unit_price'] = float(data['overall_max_unit_price'])
        except (ValueError, KeyError):
            pass
        main_data_to_save.append(data)

    return main_data_to_save


def prepare_uk_main_data(main_data):
    main_data_to_save = []
    for item in main_data:
        data = item['fields']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        try:
            data['brochure'] = [data['brochure']]
        except KeyError:
            pass
        try:
            data['overall_min_unit_price'] = float(data['overall_min_unit_price'])
        except (ValueError, KeyError):
            pass
        try:
            data['overall_max_unit_price'] = float(data['overall_max_unit_price'])
        except (ValueError, KeyError):
            pass
        main_data_to_save.append(data)

    return main_data_to_save


def delete_old_main_data():
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

        delete_sql = """
                        DELETE FROM general
                        WHERE latest_update != %s;
                    """
        cursor.execute(delete_sql, (current_date,))
        connection.commit()

        cursor.close()

        print("Rows deleted successfully.")
    except Exception as e:
        print(f"Error: {e}")


def get_all_records():
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM general")
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]
    records = [dict(zip(column_names, row)) for row in rows]
    cursor.close()
    connection.close()
    return records


def update_airtable_record(api_key, base_id, table_name, record_id, field_name, new_value):
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    url = f"https://api.airtable.com/v0/{base_id}/{table_name}"

    data = {
        "records": [
            {
                "id": record_id,
                "fields": {
                    field_name: new_value
                }
            }
        ]
    }
    try:
        response = requests.patch(url, headers=headers, data=json.dumps(data))

        if response.status_code == 200:
            print("Record updated successfully.")
        else:
            print(f"Failed to update record. Status code: {response.status_code}, Response: {response.text}")
    except Exception:
        print('error')
        pass


def gather_bali_i_main_data():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/apptB9TeBEoVa9djt/tblc0nK5MGsmjLrwe',
                                headers={
                                    "Authorization": "Bearer " + 'pat8aakZFTgNDCTlV.76cbff7a26e4da92dcf48b3115eadf8c7004fd1792748aeedcddfc192edfffb5',
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


def prepare_bali_i_main_data(main_data):
    main_data_to_save = []
    for item in main_data:
        data = item['fields']
        data['latest_update'] = date.today().strftime('%Y-%m-%d')
        try:
            data['Condo ID'] = str(data['Condo ID'])
        except (ValueError, KeyError):
            pass
        try:
            if data['brochure']:
                data['brochure'] = [data['brochure'][0]['url']]
        except KeyError:
            pass
        main_data_to_save.append(data)

    return main_data_to_save


def save_bali_i_main_data(data):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    insert_sql = """
    INSERT INTO general (
        name, address, district, units_number, link_to_condo, brochure, facilities,
        overall_available_units, units, "Condo ID", latest_update, city, companies
    )
    VALUES (
        %(name)s, %(address)s, %(district)s, %(units_number)s, %(link_to_condo)s,
        %(brochure)s, %(facilities)s, %(overall_available_units)s,
        %(units)s,
        %(Condo ID)s, %(latest_update)s, %(city)s, %(companies)s
    ) RETURNING id;
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
        for record in formatted_data:
            try:
                cursor.execute(insert_sql, record)
            except Exception:
                continue
        connection.commit()
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()
