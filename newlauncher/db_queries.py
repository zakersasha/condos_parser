import json

import requests

from config import Config


def get_old_units_data(existing_data, units):
    old_data = []
    new_units = []
    for item in existing_data:
        response = requests.get(
            f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.UNITS_TABLE_ID}/{item}',
            headers=Config.AIR_TABLE_HEADERS)
        old_data.append(response.json()['fields'])

    list_1_key_set = {(d['unit_type'], d['size_min']) for d in old_data}
    for dict_2 in units:
        if (dict_2['unit_type'], int(dict_2['size_min'])) not in list_1_key_set:
            new_units.append(dict_2)

    return new_units


def get_old_amenities_data(existing_data, amenities):
    old_data = []
    new_amenities = []
    for item in existing_data:
        response = requests.get(
            f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.AMENITIES_TABLE_ID}/{item}',
            headers=Config.AIR_TABLE_HEADERS)
        old_data.append(response.json()['fields'])

    list_1_key_set = {d['amenities_name'] for d in old_data}
    for dict_2 in amenities:
        if dict_2['amenities_name'] not in list_1_key_set:
            new_amenities.append(dict_2)

    return new_amenities


def store_data_airtable(main, units, amenities):
    records = get_all_records()

    try:
        exists_data = next(
            ([item["id"], item["fields"]['units'], item["fields"]['amenities']] for item in records['records'] if
             item["fields"]['name'] == main['name']), None)
    except KeyError:
        exists_data = None

    if exists_data:
        label = 'Updated'

        # UNITS LOGIC
        new_units = get_old_units_data(exists_data[1], units)
        new_unit_ids = save_units_data(new_units)

        # AMENITIES LOGIC
        new_amenities = get_old_amenities_data(exists_data[2], amenities)
        new_amenities_ids = save_amenities_data(new_amenities)

        url = f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.MAIN_TABLE_ID}/{exists_data[0]}'
        main['amenities'] = new_amenities_ids + exists_data[2]
        main['units'] = new_unit_ids + exists_data[1]

        json_data = {
            'fields': main
        }

        r = requests.put(url, json=json_data, headers=Config.AIR_TABLE_HEADERS)
        print(f'Data updated {r} {r.json()}')

        return label

    else:
        label = 'New'
        unit_ids = save_units_data(units)
        amenity_ids = save_amenities_data(amenities)
        save_main_data(main, unit_ids, amenity_ids)
        return label


def delete_old_units(units_data):
    for unit in units_data:
        response = requests.delete(
            f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.UNITS_TABLE_ID}/{unit}',
            headers=Config.AIR_TABLE_HEADERS)
        print(f'Units deleted {response} {response.json()}')


def delete_old_amenities(amenities_data):
    for amenity in amenities_data:
        response = requests.delete(
            f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.AMENITIES_TABLE_ID}/{amenity}',
            headers=Config.AIR_TABLE_HEADERS)
        print(f'Amenities deleted {response} {response.json()}')


def save_units_data(units):
    if len(units) == 0:
        return []
    ids_data = []

    for unit in units:
        data_to_load = {'fields': unit, "typecast": True}
        upload_json = json.dumps(data_to_load)

        url = f"https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.UNITS_TABLE_ID}"
        r = requests.post(url, data=upload_json, headers=Config.AIR_TABLE_HEADERS)
        print(f'Saving units {r}')

        ids_data.append(r.json()['id'])

    return ids_data


def save_amenities_data(amenities):
    ids_data = []

    for amenity in amenities:
        data_to_load = {'fields': amenity, "typecast": True}
        upload_json = json.dumps(data_to_load)

        url = f"https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.AMENITIES_TABLE_ID}"
        r = requests.post(url, data=upload_json, headers=Config.AIR_TABLE_HEADERS)
        print(f'Saving amenities {r}')

        ids_data.append(r.json()['id'])

    return ids_data


def save_main_data(main, unit_ids, amenity_ids):
    main['amenities'] = amenity_ids
    if len(unit_ids) > 0:
        main['units'] = unit_ids

    data_to_load = {'fields': main, "typecast": True}
    upload_json = json.dumps(data_to_load)
    url = f"https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.MAIN_TABLE_ID}"
    r = requests.post(url, data=upload_json, headers=Config.AIR_TABLE_HEADERS)
    print(f'Saving main data {r}')


def get_all_records():
    r = requests.get(f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.MAIN_TABLE_ID}',
                     headers=Config.AIR_TABLE_HEADERS)
    return r.json()
