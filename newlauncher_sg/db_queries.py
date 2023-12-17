import datetime
import json

import requests

from config import Config


def get_old_units_data(existing_data, units):
    old_data = []
    new_units = []
    upd_data = []
    if not existing_data:
        return units

    for item in existing_data:
        response = requests.get(
            f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.UNITS_TABLE_ID}/{item}',
            headers=Config.AIR_TABLE_HEADERS)
        upd_data.append(response.json())
        old_data.append(response.json()['fields'])

    changes = update_units_data(upd_data, units)

    list_1_key_set = {(d['unit_type'], d['size_min']) for d in old_data}
    for dict_2 in units:
        if (dict_2['unit_type'], int(dict_2['size_min'])) not in list_1_key_set:
            new_units.append(dict_2)

    return new_units, changes


def update_units_data(old_data, new_data):
    # Обновление юнитов
    changes = []
    for unit in new_data:
        found_data = next((data for data in old_data if
                           data['fields']['unit_type'] == unit['unit_type'] and data['fields']['size_min'] == unit[
                               'size_min']), None)

        if found_data:
            if unit != found_data['fields']:
                record_id = found_data['id']
                url = f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.UNITS_TABLE_ID}/{record_id}'
                if unit['price_min'] is None or unit['price_min'] == '':
                    del unit['price_min']
                if unit['price_max'] is None or unit['price_max'] == '':
                    del unit['price_max']
                requests.patch(url, headers=Config.AIR_TABLE_HEADERS, data=json.dumps({'fields': unit}))

                if unit.get('price_min', None) != found_data.get("fields", {}).get('price_min', None):
                    changes.append(
                        f"{unit['unit_type']} price: {found_data.get('fields', {}).get('price_min', None)} → {unit.get('price_min', None)} ")
                if unit['available_units'] != found_data['fields']['available_units']:
                    changes.append(
                        f"{unit['unit_type']} available units: {found_data['fields']['available_units']} → {unit['available_units']} ")

    # update units availability
    for old_unit in old_data:
        found = next((data for data in new_data if
                      data['unit_type'] == old_unit['fields']['unit_type'] and data['size_min'] ==
                      old_unit['fields']['size_min']), None)
        if not found:
            record_id = old_unit['id']
            old_unit['fields']['all_units'] = 0.0
            old_unit['fields']['available_units'] = 0.0
            try:
                del old_unit['fields']['Book Preview Button']
            except KeyError:
                pass
            try:
                del old_unit['fields']['Condo Images']
            except KeyError:
                pass

            url = f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.UNITS_TABLE_ID}/{record_id}'
            requests.patch(url, headers=Config.AIR_TABLE_HEADERS,
                           data=json.dumps({'fields': old_unit['fields']}))
            changes.append(
                f"{old_unit['fields']['unit_type']} available units → 0")
    return changes


def get_old_amenities_data(existing_data, amenities):
    old_data = []
    new_amenities = []
    if not existing_data:
        return amenities

    for item in existing_data:
        response = requests.get(
            f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.AMENITIES_TABLE_ID}/{item}',
            headers=Config.AIR_TABLE_HEADERS)
        old_data.append(response.json()['fields'])

    try:
        list_1_key_set = {d['amenities_name'] for d in old_data}
        for dict_2 in amenities:
            if dict_2['amenities_name'] not in list_1_key_set:
                new_amenities.append(dict_2)
    except KeyError:
        return []
    return new_amenities


def store_data_airtable(main, units, amenities):
    if units or len(units) > 0:
        records = get_all_records()

        exists_data = next(
            ([item.get("id", None), item.get("fields", {}).get('units'), item.get("fields", {}).get('amenities'),
              item.get("fields", {})] for
             item in records if
             item["fields"]['name'].replace(' @ ', ' ').lower() == main['name'].replace(' @ ', ' ').lower()),
            None)
        try:
            record_id = exists_data[0]
        except TypeError:
            label = 'New'
            unit_ids = save_units_data(units)
            amenity_ids = save_amenities_data(amenities)
            save_main_data(main, unit_ids, amenity_ids)
            return label, None, None, None

        if record_id:
            label = 'Updated'
            new_units, changes = get_old_units_data(exists_data[1], units)
            new_amenities = get_old_amenities_data(exists_data[2], amenities)

            url = f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.MAIN_TABLE_ID}/{exists_data[0]}'

            if len(new_amenities) > 0:
                new_amenities_ids = save_amenities_data(new_amenities)

                if not exists_data[2]:
                    main['amenities'] = new_amenities_ids
                else:
                    main['amenities'] = new_amenities_ids + exists_data[2]

            if len(new_units) > 0:
                new_unit_ids = save_units_data(new_units)
                if not exists_data[1]:
                    main['units'] = new_unit_ids
                else:
                    main['units'] = new_unit_ids + exists_data[1]

            try:
                del main['date_of_completion']
            except KeyError:
                pass
            try:
                del main['previewing_start_date']
            except KeyError:
                pass

            try:
                old_link = exists_data[3]['link_to_condo']
            except KeyError:
                old_link = 'empty'
            new_link = main['link_to_condo']

            if old_link != new_link and ',' not in old_link and old_link != 'empty':
                main['link_to_condo'] = f"{old_link}, {new_link}"
            elif old_link == 'empty':
                main['link_to_condo'] = new_link
            else:
                pass

            json_data = {
                'fields': main
            }

            r = requests.patch(url, json=json_data, headers=Config.AIR_TABLE_HEADERS)
            print(f'Data updated {r} {r.json()}')
            save_updated_to_file(exists_data[3], main)

            return label, new_units, main['units_number'], changes
    else:
        return None, None, None, None


def save_updated_to_file(old_data, new_data):
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"NewLauncher_sg_{current_date}.txt"

    with open(filename, "a") as file:
        diff = ''
        diff += f"\n\n{old_data.get('name')}\n"
        for key in new_data:
            if key in old_data:
                if old_data[key] != new_data[key]:
                    diff += f"Key: {key}, Old Value: {old_data[key]}, New Value: {new_data[key]}\n"
            else:
                diff += f"Key: {key}, Old Value: None, New Value: {new_data[key]}\n"

        file.write(diff)


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
        try:
            data_to_load = {'fields': unit, "typecast": True}
            upload_json = json.dumps(data_to_load)

            url = f"https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.UNITS_TABLE_ID}"
            r = requests.post(url, data=upload_json, headers=Config.AIR_TABLE_HEADERS)
            print(f'Saving units {r}')

            ids_data.append(r.json()['id'])
        except (KeyError, AttributeError):
            continue
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
    print(f'Saving main data {r.json()}')


def get_all_records():
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
