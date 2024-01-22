import json

import requests

from miami.config import base_id, units_table, general_table, AIR_TABLE_HEADERS


def get_old_units_data(existing_data, units):
    old_data = []
    new_units = []
    upd_data = []
    if not existing_data:
        return units

    for item in existing_data:
        response = requests.get(
            f'https://api.airtable.com/v0/{base_id}/{units_table}/{item}',
            headers=AIR_TABLE_HEADERS)
        upd_data.append(response.json())
        old_data.append(response.json()['fields'])

    list_1_key_set = set()

    for d in old_data:
        try:
            list_1_key_set.add((d['unit_type'], d['size_min']))
        except KeyError:
            continue
    for dict_2 in units:
        if (dict_2['unit_type'], int(dict_2['size_min'])) not in list_1_key_set:
            new_units.append(dict_2)

    return new_units


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
                url = f'https://api.airtable.com/v0/{base_id}/{units_table}/{record_id}'
                requests.patch(url, headers=AIR_TABLE_HEADERS, data=json.dumps({'fields': unit}))

    # update units availability
    for old_unit in old_data:
        found = next((data for data in new_data if
                      data['unit_type'] == old_unit['fields']['unit_type'] and data['size_min'] ==
                      old_unit['fields']['size_min']), None)
        if not found:
            record_id = old_unit['id']

            url = f'https://api.airtable.com/v0/{base_id}/{units_table}/{record_id}'
            requests.patch(url, headers=AIR_TABLE_HEADERS,
                           data=json.dumps({'fields': old_unit['fields']}))

    return changes


def store_data_airtable(main, units):
    records = get_all_records()

    exists_data = next(
        ([item.get("id", None), item.get("fields", {}).get('units'),
          item.get("fields", {})] for
         item in records if
         item["fields"]['name'].replace(' @ ', ' ').lower() == main['name'].replace(' @ ', ' ').lower()),
        None)
    try:
        record_id = exists_data[0]
    except TypeError:
        label = 'New'
        unit_ids = save_units_data(units)
        save_main_data(main, unit_ids)
        return label, None, None, None

    if record_id:
        label = 'Updated'
        new_units = get_old_units_data(exists_data[1], units)

        url = f'https://api.airtable.com/v0/{base_id}/{general_table}/{exists_data[0]}'
        if new_units:
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

        json_data = {
            'fields': main
        }

        r = requests.patch(url, json=json_data, headers=AIR_TABLE_HEADERS)
        print(f'Data updated {r} {r.json()}')


def save_units_data(units):
    if not units:
        return []
    ids_data = []

    for unit in units:
        try:
            data_to_load = {'fields': unit, "typecast": True}
            upload_json = json.dumps(data_to_load)

            url = f"https://api.airtable.com/v0/{base_id}/{units_table}"
            r = requests.post(url, data=upload_json, headers=AIR_TABLE_HEADERS)
            print(f'Saving units {r}')

            ids_data.append(r.json()['id'])
        except (KeyError, AttributeError):
            continue
    return ids_data


def save_main_data(main, unit_ids):
    if len(unit_ids) > 0:
        main['units'] = unit_ids

    data_to_load = {'fields': main, "typecast": True}
    upload_json = json.dumps(data_to_load)
    url = f"https://api.airtable.com/v0/{base_id}/{general_table}"
    r = requests.post(url, data=upload_json, headers=AIR_TABLE_HEADERS)
    print(f'Saving main data {r.json()}')


def get_all_records():
    all_records = []

    params = {
        'pageSize': 100,
    }

    while True:
        response = requests.get(f'https://api.airtable.com/v0/{base_id}/{general_table}',
                                headers=AIR_TABLE_HEADERS, params=params)
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
