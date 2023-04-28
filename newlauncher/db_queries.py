import json

import requests

from config import Config


async def store_data_airtable(main, units, amenities):
    records = await get_all_records()

    try:
        exists_data = next(
            ([item["id"], item["fields"]['units'], item["fields"]['amenities']] for item in records['records'] if
             item["fields"]['name'] == main['name']), None)
    except KeyError:
        exists_data = None

    if exists_data:
        label = 'Updated'

        unit_ids = await save_units_data(units)
        amenity_ids = await save_amenities_data(amenities)

        url = f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.MAIN_TABLE_ID}/{exists_data[0]}'
        main['amenities'] = amenity_ids
        main['units'] = unit_ids

        json_data = {
            'fields': main
        }

        r = requests.put(url, json=json_data, headers=Config.AIR_TABLE_HEADERS)
        print(f'Data updated {r} {r.json()}')

        await delete_old_units(exists_data[1])
        await delete_old_amenities(exists_data[2])

        return label

    else:
        label = 'New'
        unit_ids = await save_units_data(units)
        amenity_ids = await save_amenities_data(amenities)
        await save_main_data(main, unit_ids, amenity_ids)
        return label


async def delete_old_units(units_data):
    for unit in units_data:
        response = requests.delete(
            f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.UNITS_TABLE_ID}/{unit}',
            headers=Config.AIR_TABLE_HEADERS)
        print(f'Units deleted {response} {response.json()}')


async def delete_old_amenities(amenities_data):
    for amenity in amenities_data:
        response = requests.delete(
            f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.AMENITIES_TABLE_ID}/{amenity}',
            headers=Config.AIR_TABLE_HEADERS)
        print(f'Amenities deleted {response} {response.json()}')


async def save_units_data(units):
    ids_data = []

    for unit in units:
        data_to_load = {'fields': unit, "typecast": True}
        upload_json = json.dumps(data_to_load)

        url = f"https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.UNITS_TABLE_ID}"
        r = requests.post(url, data=upload_json, headers=Config.AIR_TABLE_HEADERS)
        print(f'Saving units {r} {r.json()}')

        ids_data.append(r.json()['id'])

    return ids_data


async def save_amenities_data(amenities):
    ids_data = []

    for amenity in amenities:
        data_to_load = {'fields': amenity, "typecast": True}
        upload_json = json.dumps(data_to_load)

        url = f"https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.AMENITIES_TABLE_ID}"
        r = requests.post(url, data=upload_json, headers=Config.AIR_TABLE_HEADERS)
        print(f'Saving amenities {r} {r.json()}')

        ids_data.append(r.json()['id'])

    return ids_data


async def save_main_data(main, unit_ids, amenity_ids):
    main['amenities'] = amenity_ids
    main['units'] = unit_ids
    data_to_load = {'fields': main, "typecast": True}
    upload_json = json.dumps(data_to_load)
    url = f"https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.MAIN_TABLE_ID}"
    requests.post(url, data=upload_json, headers=Config.AIR_TABLE_HEADERS)


async def get_all_records():
    r = requests.get(f'https://api.airtable.com/v0/{Config.AIR_TABLE_BASE_ID}/{Config.MAIN_TABLE_ID}',
                     headers=Config.AIR_TABLE_HEADERS)
    return r.json()
