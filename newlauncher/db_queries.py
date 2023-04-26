import json

import requests

from config import Config


async def store_data_airtable(main, units, amenities):
    unit_ids = await save_units_data(units)
    amenity_ids = await save_amenities_data(amenities)
    await save_main_data(main, unit_ids, amenity_ids)


async def save_units_data(units):
    ids_data = []

    for unit in units:
        data_to_load = {'fields': unit, "typecast": True}
        upload_json = json.dumps(data_to_load, default=str)
        base_id = 'appSq8avuMgvW0TWV'
        table_id = 'tblDeejlqLIGCptNa'
        url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
        r = requests.post(url, data=upload_json, headers=Config.AIR_TABLE_HEADERS)
        ids_data.append(r.json()['id'])

    return ids_data


async def save_amenities_data(amenities):
    ids_data = []
    for amenity in amenities:
        data_to_load = {'fields': amenity, "typecast": True}
        upload_json = json.dumps(data_to_load, default=str)
        base_id = 'appSq8avuMgvW0TWV'
        table_id = 'tblsH84eM0E3hbbTT'
        url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
        r = requests.post(url, data=upload_json, headers=Config.AIR_TABLE_HEADERS)
        ids_data.append(r.json()['id'])

    return ids_data


async def save_main_data(main, unit_ids, amenity_ids):
    main['amenities'] = amenity_ids
    main['units'] = unit_ids
    data_to_load = {'fields': main, "typecast": True}
    upload_json = json.dumps(data_to_load, default=str)
    base_id = 'appSq8avuMgvW0TWV'
    table_id = 'tbly6SWxIqZMh6VvW'
    url = f"https://api.airtable.com/v0/{base_id}/{table_id}"
    requests.post(url, data=upload_json, headers=Config.AIR_TABLE_HEADERS)
