import time

import requests

token = "patAtUpABzM9pPvIG.b6c28f6914a15e258f824d6e1ba066c4919c232c00028cc0c34476c477660456"
base_id = "appoHsQ6y9Ff4cWaW"


def get_general_records():
    all_records = {"ids": []}
    offset = ""
    while offset is not None:
        response = requests.get(f"https://api.airtable.com/v0/{base_id}/General copy?offset={offset}",
                                headers={"Authorization": f"Bearer {token}", "Content-type": "application/json"}).json()

        for record in response["records"]:
            if not record["fields"].get("link_to_condo", False):
                continue

            if "alnair" not in record["fields"]["link_to_condo"]:
                continue

            name = record["fields"]["name"]
            all_records[name] = {"fields": record["fields"], "id": record["id"]}
            all_records["ids"].append(record["id"])
        time.sleep(0.5)
        offset = response.get("offset", None)
    return all_records


def get_units_records(all_records):
    all_units = {}
    offset = ""
    counter = 0
    while offset is not None:
        response = requests.get(f"https://api.airtable.com/v0/{base_id}/Units Info copy?offset={offset}",
                                headers={"Authorization": f"Bearer {token}", "Content-type": "application/json"}).json()

        for record in response["records"]:
            counter += 1
            general_id = record["fields"]["General"][0]

            if general_id not in all_records["ids"]:
                continue

            if not all_units.get(general_id, False):
                all_units[general_id] = [record]

            # all_units[general_id].append()

        offset = response.get("offset", None)
    return all_units


def create_record(table_name, records):
    print(records)
    print(requests.post(f"https://api.airtable.com/v0/{base_id}/{table_name}",
                        json={"records": records, "typecast": True},
                        headers={"Authorization": f"Bearer {token}", "Content-type": "application/json"}).text)


# print(get_general_records())
#
def update_record(table_name, records, record_id):
    records["typecast"] = True
    print(requests.patch(f"https://api.airtable.com/v0/{base_id}/{table_name}/{record_id}",
                         json=records,
                         headers={"Authorization": f"Bearer {token}", "Content-type": "application/json"}).text)

# get_units_records(get_general_records())
