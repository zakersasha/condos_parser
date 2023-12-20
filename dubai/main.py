import requests
from dubai.airtable import get_general_records, create_record, update_record, get_units_records


def load_page(page_number):
    entities = []

    response = requests.get(
        f"https://api.alnair.ae/v1/rc/search?page={page_number}&mapBounds%5Beast%5D=55.55305480957031&mapBounds%5Bnorth%5D=25.41350860804229&mapBounds%5Bsouth%5D=24.89453374486885&mapBounds%5Bwest%5D=54.96871948242188&isList=1&isPin=1").json()
    for item in response["data"]["list"]:
        entities.append(item)
    return entities


def load_entities():
    page = 1
    all_entities = []
    entities = load_page(page)
    while len(entities) != 0:
        all_entities += entities
        page += 1
        entities = load_page(page)
    return all_entities


def get_addition_data_by_id(id_):
    response = requests.get(f"https://api.alnair.ae/v1/rc/view/{id_}").json()
    return response


def get_units(id_):
    response = requests.get(f"https://api.alnair.ae/v1/rc/{id_}/layouts/units").json()
    return response


def get_data_by_entity(entity):
    id = entity["id"]
    addition_data = get_addition_data_by_id(id)
    units = get_units(id)
    name = entity["title"]
    address = entity.get("address", "")
    district = addition_data.get("district", "")
    units_number = entity["stats"]["total"]["unitsCount"]
    date_of_completion = addition_data.get("predicted_completion_at", "")
    url = f"https://alnair.ae/app/view/{id}"
    brochure = ""
    documents = addition_data.get("documents", False)
    if documents:
        for doc in documents:
            if doc["title"] == "BROCHURE":
                brochure = doc["url"]
                break

    payment_plans_result = []
    payment_plans = addition_data.get('paymentPlans', False)
    if payment_plans:
        for plan in payment_plans:
            payment_plan = []
            for stage in plan["items"]:

                fix = None
                monthly_percent = None

                stage_name = stage['milestone']
                deadline = stage["when_at"]
                percent = stage["percent"]
                if percent is None:
                    fix = stage["price"]
                else:
                    total_percent = stage["total_percent"]
                    if percent != total_percent:
                        monthly_percent = percent
                    else:
                        monthly_percent = ""
                payment_plan.append({"stage_name": stage_name, "deadline": deadline, "percent": percent,
                                     "monthly_percent": monthly_percent, "fix": fix})
            payment_plans_result.append(payment_plan)
    overall_available_units = entity["stats"]["total"]["count"]

    condo_id = id
    description = addition_data.get("description", "")
    city = "Dubai"
    longitude = entity.get("longitude", "")
    latitude = entity.get("latitude", "")

    overall_min_unit_size = 0
    overall_max_unit_size = 0
    overall_min_unit_psf = 0
    overall_max_unit_psf = 0
    overall_min_unit_price = 0
    overall_max_unit_price = 0

    for unit in units:
        items = units[unit]["items"]
        for item in items:
            square_min = float(item["square_min"])
            square_max = float(item["square_max"])

            if overall_min_unit_size == 0:
                overall_min_unit_size = square_min
            if overall_max_unit_size == 0:
                overall_max_unit_size = square_max

            if square_min < overall_min_unit_size:
                overall_min_unit_size = square_min
            if square_max > overall_max_unit_size:
                overall_max_unit_size = square_max

            price_min = int(item["price_min"])
            price_max = int(item["price_max"])

            if overall_min_unit_price == 0:
                overall_min_unit_price = price_min
            if overall_max_unit_price == 0:
                overall_max_unit_price = price_max

            if price_min < overall_min_unit_price:
                overall_min_unit_price = price_min
            if price_max > overall_max_unit_price:
                overall_max_unit_price = price_max

    if overall_min_unit_size != 0 and overall_max_unit_size != 0:
        overall_min_unit_psf = round(overall_min_unit_price / overall_min_unit_size)
        overall_max_unit_psf = round(overall_max_unit_price / overall_max_unit_size)

    residential_complex_advantages = None
    if addition_data.get("catalogs", False):
        if addition_data["catalogs"].get("residential_complex_advantages", False):
            residential_complex_advantages = []
            for id_ in addition_data["catalogs"]["residential_complex_advantages"]:
                for id__ in residential_complex_info:
                    if id_ == id__["id"]:
                        residential_complex_advantages.append(id__["value"])
                        break

    units_ = []
    for unit in units:

        if unit[-1] == "0":
            unit_type = f"Studio"
            num_bedrooms = "1"
        else:
            unit_type = f"{unit[-1]} Bedroom"
            num_bedrooms = unit[-1]
        for item in units[unit]["items"]:
            title = None
            available_units = item["count"]
            price_min = item["price_min"]
            price_max = item["price_max"]
            size_min = item["square_min"]
            size_max = item["square_max"]
            psf_min = float(price_min) / float(size_min)
            psf_max = float(price_max) / float(size_max)
            if item["layout"] is not None:
                title = item["layout"].get("title", unit_type)
                if not ("bedroom" in title.lower() or "studio" in title.lower()):
                    title = unit_type
                floor_plan_image_links = get_logo_url(item["layout"]["id"])
            else:
                floor_plan_image_links = []
            is_add = True
            for index in range(len(units_)):
                unit_ = units_[index]["fields"]
                if unit_["num_bedrooms"] == num_bedrooms and unit_["size_min"] == size_min \
                        and unit_["size_max"] == size_max and unit_["unit_type"] == unit_type:
                    is_add = False

                    for image in unit_["floor_plan_image_links"]:
                        floor_plan_image_links.append(image)
                    price_min = min(price_min, unit_["price_min"])
                    price_max = max(price_max, unit_["price_max"])
                    size_min = min(size_min, unit_["size_min"])
                    size_max = max(size_max, unit_["size_max"])
                    psf_min = float(price_min) / float(size_min)
                    psf_max = float(price_max) / float(size_max)
                    units_[index] = {"fields": {"unit_type": title if title is not None else unit_type, "General": name,
                                                "available_units": available_units,
                                                "price_min": price_min, "price_max": price_max, "size_min": size_min,
                                                "size_max": size_max,
                                                "psf_min": psf_min, "psf_max": psf_max,
                                                "floor_plan_image_links": floor_plan_image_links,
                                                "num_bedrooms": num_bedrooms}}
                    break
            if is_add:
                units_.append({"fields": {"unit_type": title if title is not None else unit_type, "General": name,
                                          "available_units": available_units,
                                          "price_min": price_min, "price_max": price_max, "size_min": size_min,
                                          "size_max": size_max,
                                          "psf_min": psf_min, "psf_max": psf_max,
                                          "floor_plan_image_links": floor_plan_image_links,
                                          "num_bedrooms": num_bedrooms}})

    return {"fields": {"name": name, "Condo ID": condo_id, "address": address, "district parsing": district,
                       "units_number": units_number, "date_of_completion": date_of_completion,
                       "link_to_condo": url, "brochure": brochure, "overall_available_units": overall_available_units,
                       "description": description, "facilities": residential_complex_advantages,
                       "overall_min_unit_size": overall_min_unit_size, "overall_max_unit_size": overall_max_unit_size,
                       "overall_min_unit_psf": overall_min_unit_psf, "overall_max_unit_psf": overall_max_unit_psf,
                       "overall_min_unit_price": overall_min_unit_price,
                       "overall_max_unit_price": overall_max_unit_price,
                       "longitude": longitude, "latitude": latitude, "city": city,
                       "payment_plans": payment_plans_result}}, units_


def get_residential_complex_info():
    response = requests.get(f"https://api.alnair.ae/v1/info").json()
    return response["data"]["catalogs"]["residential_complex_advantages"]["items"]


def get_logo_url(id_):
    response = requests.get(f"https://api.alnair.ae/v1/rc/layout/{id_}").json()
    urls = []
    levels = response["levels"]
    for level in levels:
        urls.append(level["logo"])
    return urls


def parse_dubai():
    general_records_in_db = get_general_records()
    units_records_in_db = get_units_records(general_records_in_db)

    entities = load_entities()
    for entity in entities:
        print(entity)
        title = entity["title"]

        general, units = get_data_by_entity(entity)
        record = general_records_in_db.get(title, False)

        if not record:
            records_general = [general]

            create_record("General", records_general)

            for i in range(0, len(units), 10):
                create_record("Units Info", units[i: i + 10])
            continue

        for item in general["fields"].copy():
            if item == "" or general["fields"][item] == record["fields"].get(item, ""):
                general["fields"].pop(item)
        if general["fields"].get("link_to_condo", False):
            general["fields"].pop("link_to_condo")
        update_record("General", general, record["id"])
        # # Проверка условия нахождения юнита в таблице Units Info
        units_db = units_records_in_db.get(record["id"], [])
        for index, unit in enumerate(units, 0):
            unit_fields = unit["fields"]
            unit_type = unit_fields['unit_type']
            unit_num_bedrooms = unit_fields['num_bedrooms']
            unit_size_min = unit_fields['size_min']
            unit_size_max = unit_fields['size_max']

            in_db = False
            for unit_db in units_db:
                unit_db_fields = unit_db["fields"]
                unit_db_type = unit_db_fields['unit_type']
                unit_db_num_bedrooms = unit_db_fields['num_bedrooms']
                unit_db_size_min = unit_db_fields['size_min']
                unit_db_size_max = unit_db_fields['size_max']
                if unit_type == unit_db_type and unit_num_bedrooms == unit_db_num_bedrooms and unit_size_min == unit_db_size_min and unit_size_max == unit_db_size_max:
                    in_db = True
                    for item in unit_fields.copy():
                        if unit_fields[item] == unit_db_fields.get(item, ""):
                            units[index]["fields"].pop(item)

                    update_record("Units Info", units[index], unit_db["id"])

                    break

            if in_db:
                continue

            create_record("Units Info", [unit])


residential_complex_info = get_residential_complex_info()
# print(requests.get("https://api.alnair.ae/v1/rc/search?page=1&limit=30&mapBounds%5Beast%5D=55.55305480957031&mapBounds%5Bnorth%5D=25.41350860804229&mapBounds%5Bsouth%5D=24.89453374486885&mapBounds%5Bwest%5D=54.96871948242188&isList=1&isPin=1").json())
# print(requests.get("https://api.alnair.ae/v1/rc/view/1527").text)
# print(requests.get("https://api.alnair.ae/v1/rc/1527/layouts/units").text)
