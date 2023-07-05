import requests
from bs4 import BeautifulSoup

from config import Config


def get_last_page_number():
    url = f'{Config.SRX_URL}/new-condo-launch/search?sortCriteria=launchDateDesc&maxResults=12&page=1'
    r = requests.get(url, headers=Config.SRX_HEADERS)
    soup = BeautifulSoup(r.text, 'html.parser')
    max_page = soup.find("ul", {"id": "pagination"}).find_all("a")[-2].text.replace('\n', '')

    return int(max_page)


async def gather_projects_links(max_page):
    projects_links = []

    for page in range(1, max_page + 1):
        url = f'{Config.SRX_URL}/new-condo-launch/search?sortCriteria=launchDateDesc&maxResults=12&page={str(page)}'
        r = requests.get(url, headers=Config.SRX_HEADERS)
        soup = BeautifulSoup(r.text, 'html.parser')
        links = soup.findAll("a", {"class": "new-launches-search-result-box"})
        for link in links:
            projects_links.append(Config.SRX_URL + link['href'])
    return projects_links


async def gather_main_table_data(url, soup):
    images = []
    images_data = soup.find("div", {"id": "listing-photo-gallery-scroller-container"}).find_all("img")
    for image in images_data:
        images.append({"url": image["src"].replace(' ', '%20')})
    try:
        sold_units = soup.find("div", {"id": "total-units-bar-sold"}).text.split('%')[0]
    except AttributeError:
        sold_units = 0

    name = soup.find("h1").text
    if 'email' in name:
        name = await handle_email_name(url)

    page_link = url
    address = soup.find("span", {"class": "notranslate"}).text.replace('|', '').split(',')[0]
    district = soup.find("div", {"id": "project-top-details-sub-desc"}).find('a').text.split(' ')[0]
    about_data = soup.find("div", {"id": "project-key-info-content"}).find_all('td')

    if len(about_data) == 12:
        c_type = about_data[3].text.replace('Condominium', 'Private Condominium').replace('Apartment',
                                                                                          'Private Condominium')
        date_of_completion = about_data[5].text + '-01-01'
        developer = about_data[7].text
        tenure = about_data[9].text.replace('FREEHOLD', 'FreeholdStatus')
        units_number = about_data[11].text

        result = {"name": name, "link_to_condo": page_link, "address": address, "district": district, "type": c_type,
                  "date_of_completion": date_of_completion, "developer": developer, "tenure": tenure, "images": images,
                  "units_number": int(units_number)}
        if sold_units:
            result["sold_units (in % )"] = int(sold_units)
        else:
            result["sold_units (in % )"] = 0
        return result

    elif len(about_data) == 10:
        c_type = about_data[3].text.replace('Condominium', 'Private Condominium').replace('Apartment',
                                                                                          'Private Condominium')
        developer = about_data[5].text
        tenure = about_data[7].text.replace('FREEHOLD', 'FreeholdStatus')
        units_number = about_data[9].text

        result = {"name": name, "link_to_condo": page_link, "address": address, "district": district, "type": c_type,
                  "developer": developer, "tenure": tenure, "units_number": int(units_number), "images": images}
        if sold_units:
            result["sold_units (in % )"] = int(sold_units)
        else:
            result["sold_units (in % )"] = 0

        return result

    elif len(about_data) == 8:
        c_type = about_data[3].text.replace('Condominium', 'Private Condominium').replace('Apartment',
                                                                                          'Private Condominium')
        developer = about_data[5].text
        tenure = about_data[7].text.replace('FREEHOLD', 'FreeholdStatus')
        result = {"name": name, "link_to_condo": page_link, "address": address, "district": district, "type": c_type,
                  "developer": developer, "tenure": tenure, "images": images}
        if sold_units:
            result["sold_units (in % )"] = int(sold_units)
        else:
            result["sold_units (in % )"] = 0
        return result


async def gather_units_table_data(soup, main_data):
    units_main_data = []
    units_detail_data = []

    units_data = soup.find("div", {"id": "project-top-details-rooms-table"})
    try:
        for unit in units_data.find_all("tr"):
            units_count = unit.find_all("td")

            if len(units_count) == 1:
                unit_details = unit.find_all("a")
                for detail in unit_details:
                    details_data = {}

                    result_type = await handle_unit_type(detail)

                    size_min = detail['data-size'].split('/')[0]
                    try:
                        psf_min = detail['data-size'].split('/')[1]
                    except IndexError:
                        psf_min = 0

                    price_min = detail['data-price'].replace('$', '').replace(',', '')
                    details_data['size_min'] = int(size_min.replace(' sqft', '').replace(',', ''))
                    try:
                        details_data['psf_min'] = int(
                            psf_min.replace(',', '').replace(' psf(Built)', '').replace('$', ''))
                    except ValueError:
                        details_data['psf_min'] = int(
                            detail['data-size'].split('/')[2].replace(',', '').replace(' psf(Built)', '').replace(
                                '$', ''))
                    except AttributeError:
                        details_data['psf_min'] = psf_min

                    details_data['district'] = main_data['district']
                    if 'View' not in price_min:
                        details_data['price_min'] = float(round(int(price_min) / 1000000, 1))
                    if not result_type:
                        details_data['unit_type'] = 'undetected'
                    details_data['unit_type'] = result_type
                    units_detail_data.append(details_data)
    except AttributeError:
        return []

    units_response = await merge_units_data(units_main_data, units_detail_data)
    complete_response = await remove_duplicate_units(units_response)
    return complete_response


async def gather_amenities_table_data(soup):
    amenities_data = soup.find("div", {"id": "project-maps-items"}).find_all("div")
    amenities_result_data = []
    for data in amenities_data:
        try:
            amenities_type = data['category']
            for amenity in data.find_all("div", {"class": "amenity-item-name-dist"}):
                amenities_name = amenity.find("div", {"class": "amenity-name"}).find_all("div")[0].text
                distance = float(
                    amenity.find("div", {"class": "amenity-distance"}).text.replace(' km', '').replace('m', ''))
                if distance > 20:
                    distance = distance / 1000
                amenities_result_data.append(
                    {"amenities_name": amenities_name,
                     "distance": round(distance, 1),
                     "amenities_type": amenities_type})
        except KeyError:
            continue
    return amenities_result_data


async def merge_units_data(main_data, details_data):
    for data in main_data:
        no_cards_data = next((item for item in details_data if item['unit_type'] == data['unit_type']), None)
        if no_cards_data:
            continue
        else:
            details_data.append(data)
    return details_data


async def handle_unit_type(unit_type):
    if unit_type['data-rooms'] == '1 ':
        return unit_type['data-rooms'].replace('1 ', '1 bedroom')
    if unit_type['data-rooms'] == '2 ':
        return unit_type['data-rooms'].replace('2 ', '2 bedroom')
    if unit_type['data-rooms'] == '3 ':
        return unit_type['data-rooms'].replace('3 ', '3 bedroom')
    if unit_type['data-rooms'] == '4 ':
        return unit_type['data-rooms'].replace('4 ', '4 bedroom')
    if unit_type['data-rooms'] == '5 ':
        return unit_type['data-rooms'].replace('5 ', '5 bedroom')
    if unit_type['data-rooms'] == '1+1':
        return unit_type['data-rooms'].replace('1+1 ', '1+1 bedroom')
    if unit_type['data-rooms'] == '2+1 ':
        return unit_type['data-rooms'].replace('2+1 ', '2+1 bedroom')
    if unit_type['data-rooms'] == '3+1 ':
        return unit_type['data-rooms'].replace('3+1 ', '3+1 bedroom')
    if unit_type['data-rooms'] == '4+1 ':
        return unit_type['data-rooms'].replace('4+1 ', '4+1 bedroom')


async def gather_list_for_sale_data(soup, main_data):
    result_data = []
    psf_min = None
    sales_listing = soup.find_all("div", {"class": "project-details-listing-container"})
    for listing in sales_listing:
        try:
            unit_type = listing.find("div", {"class": "listing-room-no"}).text.replace('\t', '').replace('\n',
                                                                                                         '') + 'bedroom'
        except AttributeError:
            unit_type = 'undetected'
        price_min = listing.find("div", {"class": "listing-price"}).text.replace('\t', '').replace('\n', '').replace(
            ' ', '').replace('$', '').replace(',', '')

        try:
            unit_link = 'https://www.srx.com.sg' + listing.find('a', class_='project-details-top-listing')['href']
        except Exception:
            unit_link = 'no link'

        if price_min == 'Viewtooffer':
            price_min = None

        size_and_psf = listing.find("div", {"class": "project-listings-size"}).text.replace('\t', '').replace('\n', '')
        if 'psf' in size_and_psf:
            min_size = size_and_psf.split('/')[0].replace('sqft', '').replace(',', '').replace(' ', '')
            psf_min = size_and_psf.split('/')[-1].replace('psf(Built)', '').replace(',', '').replace('$', '').replace(
                ' ',
                '')

        else:
            min_size = size_and_psf.replace(' ', '').replace('sqft', '').replace(',', '')

        if psf_min and price_min and unit_type:
            result_data.append(
                {"unit_type": unit_type, "psf_min": float(psf_min), "size_min": int(min_size),
                 "price_min": float(round(int(price_min) / 1000000, 1)), "district": main_data["district"],
                 'unit_link': unit_link})
        elif not psf_min and price_min and unit_type:
            result_data.append(
                {"unit_type": unit_type, "size_min": int(min_size), "district": main_data["district"],
                 "price_min": float(round(int(price_min) / 1000000, 1)), 'unit_link': unit_link})
        elif not price_min and psf_min and unit_type:
            result_data.append(
                {"unit_type": unit_type, "psf_min": float(psf_min), "size_min": int(min_size),
                 "district": main_data["district"], 'unit_link': unit_link})
        else:
            continue
    complete_response = await remove_duplicate_units(result_data)
    return complete_response


async def combine_units_data(units_data, list_for_sale_data, detail_for_sale_data):
    data = units_data + list_for_sale_data + detail_for_sale_data
    units = [dict(t) for t in {tuple(d.items()) for d in data}]
    response = await remove_duplicate_units(units)
    complete_response = await delete_untyped_units(response)

    return complete_response


async def delete_untyped_units(data):
    filtered_data = [item for item in data if item.get('unit_type') not in [None, 'undetected', '']]
    return filtered_data


async def remove_duplicate_units(list_of_units):
    seen = set()
    result = []

    for item in list_of_units:
        key = (item['unit_type'], item['size_min'])
        try:
            if key not in seen:
                result.append(item)
                seen.add(key)
        except KeyError:
            result.append(item)
            continue

    return result


async def gather_detail_for_sale_data(main_data):
    result_data = []
    for page in range(1, 11):
        listing_url = f'https://www.srx.com.sg/search/sale/condo/{main_data["name"].replace(" ", "+")}?page={page}'
        r = requests.get(listing_url, headers=Config.SRX_HEADERS)
        soup = BeautifulSoup(r.text, 'html.parser')

        min_psf = None

        sales_listing = soup.find_all("div", {"class": "listingContainer"})

        for listing in sales_listing:
            try:
                unit_link = 'https://www.srx.com.sg' + listing.find('a', class_='listingDetailsDivLink')['href']
            except Exception:
                unit_link = 'no link'

            if listing.find("span", {"class": "notranslate"}).text == main_data["name"]:
                try:
                    unit_type = listing.find("div", {"class": "listingDetailRoomNo"}).text.replace('\t', '').replace(
                        '\n',
                        '') + 'bedroom'
                except AttributeError:
                    unit_type = 'undetected'
                size_data = listing.find("div", {"class": "listingDetailValues"}).text.replace('\t', '').replace(
                    '\n',
                    '')
                if 'psf' in size_data:
                    min_size = size_data.split('/')[0].replace('sqft', '').replace(',', '').replace(' ', '')
                    min_psf = size_data.split('/')[-1].replace('psf(Built)', '').replace(',', '').replace('$',
                                                                                                          '').replace(
                        ' ', '')
                else:
                    min_size = size_data.replace(' ', '').replace('sqft', '').replace(',', '')
                min_price = listing.find("div", {"class": "listingDetailPrice"}).text.replace('\t', '').replace(
                    '\n',
                    '').replace(
                    ' ',
                    '').replace(
                    '$', '').replace(
                    ',', '')
                if min_price == 'Viewtooffer':
                    min_price = None
                try:
                    if min_price and min_psf:
                        result_data.append(
                            {"unit_type": unit_type, "psf_min": int(min_psf), "size_min": int(min_size),
                             "price_min": float(round(int(min_price) / 1000000, 1)), "district": main_data["district"],
                             'unit_link': unit_link})
                    elif not min_psf and not min_price:
                        result_data.append(
                            {"unit_type": unit_type, "size_min": int(min_size), "district": main_data["district"],
                             'unit_link': unit_link})
                    elif not min_psf and min_price:
                        result_data.append(
                            {"unit_type": unit_type, "size_min": int(min_size),
                             "price_min": float(round(int(min_price) / 1000000, 1)),
                             "district": main_data["district"], 'unit_link': unit_link})
                    else:
                        result_data.append(
                            {"unit_type": unit_type, "size_min": int(min_size), "psf_min": int(min_psf),
                             "district": main_data["district"], 'unit_link': unit_link})
                except ValueError:
                    pass
        else:
            continue

    complete_response = await remove_duplicate_units(result_data)
    return complete_response


async def handle_email_name(url):
    a = url.split('/')[-1].split('-')
    del a[-1]
    name = ' '.join(a)
    return name
