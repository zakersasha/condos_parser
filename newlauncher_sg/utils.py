from datetime import datetime

import requests

from bs4 import BeautifulSoup

from config import Config


def gather_projects_links():
    project_links = []
    r = requests.get(f'https://www.newlaunches.sg/condominium/browse.html', headers=Config.NEW_LAUNCHER_HEADERS)
    soup = BeautifulSoup(r.text, 'html.parser')

    project_cards = soup.find_all('h2', class_='h6 m-0 p-2 text-nowrap overflow-hidden')

    for card in project_cards:
        project_links.append(card.find('a')['href'])

    return project_links


def gather_main_data(link):
    headers = {
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                      'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                      'scale=3.00; 1170x2532; 463736449) NW/3'}
    r = requests.get(link, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    table = soup.find('table', class_='table table-striped table-hover mt-3')
    data_dict = {}
    for row in table.find_all('tr'):
        key = row.find('th').get_text(strip=True)
        value = row.find('td').get_text(strip=True)

        data_dict[key] = value
    data_dict['page_link'] = link

    return data_dict, soup


def extract_main_data(soup, data):
    result_data = {}
    result_data['name'] = data['Project Name:']
    result_data['link_to_condo'] = data['page_link']

    # Extract images
    images = []
    images_data = soup.find('div', {"id": "project_photos_scroll"}).find_all('img')
    for image in images_data:
        image_src = image['data-src']
        images.append(image_src)
    result_data['images_urls'] = images

    # Extract Floor plans
    try:
        floor_plans = []
        floor_plans_data = soup.find('section', {'id': 'project_floorplans'}).find('div',
                                                                                   class_='row justify-content-center mt-3 view_project_media_items_container').find_all(
            'a')
        for plan in floor_plans_data:
            plan_href = plan['href']
            floor_plans.append(plan_href)
        result_data['floor_plans_urls'] = floor_plans
    except AttributeError:
        pass

    # Extract Site Plans
    try:
        site_plans = []
        site_plans_data = soup.find('div', class_='image_hover_overlay_container text-center shadow-lg').find('a')[
            'href']
        site_plans.append(site_plans_data)
        result_data['site_plans_urls'] = site_plans
    except AttributeError:
        pass

    if 'Type:' in data:
        result_data['type'] = data['Type:']
    if 'Developer:' in data:
        result_data['developer'] = data['Developer:']
    if 'Architect:' in data:
        result_data['architect'] = data['Architect:']
    if 'Address:' in data:
        result_data['address'] = data['Address:']
    if 'District:' in data:
        result_data['district'] = data['District:']
    if 'Total Units:' in data:
        result_data['units_number'] = float(data['Total Units:'].replace('Units', '').replace(' ', ''))
    if 'T.O.P Date:' in data:
        if 'Q' in data['T.O.P Date:']:
            date_string = data['T.O.P Date:'].split(' ')[1]
            date_obj = datetime.strptime(date_string, '%Y')
            formatted_date = date_obj.strftime('1.1.%Y')
        elif 'Completed' in data['T.O.P Date:']:
            date_string = data['T.O.P Date:'].split(' ')[0]
            date_obj = datetime.strptime(date_string, '%Y')
            formatted_date = date_obj.strftime('1.1.%Y')
        else:
            date_obj = datetime.strptime(data['T.O.P Date:'], '%b %Y')
            formatted_date = date_obj.strftime('1.%d.%Y')
        result_data['date_of_completion'] = formatted_date
    if 'Tenure:' in data:
        result_data['tenure'] = data['Tenure:']

    return result_data


def gather_amenities_data(soup):
    amenities = []
    amenities_names = soup.find('div', class_='row mt-3').find_all('p')
    for amenity in amenities_names:
        amenity_data = amenity.get_text(strip=True).replace('Boys School', '').replace('Girls School', '').replace(
            'Mixed School', '')
        if 'Search' in amenity_data:
            continue
        amenities_name = amenity_data.split('(Approx')[0]
        distance_data = amenity_data.split('(Approx')[-1].replace(' ', '').replace(')', '')
        if 'km' in distance_data:
            distance = float(distance_data.replace('km', ''))
        else:
            distance = round((float(distance_data.replace('m', '')) / 1000), 2)
        amenities.append({'amenities_name': amenities_name, 'distance': distance})
    return amenities


def gather_units_data(soup):
    units = []
    table = soup.find('section', {'id': 'project_unit_types'})
    if not table:
        return 'skip'
    try:
        units_data = table.find('table').find_all('tr')
    except AttributeError:
        return 'skip'
    for unit in units_data[1:]:
        unit_data = unit.find_all('td')

        unit_detail = {}

        unit_type = unit_data[0].get_text(strip=True)
        unit_detail['unit_type'] = unit_type

        unit_size = unit_data[1].get_text(strip=True)
        if '-' in unit_size:
            size_min = float(unit_size.split('sqft')[0].replace(' ', '').split('-')[0].replace(',', ''))
            size_max = float(unit_size.split('sqft')[0].replace(' ', '').split('-')[1].replace(',', ''))
        elif 'to be released' in unit_size:
            size_min = None
            size_max = None
        else:
            size_min = float(unit_size.split('sqft')[0].replace(' ', '').replace(',', ''))
            size_max = None
        unit_detail['size_min'] = size_min
        unit_detail['size_max'] = size_max

        unit_price = unit_data[2].get_text(strip=True)
        if 'to be released' in unit_price:
            price_min = None
            price_max = None
        else:
            if 'K' in unit_price:
                price_min = round(float('0.' + unit_price.split('$')[1].replace('K', '')), 2)
                price_max = None
            else:
                price_min = round(float(unit_price.split('$')[1].replace('M', '')), 2)
                price_max = None
        unit_detail['price_min'] = price_min
        unit_detail['price_max'] = price_max

        unit_psf = unit_data[3].get_text(strip=True)
        if 'to be released' in unit_psf:
            psf_min = None
            psf_max = None
        else:
            psf_min = round(float(unit_psf.split('$')[1].replace('psf', '').replace(',', '.')), 2)
            psf_max = None
        unit_detail['psf_min'] = psf_min
        unit_detail['psf_max'] = psf_max

        unit_availability_data = unit_data[4].get_text(strip=True)
        if 'to be released' in unit_availability_data:
            unit_availability = None
            all_units = None
        else:
            unit_availability = float(unit_availability_data.split('/')[0].replace(' ', ''))
            all_units = float(unit_availability_data.split('/')[1].replace(' ', '').replace('Units', ''))
        unit_detail['available_units'] = unit_availability
        unit_detail['all_units'] = all_units

        units.append(unit_detail)

    return units
