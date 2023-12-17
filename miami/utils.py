import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from miami.config import headers


def gather_condos_list():
    response = []
    projects_link = 'https://miamiresidential.com/new-developments/'

    r = requests.get(projects_link, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    condos_list = soup.find('div', class_='row').find_all('a')

    for condo in condos_list:
        if 'https' in condo['href']:
            response.append(condo['href'])
    return list(set(response))


def parse_page(link):
    r = requests.get(link, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    name = soup.find('div', class_='page-title').get_text(strip=True)  # str

    address = soup.find('div', class_='box-building-info').find_all('b')[-1].get_text(strip=True)  # str

    link_to_condo = link  # str

    district = soup.find('div', class_='box-building-info').find_all('b')[0].get_text(strip=True).split('/')[
        0]

    date_data = soup.find('div', class_='box-building-info').find_all('b')[0].get_text(strip=True).split('/')[
        1]
    date_obj = datetime.strptime(date_data, '%Y')
    date_of_completion = date_obj.strftime('1.1.%Y')  # str

    condo_id = int(soup.find('link', rel='shortlink')['href'].split('=')[1])  # int

    try:
        description = soup.find('div', class_='bldg-description').find_all('p')[1].get_text(strip=True)  # str
    except IndexError:
        description = soup.find('div', class_='bldg-description').find_all('p')[0].get_text(strip=True)  # str
    city = 'Miami'  # str

    pp_data = soup.find('div', class_='general-information-container').find('div', class_='info1').find_all('div')[
        -1].get_text()
    if 'Payment Plan' in pp_data:
        lines = pp_data.strip().split('\n')
        if lines and lines[0] == "Payment Plan":
            lines.pop(0)
        cleaned_lines = [line.strip() for line in lines]
        payment_plans = '\n'.join(cleaned_lines)  # str
    else:
        payment_plans = None

    facilities = ''  # str
    facilities_data = soup.find('div', class_='building-features').find_all('li')
    for facility in facilities_data:
        facilities += f'{facility.get_text(strip=True)}\n'

    features = ''  # str
    features_data = soup.find('div', class_='residence-features').find_all('li')
    for feature in features_data:
        features += f'{feature.get_text(strip=True)}\n'

    try:
        units_table = soup.find('div', class_='keyplan-table').find_all('tr')
    except AttributeError:
        units_table = None
    if units_table:
        all_sizes = []
        for row in units_table:
            try:
                temp_data = row.find_all('td')[2].get_text(strip=True)
                if '/' in temp_data:
                    try:
                        all_sizes.append(float(
                            temp_data.split(' ')[0].replace(',', '').replace('SF', '').replace('+', '').replace('sq',
                                                                                                                '')))
                    except ValueError:
                        all_sizes.append(
                            float(temp_data.split(' ')[0].replace(',', '').replace('SF', '').replace('sq', '').replace(
                                '+', '').split('-')[0]))
                        all_sizes.append(
                            float(temp_data.split(' ')[0].replace(',', '').replace('SF', '').replace('sq', '').replace(
                                '+', '').split('-')[1]))
            except IndexError:
                continue
        if len(all_sizes) > 0:
            overall_min_unit_size = min(all_sizes)  # float
            overall_max_unit_size = max(all_sizes)  # float

            price_data = soup.find('div', class_='mt-15').get_text(strip=True)
            price_match = re.search(r'\$\d+(,\d+)*', price_data)

            if price_match:
                overall_min_unit_price = float(price_match.group().replace('$', '').replace(',', ''))  # float
            else:
                overall_min_unit_price = None

            try:
                overall_min_unit_psf = round(overall_min_unit_price / overall_min_unit_size, 1)  # float
            except Exception:
                overall_min_unit_psf = None
        else:
            overall_min_unit_size = None
            overall_max_unit_size = None
            overall_min_unit_price = None
            overall_min_unit_psf = None
    else:
        overall_min_unit_size = None
        overall_max_unit_size = None
        overall_min_unit_price = None
        overall_min_unit_psf = None

    response_data = {
        'name': name,
        'address': address,
        'link_to_condo': link_to_condo,
        'district': district,
        'date_of_completion': date_of_completion,
        'Condo ID': condo_id,
        'city': city,
        'description': description,
        'facilities': facilities.splitlines(),
        'payment_plans': payment_plans,
        'overall_min_unit_size': overall_min_unit_size,
        'overall_max_unit_size': overall_max_unit_size,
        'overall_min_unit_price': overall_min_unit_price,
        'overall_min_unit_psf': overall_min_unit_psf,
        'features': features,
    }

    return response_data, soup


def parse_units_data(soup, name):
    units = []
    try:
        floor_plan_image_links = soup.find('div', class_='keyfloorplan-wrapper').find('img')['src']
        if 'https://miamiresidential.com' not in floor_plan_image_links:
            floor_plan_image_links = 'https://miamiresidential.com' + floor_plan_image_links
    except AttributeError:
        floor_plan_image_links = None
    general_name = name

    try:
        units_table = soup.find('div', class_='keyplan-table').find('tbody').find_all('tr')
    except AttributeError:
        return None

    for unit in units_table:
        try:
            unit_type = unit.find_all('td')[0].get_text(strip=True)
        except IndexError:
            return None
        try:
            size_min = float(
                unit.find_all('td')[2].get_text(strip=True).split(' ')[0].replace(',', '').replace('SF', '').replace(
                    '+', '').replace(
                    'sq', ''))
            size_max = size_min
        except ValueError:
            try:
                size_min = float(
                    unit.find_all('td')[2].get_text(strip=True).split(' ')[0].replace(',', '').replace('SF',
                                                                                                       '').replace(
                        '+', '').replace(
                        'sq', '').split('-')[0])
                size_max = float(
                    unit.find_all('td')[2].get_text(strip=True).split(' ')[0].replace(',', '').replace('SF',
                                                                                                       '').replace(
                        '+', '').replace(
                        'sq', '').split('-')[1])
            except ValueError:
                size_max = float(0)
                size_min = float(0)
        except IndexError:
            continue
        num_bedrooms = unit.find_all('td')[1].get_text(strip=True).split(' ')[0]
        if num_bedrooms == '':
            num_bedrooms = 0

        units.append({'unit_type': unit_type, 'size_min': size_min, 'size_max': size_max, 'num_bedrooms': num_bedrooms,
                      'floor_plan_image_links': floor_plan_image_links, 'general_name': general_name})

    return units
