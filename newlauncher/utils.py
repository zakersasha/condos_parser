import json
import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from config import Config


async def get_detail_page_links():
    """Gather detail condos links."""
    detail_links = []
    for page in Config.NEW_LAUNCHER_PAGES.split(','):
        r = requests.get(f'https://newlauncher.com.sg/page={page}&sort_by=NA&', headers=Config.NEW_LAUNCHER_HEADERS)
        soup = BeautifulSoup(r.text, 'html.parser')

        for i in soup.find_all('a', class_='detailhref'):
            detail_links.append(i['href'])
    return detail_links


# PROJECT DETAILS
async def gather_project_details(soup, url):
    details_data = {}
    details_data['link_to_condo'] = url

    name = soup.find("h1").text
    if 'email' in name:
        name = await handle_email_name(url)

    details_data['name'] = name.replace('Pullman Residences', 'Pullman Residence')

    project_details = soup.find("div", {"id": "section-2"}).find_all("p")

    try:
        brochure = soup.find("a", class_='btn btn-sm btn-outline-primary d-inline-flex align-items-center')['href']
        details_data['brochure'] = [{"url": brochure.replace(' ', '%20')}]
    except TypeError:
        details_data['brochure'] = None

    items = [item.text.replace('\n', '') for item in project_details]

    developers_data = str([x for x in project_details[-1].text.split('\n') if x]) \
        .replace('[', '') \
        .replace(']', '') \
        .replace("'", "")
    cleaned_items = developers_data.strip().split(',')
    cleaned_data = [item.strip() for item in cleaned_items if item.strip()]
    developers = ', '.join(cleaned_data)

    await gather_project_details_block(details_data, items, developers)

    gallery_links = []
    gallery = soup.find("div", {"id": "media-thumbnails-gallery"}).find_all("img")
    for image in gallery:
        try:
            gallery_links.append({"url": image['src'].replace(' ', '%20')})
        except KeyError:
            gallery_links.append({"url": image['data-cfsrc'].replace(' ', '%20')})
    details_data['images'] = gallery_links

    return details_data


async def gather_project_details_block(details_data, content, developers):
    details_data['address'] = content[0]
    details_data['district'] = content[1].split(' ')[0]
    details_data['previewing_start_date'] = await str_to_datetime(content[6])
    details_data['type'] = content[4]
    details_data['date_of_completion'] = await str_to_datetime(content[7])
    details_data['tenure'] = await remove_extra_white_spaces(content[8])
    details_data['units_number'] = int(content[5].split(' ')[0])
    details_data['units_size'] = content[5].split(' ')[3]
    details_data['architect'] = content[10]
    details_data['developer'] = developers

    return details_data


async def remove_extra_white_spaces(text):
    return re.sub(r'\s+', ' ', text.replace('99 YearsLeasehold ', '99 YearsLeasehold')).strip()


# PROJECT FACILITIES
async def gather_project_facilities(soup):
    facilities_data = {}
    project_facilities = []
    facilities = soup.find("div", {"class": "table-transparent text_info_list"}).find("div", {
        "class": 'swiper-wrapper'})
    for facility in facilities.find_all('li'):
        project_facilities.append(facility.text)

    site_plans_attachments = []
    attachments = soup.find("div", {"id": "site-plan-gallery"}).find_all("a")
    for attachment in attachments:
        site_plans_attachments.append({"url": attachment['href'].replace(' ', '%20')})

    facilities_data['facilities'] = project_facilities
    facilities_data['site_plans_attachments'] = site_plans_attachments

    return facilities_data


# PROJECT AMENITIES
async def gather_project_amenities(soup):
    project_amenities = []
    location_map_attachments = []

    try:
        lm_attachments = soup.find("div", {"id": "location-map-gallery"}).find_all("a")
        for attachment in lm_attachments:
            location_map_attachments.append({"url": attachment['href'].replace(' ', '%20')})
    except AttributeError:
        pass

    amenities = soup.find("div", {"id": "section-4"}).find("tbody")
    for amenity in amenities.find_all('tr'):
        data = amenity.text.replace('\n', ' ').split("  ")
        if len(data) == 3:
            try:
                amenities_data = {'amenities_type': data[0].replace(' ', ''),
                                  'amenities_name': data[1],
                                  'distance': float(data[2].split(' ')[0])}
            except ValueError:
                amenities_data = {'amenities_type': data[0].replace(' ', ''),
                                  'amenities_name': data[1],
                                  'distance': float(data[2].split('km')[0])}

        else:
            try:
                amenities_data = {'amenities_type': data[0].replace(' ', ''),
                                  'amenities_name': (data[1] + ' ' + data[2])[1:],
                                  'distance': float(amenity.text.replace('\n', ' ').split("  ")[-1].split(' ')[0])}
            except ValueError:
                amenities_data = {'amenities_type': data[0].replace(' ', ''),
                                  'amenities_name': (data[1] + ' ' + data[2])[1:],
                                  'distance': float(amenity.text.replace('\n', ' ').split("  ")[-1].split('km')[0])}
        project_amenities.append(amenities_data)

    return project_amenities, location_map_attachments


# PROJECT UNITS
async def gather_project_units(soup, details):
    project_units = []
    units = soup.find("div", {"id": "section-5"}).find("tbody")

    units_site_plans = []
    attachments = soup.find("div", {"id": "unit-mix-gallery"}).find_all("a")
    for attachment in attachments:
        units_site_plans.append({"url": attachment['href'].replace(' ', '%20')})

    for unit in units.find_all('tr'):
        unit_data = [ele for ele in unit.text.replace('\n', '  ').replace('   ', '  ').split('  ') if ele.strip()]

        if unit_data[0] == 'Overall':
            continue

        if '-' not in unit_data[2]:
            result_data = {'unit_type': unit_data[0],
                           'all_units': int(unit_data[1]),
                           'size_min': int(unit_data[2].replace(',', '')),
                           'units_site_plans': units_site_plans,
                           'district': details['district'],
                           'date_of_completion': details['date_of_completion']
                           }
        else:
            result_data = {'unit_type': unit_data[0],
                           'all_units': int(unit_data[1]),
                           'size_min': int(unit_data[2].split('-')[0].replace(' ', '').replace(',', '')),
                           'size_max': int(unit_data[2].split('-')[1].replace(' ', '').replace(',', '')),
                           'units_site_plans': units_site_plans,
                           'district': details['district'],
                           'date_of_completion': details['date_of_completion']
                           }
        project_units.append(result_data)
    return project_units


# PROJECT BALANCES
async def gather_project_balances(soup):
    project_balances = []
    project_overall = {}
    balances = soup.find("div", {"id": "section-6"}).find("tbody")
    for balance in balances.find_all('tr'):
        balance_data = [ele for ele in balance.text.replace('\n', '  ').replace('   ', '  ').split('  ') if ele.strip()]

        if balance_data[0] == 'Overall':
            project_overall['overall_available_units'] = int(balance_data[1])
            if '-' in balance_data[2]:
                project_overall['overall_min_unit_size'] = int(
                    balance_data[2].split('-')[0].replace(',', '').replace(' ', ''))
                project_overall['overall_max_unit_size'] = int(
                    balance_data[2].split('-')[1].replace(',', '').replace(' ', ''))
            else:
                project_overall['overall_min_unit_size'] = int(balance_data[2].replace(',', '').replace(' ', ''))

            if '-' in balance_data[3]:
                project_overall['overall_min_unit_psf'] = int(
                    balance_data[3].split('-')[0].replace(',', '').replace('$', '').replace(' ', ''))
                project_overall['overall_max_unit_psf'] = int(
                    balance_data[3].split('-')[1].replace(',', '').replace('$', '').replace(' ', ''))
            else:
                project_overall['overall_min_unit_psf'] = int(
                    balance_data[3].replace(',', '').replace('$', '').replace(' ', ''))

            if '-' in balance_data[4]:
                project_overall['overall_min_unit_price'] = float(
                    balance_data[4].split('-')[0].replace('$', '').replace('M', '').replace(' ', ''))
                project_overall['overall_max_unit_price'] = float(
                    balance_data[4].split('-')[1].replace('$', '').replace('M', '').replace(' ', ''))
            else:
                project_overall['overall_min_unit_price'] = float(
                    balance_data[4].replace('$', '').replace('M', '').replace(' ', ''))

        if not '-' in balance_data[4]:
            if not '-' in balance_data[3]:
                result_data = {'unit_type': balance_data[0].replace('Bedrom', 'Bedroom'),
                               'available_units': int(balance_data[1]),
                               'psf_min': float(balance_data[3].replace('$', '').replace(',', '.').replace(
                                   'To Be Confirmed', '0')),
                               'price_min': float(
                                   balance_data[4].replace('$', '').replace('M', '').replace(' ', '').replace(
                                       'ToBeConfirmed', '0')),
                               }
            else:
                result_data = {'unit_type': balance_data[0].replace('Bedrom', 'Bedroom'),
                               'available_units': int(balance_data[1]),
                               'psf_min': float(
                                   balance_data[3].split('-')[0].replace('$', '').replace(',', '.').replace(
                                       'To Be Confirmed', '0')),
                               'psf_max': float(balance_data[3].split('-')[1].replace('$', '').replace(',', '.')),
                               'price_min': float(
                                   balance_data[4].replace('$', '').replace('M', '').replace(' ', '').replace(
                                       'ToBeConfirmed', '0'))
                               }

        else:
            if not '-' in balance_data[3]:
                result_data = {'unit_type': balance_data[0].replace('Bedrom', 'Bedroom'),
                               'available_units': int(balance_data[1]),
                               'psf_min': float(balance_data[3].replace('$', '').replace(',', '.')),
                               'price_min': float(
                                   balance_data[4].split('-')[0].replace('$', '').replace('M', '').replace(' ', '')),
                               'price_max': float(
                                   balance_data[4].split('-')[1].replace('$', '').replace('M', '').replace(' ', '')),
                               }
            else:
                result_data = {'unit_type': balance_data[0].replace('Bedrom', 'Bedroom'),
                               'available_units': int(balance_data[1]),
                               'psf_min': float(balance_data[3].split('-')[0].replace('$', '').replace(',', '.')),
                               'psf_max': float(balance_data[3].split('-')[1].replace('$', '').replace(',', '.')),
                               'price_min': float(
                                   balance_data[4].split('-')[0].replace('$', '').replace('M', '').replace(' ', '')),
                               'price_max': float(
                                   balance_data[4].split('-')[1].replace('$', '').replace('M', '').replace(' ', '')),
                               }
        project_balances.append(result_data)
    return project_balances, project_overall


# FLOOR PLANS
async def gather_floor_plans(soup):
    floor_plans = soup.find("div", {"id": "section-7"}).find("tbody")
    try:
        script_data = floor_plans.find_all('script')[1].string.replace(';', '')
        floor_plans_json = json.loads(script_data.split('var unit_fp = ')[1])
    except IndexError:
        script_data = floor_plans.find_all('script')[2].string.replace(';', '')
        floor_plans_json = json.loads(script_data.split('var unit_fp = ')[1])

    return floor_plans_json


# DATA MERGING
async def merge_gathered_data(details, facilities, attachments, overall):
    complete_response = {**details, **facilities, **overall,
                         'location_map_attachments': attachments,
                         }

    return complete_response


async def merge_units_and_balances(units, balances):
    for unit in units:
        unit_type = unit['unit_type']
        unit['floor_plans'] = []

        for balance in balances:
            if balance['unit_type'] == unit_type:
                unit.update(balance)

    return units


async def merge_units_and_floor_plans(units, plans):
    for plan in plans:
        unit = next((item for item in units if item["unit_type"] == plan['unit_name']), None)
        if unit:
            unit['floor_plans'].append(plan['unit_type'])
    return units


async def str_to_datetime(date):
    try:
        date = date.replace(' or earlier', '')
        clean_date = ' '.join(date.split())
        res_date = datetime.strptime(clean_date.replace('th', '').replace('Nov', 'November') \
                                     .replace('Dec', 'December') \
                                     .replace('Jan', 'January') \
                                     .replace('Feb', 'February') \
                                     .replace('Mar', 'March') \
                                     .replace('Apr', 'April') \
                                     .replace('Sep', 'September') \
                                     .replace('Oct', 'October') \
                                     .replace('Jun', 'June') \
                                     .replace('Jul', 'July') \
                                     .replace('Aug', 'August') \
                                     .replace('1st', '1') \
                                     .replace('nd', '') \
                                     .replace('rd', ''), '%d %B %Y')
        return res_date.strftime('%Y-%m-%d')
    except ValueError:
        date = date[:-1].replace(' or earlier', '')
        clean_date = ' '.join(date.split())
        res_date = datetime.strptime(clean_date.replace('th', '').replace('Nov', 'November') \
                                     .replace('Dec', 'December') \
                                     .replace('Jan', 'January') \
                                     .replace('Feb', 'February') \
                                     .replace('Mar', 'March') \
                                     .replace('Apr', 'April') \
                                     .replace('Sep', 'September') \
                                     .replace('Oct', 'October') \
                                     .replace('Jun', 'June') \
                                     .replace('Jul', 'July') \
                                     .replace('Aug', 'August') \
                                     .replace('1st', '1') \
                                     .replace('nd', '') \
                                     .replace('rd', ''), '%d %B %Y')

        return res_date.strftime('%Y-%m-%d')


async def handle_email_name(url):
    name = url.split('/')[-1].replace('-', ' ')
    return name
