import requests
from aiohttp import ContentTypeError
from bs4 import BeautifulSoup, NavigableString, Tag

from config import Config


async def get_detail_page_links():
    """Gather detail condos links."""
    detail_links = []
    for page in Config.NEW_LAUNCHER_PAGES.split(','):
        r = requests.get(f'https://newlauncher.com.sg/page={page}&sort_by=NA&')
        soup = BeautifulSoup(r.text, 'html.parser')

        for i in soup.find_all('a', class_='detailhref'):
            detail_links.append(i['href'])
    return detail_links


# PROJECT DETAILS
async def gather_project_details(soup, url):
    details_data = {}
    name = soup.find("h1").text
    details_data['name'] = name
    details_data['link_to_condo'] = url

    project_details = soup.find("div", {"id": "section-2"})
    for e in soup.findAll('br'):
        e.extract()

    try:
        brochure = soup.find("a", class_='btn btn-sm btn-outline-primary d-inline-flex align-items-center')['href']
        details_data['link_to_brochure'] = brochure
    except TypeError:
        details_data['link_to_brochure'] = None

    for detail in project_details:
        if isinstance(detail, NavigableString):
            continue
        if isinstance(detail, Tag):
            content = detail.find_all("p")
            if content:
                items = [item.text.replace('\n', '') for item in content]
                developers = [x for x in content[-1].text.split('\n') if x]

                await gather_project_details_block(details_data, items, developers)

    gallery_links = []
    gallery = soup.find("div", {"id": "media-thumbnails-gallery"}).find_all("img")
    for image in gallery:
        gallery_links.append(image['src'].replace(' ', '%20'))
    details_data['images'] = gallery_links

    return details_data


async def gather_project_details_block(details_data, content, developers):
    details_data['address'] = content[0]
    details_data['district'] = content[1].split(' ')[0]
    details_data['previewing_start_date'] = content[6]
    details_data['date_of_completion'] = content[7].replace(' or earlier', '')
    details_data['tenure'] = content[8]
    details_data['type'] = content[5].split(' ')[0]
    details_data['units_size'] = content[5].split(' ')[3]
    details_data['architect'] = content[10]
    details_data['developer'] = developers

    return details_data


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
        site_plans_attachments.append(attachment['href'].replace(' ', '%20'))

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
            location_map_attachments.append(attachment['href'].replace(' ', '%20'))
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
async def gather_project_units(soup):
    project_units = []
    units = soup.find("div", {"id": "section-5"}).find("tbody")
    for unit in units.find_all('tr'):
        unit_data = [ele for ele in unit.text.replace('\n', '  ').replace('   ', '  ').split('  ') if ele.strip()]

        if unit_data[0] == 'Overall':
            continue

        if '-' not in unit_data[2]:
            result_data = {'unit_type': unit_data[0],
                           'all_units': int(unit_data[1]),
                           'size_max': int(unit_data[2].replace(',', '')),
                           }
        else:
            result_data = {'unit_type': unit_data[0],
                           'all_units': int(unit_data[1]),
                           'size_min': int(unit_data[2].split('-')[0].replace(' ', '').replace(',', '')),
                           'size_max': int(unit_data[2].split('-')[1].replace(' ', '').replace(',', '')),
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
                result_data = {'unit_type': balance_data[0],
                               'available_units': int(balance_data[1]),
                               'psf_min': float(balance_data[3].replace('$', '').replace(',', '.')),
                               'price_min': float(balance_data[4].replace('$', '').replace('M', '').replace(' ', '')),
                               }
            else:
                result_data = {'unit_type': balance_data[0],
                               'available_units': int(balance_data[1]),
                               'psf_min': float(balance_data[3].split('-')[0].replace('$', '').replace(',', '.')),
                               'psf_max': float(balance_data[3].split('-')[1].replace('$', '').replace(',', '.')),
                               'price_min': float(balance_data[4].replace('$', '').replace('M', '').replace(' ', ''))
                               }

        else:
            if not '-' in balance_data[3]:
                result_data = {'unit_type': balance_data[0],
                               'available_units': int(balance_data[1]),
                               'psf_min': float(balance_data[3].replace('$', '').replace(',', '.')),
                               'price_min': float(
                                   balance_data[4].split('-')[0].replace('$', '').replace('M', '').replace(' ', '')),
                               'price_max': float(
                                   balance_data[4].split('-')[1].replace('$', '').replace('M', '').replace(' ', '')),
                               }
            else:
                result_data = {'unit_type': balance_data[0],
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
    project_floor_plans = {}
    floor_plans = soup.find("div", {"id": "section-7"}).find("tbody")
    for floor_plan in floor_plans.find_all('tr'):
        floor_plan_data = [ele for ele in floor_plan.text.replace('\n', '  ').replace('   ', '  ').split('  ') if
                           ele.strip()]

        if floor_plan_data[0] in project_floor_plans:
            project_floor_plans[floor_plan_data[0]].append(floor_plan_data[1].replace(' ', ''))
        else:
            project_floor_plans[floor_plan_data[0]] = [floor_plan_data[1].replace(' ', '')]

    return project_floor_plans


# DATA MERGING
async def merge_gathered_data(details, facilities, amenities, attachments, overall, units):
    complete_response = {**details, **facilities, **overall,
                         'amenities': amenities,
                         'location_map_attachments': attachments,
                         'units': units}

    return complete_response


async def merge_units_and_balances(units, balances):
    for unit in units:
        unit_type = unit['unit_type']
        try:
            res_dict = next(item for item in balances if item["unit_type"] == unit_type)
        except StopIteration:
            continue
        unit.update(res_dict)
    return units


async def merge_units_and_floor_plans(units, plans):
    for unit in units:
        floor_plans = plans.get(unit['unit_type'])
        if not floor_plans:
            continue
        unit['floor_plans'] = floor_plans

    return units
