import requests
from bs4 import BeautifulSoup

from onthemarket.config import headers


def gather_condos_list(url):
    response = []

    for i in range(0, 2):
        projects_link = f'{url}?page={i}'

        r = requests.get(projects_link, headers=headers)
        soup = BeautifulSoup(r.text, 'html.parser')

        condos_list = soup.find_all('div', class_='main-image property-image')
        for condo in condos_list:
            link = 'https://www.onthemarket.com' + condo.find('a')['href']
            response.append(link)

    return list(set(response))


def parse_page(projects_link, city):
    r = requests.get(projects_link, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    name = soup.find('div', class_='heading').find('h1').get_text(strip=True)
    address = soup.find('div', class_='heading').find('h3').get_text(strip=True)
    link_to_condo = projects_link

    brochure_data = soup.find('div', class_='description').find_all('a')
    gathered_links = []
    for href in brochure_data:
        gathered_links.append(href['href'])
    try:
        brochure = [s for s in gathered_links if s.endswith(".pdf")][0]
    except IndexError:
        brochure = None

    parts = link_to_condo.split('/')
    condo_id = ''.join(parts[-2:])

    description = soup.find('div', class_='description').get_text(strip=True).replace('Request details',
                                                                                      '').replace(
        'About the Development', '')
    city = city

    return soup, {
        'name': name,
        'address': address,
        'link_to_condo': link_to_condo,
        'Condo ID': condo_id,
        'city': city,
        'description': description,
        'brochure': brochure,
    }


def parse_units_links(soup):
    """Получил списки всех юнитов"""
    units_page_link = 'https://www.onthemarket.com' + soup.find('a', class_='more')['href']
    condos_links = []
    try:
        for i in range(0, 10):
            r = requests.get(f'{units_page_link}&page={i}', headers=headers)
            b_soup = BeautifulSoup(r.text, 'html.parser')

            condos_data = b_soup.find('ul', class_='grid-list-tabcontent').find_all('li')
            for item in condos_data:
                condo_link = item.find('a')
                if condo_link:
                    condos_links.append('https://www.onthemarket.com' + condo_link['href'])
        return condos_links
    except AttributeError:
        return condos_links, len(condos_links)


def gather_unit_data(units_links, name):
    all_prices = []
    available_units_data = {}
    units_data = []
    tenure = None
    facilities = ''

    for link in units_links:
        print(f'u_link {link}')
        r = requests.get(link, headers=headers)
        soup = BeautifulSoup(r.text, 'html.parser')
        property_data = soup.find_all('li', class_='otm-ListItemOtmBullet before:bg-denim')
        if not tenure:
            try:
                tenure = property_data[0].get_text(strip=True).replace('Tenure: ', '')
                if 'Leasehold' in tenure:
                    tenure = 'Leasehold'
                elif 'Freehold' in tenure:
                    tenure = 'Freehold'
                else:
                    tenure = None
            except IndexError:
                tenure = None

        for prop in property_data[1:]:
            facilities += f'{prop.get_text(strip=True)}\n'

        price = float(soup.find('span', class_='mb-0 text-lg font-bold text-denim price'). \
                      get_text(strip=True).replace(',', '').replace('£', '').replace('POA', '0'))
        all_prices.append(price)

        unit_type = soup.find('h1', class_='h4').get_text(strip=True).replace(' for sale', '')
        if unit_type not in available_units_data:
            available_units_data[unit_type] = 1
        else:
            available_units_data[unit_type] += 1

        num_bedrooms = int(soup.find('div', class_='flex items-center mr-6 py-0.5').get_text(strip=True).split(' ')[0])

        r2 = requests.get(link + '#/floorplans/1', headers=headers)
        l_soup = BeautifulSoup(r2.text, 'html.parser')
        floor_plan_image_links = l_soup.find('img')['src'].replace('image', 'floor-plan')

        units_data.append({
            "unit_type": unit_type,
            "price_min": price,
            "price_max": price,
            "floor_plan_image_links": floor_plan_image_links,
            "num_bedrooms": num_bedrooms,
            "general_name": name
        })
    for unit in units_data:
        unit['available_units'] = available_units_data[unit['unit_type']]
    try:
        overall_min_unit_price = min(all_prices)
    except ValueError:
        overall_min_unit_price = None
    try:
        overall_max_unit_price = max(all_prices)
    except ValueError:
        overall_max_unit_price = None
    facility = facilities.splitlines()
    for i in facility:
        i.replace('•\t', '')

    return units_data, tenure, overall_min_unit_price, overall_max_unit_price, facility
