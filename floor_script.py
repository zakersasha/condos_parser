# TODO 1) get units with general == condo_name 2) for item in 1 upd field floor_plans_urls where unit_type == item['unit_type']
import requests
from bs4 import BeautifulSoup

links = [
         'https://www.newlaunches.sg/condominium/kovan-jewel.html',
         'https://www.newlaunches.sg/condominium/zyanya.html', 'https://www.newlaunches.sg/condominium/10-evelyn.html',
         'https://www.newlaunches.sg/condominium/jervois-prive.html',
         'https://www.newlaunches.sg/mixed-development/the-m.html',
         'https://www.newlaunches.sg/condominium/the-lumos.html',
         'https://www.newlaunches.sg/condominium/bishopsgate-residences.html',
         'https://www.newlaunches.sg/mixed-development/v-on-shenton.html',
         'https://www.newlaunches.sg/condominium/amber-skye.html',
         'https://www.newlaunches.sg/condominium/dalvey-haus.html',
         'https://www.newlaunches.sg/condominium/atlassia.html',
         'https://www.newlaunches.sg/condominium/jervois-treasures.html',
         'https://www.newlaunches.sg/condominium/concourse-skyline.html',
         'https://www.newlaunches.sg/condominium/26-newton.html',
         'https://www.newlaunches.sg/condominium/the-ritz-carlton.html',
         'https://www.newlaunches.sg/condominium/corals-at-keppel-bay.html',
         'https://www.newlaunches.sg/condominium/sky-everton.html',
         'https://www.newlaunches.sg/condominium/the-line-at-tanjong-rhu.html',
         'https://www.newlaunches.sg/condominium/sanctuary-at-newton.html',
         'https://www.newlaunches.sg/condominium/lloyd-sixtyfive.html',
         'https://www.newlaunches.sg/condominium/19-nassim.html',
         'https://www.newlaunches.sg/condominium/park-nova.html',
         'https://www.newlaunches.sg/condominium/one-pearl-bank.html',
         'https://www.newlaunches.sg/condominium/liv-at-mb.html',
         'https://www.newlaunches.sg/condominium/parc-clematis.html',
         'https://www.newlaunches.sg/condominium/neu-at-novena.html',
         'https://www.newlaunches.sg/mixed-development/1953.html',
         'https://www.newlaunches.sg/condominium/treasure-at-tampines.html',
         'https://www.newlaunches.sg/executive-condominium/ola.html',
         'https://www.newlaunches.sg/condominium/the-watergardens-at-canberra.html',
         'https://www.newlaunches.sg/condominium/dunearn-386.html',
         'https://www.newlaunches.sg/condominium/haus-on-handy.html',
         'https://www.newlaunches.sg/condominium/park-1-suites.html',
         'https://www.newlaunches.sg/condominium/midwood.html',
         'https://www.newlaunches.sg/condominium/verdale.html',
         'https://www.newlaunches.sg/condominium/mayfair-gardens.html',
         'https://www.newlaunches.sg/condominium/baywind-residences.html',
         'https://www.newlaunches.sg/condominium/the-avenir.html',
         'https://www.newlaunches.sg/condominium/one-meyer.html',
         'https://www.newlaunches.sg/condominium/riverfront-residences.html',
         'https://www.newlaunches.sg/condominium/peak-residence.html',
         'https://www.newlaunches.sg/mixed-development/the-jovell.html',
         'https://www.newlaunches.sg/condominium/infini-at-east-coast.html',
         'https://www.newlaunches.sg/condominium/petit-jervois.html',
         'https://www.newlaunches.sg/condominium/avenue-south-residence.html',
         'https://www.newlaunches.sg/executive-condominium/provence-residence.html',
         'https://www.newlaunches.sg/condominium/coastline-residences.html',
         'https://www.newlaunches.sg/mixed-development/marina-one-residences.html',
         'https://www.newlaunches.sg/condominium/the-gazania.html', 'https://www.newlaunches.sg/condominium/myra.html',
         'https://www.newlaunches.sg/condominium/twentyone-angullia-park.html',
         'https://www.newlaunches.sg/condominium/fyve-derbyshire.html',
         'https://www.newlaunches.sg/mixed-development/piccadilly-grand.html',
         'https://www.newlaunches.sg/executive-condominium/copen-grand.html',
         'https://www.newlaunches.sg/condominium/royalgreen.html',
         'https://www.newlaunches.sg/condominium/la-mariposa.html',
         'https://www.newlaunches.sg/condominium/the-iveria.html',
         'https://www.newlaunches.sg/condominium/rymden-77.html',
         'https://www.newlaunches.sg/executive-condominium/parc-central-residences.html',
         'https://www.newlaunches.sg/executive-condominium/tenet.html',
         'https://www.newlaunches.sg/mixed-development/parc-komo.html',
         'https://www.newlaunches.sg/condominium/meyerhouse.html',
         'https://www.newlaunches.sg/condominium/nouvel-18.html',
         'https://www.newlaunches.sg/condominium/the-florence-residences.html',
         'https://www.newlaunches.sg/mixed-development/dairy-farm-residences.html',
         'https://www.newlaunches.sg/condominium/wilshire-residences.html',
         'https://www.newlaunches.sg/condominium/the-commodore.html',
         'https://www.newlaunches.sg/condominium/hyll-on-holland.html',
         'https://www.newlaunches.sg/condominium/uptown-at-farrer.html',
         'https://www.newlaunches.sg/condominium/cairnhill-16.html',
         'https://www.newlaunches.sg/landed/parkwood-collection.html',
         'https://www.newlaunches.sg/condominium/stirling-residences.html',
         'https://www.newlaunches.sg/mixed-development/one-north-eden.html',
         'https://www.newlaunches.sg/mixed-development/one-holland-village-residences.html',
         'https://www.newlaunches.sg/condominium/juniper-hill.html',
         'https://www.newlaunches.sg/condominium/kent-ridge-hill-residences.html',
         'https://www.newlaunches.sg/condominium/phoenix-residences.html',
         'https://www.newlaunches.sg/landed/the-whitley-residences.html',
         'https://www.newlaunches.sg/condominium/mont-botanik-residence.html',
         'https://www.newlaunches.sg/executive-condominium/parc-greenwich.html',
         'https://www.newlaunches.sg/condominium/verticus.html', 'https://www.newlaunches.sg/condominium/riviere.html',
         'https://www.newlaunches.sg/condominium/clavon.html', 'https://www.newlaunches.sg/condominium/penrose.html',
         'https://www.newlaunches.sg/condominium/the-atelier.html',
         'https://www.newlaunches.sg/condominium/77-at-east-coast.html',
         'https://www.newlaunches.sg/landed/belgravia-ace.html',
         'https://www.newlaunches.sg/condominium/the-antares.html',
         'https://www.newlaunches.sg/condominium/the-hyde.html',
         'https://www.newlaunches.sg/mixed-development/the-woodleigh-residences.html',
         'https://www.newlaunches.sg/condominium/amber-park.html',
         'https://www.newlaunches.sg/condominium/35-gilstead.html',
         'https://www.newlaunches.sg/condominium/normanton-park.html',
         'https://www.newlaunches.sg/condominium/fourth-avenue-residences.html',
         'https://www.newlaunches.sg/condominium/3-cuscaden.html',
         'https://www.newlaunches.sg/condominium/forett-at-bukit-timah.html',
         'https://www.newlaunches.sg/condominium/nyon.html', 'https://www.newlaunches.sg/condominium/casa-al-mare.html',
         'https://www.newlaunches.sg/condominium/sloane-residences.html',
         'https://www.newlaunches.sg/mixed-development/sengkang-grand-residences.html',
         'https://www.newlaunches.sg/mixed-development/tedge.html',
         'https://www.newlaunches.sg/condominium/van-holland.html',
         'https://www.newlaunches.sg/condominium/urban-treasures.html',
         'https://www.newlaunches.sg/condominium/mooi-residences.html',
         'https://www.newlaunches.sg/condominium/parkwood-residences.html',
         'https://www.newlaunches.sg/condominium/the-lilium.html',
         'https://www.newlaunches.sg/condominium/8-hullet.html',
         'https://www.newlaunches.sg/condominium/liberte.html',
         'https://www.newlaunches.sg/landed/brighthill-residences.html',
         'https://www.newlaunches.sg/condominium/affinity-at-serangoon.html',
         'https://www.newlaunches.sg/condominium/15-holland-hill.html',
         'https://www.newlaunches.sg/condominium/mayfair-modern.html']


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
    units = []
    if 'Former' in data['Project Name:']:
        name = data['Project Name:'].split('(')[0][:-1].replace(
            ' at Brookvale', '').replace('The Botany at Dairy Farm', 'The Botany').replace('Sky Eden@Bedok', 'Sky Eden')
    else:
        name = data['Project Name:'].replace('The Botany at Dairy Farm', 'The Botany').replace(
            ' at Brookvale', '').replace('Sky Eden@Bedok', 'Sky Eden').replace('Sky Eden@Bedok', 'Sky Eden')

    table = soup.find('section', {'id': 'project_unit_types'})
    if not table:
        return None

    try:
        units_data = table.find('table').find_all('tr')
    except AttributeError:
        return None
    for unit in units_data[1:]:
        unit_data = unit.find_all('td')
        units.append(unit_data[0].get_text(strip=True).replace('(', '').replace(')', ''))

    return name


def make_fp_concat(soup):
    res = {}
    data = soup.find('section', {'id': 'project_floorplans'}).find_all('div',
                                                                       class_='image_hover_overlay_container text-center shadow-lg')
    for item in data:

        try:
            url = item.find('a')['href']
            unit_type = item.find('a')['data-caption'].split(' [')[0]
            if unit_type in res:
                res[unit_type].append(url)
            else:
                res[unit_type] = [url]
        except KeyError:
            continue
    return res


def update_airtable(unit_to_url, condo_name):
    AIR_TABLE_HEADERS = {"Authorization": "Bearer " + 'keygbB1MnX8GRvpKW', "Content-Type": "application/json",
                         'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                                       'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                                       'scale=3.00; 1170x2532; 463736449) NW/3'}

    params = (
        ('filterByFormula', f'{{General}} = "{condo_name}"'),
    )
    response = requests.get(
        f'https://api.airtable.com/v0/app0pXo7PruFurQjq/tblDzvFZ5MoqBLjKl',
        headers=AIR_TABLE_HEADERS, params=params)

    for unit in response.json()['records']:
        try:
            existing_type = unit['fields']['unit_type']
        except KeyError:
            continue
        if existing_type in unit_to_url:
            record_id = unit['id']

            data = {
                "fields": {
                    "floor_plan_image_links": ', '.join(unit_to_url[existing_type])
                }
            }

            response = requests.patch(f'https://api.airtable.com/v0/app0pXo7PruFurQjq/tblDzvFZ5MoqBLjKl/{record_id}',
                                      headers=AIR_TABLE_HEADERS,
                                      json=data)

            if response.status_code == 200:
                print(response)
            else:
                print(response.status_code)


# a = {condo_name: {unit_type: [urls], unit_type: [urls], unit_type: [urls]}}


for link in links:
    print(link)
    main, soup = gather_main_data(link)
    condo_name = extract_main_data(soup, main)
    if condo_name:
        print(condo_name)
        unit_to_url = make_fp_concat(soup)
        print(unit_to_url)
        update_airtable(unit_to_url, condo_name)
