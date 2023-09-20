import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    NEW_LAUNCHER_PAGES = os.environ.get('SITE_PAGES')
    NEW_LAUNCHER_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                      'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                      'scale=3.00; 1170x2532; 463736449) NW/3'}

    SRX_URL = 'https://www.srx.com.sg'
    SRX_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0',
        'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    }

    TG_CHAT_ID = os.environ.get('TG_CHAT_ID')
    TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')
    TG_API_URL = 'https://api.telegram.org/bot'

    AIR_TABLE_API_KEY = os.environ.get('AIR_TABLE_API_KEY')
    AIR_TABLE_HEADERS = {"Authorization": "Bearer " + AIR_TABLE_API_KEY, "Content-Type": "application/json",
                         'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                                       'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                                       'scale=3.00; 1170x2532; 463736449) NW/3'}

    AIR_TABLE_BASE_ID = os.environ.get('AIR_TABLE_BASE_ID')
    MAIN_TABLE_ID = os.environ.get('MAIN_TABLE_ID')
    AMENITIES_TABLE_ID = os.environ.get('AMENITIES_TABLE_ID')
    UNITS_TABLE_ID = os.environ.get('UNITS_TABLE_ID')

    # Postgres conf
    DB_HOST = os.environ.get('DB_HOST')
    DB_PORT = os.environ.get('DB_PORT')
    DB_PASSWORD = os.environ.get('DB_PASSWORD')
    DB_NAME = os.environ.get('DB_NAME')
    DB_TABLE_NAME = os.environ.get('DB_TABLE_NAME')
    DB_USERNAME = os.environ.get('DB_USERNAME')
