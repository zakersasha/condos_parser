import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    NEW_LAUNCHER_PAGES = os.environ.get('SITE_PAGES')
    NEW_LAUNCHER_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                      'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                      'scale=3.00; 1170x2532; 463736449) NW/3'}

    TG_CHAT_ID = os.environ.get('TG_CHAT_ID')
    TG_BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')
    TG_API_URL = 'https://api.telegram.org/bot'

    AIR_TABLE_API_KEY = os.environ.get('AIR_TABLE_API_KEY')
    AIR_TABLE_HEADERS = {"Authorization": "Bearer " + AIR_TABLE_API_KEY, "Content-Type": "application/json",
                         'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                                       'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                                       'scale=3.00; 1170x2532; 463736449) NW/3'}
