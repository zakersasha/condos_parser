import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    NEW_LAUNCHER_PAGES = os.environ.get('SITE_PAGES')
    NEW_LAUNCHER_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                      'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                      'scale=3.00; 1170x2532; 463736449) NW/3'}
