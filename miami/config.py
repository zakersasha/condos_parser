import os

from dotenv import load_dotenv

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0',
    'Accept': 'application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}
load_dotenv()
base_id = os.environ.get("MIAMI_BASE_ID")
api_key = os.environ.get("MIAMI_API_KEY")
units_table = os.environ.get("MIAMI_UNITS")
general_table = os.environ.get("MIAMI_GENERAL")

AIR_TABLE_HEADERS = {"Authorization": "Bearer " + api_key, "Content-Type": "application/json",
                     'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, '
                                   'like Gecko) Mobile/15E148 Instagram 278.0.0.19.115 (iPhone13,2; iOS 16_2; en_GB; en-GB; '
                                   'scale=3.00; 1170x2532; 463736449) NW/3'}
