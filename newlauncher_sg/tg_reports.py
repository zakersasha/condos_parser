import datetime
import time

import requests

from config import Config


def send_tg_report(data, label, new_units, total_units, units_changes):
    bot_token = Config.TG_BOT_TOKEN
    url_text = f'{Config.TG_API_URL}{bot_token}/sendMessage'

    if label == 'New':
        message = f'🆕 {data["name"]} *{label}\n\n' \
                  f'👉 District: {data["district"]} {data["address"]}\n' \
                  f'🏡 Condo: {data["link_to_condo"]}\n'

        if new_units:
            new_unit_types = '\nNew units added: '
            for item in new_units:
                new_unit_types += f'{item["unit_type"]} '

            message += new_unit_types

        if total_units:
            message += f'\nTotal units: {total_units}'
        params = {'text': message, 'chat_id': Config.TG_CHAT_ID, 'parse_mode': 'HTML'}

        requests.post(url_text, params=params)
        time.sleep(2)

    if label == 'Updated':
        message = f'✅ {data["name"]}\n\n' \
                  f'👉 District: {data["district"]} {data["address"]}\n' \
                  f'🏡 Condo: {data["link_to_condo"]}\n\n'
        if new_units:
            message_upd = 'New units added: '
            for item in new_units:
                message_upd += f'{item["unit_type"]} '
            message += message_upd
            message += '\n'
        if len(units_changes) > 0:
            for row in units_changes:
                message += f'{row}\n'

        params = {'text': message, 'chat_id': Config.TG_CHAT_ID, 'parse_mode': 'HTML'}

        requests.post(url_text, params=params)
        time.sleep(2)


def send_updates_file():
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"NewLauncher_sg_{current_date}.txt"
    bot_token = Config.TG_BOT_TOKEN
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    params = {
        "chat_id": Config.TG_CHAT_ID
    }

    files = {
        "document": open(filename, "rb")
    }

    requests.post(url, params=params, files=files)
