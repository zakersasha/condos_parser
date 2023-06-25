import asyncio
import datetime

import requests

from config import Config


async def send_tg_report(data, label, new_units, total_units, old_available_units):
    if label == 'New':
        message = f'🆕 {data["name"]}\n\n' \
                  f'👉 District: {data["district"]} {data["address"]}\n' \
                  f'🏡 Condo: {data["link_to_condo"]}\n\n'
        if new_units:
            new_unit_types = '\nNew units added: '
            for item in new_units:
                new_unit_types += f'{item["unit_type"]} '

            message += new_unit_types

        try:
            if data['overall_available_units']:
                message += f'\nAvailable units: {data["overall_available_units"]}'
        except (KeyError, AttributeError):
            pass

        if total_units:
            message += f'\nTotal units: {total_units}'

        bot_token = Config.TG_BOT_TOKEN

        url_text = f'{Config.TG_API_URL}{bot_token}/sendMessage'
        params = {'text': message, 'chat_id': Config.TG_CHAT_ID, 'parse_mode': 'HTML'}

        requests.post(url_text, params=params)
        await asyncio.sleep(1)

    if label == 'Updated':
        message = f'✅ {data["name"]}\n\n' \
                  f'👉 District: {data["district"]} {data["address"]}\n' \
                  f'🏡 Condo: {data["link_to_condo"]}\n\n'
        if new_units:
            message_upd = 'New units added: '
            for item in new_units:
                message_upd += f'{item["unit_type"]} '
            message += message_upd

        try:
            if data['overall_available_units'] and data['overall_available_units'] != old_available_units:
                message += f'Available units: {old_available_units} → {data["overall_available_units"]}'
        except (KeyError, AttributeError):
            pass

        bot_token = Config.TG_BOT_TOKEN

        url_text = f'{Config.TG_API_URL}{bot_token}/sendMessage'
        params = {'text': message, 'chat_id': Config.TG_CHAT_ID, 'parse_mode': 'HTML'}

        requests.post(url_text, params=params)
        await asyncio.sleep(1)


async def send_updates_file():
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"SRX_{current_date}.txt"
    bot_token = Config.TG_BOT_TOKEN
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    params = {
        "chat_id": Config.TG_CHAT_ID
    }

    files = {
        "document": open(filename, "rb")
    }

    requests.post(url, params=params, files=files)
