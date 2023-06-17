import asyncio
import datetime

import requests

from config import Config


async def send_tg_report(data, label, new_units, total_units):
    if label:
        message = f'✅ {data["name"]} *{label}\n\n' \
                  f'👉 District: {data["district"]} {data["address"]}\n' \
                  f'🗓 Start date of previews: {data["previewing_start_date"]}\n\n' \
                  f'🏡 Condo: {data["link_to_condo"]}\n'

        if data.get('brochure'):
            message += f'📔 Brochure: {data["brochure"][0]["url"]}\n'

        if new_units:
            message_upd = 'New units added: '
            for item in new_units:
                message_upd += f'{item["unit_type"]} '
            message += message_upd
            message += f'\n Total units: {total_units}'

        bot_token = Config.TG_BOT_TOKEN

        url_text = f'{Config.TG_API_URL}{bot_token}/sendMessage'
        params = {'text': message, 'chat_id': Config.TG_CHAT_ID, 'parse_mode': 'HTML'}

        requests.post(url_text, params=params)
        await asyncio.sleep(2)


async def send_updates_file():
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    filename = f"NewLauncher_{current_date}.txt"
    bot_token = Config.TG_BOT_TOKEN
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    params = {
        "chat_id": Config.TG_CHAT_ID
    }

    files = {
        "document": open(filename, "rb")
    }

    requests.post(url, params=params, files=files)
