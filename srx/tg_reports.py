import asyncio

import requests

from config import Config


async def send_tg_report(data, label, new_units, total_units):
    if label:
        message = f'✅ {data["name"]} *{label}\n\n' \
                  f'👉 District: {data["district"]} {data["address"]}\n' \
                  f'🏡 Condo: {data["link_to_condo"]}\n\n'
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
