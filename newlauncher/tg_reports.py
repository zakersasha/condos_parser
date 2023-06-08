import asyncio

import requests

from config import Config


async def send_tg_report(data, label):
    if label:
        message = f'✅ {data["name"]} *{label}\n\n' \
                  f'👉 District: {data["district"]} {data["address"]}\n' \
                  f'🗓 Start date of previews: {data["previewing_start_date"]}\n\n' \
                  f'🏡 Condo: {data["link_to_condo"]}\n'

        if data.get('brochure'):
            message += f'📔 Brochure: {data["brochure"][0]["url"]}\n'

        bot_token = Config.TG_BOT_TOKEN

        url_text = f'{Config.TG_API_URL}{bot_token}/sendMessage'
        params = {'text': message, 'chat_id': Config.TG_CHAT_ID, 'parse_mode': 'HTML'}

        requests.post(url_text, params=params)
        await asyncio.sleep(2)
