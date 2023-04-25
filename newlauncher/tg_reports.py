from datetime import datetime

import requests

from config import Config


async def send_tg_report(data):
    current_date = datetime.today()
    message = f'✅ {data["name"]}\n\n' \
              f'📩 {data["district"]} {data["address"]}\n' \
              f'🗓 {current_date.strftime("%d")} {current_date.strftime("%B")} {current_date.strftime("%Y")}\n\n' \
              f'🏡 Condo:{data["link_to_condo"]}\n'

    if data.get('link_to_brochure'):
        message += f'📔 Brochure:{data["link_to_brochure"]}\n'

    bot_token = Config.TG_BOT_TOKEN
    chat_id = Config.TG_CHAT_ID

    url_text = f'{Config.TG_API_URL}{bot_token}/sendMessage?chat_id={chat_id}&text={message}'
    requests.post(url_text)
