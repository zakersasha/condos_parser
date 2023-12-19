"""
На уровне всех кондо в базе:
- Время последнего обновления базы данных
- Количество новых кондо
- Количество кондо с overall_available_units>0 (в сравнении с предыдущим днем: указать фактическое количество и прирост)
"""
import requests


def condo_db_report(date_time, new_condos, fact_condos, available_condos_count):
    message = f'Последнее обновление БД: {date_time}\n\n' \
              f'Новых кондо: ➕ {new_condos}\n\n' \
              f'Всего кондо: {fact_condos}\n' \
              f'Кондо с доступными юнитами: ➕ {available_condos_count}\n'

    bot_token = '6559406117:AAHwaGZTdRnB259blt5A9EX7VU-oX2YL5nw'
    chat_id = '-1002134207391'

    url_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}'
    requests.post(url_text)


"""
На уровне selected (отдельно по каждому партнеру):
Время последнего обновления базы данных
Количество новых кондо
Количество кондо с overall_available_units > 0 (в сравнении с предыдущим днем: указать фактическое количество и прирост)
Количество кондо с min_price > 0 (в сравнении с предыдущим днем: указать фактическое количество и прирост)
Список кондо, у которых появились брошюры в airtable (поле brochure было пустым, стало заполненным)
"""


def condo_partner_report(partner,
                         date_time,
                         new_condos,
                         available_condos_all,
                         available_condos_count,
                         min_price_condos_all,
                         min_price_condos_counter):
    message = f'Партнер: {partner}\n\n' \
              f'Последнее обновление БД: {date_time}\n' \
              f'Новых кондо: ➕ {new_condos}\n\n' \
              f'Кондо с доступными юнитами всего: {available_condos_all}\n' \
              f'Кондо с доступными юнитами прирост: ➕ {available_condos_count}\n\n' \
              f'Кондо с ценой всего: {min_price_condos_all}\n' \
              f'Кондо с ценой прирост: ➕ {min_price_condos_counter}\n'

    bot_token = '6559406117:AAHwaGZTdRnB259blt5A9EX7VU-oX2YL5nw'
    chat_id = '-1002134207391'

    url_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}'
    requests.post(url_text)
