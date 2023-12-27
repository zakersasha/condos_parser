"""
На уровне всех кондо в базе:
- Время последнего обновления базы данных
- Количество новых кондо
- Количество кондо с overall_available_units>0 (в сравнении с предыдущим днем: указать фактическое количество и прирост)
"""
import requests


def condo_db_report(city, date_time, new_condos, fact_condos, available_condos_count):
    message = f'Город: {city}\n\n' \
              f'Последнее обновление БД: {date_time}\n\n' \
              f'Новых кондо: ➕ {new_condos}\n\n' \
              f'Всего кондо с доступными юнитами: {fact_condos}\n' \
              f'Кондо с доступными юнитами прирост: ➕ {available_condos_count}\n'

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
                         min_price_condos_counter,
                         new_brochures_condos_list):
    if len(new_brochures_condos_list) == 0:
        condos_list = 'Нет'
    else:
        condos_list = ','.join(new_brochures_condos_list)
    message = f'Партнер: {partner}\n\n' \
              f'Последнее обновление БД: {date_time}\n' \
              f'Новых кондо: ➕ {new_condos}\n\n' \
              f'Кондо с доступными юнитами всего: {available_condos_all}\n' \
              f'Кондо с доступными юнитами прирост: ➕ {available_condos_count}\n\n' \
              f'Кондо с ценой всего: {min_price_condos_all}\n' \
              f'Кондо с ценой прирост: ➕ {min_price_condos_counter}\n' \
              f'Новые брошюры: {condos_list}\n'

    bot_token = '6559406117:AAHwaGZTdRnB259blt5A9EX7VU-oX2YL5nw'
    chat_id = '-1002134207391'

    url_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}'
    requests.post(url_text)


"""
Dubai. Kofman и 7Spaces
Необходимо рассчитать:
-Количество кондо, которые выбрал партнер (поле selected)
-Количество кондо в поле companies (названия партнеров)
-Количество кондо с overall_available_units>0
Для кондо с overall_available_units>0 и companies, который содержит название партнеров (Kofman или 7Spaces), посчитать заполняемость полей = Количество заполненных полей / Максимальное количество полей * 100%

Столбцы, по которым нужно рассчитывать заполненность полей:

Address
District
overall_available_units
date_of_completion
facilities
overall_min_unit_size
overall_min_unit_psf
overall_min_unit_price
description
payment_plans
Units (типы юнитов) - есть ли в разрезе кондо типы юнитов на вкладке units, да/нет, считаем кондо с “да”.

"""


def kofman_general_report(select_count, company_count, available_count, complete_percentage, units_complete_percentage,
                          percentage_size_min, percentage_num_bedrooms, percentage_floor_plan_image_links,
                          percentage_price_min, percentage_psf_min):
    message = f'Партнер: Kofman\n\n' \
              f'Количество выбранных кондо: {select_count}\n' \
              f'Количество кондо компании: {company_count}\n' \
              f'Кондо с доступными юнитами: {available_count}\n' \
              f'Процент полноты данных: {complete_percentage} %\n' \
              f'Процент полноты данных юнитов: {units_complete_percentage} %\n\n' \
              f'Процент полноты size_min: {percentage_size_min} %\n' \
              f'Процент полноты num_bedrooms: {percentage_num_bedrooms} %\n' \
              f'Процент полноты floor_plan_image_links: {percentage_floor_plan_image_links} %\n' \
              f'Процент полноты price_min: {percentage_price_min} %\n' \
              f'Процент полноты psf_min: {percentage_psf_min} %\n'

    bot_token = '6559406117:AAHwaGZTdRnB259blt5A9EX7VU-oX2YL5nw'
    chat_id = '-1002134207391'

    url_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}'
    requests.post(url_text)


def seven_spaces_general_report(select_count, company_count, available_count, complete_percentage,
                                units_complete_percentage, percentage_size_min, percentage_num_bedrooms,
                                percentage_floor_plan_image_links, percentage_price_min, percentage_psf_min):
    message = f'Партнер: 7Spaces\n\n' \
              f'Количество выбранных кондо: {select_count}\n' \
              f'Количество кондо компании: {company_count}\n' \
              f'Кондо с доступными юнитами: {available_count}\n' \
              f'Процент полноты данных: {complete_percentage} %\n' \
              f'Процент полноты данных юнитов: {units_complete_percentage} %\n\n' \
              f'Процент полноты size_min: {percentage_size_min} %\n' \
              f'Процент полноты num_bedrooms: {percentage_num_bedrooms} %\n' \
              f'Процент полноты floor_plan_image_links: {percentage_floor_plan_image_links} %\n' \
              f'Процент полноты price_min: {percentage_price_min} %\n' \
              f'Процент полноты psf_min: {percentage_psf_min} %\n'

    bot_token = '6559406117:AAHwaGZTdRnB259blt5A9EX7VU-oX2YL5nw'
    chat_id = '-1002134207391'

    url_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}'
    requests.post(url_text)


"""
Miami. Wolsen
Необходимо рассчитать:
-Количество кондо, которые выбрал партнер (поле selected)
-Количество кондо с брошюрами (поле companies)
-Количество кондо с overall_available_units>0
Для кондо с overall_available_units>0 и companies, который содержит название партнеров (Wolsen), посчитать заполняемость полей = Количество заполненных полей / Максимальное количество полей * 100%
Столбцы, по которым нужно считать заполненность:
Address
District
units_number
date_of_completion
facilities
overall_min_unit_size
overall_min_unit_psf
overall_min_unit_price
description
payment_ plans
Payment_plans_attached*
Units (типы юнитов)

Расчет нужно делать только по тем кондо, которые выбраны для составления подборок (столбец companies содержит название партнера: Wolsen).
"""


def wolsen_general_report(select_count, company_count, available_count, complete_percentage, units_complete_percentage,
                          percentage_size_min, percentage_num_bedrooms, percentage_floor_plan_image_links):
    message = f'Партнер: Wolsen\n\n' \
              f'Количество выбранных кондо: {select_count}\n' \
              f'Количество кондо компании: {company_count}\n' \
              f'Кондо с доступными юнитами: {available_count}\n' \
              f'Процент полноты данных: {complete_percentage} %\n' \
              f'Процент полноты данных юнитов: {units_complete_percentage} %\n\n' \
              f'Процент полноты size_min: {percentage_size_min} %\n' \
              f'Процент полноты num_bedrooms: {percentage_num_bedrooms} %\n' \
              f'Процент полноты floor_plan_image_links: {percentage_floor_plan_image_links} %\n'

    bot_token = '6559406117:AAHwaGZTdRnB259blt5A9EX7VU-oX2YL5nw'
    chat_id = '-1002134207391'

    url_text = f'https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}'
    requests.post(url_text)
