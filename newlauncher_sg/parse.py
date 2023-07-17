from newlauncher_sg.db_queries import store_data_airtable
from newlauncher_sg.tg_reports import send_updates_file, send_tg_report
from newlauncher_sg.utils import gather_projects_links, gather_main_data, extract_main_data, gather_amenities_data, \
    gather_units_data


def parse_new_launcher_sg():
    """Parse, handle & save new launcher data."""
    links = ['https://www.newlaunches.sg/condominium/mattar-residences.html']  # Добыча ссылок на кондо

    for link in links:
        print(link)
        main_table_data, soup = gather_main_data(link)  # Все данные со страницы
        main = extract_main_data(soup, main_table_data)  # Данные основной таблицы
        amenities_table_data = gather_amenities_data(soup)  # Данные таблицы amenities
        units_table_data = gather_units_data(soup)  # Данные таблицы units
        if units_table_data == 'skip':
            print('here2')
            continue

        label, new_units, total_units = store_data_airtable(main, units_table_data, amenities_table_data)

        # await send_tg_report(main, label, new_units, total_units)

        # try:
        #     await send_updates_file()
        # except Exception:
        #     pass
