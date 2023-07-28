from newlauncher_sg.db_queries import store_data_airtable
from newlauncher_sg.tg_reports import send_updates_file, send_tg_report
from newlauncher_sg.utils import gather_projects_links, gather_main_data, extract_main_data, gather_amenities_data, \
    gather_units_data


async def parse_new_launcher_sg():
    """Parse, handle & save new launcher data."""
    links = gather_projects_links()  # Добыча ссылок на кондо

    for link in links:
        print(link)
        try:
            main_table_data, soup = gather_main_data(link)  # Все данные со страницы
            main = extract_main_data(soup, main_table_data)  # Данные основной таблицы
            amenities_table_data = gather_amenities_data(soup)  # Данные таблицы amenities
            units_table_data = gather_units_data(soup, main)  # Данные таблицы units
            if units_table_data == 'skip':
                continue

            label, new_units, total_units, units_changes = store_data_airtable(main, units_table_data,
                                                                               amenities_table_data)
            send_tg_report(main, label, new_units, total_units, units_changes)
        except Exception:
            continue
    try:
        send_updates_file()
    except Exception:
        pass
