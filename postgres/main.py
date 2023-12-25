import time
from datetime import datetime

from postgres.amenities import gather_amenities_data, save_amenities_data, prepare_amenities_data, \
    delete_old_amenities_data
from postgres.db_queries import get_new_condos, get_available_condos, get_condos_count, get_available_condos_count, \
    get_price_condos_count, gather_select_count, gather_company_count, gather_available_count, \
    gather_complete_percentage, gather_complete_percentage_no_units, gather_units_complete_percentage, \
    get_brochure_condos_list
from postgres.general import gather_main_data, prepare_main_data, save_main_data, delete_old_main_data, \
    gather_miami_main_data, prepare_miami_main_data, save_miami_main_data, gather_uk_main_data, prepare_uk_main_data, \
    save_uk_main_data, gather_dubai_main_data, prepare_dubai_main_data, save_dubai_main_data, get_all_records, \
    gather_oman_main_data, prepare_oman_main_data, save_oman_main_data
from postgres.reports import condo_db_report, condo_partner_report, kofman_general_report, seven_spaces_general_report, \
    wolsen_general_report
from postgres.units import gather_units_data, prepare_units_data, save_units_data, delete_old_units_data, \
    gather_miami_units_data, prepare_miami_units_data, save_miami_units_data, gather_uk_units_data, save_uk_units_data, \
    prepare_uk_units_data, gather_dubai_units_data, prepare_dubai_units_data, save_dubai_units_data, \
    delete_units_with_no_general


def postgres_integration():
    print('Postgres recording started')

    # condo_db_report
    m_old_new_condos = get_new_condos('Miami')
    m_old_available_condos = get_available_condos('Miami')
    d_old_new_condos = get_new_condos('Dubai')
    d_old_available_condos = get_available_condos('Dubai')
    s_old_new_condos = get_new_condos('Singapore')
    s_old_available_condos = get_available_condos('Singapore')
    seven_spaces_old = get_condos_count('7Spaces')
    kofman_old = get_condos_count('Kofman')
    wolsen_old = get_condos_count('Wolsen')
    saola_old = get_condos_count('Saola')
    ss_old = get_available_condos_count('7Spaces')
    k_old = get_available_condos_count('Kofman')
    w_old = get_available_condos_count('Wolsen')
    s_old = get_available_condos_count('Saola')
    ss1_old = get_price_condos_count('7Spaces')
    k1_old = get_price_condos_count('Kofman')
    w1_old = get_price_condos_count('Wolsen')
    s1_old = get_price_condos_count('Saola')
    ss_condos_list_old = get_brochure_condos_list('7Spaces')
    k_condos_list_old = get_brochure_condos_list('Kofman')
    w_condos_list_old = get_brochure_condos_list('Wolsen')
    s_condos_list_old = get_brochure_condos_list('Saola')

    # dubai general
    delete_old_units_data()
    dubai_main_data = gather_dubai_main_data()
    dubai_main_data_to_save = prepare_dubai_main_data(dubai_main_data)
    save_dubai_main_data(dubai_main_data_to_save)
    print('dubai general table updated')

    # oman general
    oman_main_data = gather_oman_main_data()
    oman_main_data_to_save = prepare_oman_main_data(oman_main_data)
    save_oman_main_data(oman_main_data_to_save)
    print('oman general table updated')

    # miami general
    miami_main_data = gather_miami_main_data()
    miami_main_data_to_save = prepare_miami_main_data(miami_main_data)
    save_miami_main_data(miami_main_data_to_save)
    print('miami general table updated')

    # uk general
    uk_main_data = gather_uk_main_data()
    uk_main_data_to_save = prepare_uk_main_data(uk_main_data)
    save_uk_main_data(uk_main_data_to_save)
    print('uk general table updated')

    # general
    main_data = gather_main_data()
    main_data_to_save = prepare_main_data(main_data)
    save_main_data(main_data_to_save)
    delete_old_main_data()
    print('general table updated')

    all_general_data = get_all_records()

    # units
    units_data = gather_units_data()
    units_data_to_save = prepare_units_data(units_data, all_general_data)
    save_units_data(units_data_to_save)
    print('units table updated')

    # miami units
    miami_units_data = gather_miami_units_data()
    miami_units_data_to_save = prepare_miami_units_data(miami_units_data, all_general_data)
    save_miami_units_data(miami_units_data_to_save)
    print('miami units table updated')

    # uk units
    uk_units_data = gather_uk_units_data()
    uk_units_data_to_save = prepare_uk_units_data(uk_units_data, all_general_data)
    save_uk_units_data(uk_units_data_to_save)
    print('uk units table updated')

    # dubai units
    dubai_units_data = gather_dubai_units_data()
    dubai_units_data_to_save = prepare_dubai_units_data(dubai_units_data, all_general_data)
    save_dubai_units_data(dubai_units_data_to_save)
    print('dubai units table updated')

    delete_units_with_no_general()

    # amenities
    try:
        amenities_data = gather_amenities_data()
        amenities_data_to_save = prepare_amenities_data(amenities_data)
        delete_old_amenities_data()
        save_amenities_data(amenities_data_to_save)
        print('amenities table updated')
    except Exception as e:
        print(e)
        pass

    # condo_db_report
    m_new_new_condos = get_new_condos('Miami')
    m_new_available_condos = get_available_condos('Miami')

    # condo_db_report
    m_new_counter = m_new_new_condos - m_old_new_condos
    if m_new_counter < 0:
        m_new_counter = 0
    # condo_db_report
    m_available_counter = m_new_available_condos - m_old_available_condos
    if m_available_counter < 0:
        m_available_counter = 0

    d_new_new_condos = get_new_condos('Dubai')
    d_new_available_condos = get_available_condos('Dubai')

    # condo_db_report
    d_new_counter = d_new_new_condos - d_old_new_condos
    if d_new_counter < 0:
        d_new_counter = 0
    # condo_db_report
    d_available_counter = d_new_available_condos - d_old_available_condos
    if d_available_counter < 0:
        d_available_counter = 0

    s_new_new_condos = get_new_condos('Singapore')
    s_new_available_condos = get_available_condos('Singapore')

    # condo_db_report
    s_new_counter = s_new_new_condos - s_old_new_condos
    if s_new_counter < 0:
        s_new_counter = 0
    # condo_db_report
    s_available_counter = s_new_available_condos - s_old_available_condos
    if s_available_counter < 0:
        s_available_counter = 0

    seven_spaces_new = get_condos_count('7Spaces')
    kofman_new = get_condos_count('Kofman')
    wolsen_new = get_condos_count('Wolsen')
    saola_new = get_condos_count('Saola')

    seven_spaces_counter = seven_spaces_new - seven_spaces_old
    if seven_spaces_counter < 0:
        seven_spaces_counter = 0
    kofman_counter = kofman_new - kofman_old
    if kofman_counter < 0:
        kofman_counter = 0
    wolsen_counter = wolsen_new - wolsen_old
    if wolsen_counter < 0:
        wolsen_counter = 0
    saola_counter = saola_new - saola_old
    if saola_counter < 0:
        saola_counter = 0

    ss_new = get_available_condos_count('7Spaces')
    k_new = get_available_condos_count('Kofman')
    w_new = get_available_condos_count('Wolsen')
    s_new = get_available_condos_count('Saola')

    ss_counter = ss_new - ss_old
    if ss_counter < 0:
        ss_counter = 0
    k_counter = k_new - k_old
    if k_counter < 0:
        k_counter = 0
    w_counter = w_new - w_old
    if w_counter < 0:
        w_counter = 0
    s_counter = s_new - s_old
    if s_counter < 0:
        s_counter = 0

    ss1_new = get_price_condos_count('7Spaces')
    k1_new = get_price_condos_count('Kofman')
    w1_new = get_price_condos_count('Wolsen')
    s1_new = get_price_condos_count('Saola')

    ss1_counter = ss1_new - ss1_old
    if ss1_counter < 0:
        ss1_counter = 0
    k1_counter = k1_new - k1_old
    if k1_counter < 0:
        k1_counter = 0
    w1_counter = w1_new - w1_old
    if w1_counter < 0:
        w1_counter = 0
    s1_counter = s1_new - s1_old
    if s1_counter < 0:
        s1_counter = 0

    print('sleeping')
    time.sleep(20)

    ss_condos_list_new = get_brochure_condos_list('7Spaces')
    k_condos_list_new = get_brochure_condos_list('Kofman')
    w_condos_list_new = get_brochure_condos_list('Wolsen')
    s_condos_list_new = get_brochure_condos_list('Saola')

    ss_condos_list = []
    for item in ss_condos_list_old:
        if item not in ss_condos_list_new:
            ss_condos_list.append(item)
    k_condos_list = []
    for item in k_condos_list_old:
        if item not in k_condos_list_new:
            k_condos_list.append(item)
    w_condos_list = []
    for item in w_condos_list_old:
        if item not in w_condos_list_new:
            w_condos_list.append(item)
    s_condos_list = []
    for item in s_condos_list_old:
        if item not in s_condos_list_new:
            s_condos_list.append(item)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    condo_db_report('Miami', now, m_new_counter, m_new_available_condos, m_available_counter)
    condo_db_report('Dubai', now, d_new_counter, d_new_available_condos, d_available_counter)
    condo_db_report('Singapore', now, s_new_counter, s_new_available_condos, s_available_counter)

    condo_partner_report('7Spaces', now, seven_spaces_counter, ss_new, ss_counter,
                         ss1_new, ss1_counter, ss_condos_list)
    condo_partner_report('Kofman', now, kofman_counter, k_new, k_counter,
                         k1_new, k1_counter, k_condos_list)
    condo_partner_report('Wolsen', now, wolsen_counter, w_new, w_counter,
                         w1_new, w1_counter, w_condos_list)
    condo_partner_report('Saola', now, saola_counter, s_new, s_counter,
                         s1_new, s1_counter, s_condos_list)
    make_tg_reports()


def make_tg_reports():
    select_count = gather_select_count('Kofman')
    company_count = gather_company_count('Kofman')
    available_count = gather_available_count('Kofman')
    complete_percentage = gather_complete_percentage('Kofman')
    try:
        units_complete_percentage = gather_units_complete_percentage('Kofman')
    except Exception:
        units_complete_percentage = 0
    kofman_general_report(select_count, company_count, available_count, round(complete_percentage, 2),
                          round(units_complete_percentage, 2))

    select_count = gather_select_count('7Spaces')
    company_count = gather_company_count('7Spaces')
    available_count = gather_available_count('7Spaces')
    complete_percentage = gather_complete_percentage('7Spaces')
    try:
        units_complete_percentage = gather_units_complete_percentage('7Spaces')
    except Exception:
        units_complete_percentage = 0
    seven_spaces_general_report(select_count, company_count, available_count, round(complete_percentage, 2),
                                round(units_complete_percentage, 2))

    select_count = gather_select_count('Wolsen')
    company_count = gather_company_count('Wolsen')
    available_count = gather_available_count('Wolsen')
    complete_percentage = gather_complete_percentage_no_units('Wolsen')
    try:
        units_complete_percentage = gather_units_complete_percentage('Wolsen')
    except Exception:
        units_complete_percentage = 0
    wolsen_general_report(select_count, company_count, available_count, round(complete_percentage, 2),
                          round(units_complete_percentage, 2))
