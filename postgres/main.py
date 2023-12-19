from datetime import datetime

from postgres.amenities import gather_amenities_data, save_amenities_data, prepare_amenities_data, \
    delete_old_amenities_data
from postgres.db_queries import get_new_condos, get_available_condos, get_condos_count, get_available_condos_count, \
    get_price_condos_count
from postgres.general import gather_main_data, prepare_main_data, save_main_data, delete_old_main_data, \
    gather_miami_main_data, prepare_miami_main_data, save_miami_main_data, gather_uk_main_data, prepare_uk_main_data, \
    save_uk_main_data, gather_dubai_main_data, prepare_dubai_main_data, save_dubai_main_data, get_all_records, \
    gather_oman_main_data, prepare_oman_main_data, save_oman_main_data
from postgres.reports import condo_db_report, condo_partner_report
from postgres.units import gather_units_data, prepare_units_data, save_units_data, delete_old_units_data, \
    gather_miami_units_data, prepare_miami_units_data, save_miami_units_data, gather_uk_units_data, save_uk_units_data, \
    prepare_uk_units_data, gather_dubai_units_data, prepare_dubai_units_data, save_dubai_units_data, \
    delete_units_with_no_general


def postgres_integration():
    print('Postgres recording started')

    # condo_db_report
    old_new_condos = get_new_condos()
    old_available_condos = get_available_condos()
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
    new_new_condos = get_new_condos()
    new_available_condos = get_available_condos()

    # condo_db_report
    new_counter = new_new_condos - old_new_condos
    if new_counter < 0:
        new_counter = 0
    # condo_db_report
    available_counter = new_available_condos - old_available_condos
    if available_counter < 0:
        available_counter = 0

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

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    condo_db_report(now, new_counter, new_available_condos, available_counter)
    condo_partner_report('7Spaces', now, seven_spaces_counter, ss_new, ss_counter,
                         ss1_new, ss1_counter)
    condo_partner_report('Kofman', now, kofman_counter, k_new, k_counter,
                         k1_new, k1_counter)
    condo_partner_report('Wolsen', now, wolsen_counter, w_new, w_counter,
                         w1_new, w1_counter)
    condo_partner_report('Saola', now, saola_counter, s_new, s_counter,
                         s1_new, s1_counter)
