from datetime import datetime

from postgres.amenities import gather_amenities_data, save_amenities_data, prepare_amenities_data, \
    delete_old_amenities_data
from postgres.db_queries import get_new_condos, get_available_condos, get_condos_count, get_available_condos_count, \
    get_price_condos_count, gather_select_count, gather_company_count, gather_available_count, \
    gather_complete_percentage, gather_complete_percentage_no_units, gather_units_complete_percentage, \
    get_brochure_condos_list, gather_wolsen_units_complete_percentage, gather_wolsen_fields_percentage, \
    gather_other_fields_percentage, k7_general_fields_percentage, w_general_fields_percentage
from postgres.general import gather_main_data, prepare_main_data, save_main_data, delete_old_main_data, \
    gather_miami_main_data, prepare_miami_main_data, save_miami_main_data, gather_uk_main_data, prepare_uk_main_data, \
    save_uk_main_data, gather_dubai_main_data, prepare_dubai_main_data, save_dubai_main_data, get_all_records, \
    gather_oman_main_data, prepare_oman_main_data, save_oman_main_data, gather_bali_main_data, prepare_bali_main_data, \
    save_bali_main_data, update_airtable_record, gather_bali_i_main_data, prepare_bali_i_main_data, \
    save_bali_i_main_data
from postgres.reports import condo_db_report, condo_partner_report, kofman_general_report, seven_spaces_general_report, \
    wolsen_general_report, condo_partner_report_k7, condo_partner_report_w
from postgres.units import gather_units_data, prepare_units_data, save_units_data, delete_old_units_data, \
    gather_miami_units_data, prepare_miami_units_data, save_miami_units_data, gather_uk_units_data, save_uk_units_data, \
    prepare_uk_units_data, gather_dubai_units_data, prepare_dubai_units_data, save_dubai_units_data, \
    delete_units_with_no_general, gather_bali_units_data, prepare_bali_units_data, save_bali_units_data, \
    check_today_sync, gather_bali_i_units_data, prepare_bali_i_units_data, save_bali_i_units_data


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
    dubai_main_data = gather_dubai_main_data()
    dubai_main_data_to_save = prepare_dubai_main_data(dubai_main_data)
    save_dubai_main_data(dubai_main_data_to_save)
    print('dubai general table updated')

    all_general_data = get_all_records()

    # dubai units
    dubai_units_data = gather_dubai_units_data()
    dubai_units_data_to_save = prepare_dubai_units_data(dubai_units_data, all_general_data)
    save_dubai_units_data(dubai_units_data_to_save)
    print('dubai units table updated')

    # bali general
    bali_main_data = gather_bali_main_data()
    bali_main_data_to_save = prepare_bali_main_data(bali_main_data)
    save_bali_main_data(bali_main_data_to_save)
    print('bali general table updated')

    all_general_data = get_all_records()

    # bali units
    bali_units_data = gather_bali_units_data()
    bali_units_data_to_save = prepare_bali_units_data(bali_units_data, all_general_data)
    save_bali_units_data(bali_units_data_to_save)
    print('bali units table updated')

    # bali intermark
    bali_i_main_data = gather_bali_i_main_data()
    bali_i_main_data_to_save = prepare_bali_i_main_data(bali_i_main_data)
    save_bali_i_main_data(bali_i_main_data_to_save)
    print('bali Intermark general table updated')

    all_general_data = get_all_records()

    # bali intermark units
    bali_i_units_data = gather_bali_i_units_data()
    bali_i_units_data_to_save = prepare_bali_i_units_data(bali_i_units_data, all_general_data)
    save_bali_i_units_data(bali_i_units_data_to_save)
    print('bali Intermark units table updated')

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

    all_general_data = get_all_records()

    # miami units
    miami_units_data = gather_miami_units_data()
    miami_units_data_to_save = prepare_miami_units_data(miami_units_data, all_general_data)
    save_miami_units_data(miami_units_data_to_save)
    print('miami units table updated')

    # uk general
    uk_main_data = gather_uk_main_data()
    uk_main_data_to_save = prepare_uk_main_data(uk_main_data)
    save_uk_main_data(uk_main_data_to_save)
    print('uk general table updated')

    all_general_data = get_all_records()

    # uk units
    uk_units_data = gather_uk_units_data()
    uk_units_data_to_save = prepare_uk_units_data(uk_units_data, all_general_data)
    save_uk_units_data(uk_units_data_to_save)
    print('uk units table updated')

    # general
    main_data = gather_main_data()
    main_data_to_save = prepare_main_data(main_data)
    save_main_data(main_data_to_save)
    print('general table updated')

    all_general_data = get_all_records()

    # units
    units_data = gather_units_data()
    units_data_to_save = prepare_units_data(units_data, all_general_data)
    save_units_data(units_data_to_save)
    print('units table updated')

    delete_units_with_no_general()
    delete_old_main_data()
    delete_old_units_data()

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

    fields_percentage_completion_7 = k7_general_fields_percentage(
        '7Spaces')

    fields_percentage_completion_k = k7_general_fields_percentage(
        'Kofman')

    fields_percentage_completion_w = w_general_fields_percentage(
        'Wolsen')

    condo_partner_report_k7('7Spaces', now, seven_spaces_counter, ss_new, ss_counter,
                            ss1_new, ss1_counter, ss_condos_list, fields_percentage_completion_7)
    condo_partner_report_k7('Kofman', now, kofman_counter, k_new, k_counter,
                            k1_new, k1_counter, k_condos_list, fields_percentage_completion_k)
    condo_partner_report_w('Wolsen', now, wolsen_counter, w_new, w_counter,
                           w1_new, w1_counter, w_condos_list, fields_percentage_completion_w)
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
    percentage_size_min, percentage_num_bedrooms, percentage_floor_plan_image_links, percentage_price_min, percentage_psf_min = gather_other_fields_percentage(
        'Kofman')
    seven_spaces_general_report(select_count,
                                company_count,
                                available_count,
                                round(complete_percentage, 2),
                                round(units_complete_percentage, 2),
                                round(percentage_size_min, 2),
                                round(percentage_num_bedrooms, 2),
                                round(percentage_floor_plan_image_links, 2),
                                round(percentage_price_min, 2),
                                round(percentage_psf_min, 2))
    kofman_general_report(select_count, company_count, available_count, round(complete_percentage, 2),
                          round(units_complete_percentage, 2),
                          round(percentage_size_min, 2),
                          round(percentage_num_bedrooms, 2),
                          round(percentage_floor_plan_image_links, 2),
                          round(percentage_price_min, 2),
                          round(percentage_psf_min, 2))

    select_count = gather_select_count('7Spaces')
    company_count = gather_company_count('7Spaces')
    available_count = gather_available_count('7Spaces')
    complete_percentage = gather_complete_percentage('7Spaces')
    try:
        units_complete_percentage = gather_units_complete_percentage('7Spaces')
    except Exception:
        units_complete_percentage = 0
    percentage_size_min, percentage_num_bedrooms, percentage_floor_plan_image_links, percentage_price_min, percentage_psf_min = gather_other_fields_percentage(
        '7Spaces')
    seven_spaces_general_report(select_count,
                                company_count,
                                available_count,
                                round(complete_percentage, 2),
                                round(units_complete_percentage, 2),
                                round(percentage_size_min, 2),
                                round(percentage_num_bedrooms, 2),
                                round(percentage_floor_plan_image_links, 2),
                                round(percentage_price_min, 2),
                                round(percentage_psf_min, 2))

    select_count = gather_select_count('Wolsen')
    company_count = gather_company_count('Wolsen')
    available_count = gather_available_count('Wolsen')
    complete_percentage = gather_complete_percentage_no_units('Wolsen')
    try:
        units_complete_percentage = gather_wolsen_units_complete_percentage('Wolsen')
    except Exception:
        units_complete_percentage = 0
    percentage_size_min, percentage_num_bedrooms, percentage_floor_plan_image_links = gather_wolsen_fields_percentage(
        'Wolsen')
    wolsen_general_report(select_count, company_count, available_count, round(complete_percentage, 2),
                          round(units_complete_percentage, 2), round(percentage_size_min, 2),
                          round(percentage_num_bedrooms, 2), round(percentage_floor_plan_image_links, 2))


def fill_empty_overall_fields(raw_data):
    units_data = []
    fields_to_keep = ['unit_type', 'price_min', 'available_units', 'size_min', 'size_max', 'price_max',
                      'General']
    for item in raw_data:
        try:
            if item['fields']['General']:
                new_dict = {key: item['fields'][key] for key in fields_to_keep if key in item['fields']}
                units_data.append(new_dict)
        except KeyError:
            continue

    overall_available_units_data = {}
    for item in units_data:
        if 'available_units' in item and 'General' in item and item['General']:
            general_key = item['General'][0]
            if general_key in overall_available_units_data:
                overall_available_units_data[general_key] += item['available_units']
            else:
                overall_available_units_data[general_key] = item['available_units']

    overall_min_unit_size_data = {}
    for item in units_data:
        if 'size_min' in item and 'General' in item and item['General']:
            general_key = item['General'][0]
            if general_key in overall_min_unit_size_data:
                overall_min_unit_size_data[general_key] = min(overall_min_unit_size_data[general_key], item['size_min'])
            else:
                overall_min_unit_size_data[general_key] = item['size_min']

    overall_max_unit_size_data = {}
    for item in units_data:
        if 'size_max' in item and 'General' in item and item['General']:
            general_key = item['General'][0]
            if general_key in overall_max_unit_size_data:
                overall_max_unit_size_data[general_key] = max(overall_max_unit_size_data[general_key], item['size_max'])
            else:
                overall_max_unit_size_data[general_key] = item['size_max']

    for i in units_data:
        try:
            i['overall_min_unit_psf'] = i['price_min'] / i['size_min']
        except Exception:
            pass
        try:
            i['overall_max_unit_psf'] = i['price_max'] / i['size_max']
        except Exception:
            continue
    overall_min_unit_psf_data = {}
    for item in units_data:
        if 'overall_min_unit_psf' in item and 'General' in item and item['General']:
            general_key = item['General'][0]
            if general_key in overall_min_unit_psf_data:
                overall_min_unit_psf_data[general_key] = min(overall_min_unit_psf_data[general_key],
                                                             item['overall_min_unit_psf'])
            else:
                overall_min_unit_psf_data[general_key] = item['overall_min_unit_psf']

    overall_max_unit_psf_data = {}
    for item in units_data:
        if 'overall_max_unit_psf' in item and 'General' in item and item['General']:
            general_key = item['General'][0]
            if general_key in overall_max_unit_psf_data:
                overall_max_unit_psf_data[general_key] = max(overall_max_unit_psf_data[general_key],
                                                             item['overall_max_unit_psf'])
            else:
                overall_max_unit_psf_data[general_key] = item['overall_max_unit_psf']

    overall_min_unit_price = {}
    for item in units_data:
        if 'price_min' in item and 'General' in item and item['General']:
            general_key = item['General'][0]
            if general_key in overall_min_unit_price:
                overall_min_unit_price[general_key] = min(overall_min_unit_price[general_key],
                                                          item['price_min'])
            else:
                overall_min_unit_price[general_key] = item['price_min']

    overall_max_unit_price = {}
    for item in units_data:
        if 'price_max' in item and 'General' in item and item['General']:
            general_key = item['General'][0]
            if general_key in overall_max_unit_price:
                overall_max_unit_price[general_key] = max(overall_max_unit_price[general_key],
                                                          item['price_max'])
            else:
                overall_max_unit_price[general_key] = item['price_max']

    return overall_available_units_data, \
           overall_min_unit_size_data, \
           overall_max_unit_size_data, \
           overall_min_unit_psf_data, \
           overall_max_unit_psf_data, \
           overall_min_unit_price, \
           overall_max_unit_price


def singapore_overall_update():
    print('singapore overall')
    # SAOLA OVERALL
    overall_available_units_data, \
    overall_min_unit_size_data, \
    overall_max_unit_size_data, \
    overall_min_unit_psf_data, \
    overall_max_unit_psf_data, \
    overall_min_unit_price, \
    overall_max_unit_price = fill_empty_overall_fields(gather_units_data())

    for general_id, new_value in overall_available_units_data.items():
        update_airtable_record('keygbB1MnX8GRvpKW', 'app0pXo7PruFurQjq', 'tblJObfY0ty6D34wb', general_id,
                               'overall_available_units', new_value)

    for general_id, new_value in overall_min_unit_size_data.items():
        update_airtable_record('keygbB1MnX8GRvpKW', 'app0pXo7PruFurQjq', 'tblJObfY0ty6D34wb', general_id,
                               'overall_min_unit_size', new_value)

    for general_id, new_value in overall_max_unit_size_data.items():
        update_airtable_record('keygbB1MnX8GRvpKW', 'app0pXo7PruFurQjq', 'tblJObfY0ty6D34wb', general_id,
                               'overall_max_unit_size', new_value)

    for general_id, new_value in overall_min_unit_psf_data.items():
        update_airtable_record('keygbB1MnX8GRvpKW', 'app0pXo7PruFurQjq', 'tblJObfY0ty6D34wb', general_id,
                               'overall_min_unit_psf', new_value)

    for general_id, new_value in overall_max_unit_psf_data.items():
        update_airtable_record('keygbB1MnX8GRvpKW', 'app0pXo7PruFurQjq', 'tblJObfY0ty6D34wb', general_id,
                               'overall_max_unit_psf', new_value)

    for general_id, new_value in overall_min_unit_price.items():
        update_airtable_record('keygbB1MnX8GRvpKW', 'app0pXo7PruFurQjq', 'tblJObfY0ty6D34wb', general_id,
                               'overall_min_unit_price', new_value)

    for general_id, new_value in overall_max_unit_price.items():
        update_airtable_record('keygbB1MnX8GRvpKW', 'app0pXo7PruFurQjq', 'tblJObfY0ty6D34wb', general_id,
                               'overall_max_unit_price', new_value)


def miami_overall_update():
    print('Miami overall')
    # MIAMI
    overall_available_units_data, \
    overall_min_unit_size_data, \
    overall_max_unit_size_data, \
    overall_min_unit_psf_data, \
    overall_max_unit_psf_data, \
    overall_min_unit_price, \
    overall_max_unit_price = fill_empty_overall_fields(gather_miami_units_data())

    for general_id, new_value in overall_available_units_data.items():
        update_airtable_record(
            'pat01eANVrLAmHO9g.d01b80d2e2b1f45656284ce5ec987e5b06393623f54daf907415b0352cf5a0d7', 'app9O58fJIVtHvrHn',
            'tblSdjZ6UKZUQ7FU8', general_id,
            'overall_available_units', new_value)

    for general_id, new_value in overall_min_unit_size_data.items():
        update_airtable_record(
            'pat01eANVrLAmHO9g.d01b80d2e2b1f45656284ce5ec987e5b06393623f54daf907415b0352cf5a0d7', 'app9O58fJIVtHvrHn',
            'tblSdjZ6UKZUQ7FU8', general_id,
            'overall_min_unit_size', new_value)

    for general_id, new_value in overall_max_unit_size_data.items():
        update_airtable_record(
            'pat01eANVrLAmHO9g.d01b80d2e2b1f45656284ce5ec987e5b06393623f54daf907415b0352cf5a0d7', 'app9O58fJIVtHvrHn',
            'tblSdjZ6UKZUQ7FU8', general_id,
            'overall_max_unit_size', new_value)

    for general_id, new_value in overall_min_unit_psf_data.items():
        update_airtable_record(
            'pat01eANVrLAmHO9g.d01b80d2e2b1f45656284ce5ec987e5b06393623f54daf907415b0352cf5a0d7', 'app9O58fJIVtHvrHn',
            'tblSdjZ6UKZUQ7FU8', general_id,
            'overall_min_unit_psf', new_value)

    for general_id, new_value in overall_max_unit_psf_data.items():
        update_airtable_record(
            'pat01eANVrLAmHO9g.d01b80d2e2b1f45656284ce5ec987e5b06393623f54daf907415b0352cf5a0d7', 'app9O58fJIVtHvrHn',
            'tblSdjZ6UKZUQ7FU8', general_id,
            'overall_max_unit_psf', new_value)

    for general_id, new_value in overall_min_unit_price.items():
        update_airtable_record(
            'pat01eANVrLAmHO9g.d01b80d2e2b1f45656284ce5ec987e5b06393623f54daf907415b0352cf5a0d7', 'app9O58fJIVtHvrHn',
            'tblSdjZ6UKZUQ7FU8', general_id,
            'overall_min_unit_price', new_value)

    for general_id, new_value in overall_max_unit_price.items():
        update_airtable_record(
            'pat01eANVrLAmHO9g.d01b80d2e2b1f45656284ce5ec987e5b06393623f54daf907415b0352cf5a0d7', 'app9O58fJIVtHvrHn',
            'tblSdjZ6UKZUQ7FU8', general_id,
            'overall_max_unit_price', new_value)


def dubai_overall_update():
    print('Dubai overall')
    overall_available_units_data, \
    overall_min_unit_size_data, \
    overall_max_unit_size_data, \
    overall_min_unit_psf_data, \
    overall_max_unit_psf_data, \
    overall_min_unit_price, \
    overall_max_unit_price = fill_empty_overall_fields(gather_dubai_units_data())
    print('here')
    for general_id, new_value in overall_available_units_data.items():
        update_airtable_record(
            'patchZXglSCP5RnWW.26392eeef90ff792693a091fa1e8e882881f0cf3cc9c4a719ba7c6bc91b1db25', 'appoHsQ6y9Ff4cWaW',
            'tbl76GHXJbJGdOanH', general_id,
            'overall_available_units', new_value)

    for general_id, new_value in overall_min_unit_size_data.items():
        update_airtable_record(
            'patchZXglSCP5RnWW.26392eeef90ff792693a091fa1e8e882881f0cf3cc9c4a719ba7c6bc91b1db25', 'appoHsQ6y9Ff4cWaW',
            'tbl76GHXJbJGdOanH', general_id,
            'overall_min_unit_size', new_value)

    for general_id, new_value in overall_max_unit_size_data.items():
        update_airtable_record(
            'patchZXglSCP5RnWW.26392eeef90ff792693a091fa1e8e882881f0cf3cc9c4a719ba7c6bc91b1db25', 'appoHsQ6y9Ff4cWaW',
            'tbl76GHXJbJGdOanH', general_id,
            'overall_max_unit_size', new_value)

    for general_id, new_value in overall_min_unit_psf_data.items():
        update_airtable_record(
            'patchZXglSCP5RnWW.26392eeef90ff792693a091fa1e8e882881f0cf3cc9c4a719ba7c6bc91b1db25', 'appoHsQ6y9Ff4cWaW',
            'tbl76GHXJbJGdOanH', general_id,
            'overall_min_unit_psf', new_value)

    for general_id, new_value in overall_max_unit_psf_data.items():
        update_airtable_record(
            'patchZXglSCP5RnWW.26392eeef90ff792693a091fa1e8e882881f0cf3cc9c4a719ba7c6bc91b1db25', 'appoHsQ6y9Ff4cWaW',
            'tbl76GHXJbJGdOanH', general_id,
            'overall_max_unit_psf', new_value)

    for general_id, new_value in overall_min_unit_price.items():
        update_airtable_record(
            'patchZXglSCP5RnWW.26392eeef90ff792693a091fa1e8e882881f0cf3cc9c4a719ba7c6bc91b1db25', 'appoHsQ6y9Ff4cWaW',
            'tbl76GHXJbJGdOanH', general_id,
            'overall_min_unit_price', new_value)

    for general_id, new_value in overall_max_unit_price.items():
        update_airtable_record(
            'patchZXglSCP5RnWW.26392eeef90ff792693a091fa1e8e882881f0cf3cc9c4a719ba7c6bc91b1db25', 'appoHsQ6y9Ff4cWaW',
            'tbl76GHXJbJGdOanH', general_id,
            'overall_max_unit_price', new_value)


def uk_overall_update():
    print('UK overall')
    overall_available_units_data, \
    overall_min_unit_size_data, \
    overall_max_unit_size_data, \
    overall_min_unit_psf_data, \
    overall_max_unit_psf_data, \
    overall_min_unit_price, \
    overall_max_unit_price = fill_empty_overall_fields(gather_uk_units_data())

    for general_id, new_value in overall_available_units_data.items():
        update_airtable_record(
            'patMehsoohn9gsPhO.084730f4e5118c35fcdb70dd3345d4e13e3b15beaed541456cf20ec3140e7795', 'app8DMFTDLafaMcKg',
            'tblR20wKONeGjoqX1', general_id,
            'overall_available_units', new_value)

    for general_id, new_value in overall_min_unit_size_data.items():
        update_airtable_record(
            'patMehsoohn9gsPhO.084730f4e5118c35fcdb70dd3345d4e13e3b15beaed541456cf20ec3140e7795', 'app8DMFTDLafaMcKg',
            'tblR20wKONeGjoqX1', general_id,
            'overall_min_unit_size', new_value)

    for general_id, new_value in overall_max_unit_size_data.items():
        update_airtable_record(
            'patMehsoohn9gsPhO.084730f4e5118c35fcdb70dd3345d4e13e3b15beaed541456cf20ec3140e7795', 'app8DMFTDLafaMcKg',
            'tblR20wKONeGjoqX1', general_id,
            'overall_max_unit_size', new_value)

    for general_id, new_value in overall_min_unit_psf_data.items():
        update_airtable_record(
            'patMehsoohn9gsPhO.084730f4e5118c35fcdb70dd3345d4e13e3b15beaed541456cf20ec3140e7795', 'app8DMFTDLafaMcKg',
            'tblR20wKONeGjoqX1', general_id,
            'overall_min_unit_psf', new_value)

    for general_id, new_value in overall_max_unit_psf_data.items():
        update_airtable_record(
            'patMehsoohn9gsPhO.084730f4e5118c35fcdb70dd3345d4e13e3b15beaed541456cf20ec3140e7795', 'app8DMFTDLafaMcKg',
            'tblR20wKONeGjoqX1', general_id,
            'overall_max_unit_psf', new_value)

    for general_id, new_value in overall_min_unit_price.items():
        update_airtable_record(
            'patMehsoohn9gsPhO.084730f4e5118c35fcdb70dd3345d4e13e3b15beaed541456cf20ec3140e7795', 'app8DMFTDLafaMcKg',
            'tblR20wKONeGjoqX1', general_id,
            'overall_min_unit_price', new_value)

    for general_id, new_value in overall_max_unit_price.items():
        update_airtable_record(
            'patMehsoohn9gsPhO.084730f4e5118c35fcdb70dd3345d4e13e3b15beaed541456cf20ec3140e7795', 'app8DMFTDLafaMcKg',
            'tblR20wKONeGjoqX1', general_id,
            'overall_max_unit_price', new_value)


def bali_m_overall_update():
    print('Bali MARV overall')
    overall_available_units_data, \
    overall_min_unit_size_data, \
    overall_max_unit_size_data, \
    overall_min_unit_psf_data, \
    overall_max_unit_psf_data, \
    overall_min_unit_price, \
    overall_max_unit_price = fill_empty_overall_fields(gather_uk_units_data())

    for general_id, new_value in overall_available_units_data.items():
        update_airtable_record(
            'patZ36V2m2fbzEGCr.3f90bb8375f018885977f3dd3e761da6915e1e5ab9be493ab1bfd9c6437e670c', 'app55xAPfpJD3zubt',
            'tblOuLrGqrN4cbIoe', general_id,
            'overall_available_units', new_value)

    for general_id, new_value in overall_min_unit_size_data.items():
        update_airtable_record(
            'patZ36V2m2fbzEGCr.3f90bb8375f018885977f3dd3e761da6915e1e5ab9be493ab1bfd9c6437e670c', 'app55xAPfpJD3zubt',
            'tblOuLrGqrN4cbIoe', general_id,
            'overall_min_unit_size', new_value)

    for general_id, new_value in overall_max_unit_size_data.items():
        update_airtable_record(
            'patZ36V2m2fbzEGCr.3f90bb8375f018885977f3dd3e761da6915e1e5ab9be493ab1bfd9c6437e670c', 'app55xAPfpJD3zubt',
            'tblOuLrGqrN4cbIoe', general_id,
            'overall_max_unit_size', new_value)

    for general_id, new_value in overall_min_unit_psf_data.items():
        update_airtable_record(
            'patZ36V2m2fbzEGCr.3f90bb8375f018885977f3dd3e761da6915e1e5ab9be493ab1bfd9c6437e670c', 'app55xAPfpJD3zubt',
            'tblOuLrGqrN4cbIoe', general_id,
            'overall_min_unit_psf', new_value)

    for general_id, new_value in overall_max_unit_psf_data.items():
        update_airtable_record(
            'patZ36V2m2fbzEGCr.3f90bb8375f018885977f3dd3e761da6915e1e5ab9be493ab1bfd9c6437e670c', 'app55xAPfpJD3zubt',
            'tblOuLrGqrN4cbIoe', general_id,
            'overall_max_unit_psf', new_value)

    for general_id, new_value in overall_min_unit_price.items():
        update_airtable_record(
            'patZ36V2m2fbzEGCr.3f90bb8375f018885977f3dd3e761da6915e1e5ab9be493ab1bfd9c6437e670c', 'app55xAPfpJD3zubt',
            'tblOuLrGqrN4cbIoe', general_id,
            'overall_min_unit_price', new_value)

    for general_id, new_value in overall_max_unit_price.items():
        update_airtable_record(
            'patZ36V2m2fbzEGCr.3f90bb8375f018885977f3dd3e761da6915e1e5ab9be493ab1bfd9c6437e670c', 'app55xAPfpJD3zubt',
            'tblOuLrGqrN4cbIoe', general_id,
            'overall_max_unit_price', new_value)


def bali_i_overall_update():
    print('Bali Intermark overall')
    overall_available_units_data, \
    overall_min_unit_size_data, \
    overall_max_unit_size_data, \
    overall_min_unit_psf_data, \
    overall_max_unit_psf_data, \
    overall_min_unit_price, \
    overall_max_unit_price = fill_empty_overall_fields(gather_uk_units_data())

    for general_id, new_value in overall_available_units_data.items():
        update_airtable_record(
            'pat8aakZFTgNDCTlV.76cbff7a26e4da92dcf48b3115eadf8c7004fd1792748aeedcddfc192edfffb5', 'apptB9TeBEoVa9djt',
            'tblc0nK5MGsmjLrwe', general_id,
            'overall_available_units', new_value)

    for general_id, new_value in overall_min_unit_size_data.items():
        update_airtable_record(
            'pat8aakZFTgNDCTlV.76cbff7a26e4da92dcf48b3115eadf8c7004fd1792748aeedcddfc192edfffb5', 'apptB9TeBEoVa9djt',
            'tblc0nK5MGsmjLrwe', general_id,
            'overall_min_unit_size', new_value)

    for general_id, new_value in overall_max_unit_size_data.items():
        update_airtable_record(
            'pat8aakZFTgNDCTlV.76cbff7a26e4da92dcf48b3115eadf8c7004fd1792748aeedcddfc192edfffb5', 'apptB9TeBEoVa9djt',
            'tblc0nK5MGsmjLrwe', general_id,
            'overall_max_unit_size', new_value)

    for general_id, new_value in overall_min_unit_psf_data.items():
        update_airtable_record(
            'pat8aakZFTgNDCTlV.76cbff7a26e4da92dcf48b3115eadf8c7004fd1792748aeedcddfc192edfffb5', 'apptB9TeBEoVa9djt',
            'tblc0nK5MGsmjLrwe', general_id,
            'overall_min_unit_psf', new_value)

    for general_id, new_value in overall_max_unit_psf_data.items():
        update_airtable_record(
            'pat8aakZFTgNDCTlV.76cbff7a26e4da92dcf48b3115eadf8c7004fd1792748aeedcddfc192edfffb5', 'apptB9TeBEoVa9djt',
            'tblc0nK5MGsmjLrwe', general_id,
            'overall_max_unit_psf', new_value)

    for general_id, new_value in overall_min_unit_price.items():
        update_airtable_record(
            'pat8aakZFTgNDCTlV.76cbff7a26e4da92dcf48b3115eadf8c7004fd1792748aeedcddfc192edfffb5', 'apptB9TeBEoVa9djt',
            'tblc0nK5MGsmjLrwe', general_id,
            'overall_min_unit_price', new_value)

    for general_id, new_value in overall_max_unit_price.items():
        update_airtable_record(
            'pat8aakZFTgNDCTlV.76cbff7a26e4da92dcf48b3115eadf8c7004fd1792748aeedcddfc192edfffb5', 'apptB9TeBEoVa9djt',
            'tblc0nK5MGsmjLrwe', general_id,
            'overall_max_unit_price', new_value)


def call_overall_scripts():
    uk_overall_update()
    miami_overall_update()
    singapore_overall_update()
    dubai_overall_update()
    bali_m_overall_update()
    bali_i_overall_update()
