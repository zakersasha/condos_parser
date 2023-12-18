from postgres.amenities import gather_amenities_data, save_amenities_data, prepare_amenities_data, \
    delete_old_amenities_data
from postgres.general import gather_main_data, prepare_main_data, save_main_data, delete_old_main_data, \
    gather_miami_main_data, prepare_miami_main_data, save_miami_main_data, gather_uk_main_data, prepare_uk_main_data, \
    save_uk_main_data, gather_dubai_main_data, prepare_dubai_main_data, save_dubai_main_data, get_all_records, \
    gather_oman_main_data, prepare_oman_main_data, save_oman_main_data
from postgres.units import gather_units_data, prepare_units_data, save_units_data, delete_old_units_data, \
    gather_miami_units_data, prepare_miami_units_data, save_miami_units_data, gather_uk_units_data, save_uk_units_data, \
    prepare_uk_units_data, gather_dubai_units_data, prepare_dubai_units_data, save_dubai_units_data, \
    delete_units_with_no_general


def postgres_integration():
    print('Postgres recording started')

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
    print('miami general table updated')

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
    # try:
    #     amenities_data = gather_amenities_data()
    #     amenities_data_to_save = prepare_amenities_data(amenities_data)
    #     delete_old_amenities_data()
    #     save_amenities_data(amenities_data_to_save)
    #     print('amenities table updated')
    # except Exception as e:
    #     print(e)
    #     pass
postgres_integration()