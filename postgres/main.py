from postgres.amenities import gather_amenities_data, save_amenities_data, prepare_amenities_data, \
    delete_old_amenities_data
from postgres.general import gather_main_data, prepare_main_data, save_main_data, delete_old_main_data
from postgres.units import gather_units_data, prepare_units_data, save_units_data, delete_old_units_data


def postgres_integration():
    print('Postgres recording started')

    # general
    try:
        main_data = gather_main_data()
        main_data_to_save = prepare_main_data(main_data)
        save_main_data(main_data_to_save)
        delete_old_main_data()
        print('general table updated')
    except Exception as e:
        print(e)
        pass

    # units
    try:
        units_data = gather_units_data()
        units_data_to_save = prepare_units_data(units_data)
        delete_old_units_data()
        save_units_data(units_data_to_save)
        print('units table updated')
    except Exception as e:
        print(e)
        pass

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
