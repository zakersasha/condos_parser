import psycopg2

from postgres.db import db_params


def get_brochure_condos_list(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
        SELECT name
        FROM general
        WHERE brochure IS NULL
        AND %s = ANY(companies)
    """

    try:
        cursor.execute(query, (company,))
        result = cursor.fetchall()
        return [row[0] for row in result]
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def get_new_condos(city):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
    SELECT COUNT(*) FROM general
    WHERE city = %s
    """

    try:
        cursor.execute(query, (city,))
        result = cursor.fetchone()
        return result[0]
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def get_available_condos(city):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
        SELECT COUNT(*) FROM general
        WHERE city = %s AND overall_available_units > 0
        """

    try:
        cursor.execute(query, (city,))
        result = cursor.fetchone()
        return result[0]
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при выполнении запроса:", e)
    finally:
        cursor.close()
        connection.close()


def get_condos_count(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
                SELECT COUNT(*) FROM general
                WHERE %s = ANY(selected)
                """

    try:
        cursor.execute(query, (company,))
        result = cursor.fetchone()
        return result[0]
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при выполнении запроса:", e)
    finally:
        cursor.close()
        connection.close()


def get_available_condos_count(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
                SELECT COUNT(*) FROM general
                WHERE %s = ANY(selected) AND overall_available_units > 0
                """

    try:
        cursor.execute(query, (company,))
        result = cursor.fetchone()
        return result[0]
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при выполнении запроса:", e)
    finally:
        cursor.close()
        connection.close()


def get_price_condos_count(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
                SELECT COUNT(*) FROM general
                WHERE %s = ANY(selected) AND overall_min_unit_price > 0
                """

    try:
        cursor.execute(query, (company,))
        result = cursor.fetchone()
        return result[0]
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при выполнении запроса:", e)
    finally:
        cursor.close()
        connection.close()


def gather_select_count(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
                    SELECT COUNT(*) FROM general
                    WHERE %s = ANY(selected)
                    """

    try:
        cursor.execute(query, (company,))
        result = cursor.fetchone()
        return result[0]
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при выполнении запроса:", e)
    finally:
        cursor.close()
        connection.close()


def gather_company_count(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
                    SELECT COUNT(*) FROM general
                    WHERE %s = ANY(companies)
                    """

    try:
        cursor.execute(query, (company,))
        result = cursor.fetchone()
        return result[0]
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при выполнении запроса:", e)
    finally:
        cursor.close()
        connection.close()


def gather_available_count(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
                        SELECT COUNT(*) FROM general
                        WHERE %s = ANY(companies) AND overall_available_units > 0 
                        """

    try:
        cursor.execute(query, (company,))
        result = cursor.fetchone()
        return result[0]
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при выполнении запроса:", e)
    finally:
        cursor.close()
        connection.close()


def gather_complete_percentage(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
                SELECT 
                    (COALESCE(address::text, '') <> '')::int +
                    (COALESCE(district::text, '') <> '')::int +
                    (overall_available_units > 0)::int +
                    (COALESCE(date_of_completion::text, '') <> '')::int +
                    (COALESCE(facilities::text, '') <> '')::int +
                    (COALESCE(overall_min_unit_size::text, '') <> '')::int +
                    (COALESCE(overall_min_unit_psf::text, '') <> '')::int +
                    (COALESCE(overall_min_unit_price::text, '') <> '')::int +
                    (COALESCE(description::text, '') <> '')::int +
                    (COALESCE(payment_plans::text, '') <> '')::int +
                    (COALESCE(units::text, '') <> '')::int AS filled_fields,
                    11 AS total_fields,
                    ((COALESCE(address::text, '') <> '')::int +
                    (COALESCE(district::text, '') <> '')::int +
                    (overall_available_units > 0)::int +
                    (COALESCE(date_of_completion::text, '') <> '')::int +
                    (COALESCE(facilities::text, '') <> '')::int +
                    (COALESCE(overall_min_unit_size::text, '') <> '')::int +
                    (COALESCE(overall_min_unit_psf::text, '') <> '')::int +
                    (COALESCE(overall_min_unit_price::text, '') <> '')::int +
                    (COALESCE(description::text, '') <> '')::int +
                    (COALESCE(payment_plans::text, '') <> '')::int +
                    (COALESCE(units::text, '') <> '')::int) * 100.0 / 11 AS percentage_filled
                FROM 
                    general
                WHERE 
                    %s = ANY(companies) AND overall_available_units > 0 
            """

    cursor.execute(query, (company,))
    result = cursor.fetchone()
    percentage_filled = result[2]

    cursor.close()
    connection.close()

    return percentage_filled


def gather_complete_percentage_no_units(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
                SELECT 
                    (COALESCE(address::text, '') <> '')::int +
                    (COALESCE(district::text, '') <> '')::int +
                    (COALESCE(units_number::text, '') <> '')::int +
                    (COALESCE(date_of_completion::text, '') <> '')::int +
                    (COALESCE(facilities::text, '') <> '')::int +
                    (COALESCE(overall_min_unit_size::text, '') <> '')::int +
                    (COALESCE(overall_min_unit_psf::text, '') <> '')::int +
                    (COALESCE(overall_min_unit_price::text, '') <> '')::int +
                    (COALESCE(description::text, '') <> '')::int +
                    (COALESCE(payment_plans::text, '') <> '')::int +
                    (COALESCE(units::text, '') <> '')::int AS filled_fields,
                    11 AS total_fields,
                    ((COALESCE(address::text, '') <> '')::int +
                    (COALESCE(district::text, '') <> '')::int +
                    (COALESCE(units_number::text, '') <> '')::int +
                    (COALESCE(date_of_completion::text, '') <> '')::int +
                    (COALESCE(facilities::text, '') <> '')::int +
                    (COALESCE(overall_min_unit_size::text, '') <> '')::int +
                    (COALESCE(overall_min_unit_psf::text, '') <> '')::int +
                    (COALESCE(overall_min_unit_price::text, '') <> '')::int +
                    (COALESCE(description::text, '') <> '')::int +
                    (COALESCE(payment_plans::text, '') <> '')::int +
                    (COALESCE(units::text, '') <> '')::int) * 100.0 / 11 AS percentage_filled
                FROM 
                    general
                WHERE 
                    %s = ANY(companies)
            """

    cursor.execute(query, (company,))
    result = cursor.fetchone()
    percentage_filled = result[2]

    cursor.close()
    connection.close()

    return percentage_filled


def gather_units_complete_percentage(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    query = """
            SELECT 
    (COUNT(units.size_min) + COUNT(units.price_min) + COUNT(units.psf_min) + COUNT(units.num_bedrooms) + COUNT(units.floor_plan_image_links)) * 100.0 / 
    (SELECT COUNT(*) * 5 FROM units WHERE general_id IN (SELECT id FROM general WHERE %s = ANY(companies))) AS percentage_filled
FROM 
    units 
WHERE 
    general_id IN (SELECT id FROM general WHERE %s = ANY(companies))
                
    """
    cursor.execute(query, (company, company,))
    result = cursor.fetchone()
    percentage_filled = result[0] if result else None

    cursor.close()
    connection.close()

    return percentage_filled


def gather_wolsen_units_complete_percentage(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    query = """
            SELECT 
    (COUNT(units.size_min) + COUNT(units.num_bedrooms) + COUNT(units.floor_plan_image_links)) * 100.0 / 
    (SELECT COUNT(*) * 3 FROM units WHERE general_id IN (SELECT id FROM general WHERE %s = ANY(companies))) AS percentage_filled
FROM 
    units 
WHERE 
    general_id IN (SELECT id FROM general WHERE %s = ANY(companies))

    """
    cursor.execute(query, (company, company,))
    result = cursor.fetchone()
    percentage_filled = result[0] if result else None

    cursor.close()
    connection.close()

    return percentage_filled


def gather_wolsen_fields_percentage(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM units WHERE general_id IN (SELECT id FROM general WHERE %s = ANY(companies))",
            (company,))
        total_units = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(size_min) FROM units WHERE general_id IN (SELECT id FROM general WHERE %s = ANY(companies)) AND size_min IS NOT NULL",
            (company,))
        filled_size_min = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(num_bedrooms) FROM units WHERE general_id IN (SELECT id FROM general WHERE %s = ANY(companies)) AND num_bedrooms IS NOT NULL",
            (company,))
        filled_num_bedrooms = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(floor_plan_image_links) FROM units WHERE general_id IN (SELECT id FROM general WHERE %s = ANY(companies)) AND floor_plan_image_links IS NOT NULL",
            (company,))
        filled_floor_plan_image_links = cursor.fetchone()[0]

        percentage_size_min = (filled_size_min / total_units) * 100 if total_units > 0 else 0
        percentage_num_bedrooms = (filled_num_bedrooms / total_units) * 100 if total_units > 0 else 0
        percentage_floor_plan_image_links = (
                                                    filled_floor_plan_image_links / total_units) * 100 if total_units > 0 else 0

        cursor.close()
        connection.close()
        return percentage_size_min, percentage_num_bedrooms, percentage_floor_plan_image_links

    except psycopg2.Error as e:
        print("Ошибка при работе с базой данных:", e)


def gather_other_fields_percentage(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM units WHERE general_id IN (SELECT id FROM general WHERE %s = ANY(companies))",
            (company,))
        total_units = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(size_min) FROM units WHERE general_id IN (SELECT id FROM general WHERE %s = ANY(companies)) AND size_min IS NOT NULL",
            (company,))
        filled_size_min = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(num_bedrooms) FROM units WHERE general_id IN (SELECT id FROM general WHERE %s = ANY(companies)) AND num_bedrooms IS NOT NULL",
            (company,))
        filled_num_bedrooms = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(floor_plan_image_links) FROM units WHERE general_id IN (SELECT id FROM general WHERE %s = ANY(companies)) AND floor_plan_image_links IS NOT NULL",
            (company,))
        filled_floor_plan_image_links = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(price_min) FROM units WHERE general_id IN (SELECT id FROM general WHERE %s = ANY(companies)) AND price_min IS NOT NULL",
            (company,))
        filled_price_min = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(psf_min) FROM units WHERE general_id IN (SELECT id FROM general WHERE %s = ANY(companies)) AND psf_min IS NOT NULL",
            (company,))
        filled_psf_min = cursor.fetchone()[0]

        percentage_size_min = (filled_size_min / total_units) * 100 if total_units > 0 else 0
        percentage_num_bedrooms = (filled_num_bedrooms / total_units) * 100 if total_units > 0 else 0
        percentage_floor_plan_image_links = (
                                                    filled_floor_plan_image_links / total_units) * 100 if total_units > 0 else 0
        percentage_price_min = (filled_price_min / total_units) * 100 if total_units > 0 else 0
        percentage_psf_min = (filled_psf_min / total_units) * 100 if total_units > 0 else 0

        cursor.close()
        connection.close()
        return percentage_size_min, percentage_num_bedrooms, percentage_floor_plan_image_links, percentage_price_min, percentage_psf_min

    except psycopg2.Error as e:
        print("Ошибка при работе с базой данных:", e)


def k7_general_fields_percentage(company):
    connection = psycopg2.connect(**db_params)

    fields_to_check = [
        'Address', 'District', 'overall_available_units', 'date_of_completion', 'facilities',
        'overall_min_unit_size', 'overall_min_unit_psf', 'overall_min_unit_price', 'description',
        'payment_plans', 'Units'
    ]

    completion_percentages = []
    for field in fields_to_check:
        query = f"SELECT (COUNT(*) FILTER (WHERE {field} IS NOT NULL) * 100.0) / COUNT(*) AS completion_percentage FROM general WHERE %s = ANY(companies)"
        cursor = connection.cursor()
        cursor.execute(query, (company,))

        completion_percentage = cursor.fetchone()[0]
        completion_percentages.append(completion_percentage)

        cursor.close()
    connection.close()

    return completion_percentages


def w_general_fields_percentage(company):
    connection = psycopg2.connect(**db_params)

    fields_to_check = [
        'Address', 'District', 'units_number', 'date_of_completion', 'facilities',
        'overall_min_unit_size', 'overall_min_unit_psf', 'overall_min_unit_price', 'description',
        'payment_plans', 'payment_plans_attached', 'Units'
    ]

    completion_percentages = []
    for field in fields_to_check:
        query = f"SELECT (COUNT(*) FILTER (WHERE {field} IS NOT NULL) * 100.0) / COUNT(*) AS completion_percentage FROM general WHERE %s = ANY(companies)"
        cursor = connection.cursor()
        cursor.execute(query, (company,))

        completion_percentage = cursor.fetchone()[0]
        completion_percentages.append(completion_percentage)

        cursor.close()
    connection.close()

    return completion_percentages
