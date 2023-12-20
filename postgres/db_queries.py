import psycopg2

from postgres.db import db_params


def get_new_condos():
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
    SELECT COUNT(*) FROM general
    WHERE city IN ('Dubai', 'Miami', 'Singapore')
    """

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0]
    except psycopg2.Error as e:
        connection.rollback()
        print("Ошибка при вставке записей:", e)
    finally:
        cursor.close()
        connection.close()


def get_available_condos():
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
        SELECT COUNT(*) FROM general
        WHERE city IN ('Dubai', 'Miami', 'Singapore') AND overall_available_units > 0
        """

    try:
        cursor.execute(query)
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
    (SELECT COUNT(*) * 5 FROM units WHERE general_id IN (SELECT id FROM general WHERE overall_available_units > 0 AND %s = ANY(companies))) AS percentage_filled
FROM 
    units 
WHERE 
    general_id IN (SELECT id FROM general WHERE overall_available_units > 0 AND %s = ANY(companies))
                
    """
    cursor.execute(query, (company, company,))
    result = cursor.fetchone()
    percentage_filled = result[0] if result else None

    cursor.close()
    connection.close()

    return percentage_filled
