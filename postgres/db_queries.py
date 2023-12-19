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


def get_available_condos_count(company):
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


def get_price_condos_count(company):
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    query = """
                SELECT COUNT(*) FROM general
                WHERE %s = ANY(companies) AND overall_min_unit_price > 0
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
