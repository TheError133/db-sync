import psycopg2 as pg

CREDS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'port': 5432
}


def execute_script(sql: str, fetch: bool = True) -> list | None:
    with pg.connect(**CREDS) as connection, connection.cursor() as cursor:
        cursor.execute(sql)
        if fetch:
            rows = cursor.fetchall()
            columns = [x[0] for x in cursor.description]
            data = [{col: val for col, val in zip(columns, row)} for row in rows]
            return data
