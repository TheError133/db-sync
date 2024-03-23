import vertica_python as vp

CREDS = {
    'dbname': 'vmart',
    'user': 'dbadmin',
    'password': 'dbadmin',
    'port': 5433
}


def execute_script(sql: str, fetch: bool = True) -> list | None:
    with vp.connect(**CREDS) as connection, connection.cursor() as cursor:
        cursor.execute(sql)
        if fetch:
            rows = cursor.fetchall()
            columns = [x[0] for x in cursor.description]
            data = [{col: val for col, val in zip(columns, row)} for row in rows]
            return data
