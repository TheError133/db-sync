import json
from os.path import join

import postgres as pg
import vertica as ve
from file import get_script_content

PG_SCRIPT_FOLDER = join('scripts', 'pg')
VE_SCRIPT_FOLDER = join('scripts', 've')


def get_source_data() -> list:
    sql = get_script_content(join(PG_SCRIPT_FOLDER, 'get_data.sql'))
    return pg.execute_script(sql)
