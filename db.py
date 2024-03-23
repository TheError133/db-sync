import json
from os.path import join

import postgres as pg
import vertica as ve
from file import get_script_content
from misc import append_hash_to_data

PG_SCRIPT_FOLDER = join('scripts', 'pg')
VE_SCRIPT_FOLDER = join('scripts', 've')


def get_source_data() -> list:
    sql = get_script_content(join(PG_SCRIPT_FOLDER, 'get_data.sql'))
    return append_hash_to_data(pg.execute_script(sql))
