import json
from os.path import join
from datetime import datetime

import postgres as pg
import vertica as ve
from file import get_script_content
from misc import append_hash_to_data, dump_json_values

PG_SCRIPT_FOLDER = join('scripts', 'pg')
VE_SCRIPT_FOLDER = join('scripts', 've')


def get_source_data() -> list:
    sql = get_script_content(join(PG_SCRIPT_FOLDER, 'get_data.sql'))
    batch_id = 'test.entity|' + datetime.now().strftime('%Y%m%d%H%M%S')
    return dump_json_values(
        append_hash_to_data(
            pg.execute_script(sql),
            {'__batch_id': batch_id}
        )
    )
