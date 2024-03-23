import postgres as pg
import vertica as ve

if __name__ == '__main__':
    sql = 'select 1 as val'
    data_pg = pg.execute_script(sql)
    data_ve = ve.execute_script(sql)

    print()
