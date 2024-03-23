from postgres import execute_script


if __name__ == '__main__':
    data = execute_script('select 1 as val')

    print()
