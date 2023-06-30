import json

import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'my_new_db'

    params = config()
    conn = None

    create_database(db_name, params)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(db_name, params: dict) -> None:
    """Создает новую базу данных."""
    conn = psycopg2.connect(dbname = 'postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute (f'DROP DATABASE {db_name}')
    cur.execute(f'CREATE DATABASE {db_name}')

    cur.close()
    conn.close()

def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""

    with open (script_file, 'r') as f:
        sql_script = f.read()
    conn = psycopg2.connect (
        host="localhost",
        dbname="my_new_db",
        user="postgres",
        password="Bension1904++")
    cur = conn.cursor()
    cur.execute(sql_script)
    conn.commit ( )
    cur.close()
    conn.close ( )

def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    conn = psycopg2.connect (
        host="localhost",
        dbname="my_new_db",
        user="postgres",
        password="Bension1904++")
    cur = conn.cursor( )
    cur.execute("""
            CREATE TABLE suppliers (
                company_name VARCHAR NOT NULL,
                contact VARCHAR,
                address VARCHAR,
                phone VARCHAR,
                fax VARCHAR,
                homepage VARCHAR,
                products VARCHAR
                
            )
        """)
    conn.commit ( )
    cur.close()
    conn.close ( )


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    modified_data = []
    all_data = []
    final_data = []
    superfinal_data = []
    with open (json_file, 'r') as f:
      data_json = json.load(f)
      count = 0
      count1 = 0
      count2 = 0
      for row in data_json:
        row_list_data = list(row.values ( ))
        all_data.append(row_list_data[0:6])
        modified_data.append(row_list_data[6:][0])
        while count1 < len(modified_data[count]):
            final = all_data[count] + modified_data[count][count1].split(",")
            count1 += 1
            final_data.append(final)
        count += 1

        superfinal_data.append(final_data)

    return superfinal_data

print(len(get_suppliers_data('suppliers.json')))
print(get_suppliers_data('suppliers.json'))


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    conn = psycopg2.connect (
        host="localhost",
        dbname="my_new_db",
        user="postgres",
        password="Bension1904++")
    cur = conn.cursor ( )
    cur.executemany("INSERT INTO suppliers(company_name, contact, address, phone, fax, homepage, products) VALUES(%s,%s,%s,%s,%s,%s,%s)", get_suppliers_data('suppliers.json'))
    conn.commit ( )
    cur.close()
    conn.close ( )


def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    pass


if __name__ == '__main__':
    main()
