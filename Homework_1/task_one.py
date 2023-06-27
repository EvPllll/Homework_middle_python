import json
import sqlite3
from sqlite3 import Error

def get_data(file_name: str) -> list:
    '''
    Читает файл JSON и возвращает данные
    :param file_name: название файла JSON
    :return: содержимое файла JSON
    '''
    with open(file_name, 'r', encoding='UTF-8') as file:
        return json.load(file)


def work_with_db():
    '''
    Соединяется с БД, создаёт таблицу и заполняет её данными из файла JSON
    :return: функция ничего не возвращает
    '''
    try:
        db_name = 'hw1.db'
        connection = sqlite3.connect(db_name)
        cursor = connection.cursor()
        print('Соединение с БД успешно!')

        table_name = 'okved'
        create_table = f'''CREATE TABLE IF NOT EXISTS {table_name} (code varchar,
                                                                    parent_code varchar,
                                                                    section varchar,
                                                                    name varchar,
                                                                    comment varchar);'''
        cursor.execute(create_table)
        print(f'Таблица {table_name} успешно создана в БД {db_name}!')

        data = get_data('okved_2.json')

        insert_data = '''INSERT INTO okved (code, 
                                            parent_code, 
                                            section, 
                                            name, 
                                            comment) 
                         VALUES (:code,
                                 :parent_code,
                                 :section,
                                 :name,
                                 :comment);'''
        cursor.executemany(insert_data, data)
        print(f'Данные успешно записаны в таблицу {table_name} БД {db_name}')

        connection.commit()

    except (Exception, Error) as error:
        print(f'Ошибка: {error}')

    finally:
        if connection:
            cursor.close()
            connection.close()
            print(f'Соединение с БД {db_name} закрыто!')


if __name__ == '__main__':
    work_with_db()