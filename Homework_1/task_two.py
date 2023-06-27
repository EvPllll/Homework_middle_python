import zipfile, json, sqlite3
from sqlite3 import Error


def write_data_to_db(connection: sqlite3, cursor: sqlite3, table_name: str, zip_name: str = 'egrul.json.zip') -> None:
    '''
    Читает файлы архива и записывает нужные данные в БД
    :param cursor: курсор присоединённой БД
    :param table_name: название таблицы в которую будет осуществляться запись данных
    :param zip_name: название архива с файлами JSON
    :return: функция ничего не возвращает
    '''
    count = 0
    with zipfile.ZipFile(zip_name, 'r') as zip_object:
        name_list = zip_object.namelist()
        for name in name_list:
            with zip_object.open(name) as file:
                json_data = file.read()
                data = json.loads(json_data)
                for item in data:
                    try:
                        if item['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'][0:2] == '61':
                            if item['inn'] == "":
                                item['inn'] = 0
                            insert_data_to_db = f'''
                            INSERT INTO {table_name} (code, inn, name, kpp, orgn)
                            VALUES (?, ?, ?, ?, ?)'''
                            row = (item['data']['СвОКВЭД']['СвОКВЭДОсн']['КодОКВЭД'],
                                   item['inn'],
                                   item['full_name'],
                                   item['kpp'],
                                   item['ogrn'])
                            cursor.executemany(insert_data_to_db, (row,))
                            count += 1
                            print(f'Данные успешно записаны в БД. Всего записей: {count}.')
                    except (Exception, Error) as error:
                        pass

            connection.commit()


def work_with_db():
    '''
    Осуществляет соединение с БД, получение данных и их запись в БД
    :return: функция ничего не возвращает
    '''
    try:
        connection = sqlite3.connect('hw1.db')
        cursor = connection.cursor()

        table_name = 'telecom_companies'
        create_table_script = f'''CREATE TABLE IF NOT EXISTS {table_name} (code varchar,
                                                             inn bigint,
                                                             name varchar,
                                                             kpp bigint,
                                                             orgn bigint);'''
        cursor.execute(create_table_script)
        write_data_to_db(connection, cursor, table_name)
    except (Exception, Error) as error:
        print(f'Ошибка: {error}')
    finally:
        if connection:
            cursor.close()
            connection.close()
            print('Соединение с БД закрыто.')


if __name__ == '__main__':
    work_with_db()