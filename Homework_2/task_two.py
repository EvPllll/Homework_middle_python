import time, sqlite3
from sqlite3 import Error

import requests


def get_and_write_data_in_db(connect: sqlite3,
                             cursor: sqlite3,
                             table_name: str = 'vacancies_api',
                             url="https://api.hh.ru/vacancies") -> None:
    '''
    Функция парсит ресурс посредством API, получает данные и записывает их в БД
    :param connect: коннект с БД
    :param cursor: курсов присоединённой БД
    :param table_name: название таблицы
    :param url: адрес ресурса для парсинга
    :return: функция ничего не возвращает
    '''

    count = 1

    for page in range(0, 10):
        url_params = url_params = {"text": "middle python",
                                   "search_field": "name",
                                   "per_page": "100",
                                   "page": f"{str(page)}"}

        result = requests.get(url, params=url_params)
        vacancies = result.json().get('items')

        for i, vacancy in enumerate(vacancies):
            try:
                result = requests.get(vacancy['url'])
                vacancy = result.json()

                key_skills_list = []

                company_name = vacancy['employer']['name']
                position = vacancy['name']
                job_description = vacancy['description']
                key_skills = vacancy['key_skills']
                for skill in key_skills:
                    skill = skill['name']
                    key_skills_list.append(skill)
                key_skills_for_write = ', '.join(key_skills_list)

                if key_skills_for_write == '':  # если нет ключевых навыков, то пропускаем вакансию
                    continue
                else:
                    # Пусть записывает топ скиллы в текстовый файл.
                    # Для ДЗ3 напишу функцию, которая прочитает файл и посчитает топ скиллов.
                    with open('Key_skills.txt', 'a', encoding='UTF-8') as file:
                        file.write(key_skills_for_write)

                    insert_data_operation = f'''INSERT INTO {table_name} (id,
                                                                          company_name,
                                                                          position,
                                                                          job_description,
                                                                          key_skills)
                                                VALUES (?, ?, ?, ?, ?);'''

                    cursor.execute(insert_data_operation, (count,
                                                           company_name,
                                                           position,
                                                           job_description,
                                                           key_skills_for_write))

                    connect.commit()
                    print(f'Данные успешно записаны в БД. Количество записей: {count}')
                    count += 1

                    if count == 101: # прекращаем запись после сотой вакансии
                        return

                    time.sleep(0.5)

            except (Exception, Error) as error:
                pass


def get_connect_to_db_and_go_working(db_name: str = 'hw2.db') -> None:
    '''
    Соединяется с БД, создаёт таблицу и начинает работу по парсингу и записи данных в таблицу.
    :param db_name: Название БД
    :return: Функция ничего не возвращает
    '''
    try:
        connect = sqlite3.connect(db_name)
        cursor = connect.cursor()

        print(f'Соединение с БД {db_name} успешно установлено!')

        table_name = 'vacancies_api'
        create_table_operation = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (id bigint,
                                                 company_name varchar,
                                                 position varchar,
                                                 job_description text,
                                                 key_skills varchar);'''
        cursor.execute(create_table_operation)
        print(f'Таблица {table_name} успешно создана в БД {db_name}!')

        get_and_write_data_in_db(connect, cursor)

    except (Exception, Error) as error:
        print(f'Ошибка: {error}')

    finally:
        if connect:
            cursor.close()
            connect.close()
            print(f'Соединение с БД {db_name} закрыто!')


if __name__ == '__main__':
    get_connect_to_db_and_go_working()