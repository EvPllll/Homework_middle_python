import time, sqlite3
from sqlite3 import Error

import requests
from bs4 import BeautifulSoup


def go_parsing_and_write_data_in_db(connect: sqlite3,
                                    cursor: sqlite3,
                                    table_name: str = 'vacancies',
                                    url: str = 'https://hh.ru/search/vacancy') -> None:
    '''
    Отправляет запросы на hh.ru, получает данные вакансий и осуществляет запись в БД
    :param connect: соединение с БД
    :param cursor: курсор присоединённой БД
    :param table_name: название таблицы в которую осуществляется запись данных
    :param url: адрес ресурса для парсинга данных
    :return:функция ничего не возвращает
    '''

    user_agent = {'User-agent': 'Mozilla/5.0'}

    count = 1

    for i in range(0, 10): # десяти страниц должно быть достаточно, что бы найти 20 вакансий с ключевыми навыками

        url_params = {
            "text": "middle python",
            "search_field": "name",
            "per_page": "20",
            "page": f"{str(i)}" # странички перелистываются с помощью переменной i цикле for
            }

        result = requests.get(url, headers=user_agent, params=url_params)

        soup = BeautifulSoup(result.content.decode(), 'lxml')
        vacancies = soup.find_all('a', attrs={'data-qa': 'serp-item__title'})

        for vacancy in vacancies:
            try:
                list_of_key_skills_for_vacancy = [] # сюда будет осуществляться запись ключевых навыков вакансии

                vacancy_url = vacancy.get('href')
                result = requests.get(vacancy_url, headers=user_agent)
                data = BeautifulSoup(result.content.decode(), 'lxml')

                position = data.find('h1').text
                company_name = data.find('a', attrs={'data-qa': 'vacancy-company-name'}).text
                job_description = data.find('div', attrs={'data-qa': 'vacancy-description'}).text
                key_skills = data.find_all('span', attrs={'data-qa': 'bloko-tag__text'})

                for skill in key_skills:
                    key_skill = skill.text
                    list_of_key_skills_for_vacancy.append(key_skill)

                key_skills_fow_writing = ', '.join(list_of_key_skills_for_vacancy) # переводим список в строку

                if key_skills_fow_writing == '': # если нет ключевых навыков, то пропускаем вакансию
                    continue
                else:
                    insert_data_operation = f'''
                    INSERT INTO {table_name} (id,
                                              company_name,
                                              position,
                                              job_description,
                                              key_skills)
                    VALUES (?, ?, ?, ?, ?);'''

                    cursor.execute(insert_data_operation, (count,
                                                           company_name,
                                                           position,
                                                           job_description,
                                                           key_skills_fow_writing))

                    connect.commit()
                    print(f'Данные успешно записаны в БД. Количество записей: {count}')

                    count += 1
                    time.sleep(0.5)

                    if count == 21: # когда записали 20-ть вакансий, прекращаем работу
                        return

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

        table_name = 'vacancies'
        create_table_operation = f'''
        CREATE TABLE IF NOT EXISTS {table_name} (id bigint,
                                                 company_name varchar,
                                                 position varchar,
                                                 job_description text,
                                                 key_skills varchar);'''
        cursor.execute(create_table_operation)
        print(f'Таблица {table_name} успешно создана в БД {db_name}!')

        go_parsing_and_write_data_in_db(connect, cursor)

    except (Exception, Error) as error:
        print(f'Ошибка: {error}')

    finally:
        if connect:
            cursor.close()
            connect.close()
            print(f'Соединение с БД {db_name} закрыто!')

# конечно, таким образом можно было записать и 100 вакансий с ключевыми навыками, но разрешено 20-ть, поэтому 20-ть.


if __name__ == '__main__':
    get_connect_to_db_and_go_working()

