#!/usr/bin/python3
import psycopg2
import os
import re
from datetime import datetime
import py_scripts.logics # мой модуль для упраления процессом загрузки и обработки данных в БД edu
    
def main_process(path = '/home/deaian/yunv/project'):  #'C:\Users\Admin\Desktop\project'
    try:
        list_of_files = os.listdir(path)
    except Exception as error:
        print(f'{datetime.now} Указанная директория {path} отсутствует\n{error}')
    else:
        # если папка с файлами существует
        # определяем за какие даты есть файлы в папке
        dates_in_folder = []
        for file in list_of_files:
            if re.findall(r'\d{8}', file):
                dates_in_folder.extend(re.findall(r'\d{8}', file))
        dates_in_folder = sorted(list(set(dates_in_folder)))

        # если есть файлы хотя бы за одну дату
        if dates_in_folder:
            # подключаемся к БД edu
            try:
                conn_edu = psycopg2.connect(database = "edu",
                                            host = "de-edu-db.chronosavant.ru",
                                            user = "deaian",
                                            password = 'sarumanthewhite',
                                            port = "5432")
                conn_edu.autocommit = False
                cursor_edu = conn_edu.cursor()
            except Exception as error:
                print(f'{datetime.now()} При подключении к БД edu произошла ошибка:\n{error}')
            else: 
                # если подключились к БД edu подключаемся к БД bank
                try:
                    conn_bank = psycopg2.connect(database = "bank",
                                                 host = "de-edu-db.chronosavant.ru",
                                                 user = "bank_etl",
                                                 password = 'bank_etl_password',
                                                 port = "5432")
                    conn_bank.autocommit = False
                    cursor_bank = conn_bank.cursor()
                except Exception as error:
                    print(f'{datetime.now()} При подключении к БД bank произошла ошибка:\n{error}')
                else: # если соединение с БД edu и bank доступны:

                    # иттерируем по датам из папки с файлами
                    for date in dates_in_folder:
                        date_reversed = date[-4:] + '-' + date[2:4] + '-' + date[:2]
                        print(f'{datetime.now()} В папке есть файл(ы) на дату {date_reversed}')
                        query = f'''SELECT
                                        *
                                        FROM DEAIAN.YUNV_META_DATE_POINTS
                                            WHERE date_point = TO_DATE('{date_reversed}', 'YYYY-MM-DD');'''
                        cursor_edu.execute(query)
                        row = cursor_edu.fetchall() # получаем метаданные на дату

                        if not row: # если на данную дату ранее не было загружено информации
                            row_to_write = [date_reversed, 0, 0, 0, 0, 0, 0, 0, 0]
                            py_scripts.logics.logic_0(row_to_write=row_to_write,
                                                      conn_bank = conn_bank, 
                                                      cursor_bank = cursor_bank,
                                                      conn_edu=conn_edu,
                                                      cursor_edu=cursor_edu,
                                                      files=list_of_files,
                                                      path=path,
                                                      date=date)
                        else: # если на данную дату ранее была загружена информация
                            row_to_write = list(row[0][0:9])
                            row_to_write[0] = date_reversed
                            py_scripts.logics.logic_0(row_to_write=row_to_write,
                                                      conn_bank = conn_bank, 
                                                      cursor_bank = cursor_bank,
                                                      conn_edu=conn_edu,
                                                      cursor_edu=cursor_edu,
                                                      files=list_of_files,
                                                      path=path,
                                                      date=date)
            cursor_edu.close()
            conn_edu.close()
            cursor_bank.close()
            conn_bank.close()
        else:
            print(f'{datetime.now()} В папке отсутствуют файлы с данными на текущую дату')        

if __name__ == '__main__':
    main_process()
