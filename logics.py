'''
модуль для упраления процессом загрузки и обработки данных в БД edu
'''
from datetime import datetime
import os
import py_scripts.downloads # мой модуль для загрузки данных в БД edu
import py_scripts.events # мой модуль для определения типов мошенничества
def logic_0(row_to_write,
            conn_bank,
            cursor_bank,
            conn_edu,
            cursor_edu,
            files,
            path: str,
            date: str) -> None:
    '''
       Отвечает за логику загрузки и обработки информации
       Входные данные:
           row_to_write: list - мета данные об обработанных файлах и сделанных отчетах на дату
           conn_bank          - передача подключения к БД bank
           cursor_bank        - курсор подключения к БД bank
           conn_edu           - передача подключения к БД edu
           cursor_edu         - курсор подключения к БД edu
           files: list        - список файлов, доступных для загрузки
           path: str          - путь до папки с файлами
           date: str          - дата, за которую берется файл в формате 'DDMMYYYY'(часть названия файла)
    '''

    # загружаем таблицы из БД bank
    table_names = ['clients', 'accounts', 'cards']
    for table_name in table_names:
        try:
            py_scripts.downloads.bank_tables(table_name=table_name,
                                  conn_source=conn_bank,
                                  cursor_source=cursor_bank,
                                  conn_target=conn_edu,
                                  cursor_target=cursor_edu)
        except Exception as error:
            print(f'{datetime.now()} При загрузке таблицы bank.{table_name} произошла\
                                     ошибка:\n{error}\nотчет построен без учета обновления данной таблицы')
        else:
            print(f'{datetime.now()} Таблица bank.{table_name} - загружена в БД edu')

    # загружаем файл transactions_{date}.txt
    if f'transactions_{date}.txt' in files:
        print(f'{datetime.now()} transactions_{date}.txt - готов к загрузке в БД')
        if row_to_write[1] == 0:
            try:
                py_scripts.downloads.transactions(path_to_folder=path, 
                                       conn=conn_edu,
                                       date=date,
                                       cursor=cursor_edu)
            except Exception as error:
                print(f'{datetime.now()} При загрузке файла transactions_{date}.txt произошла ошибка:\n{error}')
            else:
                row_to_write[1] = 1
                print(f'{datetime.now()} transactions_{date}.txt - загружен в БД')
                os.rename(path + f'/transactions_{date}.txt',
                          path + '/archive' + f'/transactions_{date}.txt.backup')
        else:
            print(f'{datetime.now()} transactions_{date}.txt ранее был уже загружен в БД, данный файл будет удален')
            os.remove(path + f'/transactions_{date}.txt')

    # загружаем файл passport_blacklist_{date}.xlsx
    if f'passport_blacklist_{date}.xlsx' in files:
        print(f'{datetime.now()} passport_blacklist_{date}.xlsx - готов к загрузке в БД')
        if row_to_write[2] == 0:
            try:
                py_scripts.downloads.passport_blacklist(path_to_folder=path,
                                             conn=conn_edu,
                                             cursor=cursor_edu,
                                             date=date)
            except Exception as error:
                print(f'{datetime.now()} При загрузке файла passport_blacklist_{date}.xlsx произошла ошибка:\n{error}')
            else:
                row_to_write[2] = 1
                print(f'{datetime.now()} passport_blacklist_{date}.xlsx - загружен в БД')
                os.rename(path + f'/passport_blacklist_{date}.xlsx',
                          path + '/archive' + f'/passport_blacklist_{date}.xlsx.backup')
        else:
            print(f'{datetime.now()} passport_blacklist_{date}.xlsx ранее был уже загружен в БД, данный файл будет удален')
            os.remove(path + f'/passport_blacklist_{date}.xlsx')

    # загружаем файл terminals_{date}.xlsx
    if f'terminals_{date}.xlsx' in files:
        print(f'{datetime.now()} terminals_{date}.xlsx - готов к загрузке в БД')
        if row_to_write[3] == 0:
            try:
                py_scripts.downloads.terminals(path_to_folder=path,
                                    conn=conn_edu, 
                                    cursor=cursor_edu,
                                    date=date)
            except Exception as error:
                print(f'{datetime.now()} При загрузке файла terminals_{date}.xlsx произошла ошибка:\n{error}')
            else:
                row_to_write[3] = 1
                print(f'{datetime.now()} terminals_{date}.xlsx - загружен в БД')
                os.rename(path + f'/terminals_{date}.xlsx',
                          path + '/archive' + f'/terminals_{date}.xlsx.backup')
        else:
            print(f'{datetime.now()} terminals_{date}.xlsx ранее был уже загружен в БД, данный файл будет удален')
            os.remove(path + f'\\terminals_{date}.xlsx')

    

    if row_to_write[1] == 1 and row_to_write[5] == 0:
        try:
            py_scripts.events.event_type_2(conn=conn_edu, cursor=cursor_edu, date=date)
        except Exception as error:
            print(f'{datetime.now()} При определении случаев cовершения операций при недействующем или просроченном договоре произошла ошибка:\n{error}')
        else:
            print(f'{datetime.now()} Определение случаев совершения операций при недействующем или просроченном договоре - выполнено')
        row_to_write[5] = 1
    if row_to_write[1] == 1 and row_to_write[7] == 0:
        try:
            py_scripts.events.event_type_4(conn=conn_edu, cursor=cursor_edu, date=date)
        except Exception as error:
            print(f'{datetime.now()} При определении случаев подбора суммы произошла ошибка:\n{error}')
        else:
            print(f'{datetime.now()} Определение случаев попыток подбора суммы - выполнено')
        row_to_write[7] = 1
    if row_to_write[1] == 1 and row_to_write[2] == 1 and row_to_write[4] == 0:
        try:
            py_scripts.events.event_type_1(conn=conn_edu, cursor=cursor_edu, date=date)
        except Exception as error:
            print(f'{datetime.now()} При определении случаев совершения операций при просроченном или заблокированном паспорте произошла ошибка:\n{error}')
        else:
            print(f'{datetime.now()} Определение случаев cовершения операций при просроченном или заблокированном паспорте - выполнено')
        row_to_write[4] = 1
    if row_to_write[1] == 1 and row_to_write[3] == 1 and row_to_write[6] == 0:
        try:
            py_scripts.events.event_type_3(conn=conn_edu, cursor=cursor_edu, date=date)
        except Exception as error:
            print(f'{datetime.now()} При определении случаев совершения операций в разных городах в течене одного часа произошла ошибка:\n{error}')
        print(f'{datetime.now()} Определение случаев совершениея операций в разных городах в течене одного часа - выполнено')
        row_to_write[6] = 1
    row_to_write[8] = sum(row_to_write[4:8])
    cursor_edu.execute('''INSERT INTO DEAIAN.YUNV_META_DATE_POINTS(
                             date_point,
                             transactions_flg,
                             passport_blacklist_flg,
                             terminals_flg,
                             event_type_1_rep,
                             event_type_2_rep,
                             event_type_3_rep,
                             event_type_4_rep,
                             event_type_all_rep)
                             VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                          row_to_write)
    conn_edu.commit()
