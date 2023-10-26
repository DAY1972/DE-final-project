'''
Модуль с функциями для загрузки таблиц
'''
import pandas as pd

def bank_tables(table_name: str, conn_source, cursor_source, conn_target, cursor_target) -> None:
    '''
        Инкриментальная загрузка таблиц clients, accounts, cards из БД bank в БД edu
        в таблицы: YUNV_DWH_DIM_clients_HIST, 
                   YUNV_DWH_DIM_accounts_HIST,
                   YUNV_DWH_DIM_cards _HIST
        с преобразованием из SCD1 в SCD2
        Входные данные:
        table_name: str - 'clients', 'accounts' или 'cards'
        conn_source     - передача подключения к БД bank
        cursor_source   - курсор подключения к БД bank
        conn_target     - передача подключения к БД edu
        cursor_target   - курсор подключения к БД edu
    '''
    # перечень атрибутов в таблицах из БД bank 
    dic_tables_x_attributes_source = {'clients':['client_id', 
                                                 'last_name',
                                                 'first_name',
                                                 'patronymic',
                                                 'date_of_birth',
                                                 'passport_num',
                                                 'passport_valid_to',
                                                 'phone',
                                                 'create_dt',
                                                 'update_dt'],
                                      'accounts':['account',
                                                  'valid_to',
                                                  'client',
                                                  'create_dt',
                                                  'update_dt'],
                                      'cards':['card_num',
                                               'account',
                                               'create_dt',
                                               'update_dt']}
    dic_tables_x_attributes_target = {'clients':['client_id', 
                                                 'last_name',
                                                 'first_name',
                                                 'patronymic',
                                                 'date_of_birth',
                                                 'passport_num',
                                                 'passport_valid_to',
                                                 'phone'],
                                      'accounts':['account_num',
                                                  'valid_to',
                                                  'client'],
                                      'cards':['card_num',
                                               'account_num']}

    table_atributes_list_source = dic_tables_x_attributes_source[table_name]
    table_atributes_list_target = dic_tables_x_attributes_target[table_name]
    s = ''
    table_atributes_full_source = ''
    table_atributes_without_3_last_target = ''
    for i in range(len(table_atributes_list_source)):
        if i == 0:
            table_atributes_full_source += table_atributes_list_source[i]
            s += '%s'
        else:
            table_atributes_full_source = table_atributes_full_source + ', ' + table_atributes_list_source[i]
            s += ', %s'

    for i in range(len(table_atributes_list_target)):
        if i == 0:
            table_atributes_without_3_last_target += table_atributes_list_target[i]
        else:
            table_atributes_without_3_last_target += ', ' + table_atributes_list_target[i]


    # ИНКРИМЕНТАЛЬНАЯ ЗАГРУЗКА
    cursor_target.execute(f"""select 
                                                 max_update_dt -- дата последнего обновления из источника
                                               from deaian.yunv_meta
    	                                         where schema_name = 'bank' and table_name = '{table_name}'""")
    max_update_dt = cursor_target.fetchall()
    if not max_update_dt:
        # загрузка из источника bank в python
        cursor_source.execute(f'''SELECT 
                                      *
                                  FROM bank.info.{table_name};
                              ''')
    else:
        # загрузка из источника bank в python
        max_update_dt = str(max_update_dt[0][0]).split(sep=' ')[0]
        cursor_source.execute(f'''SELECT 
                                      *
                                  FROM bank.info.{table_name}
                                      where
                                          (update_dt IS NULL
                                        AND
                                          create_dt > to_date('{max_update_dt}', 'YYYY-MM-DD')::timestamp(1))
                                      OR
                                          (update_dt IS NOT NULL
                                        AND
                                          update_dt > to_date('{max_update_dt}', 'YYYY-MM-DD')::timestamp(1));
                          ''')
    
    data = cursor_source.fetchall()
    
    # изменение данных для отладки
    # df_data = pd.DataFrame(data)
    # тут изменения данных
    # data = df_data.values.to_list()


    # очистка STG слоя БД deaian
    cursor_target.execute(f'''DELETE FROM DEAIAN.YUNV_STG_{table_name};
                              DELETE FROM DEAIAN.YUNV_STG_del;''')
    
    # Загрузка в STG слой БД deaian
    cursor_target.executemany(f'''INSERT INTO DEAIAN.YUNV_STG_{table_name}(
                                      {table_atributes_full_source}) VALUES ({s})''', data)
    
    # все id, которые есть в источнике на момент запроса
    # для удаления
    cursor_source.execute(f'''
                              select
                                  bank.info.{table_name}.{table_atributes_list_source[0]}
                                from bank.info.{table_name};
                          ''')
    data = cursor_source.fetchall()
    #print(data)
    cursor_target.executemany(f'''INSERT INTO DEAIAN.YUNV_STG_del(id) VALUES (%s)''', data)

    # Применение данных в приемник DDS (вставка в БД deaian)
    cursor_target.execute(f"""
                             insert into DEAIAN.YUNV_DWH_DIM_{table_name}_HIST({table_atributes_without_3_last_target},
                                                                                effective_from,
                                                                                effective_to,
                                                                                processed_dt,
                                                                                deleted_flg)
                                 select
	                             {', '.join(['DEAIAN.YUNV_STG_' + table_name + '.' + atribute for atribute in table_atributes_list_source[:-2]])},
                                     CASE                                              -- effectiv_from
                                         WHEN {table_atributes_list_source[-1]} IS NULL       -- update_dt
                                         THEN {table_atributes_list_source[-2]}               -- create_dt
                                         ELSE {table_atributes_list_source[-1]}               -- update_dt
                                     END, 
	                             to_date('9999-12-31','YYYY-MM-DD')::timestamp(1), -- effective_to 
	                             now()::timestamp(1),                              -- processed_dt
	                             'N'                                               -- deleted_flg
                                   from DEAIAN.YUNV_STG_{table_name}
                                     left join
                                       DEAIAN.YUNV_DWH_DIM_{table_name}_HIST
                                     on
                                       DEAIAN.YUNV_STG_{table_name}.{table_atributes_list_source[0]} = DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{table_atributes_list_target[0]}
                                     where
                                       -- вставляются записи с id, которого нет в DDS слое 
                                       DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{table_atributes_list_target[0]} is null
                                     OR
                                       -- или всталяются записи с имеющимся id, которые актуальны как удаленные, и востановливаются с тем же id 
                                       (cast(DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.effective_to as timestamp(1)) = to_date('9999-12-31','YYYY-MM-DD')::timestamp(1))
                                     AND
                                       (DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.deleted_flg = 'Y');

                          """)
    
    # Применение данных в приемник DDS (обновление в БД deaian)
    condition_0 = ''
    # синтез условия фильтрации: хотя бы одно из значащих полей не равно полю в имеющейся записи
    for i, atribute_source in enumerate(table_atributes_list_source[1:-2]):
        atribute_target = table_atributes_list_target[i+1]
        if i == 0:
            condition_0 += f'''((DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{atribute_target} <> DEAIAN.YUNV_STG_{table_name}.{atribute_source})
                             OR (DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{atribute_target} IS NULL AND DEAIAN.YUNV_STG_{table_name}.{atribute_source} IS NOT NULL)
                             OR (DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{atribute_target} IS NOT NULL AND DEAIAN.YUNV_STG_{table_name}.{atribute_source} IS NULL))'''
        else:
            condition_0 += f''' OR ((DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{atribute_target} <> DEAIAN.YUNV_STG_{table_name}.{atribute_source})
                             OR (DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{atribute_target} IS NULL AND DEAIAN.YUNV_STG_{table_name}.{atribute_source} IS NOT NULL)
                             OR (DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{atribute_target} IS NOT NULL AND DEAIAN.YUNV_STG_{table_name}.{atribute_source} IS NULL))'''

    cursor_target.execute(f'''
                          insert into DEAIAN.YUNV_DWH_DIM_{table_name}_HIST({table_atributes_without_3_last_target},
                                                                            effective_from,
                                                                            effective_to,
                                                                            processed_dt,
                                                                            deleted_flg)
                              select
                                  {', '.join(['TMP.' + atribute for atribute in table_atributes_list_source[:-2]])},
	                          tmp.update_dt,
	                          to_date('9999-12-31','YYYY-MM-DD')::timestamp(1),
	                          now()::timestamp(1),
	                          'N'
                                from 
                                    (select
                                        {', '.join(['DEAIAN.YUNV_STG_' + table_name + '.' + atribute for atribute in table_atributes_list_source])} 
	                              from DEAIAN.YUNV_STG_{table_name}
	                                inner join	                                  
	                                  DEAIAN.YUNV_DWH_DIM_{table_name}_HIST
	                                on
	                                  -- есть поля с одинаковым id в обеих таблицах
	                                  (DEAIAN.YUNV_STG_{table_name}.{table_atributes_list_source[0]} = DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{table_atributes_list_target[0]})                                          
	                                where
	                                -- и effective_to = технической бесконечности, т.е. запись актуальна на данный момент
	                                (DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.effective_to = to_date ('9999-12-31','YYYY-MM-DD')::timestamp(1))
	                                AND
	                                -- и актуальная запись не удалена (запись с тем же id после удаления востанавливается в блоке (вставка))
	                                (DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.deleted_flg = 'N')
	                                AND
                                          -- где хотя бы одно из значащих полей не равно полю в имеющейся записи (NULL SAFE сравнение)
                                          {condition_0}
                                          ) as TMP;
                                          
                          ''')
    
    
    # Применение данных в приемник DDS (удаление из БД deaian)
    cursor_target.execute(f'''
                          insert into DEAIAN.YUNV_DWH_DIM_{table_name}_HIST({table_atributes_without_3_last_target},
                                                                             effective_from,
                                                                             effective_to,
                                                                             processed_dt,
                                                                             deleted_flg)
                              select
                                      {table_atributes_without_3_last_target},
                                      now()::timestamp(1),--effective_from,
                                      effective_to,       --остается равным технической бесконечности
                                      now()::timestamp(1),--processed_dt,
                                      'Y'                 --заменяем флаг 'N' на 'Y'
                                  from DEAIAN.YUNV_DWH_DIM_{table_name}_HIST
                                      where DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{table_atributes_list_target[0]} 
                                          IN
                                          -- выбираем записи с id, которых уже нет в источнике на текущий запрос
	                                  (select
		                                  DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{table_atributes_list_target[0]}
	                                      from DEAIAN.YUNV_DWH_DIM_{table_name}_HIST
	                                         LEFT JOIN
	                                           DEAIAN.YUNV_STG_del
	                                        on
	                                           DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{table_atributes_list_target[0]} = DEAIAN.YUNV_STG_del.id
	                                        where
                                                   -- которых уже нет в источнике 
                                                   DEAIAN.YUNV_STG_del.id is null)
                                                   --и, которые имеют активную не удаленную версию
	                                         AND
	                                           (DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.effective_to = to_date('9999-12-31','YYYY-MM-DD')::timestamp(1))
                                                 AND
                                                   (DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.deleted_flg = 'N');
                          ''')
    
    # Применение данных в приемник DDS (изменение effective_to для не актуальных записей после обновления и "удаления") БД deaian
    cursor_target.execute(f'''
                          update DEAIAN.YUNV_DWH_DIM_{table_name}_HIST
                              set
                                      effective_to = TMP.effective_to_new
                                  from 
                                      (select 
                                               *,
                                               coalesce(cast(lead(DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.effective_from) over
                                                   (partition by DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{table_atributes_list_target[0]}
                                                   order by DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.effective_from) - interval '1 milliseconds' as timestamp(1)),
                                                   to_date('9999-12-31', 'YYYY-MM-DD')::timestamp(1)) as effective_to_new
                                           from DEAIAN.YUNV_DWH_DIM_{table_name}_HIST) as TMP
                                      where 
                                          DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.{table_atributes_list_target[0]} = TMP.{table_atributes_list_target[0]} 
                                        and
                                          DEAIAN.YUNV_DWH_DIM_{table_name}_HIST.effective_from = TMP.effective_from;
                          ''')
    
    # Сохраняем состояние загрузки в метаданные БД deaian
    if not max_update_dt:
        cursor_target.execute(f'''insert into deaian.yunv_meta(schema_name,
                                                               table_name,
                                                               max_update_dt)
                                      select
                                          'bank',
                                          '{table_name}',
                                          CASE
                                              WHEN MAX(update_dt) IS NULL
                                              THEN MAX(create_dt)
                                              ELSE MAX(update_dt)
                                          END
                                        FROM DEAIAN.YUNV_STG_{table_name}
                                        
                             ''')
    else:
        cursor_target.execute(f'''                          
                          update deaian.yunv_meta
                              set
                                max_update_dt = coalesce((select max(update_dt) from DEAIAN.YUNV_STG_{table_name}),
                                                  (select max(max_update_dt) from deaian.yunv_meta))
                                  where schema_name = 'bank' and table_name = '{table_name}'
                                        and max_update_dt = (select max(max_update_dt) from deaian.yunv_meta)
                          ''')

    # очистка STG слоя БД deaian
    cursor_target.execute(f'''DELETE FROM DEAIAN.YUNV_STG_{table_name};
                              DELETE FROM DEAIAN.YUNV_STG_del;
                          ''')
    if table_name == 'clients':
        cursor_target.execute('''
                                     update DEAIAN.YUNV_DWH_DIM_clients_HIST
                                         set
                                             passport_valid_to = to_date('9999-12-31', 'YYYY-MM-DD')
                                           where passport_valid_to IS NULL;
                              ''')
    conn_target.commit()


def transactions(path_to_folder: str, date: str, conn, cursor) -> None:
    '''
        Загружает актуальные данные из файла transactions_{date}.txt
        в таблицу YUNV_DWH_FACT_transactions БД deaian
        Входные данные:
        path_to_folder: str - путь до папки с файлом
        date: str - дата, за которую берется файл в формате 'DDMMYYYY'(часть названия файла)
        conn - передача подключения к БД edu
        cursor - курсор подключения к БД edu
    '''
    date_reversed = date[-4:] + '-' + date[2:4] + '-' + date[:2]
    # очистка STG слоя БД deaian
    cursor.execute('DELETE FROM DEAIAN.YUNV_STG_transactions')
    path_to_file = path_to_folder + '/' + f'transactions_{date}.txt'
    df_transactions = pd.read_csv(path_to_file, sep=';', header=0,
                                  index_col=None)
    # изменяем разделитель в цифрах с ',' на '.'
    df_transactions['amount'] = df_transactions['amount'].str.replace(pat=',',
                                                                      repl='.')
    cursor.executemany('''INSERT INTO DEAIAN.YUNV_STG_transactions(
                              trans_id,
                              trans_date,
                              amt,
                              card_num,
                              oper_type,
                              oper_result,
                              terminal)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)''',
                               df_transactions.values.tolist())
    
    cursor.execute(f'''
                   INSERT INTO DEAIAN.YUNV_DWH_FACT_transactions(
                           trans_id,
                           trans_date,
                           amt,
                           card_num,
                           oper_type,
                           oper_result,
                           terminal)
                       SELECT
                               DEAIAN.YUNV_STG_transactions.*
                           FROM
                                   DEAIAN.YUNV_STG_transactions
                               LEFT JOIN
                                   DEAIAN.YUNV_DWH_FACT_transactions
                               ON
                                   DEAIAN.YUNV_STG_transactions.trans_id = DEAIAN.YUNV_DWH_FACT_transactions.trans_id
                               WHERE
                                   -- если trans_id только уникальные и в STG появился новый уникальный trans_id
                                   DEAIAN.YUNV_DWH_FACT_transactions.trans_id IS NULL
                                 OR
                                   -- или если trans_id не уникальные и если в STG появилась новая по trans_date запись для имеющегося trans_id
                                   DEAIAN.YUNV_STG_transactions.trans_date <> DEAIAN.YUNV_DWH_FACT_transactions.trans_date;

                    update DEAIAN.YUNV_DWH_FACT_transactions
                        set
                            file_date = TO_DATE('{date_reversed}', 'YYYY-MM-DD')
                          where file_date is null;
                   ''')
    # очистка STG слоя БД deaian
    cursor.execute('DELETE FROM DEAIAN.YUNV_STG_transactions')
    conn.commit()
    
def passport_blacklist(path_to_folder: str, date: str, conn, cursor) -> None:
    '''
        Загружает актуальные данные из файла passport_blacklist_{date}.xlsx
        в таблицу YUNV_DWH_FACT_passport_blacklist БД deaian
        Входные данные:
        path_to_folder: str - путь до папки с файлом
        date: str - дата, за которую берется файл в формате 'DDMMYYYY'(часть названия файла)
        conn - передача подключения к БД edu
        cursor - курсор подключения к БД edu
    '''
    date_reversed = date[-4:] + '-' + date[2:4] + '-' + date[:2]
    cursor.execute('DELETE FROM DEAIAN.YUNV_STG_passport_blacklist')
    path_to_file = path_to_folder + '/' + f'passport_blacklist_{date}.xlsx'
    date_as_in_file = date[-4:]+ '-' + date[2:4] + '-' + date[:2]
    df_passport_blacklist = pd.read_excel(path_to_file,
                                          sheet_name='blacklist',
                                          header=0,
                                          index_col=None)
    
    cursor.executemany('''INSERT INTO DEAIAN.YUNV_STG_passport_blacklist(
                                  entry_dt,
                                  passport_num)
                              VALUES (%s, %s)''',
                       df_passport_blacklist[df_passport_blacklist['date']\
                                             == date_as_in_file].values.tolist())
    
    cursor.execute(f'''
                       insert into DEAIAN.YUNV_DWH_FACT_passport_blacklist(
                               entry_dt,
                               passport_num,
                               processed_dt)
                           select
                                   DEAIAN.YUNV_STG_passport_blacklist.entry_dt,
                                   DEAIAN.YUNV_STG_passport_blacklist.passport_num,
                                   now()::timestamp(1)
                               from DEAIAN.YUNV_STG_passport_blacklist 
                                 left join
                                    DEAIAN.YUNV_DWH_FACT_passport_blacklist
                                 on
                                    DEAIAN.YUNV_STG_passport_blacklist.passport_num = DEAIAN.YUNV_DWH_FACT_passport_blacklist.passport_num
                               where
                                    DEAIAN.YUNV_DWH_FACT_passport_blacklist.passport_num is null;
                                    
                       update DEAIAN.YUNV_DWH_FACT_passport_blacklist
                           set
                               file_date = TO_DATE('{date_reversed}', 'YYYY-MM-DD')
                             where file_date is null;
                   ''')
    cursor.execute('DELETE FROM DEAIAN.YUNV_STG_passport_blacklist')
    conn.commit()

def terminals(path_to_folder: str, date: str, conn, cursor) -> None:
    '''
        Загружает актуальные данные из файла terminals_{date}.xlsx
        в таблицу YUNV_DWH_DIM_terminals_HIST БД deaian
        Входные данные:
        path_to_folder: str - путь до папки с файлом
        date: str - дата, за которую берется файл в формате 'DDMMYYYY'(часть названия файла)
        conn - передача подключения к БД edu
        cursor - курсор подключения к БД edu
    '''
    date_reversed = date[-4:] + '-' + date[2:4] + '-' + date[:2]
    cursor.execute('DELETE FROM DEAIAN.YUNV_STG_terminals')
    path_to_file = path_to_folder + '/' + f'terminals_{date}.xlsx'
    df_terminals = pd.read_excel(path_to_file, sheet_name='terminals',
                                 header=0, index_col=None)
    cursor.executemany('''INSERT INTO DEAIAN.YUNV_STG_terminals(
                                  terminal_id,
                                  terminal_type,
                                  terminal_city,
                                  terminal_address)
                              VALUES (%s, %s, %s, %s)''',
                       df_terminals.values.tolist())
    
    cursor.execute(f'''
        -- Применение данных в приемник DDS (вставка банкоматов с новым terminal_id)
        update DEAIAN.YUNV_STG_terminals
            set
                processed_dt = to_date('{date_reversed}', 'YYYY-MM-DD')
              where processed_dt is null;

        insert into DEAIAN.YUNV_DWH_DIM_terminals_HIST(
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                effective_from,
                effective_to,
                deleted_flg,
                file_date)
            select
                   DEAIAN.YUNV_STG_terminals.*,
                   to_date('9999-12-31','YYYY-MM-DD'),
                   'N',
                   TO_DATE('{date_reversed}', 'YYYY-MM-DD')
               from DEAIAN.YUNV_STG_terminals
                 left join
                    DEAIAN.YUNV_DWH_DIM_terminals_HIST
                 on
                    DEAIAN.YUNV_STG_terminals.terminal_id = DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_id
                 where
                    --если такого terminal_id еще нет в базе
                    DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_id is null 
                   OR  
                    --или есть актуальная весрсия удаленного терминал с таким же terminal_id
                    (DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_id is not null
                     AND
                     DEAIAN.YUNV_DWH_DIM_terminals_HIST.effective_to = to_date ('9999-12-31','YYYY-MM-DD')
                     AND
                     DEAIAN.YUNV_DWH_DIM_terminals_HIST.deleted_flg = 'Y');

        -- Применение данных в приемник DDS (обновление данных для банкоматов имеющих в БД terminal_id)
        insert into DEAIAN.YUNV_DWH_DIM_terminals_HIST(
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                effective_from,
                effective_to,
                deleted_flg,
                file_date)
            select
                    TMP.*,
                    to_date('9999-12-31','YYYY-MM-DD'),
                   'N',
                   TO_DATE('{date_reversed}', 'YYYY-MM-DD')
                from
                    (select
                            DEAIAN.YUNV_STG_terminals.*
	                    from DEAIAN.YUNV_STG_terminals
	                      inner join
	                         DEAIAN.YUNV_DWH_DIM_terminals_HIST
	                      on
	                         --находим в обеих таблицах записи с одинаковым terminal_id
	                         DEAIAN.YUNV_STG_terminals.terminal_id = DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_id
	                        AND
                                 -- и effective_to = технической бесконечности, т.е. запись актуальна на данный момент
                                 (DEAIAN.YUNV_DWH_DIM_terminals_HIST.effective_to = to_date ('9999-12-31','YYYY-MM-DD'))
                                AND
                                 -- и актуальная запись не удалена (запись с тем же id после удаления востанавливается в блоке (вставка))
                                 (DEAIAN.YUNV_DWH_DIM_terminals_HIST.deleted_flg = 'N')
	                       where
	                         -- где есть разные значениями хотя бы для одного из трех полей
	                        ((DEAIAN.YUNV_STG_terminals.terminal_type <> DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_type) 
	                           or (DEAIAN.YUNV_STG_terminals.terminal_type is null and DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_type is not null)
	                           or (DEAIAN.YUNV_STG_terminals.terminal_type is not null and DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_type is null))
	                     or ((DEAIAN.YUNV_STG_terminals.terminal_city <> DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_city)
	                           or (DEAIAN.YUNV_STG_terminals.terminal_city is null and DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_city is not null)
	                           or (DEAIAN.YUNV_STG_terminals.terminal_city is not null and DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_city is null))
	                     or ((DEAIAN.YUNV_STG_terminals.terminal_address <> DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_address)
	                           or (DEAIAN.YUNV_STG_terminals.terminal_address is null and DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_address is not null)
	                           or (DEAIAN.YUNV_STG_terminals.terminal_address is not null and DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_address is null))                            
	            ) as TMP;
	        
        -- Применение данных в приемник DDS (удаление по отсутствующему terminal_id)
        insert into DEAIAN.YUNV_DWH_DIM_terminals_HIST(
                terminal_id,
                terminal_type,
                terminal_city,
                terminal_address,
                effective_from,
                effective_to,
                deleted_flg,
                file_date)
            select
                    DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_id,
                    DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_type,
                    DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_city,
                    DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_address,
                    TO_DATE('{date_reversed}', 'YYYY-MM-DD'), --effective_from
                    to_date('9999-12-31','YYYY-MM-DD'), --effective_to
                    'Y', --deleted_flg
                    TO_DATE('{date_reversed}', 'YYYY-MM-DD')
                from
                    DEAIAN.YUNV_DWH_DIM_terminals_HIST
                  left join
                    DEAIAN.YUNV_STG_terminals
                  ON
                    DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_id = DEAIAN.YUNV_STG_terminals.terminal_id
                  where
                    --где нет такого terminal_id в источнике
                    DEAIAN.YUNV_STG_terminals.terminal_id IS NULL
                    -- и терминал с таким terminal_id имеет открытую неудаленную запись
                    AND
                    DEAIAN.YUNV_DWH_DIM_terminals_HIST.effective_to = to_date('9999-12-31','YYYY-MM-DD')
                    AND
                    DEAIAN.YUNV_DWH_DIM_terminals_HIST.deleted_flg = 'N';
            
        -- Применение данных в приемник DDS (изменение effective_to для не актуальных записей после обновления и 'удаления')
        update DEAIAN.YUNV_DWH_DIM_terminals_HIST
            set
               effective_to = TMP.effective_to_new
            from
                (select 
                     *,
                     coalesce(cast(lead(DEAIAN.YUNV_DWH_DIM_terminals_HIST.effective_from) over
                     (partition by DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_id  order by DEAIAN.YUNV_DWH_DIM_terminals_HIST.effective_from) - interval '1 day' as date),
                     to_date('9999-12-31', 'YYYY-MM-DD')) as effective_to_new
                   from DEAIAN.YUNV_DWH_DIM_terminals_HIST) as TMP
               where
                   DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_id  = TMP.terminal_id
                 and
                   DEAIAN.YUNV_DWH_DIM_terminals_HIST.effective_from = TMP.effective_from;
                ''')
    cursor.execute('DELETE FROM DEAIAN.YUNV_STG_terminals')
    conn.commit()
