'''
модуль для для определения типов мошенничества
'''
def event_type_1(conn, cursor, date: str) -> None:
    '''
        Определение случаев cовершения операций при просроченном или заблокированном паспорте
        Входные данные:
        conn     - передача подключения к БД edu
        cursor   - курсор подключения к БД edu
        date: str - дата, за которую определяется событие в формате 'DDMMYYYY'
    '''
    date_reversed = date[-4:] + '-' + date[2:4] + '-' + date[:2]
    cursor.execute(f"""insert into DEAIAN.YUNV_REP_FRAUD (event_dt,
                                   passport,
                                   fio,
                                   phone,
                                   event_type,
                                   report_dt)
    select
        DEAIAN.YUNV_DWH_FACT_transactions.trans_date as event_dt,
        DEAIAN.YUNV_DWH_DIM_clients_HIST.passport_num as passport,
        DEAIAN.YUNV_DWH_DIM_clients_HIST.last_name || ' ' || DEAIAN.YUNV_DWH_DIM_clients_HIST.first_name || ' ' || DEAIAN.YUNV_DWH_DIM_clients_HIST.patronymic as fio,
        DEAIAN.YUNV_DWH_DIM_clients_HIST.phone as phone,
        1 as event_type,
        cast(DEAIAN.YUNV_DWH_FACT_transactions.trans_date as date) as report_dt
      from DEAIAN.YUNV_DWH_FACT_transactions
        left join DEAIAN.YUNV_DWH_DIM_cards_HIST
        -- если на дату файла совершена операция с картой с card_num, то такой номер действующий
        on DEAIAN.YUNV_DWH_FACT_transactions.card_num = DEAIAN.YUNV_DWH_DIM_cards_HIST.card_num
        left join DEAIAN.YUNV_DWH_DIM_accounts_HIST
        on DEAIAN.YUNV_DWH_DIM_accounts_HIST.account_num = DEAIAN.YUNV_DWH_DIM_cards_HIST.account_num
        left join DEAIAN.YUNV_DWH_DIM_clients_HIST
        on DEAIAN.YUNV_DWH_DIM_clients_HIST.client_id = DEAIAN.YUNV_DWH_DIM_accounts_HIST.client
      where
        (
        -- если в таблицу уже залиты транзакции за несколько дат, фильтруем по дате заливаемых файлов
        cast(DEAIAN.YUNV_DWH_FACT_transactions.trans_date as date) = to_date('{date_reversed}', 'YYYY-MM-DD')
        and
        -- если на момент транзакции существовал account, привязанный к карте
        DEAIAN.YUNV_DWH_FACT_transactions.trans_date BETWEEN DEAIAN.YUNV_DWH_DIM_accounts_HIST.effective_from AND DEAIAN.YUNV_DWH_DIM_accounts_HIST.effective_to
        and
        -- account не был удаленный
        DEAIAN.YUNV_DWH_DIM_accounts_HIST.deleted_flg = 'N'
        and
        -- если паспорт действителен до даты менее даты транзакции
        cast(DEAIAN.YUNV_DWH_DIM_clients_HIST.passport_valid_to as date) < cast(DEAIAN.YUNV_DWH_FACT_transactions.trans_date as date)
        and
        -- если клиент не был удален на дату транзакции
        DEAIAN.YUNV_DWH_DIM_clients_HIST.deleted_flg = 'N'
        and
        -- в момент совершения транзакции
        DEAIAN.YUNV_DWH_FACT_transactions.trans_date BETWEEN DEAIAN.YUNV_DWH_DIM_clients_HIST.effective_from AND DEAIAN.YUNV_DWH_DIM_clients_HIST.effective_to
        )
        or 
        (
        -- если в таблицу уже залиты транзакции за несколько дат, фильтруем по дате заливаемых файлов
        cast(DEAIAN.YUNV_DWH_FACT_transactions.trans_date as date) = to_date('{date_reversed}', 'YYYY-MM-DD')
        and
        -- если на момент транзакции существовал account, привязанный к карте
        DEAIAN.YUNV_DWH_FACT_transactions.trans_date BETWEEN DEAIAN.YUNV_DWH_DIM_accounts_HIST.effective_from AND DEAIAN.YUNV_DWH_DIM_accounts_HIST.effective_to
        and
        -- account не был удаленный
        DEAIAN.YUNV_DWH_DIM_accounts_HIST.deleted_flg = 'N'
        and
        -- номер паспорта находился в черном списке на дату ранее или равную дате транзакции
        DEAIAN.YUNV_DWH_DIM_clients_HIST.passport_num in (select 
                                                               DEAIAN.YUNV_DWH_FACT_passport_blacklist.passport_num 
                                                            from 
                                                               DEAIAN.YUNV_DWH_FACT_passport_blacklist 
                                                             where 
                                                               DEAIAN.YUNV_DWH_FACT_passport_blacklist.entry_dt <= 
                                                               cast(DEAIAN.YUNV_DWH_FACT_transactions.trans_date as date))
        )""")
    conn.commit()


def event_type_2(conn, cursor, date: str) -> None:
    '''
        Определение случаев совершения операций при недействующем договоре
        Входные данные:
        conn     - передача подключения к БД edu
        cursor   - курсор подключения к БД edu
        date: str - дата, за которую определяется событие в формате 'DDMMYYYY'
    '''
    date_reversed = date[-4:] + '-' + date[2:4] + '-' + date[:2]
    cursor.execute(f"""insert into DEAIAN.YUNV_REP_FRAUD (event_dt,
                                   passport,
                                   fio,
                                   phone,
                                   event_type,
                                   report_dt)
    select
        DEAIAN.YUNV_DWH_FACT_transactions.trans_date as event_dt,
        DEAIAN.YUNV_DWH_DIM_clients_HIST.passport_num as passport,
        DEAIAN.YUNV_DWH_DIM_clients_HIST.last_name || ' ' || DEAIAN.YUNV_DWH_DIM_clients_HIST.first_name || ' ' || DEAIAN.YUNV_DWH_DIM_clients_HIST.patronymic as fio,
        DEAIAN.YUNV_DWH_DIM_clients_HIST.phone as phone,
        2 as event_type,
        cast(DEAIAN.YUNV_DWH_FACT_transactions.trans_date as date) as report_dt
      from DEAIAN.YUNV_DWH_FACT_transactions
        left join DEAIAN.YUNV_DWH_DIM_cards_HIST
        -- если на дату файла совершена операция с картой с card_num, то такой номер действующий
        on DEAIAN.YUNV_DWH_FACT_transactions.card_num = DEAIAN.YUNV_DWH_DIM_cards_HIST.card_num
        left join DEAIAN.YUNV_DWH_DIM_accounts_HIST
        -- наверное, есть несколько счетов, в том числе удаленные, привязанных к карте
        on DEAIAN.YUNV_DWH_DIM_accounts_HIST.account_num = DEAIAN.YUNV_DWH_DIM_cards_HIST.account_num
        left join DEAIAN.YUNV_DWH_DIM_clients_HIST
        on DEAIAN.YUNV_DWH_DIM_clients_HIST.client_id = DEAIAN.YUNV_DWH_DIM_accounts_HIST.client
      where
        -- если в таблицу уже залиты транзакции за несколько дат, фильтруем по дате заливаемых файлов
        cast(DEAIAN.YUNV_DWH_FACT_transactions.trans_date as date) = to_date('{date_reversed}', 'YYYY-MM-DD')
        and
        -- если на момент транзакции существовал account, привязанный к карте
        DEAIAN.YUNV_DWH_FACT_transactions.trans_date BETWEEN DEAIAN.YUNV_DWH_DIM_accounts_HIST.effective_from AND DEAIAN.YUNV_DWH_DIM_accounts_HIST.effective_to
        and
        -- account не был удаленный
        DEAIAN.YUNV_DWH_DIM_accounts_HIST.deleted_flg = 'N'
        and
        -- данный account не действовал
        DEAIAN.YUNV_DWH_DIM_accounts_HIST.valid_to < cast(DEAIAN.YUNV_DWH_FACT_transactions.trans_date as date)
        """)
    conn.commit()

def event_type_3(conn, cursor, date: str) -> None:
    '''
        Определение случаев совершениея операций в разных городах в течене одного часа
        Входные данные:
        conn     - передача подключения к БД edu
        cursor   - курсор подключения к БД edu
        date: str - дата, за которую определяется событие в формате 'DDMMYYYY'
    '''
    date_reversed = date[-4:] + '-' + date[2:4] + '-' + date[:2]
    cursor.execute(f"""with TMP as (
    select
        DEAIAN.YUNV_DWH_FACT_transactions.trans_date as event_dt,
        DEAIAN.YUNV_DWH_DIM_clients_HIST.passport_num as passport,
        DEAIAN.YUNV_DWH_DIM_clients_HIST.last_name || ' ' || DEAIAN.YUNV_DWH_DIM_clients_HIST.first_name || ' ' || DEAIAN.YUNV_DWH_DIM_clients_HIST.patronymic as fio,
        DEAIAN.YUNV_DWH_DIM_clients_HIST.phone as phone,
        3 as event_type,
        cast(DEAIAN.YUNV_DWH_FACT_transactions.trans_date as date) as report_dt,
        -- поля не входят в отчет
        case
        	when DEAIAN.YUNV_DWH_FACT_transactions.trans_date - lag(DEAIAN.YUNV_DWH_FACT_transactions.trans_date) OVER 
                                                            (PARTITION BY DEAIAN.YUNV_DWH_FACT_transactions.card_num 
                                                               ORDER BY DEAIAN.YUNV_DWH_FACT_transactions.trans_date) <= interval '1 hour'
            then 'Y'
            else 'N'
        end as delta_time,
        case 
        	when DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_city <> lag( DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_city) OVER 
                                                            (PARTITION BY DEAIAN.YUNV_DWH_FACT_transactions.card_num 
                                                               ORDER BY DEAIAN.YUNV_DWH_FACT_transactions.trans_date)
            then 'Y'
            else 'N'
        end as delta_city
      from DEAIAN.YUNV_DWH_FACT_transactions
        left join DEAIAN.YUNV_DWH_DIM_cards_HIST
        on DEAIAN.YUNV_DWH_FACT_transactions.card_num = DEAIAN.YUNV_DWH_DIM_cards_HIST.card_num
        left join DEAIAN.YUNV_DWH_DIM_accounts_HIST
        on DEAIAN.YUNV_DWH_DIM_accounts_HIST.account_num = DEAIAN.YUNV_DWH_DIM_cards_HIST.account_num
        left join DEAIAN.YUNV_DWH_DIM_clients_HIST
        on DEAIAN.YUNV_DWH_DIM_clients_HIST.client_id = DEAIAN.YUNV_DWH_DIM_accounts_HIST.client
        left join DEAIAN.YUNV_DWH_DIM_terminals_HIST
        on DEAIAN.YUNV_DWH_DIM_terminals_HIST.terminal_id = DEAIAN.YUNV_DWH_FACT_transactions.terminal
      where
        -- если в таблицу уже залиты транзакции за несколько дат, фильтруем по дате меньше или равно даты заливаемых файлов
        cast(DEAIAN.YUNV_DWH_FACT_transactions.trans_date as date) <= to_date('{date_reversed}', 'YYYY-MM-DD')
        and
        -- если в таблицу уже залиты данные про терминалы за несколько дат, фильтруем по дате меньше или равно даты заливаемых файлов
        DEAIAN.YUNV_DWH_DIM_terminals_HIST.file_date <= to_date('{date_reversed}', 'YYYY-MM-DD')
        and
        -- запись по терминалу должна быть активной
        DEAIAN.YUNV_DWH_DIM_terminals_HIST.deleted_flg = 'N'
        and
        -- на момент совершения транзакции
        DEAIAN.YUNV_DWH_FACT_transactions.trans_date BETWEEN DEAIAN.YUNV_DWH_DIM_terminals_HIST.effective_from and DEAIAN.YUNV_DWH_DIM_terminals_HIST.effective_to
)

    insert into DEAIAN.YUNV_REP_FRAUD (event_dt,
                                   passport,
                                   fio,
                                   phone,
                                   event_type,
                                   report_dt)
    select 
        TMP.event_dt,
        TMP.passport,
        TMP.fio,
        TMP.phone,
        TMP.event_type,
        TMP.report_dt
      from TMP 
        where 
          TMP.delta_time = 'Y' and TMP.delta_city = 'Y'
          and
          TMP.report_dt = to_date('{date_reversed}', 'YYYY-MM-DD')""")

    conn.commit()

def event_type_4(conn, cursor, date: str) -> None:
    '''
        Определение случаев попыток подбора суммы
        Входные данные:
        conn     - передача подключения к БД edu
        cursor   - курсор подключения к БД edu
        date: str - дата, за которую определяется событие в формате 'DDMMYYYY'
    '''
    date_reversed = date[-4:] + '-' + date[2:4] + '-' + date[:2]
    cursor.execute(f"""with TMP as (
    select
        DEAIAN.YUNV_DWH_FACT_transactions.trans_date as event_dt,
        DEAIAN.YUNV_DWH_DIM_clients_HIST.passport_num as passport,
        DEAIAN.YUNV_DWH_DIM_clients_HIST.last_name || ' ' || DEAIAN.YUNV_DWH_DIM_clients_HIST.first_name || ' ' || DEAIAN.YUNV_DWH_DIM_clients_HIST.patronymic as fio,
        DEAIAN.YUNV_DWH_DIM_clients_HIST.phone as phone,
        4 as event_type,
        cast(DEAIAN.YUNV_DWH_FACT_transactions.trans_date as date) as report_dt,
        -- поля не входят в отчет
        case
        	when DEAIAN.YUNV_DWH_FACT_transactions.trans_date - lag(DEAIAN.YUNV_DWH_FACT_transactions.trans_date, 3) OVER 
                                                            (PARTITION BY DEAIAN.YUNV_DWH_FACT_transactions.card_num 
                                                               ORDER BY DEAIAN.YUNV_DWH_FACT_transactions.trans_date) <= interval '20 minute'
                 and 
                 lag(DEAIAN.YUNV_DWH_FACT_transactions.oper_result, 3) OVER 
                                                            (PARTITION BY DEAIAN.YUNV_DWH_FACT_transactions.card_num 
                                                               ORDER BY DEAIAN.YUNV_DWH_FACT_transactions.trans_date) = 'REJECT'
                 and 
                 lag(DEAIAN.YUNV_DWH_FACT_transactions.oper_result, 2) OVER 
                                                            (PARTITION BY DEAIAN.YUNV_DWH_FACT_transactions.card_num 
                                                               ORDER BY DEAIAN.YUNV_DWH_FACT_transactions.trans_date) = 'REJECT'
                 and 
                 lag(DEAIAN.YUNV_DWH_FACT_transactions.oper_result, 1) OVER 
                                                            (PARTITION BY DEAIAN.YUNV_DWH_FACT_transactions.card_num 
                                                               ORDER BY DEAIAN.YUNV_DWH_FACT_transactions.trans_date) = 'REJECT'
                 and 
                 DEAIAN.YUNV_DWH_FACT_transactions.oper_result = 'SUCCESS'
                 
                 and 
                 (lag(DEAIAN.YUNV_DWH_FACT_transactions.amt, 3) OVER 
                                                            (PARTITION BY DEAIAN.YUNV_DWH_FACT_transactions.card_num 
                                                               ORDER BY DEAIAN.YUNV_DWH_FACT_transactions.trans_date) >
                 lag(DEAIAN.YUNV_DWH_FACT_transactions.amt, 2) OVER 
                                                            (PARTITION BY DEAIAN.YUNV_DWH_FACT_transactions.card_num 
                                                               ORDER BY DEAIAN.YUNV_DWH_FACT_transactions.trans_date)) 
                and 
                 (lag(DEAIAN.YUNV_DWH_FACT_transactions.amt, 2) OVER 
                                                            (PARTITION BY DEAIAN.YUNV_DWH_FACT_transactions.card_num 
                                                               ORDER BY DEAIAN.YUNV_DWH_FACT_transactions.trans_date) >
                 lag(DEAIAN.YUNV_DWH_FACT_transactions.amt, 1) OVER 
                                                            (PARTITION BY DEAIAN.YUNV_DWH_FACT_transactions.card_num 
                                                               ORDER BY DEAIAN.YUNV_DWH_FACT_transactions.trans_date))
               and 
                 (lag(DEAIAN.YUNV_DWH_FACT_transactions.amt, 1) OVER 
                                                            (PARTITION BY DEAIAN.YUNV_DWH_FACT_transactions.card_num 
                                                               ORDER BY DEAIAN.YUNV_DWH_FACT_transactions.trans_date) >
                 DEAIAN.YUNV_DWH_FACT_transactions.amt)
            then 'Y'
            else 'N'
        end as flag
      from DEAIAN.YUNV_DWH_FACT_transactions
        left join DEAIAN.YUNV_DWH_DIM_cards_HIST
        on DEAIAN.YUNV_DWH_FACT_transactions.card_num = DEAIAN.YUNV_DWH_DIM_cards_HIST.card_num
        left join DEAIAN.YUNV_DWH_DIM_accounts_HIST
        on DEAIAN.YUNV_DWH_DIM_accounts_HIST.account_num = DEAIAN.YUNV_DWH_DIM_cards_HIST.account_num
        left join DEAIAN.YUNV_DWH_DIM_clients_HIST
        on DEAIAN.YUNV_DWH_DIM_clients_HIST.client_id = DEAIAN.YUNV_DWH_DIM_accounts_HIST.client
      where
        cast(DEAIAN.YUNV_DWH_FACT_transactions.trans_date as date) <= to_date('{date_reversed}', 'YYYY-MM-DD')
)

    insert into DEAIAN.YUNV_REP_FRAUD (event_dt,
                                   passport,
                                   fio,
                                   phone,
                                   event_type,
                                   report_dt)
    select 
        TMP.event_dt,
        TMP.passport,
        TMP.fio,
        TMP.phone,
        TMP.event_type,
        TMP.report_dt
      from TMP 
        where 
          TMP.flag = 'Y' and TMP.report_dt = to_date('{date_reversed}', 'YYYY-MM-DD')""")
    conn.commit()
