from configparser import ConfigParser

import psycopg2


def config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} is not found in the {1} file.'.format(section, filename))
    return db


def create_database(database_name: str, params: dict) -> None:
    conn = psycopg2.connect(dbname='postgres', **params)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")
    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)
    with conn.cursor() as cur:
        cur.execute('CREATE SCHEMA IF NOT EXISTS stack')

        cur.execute('''
            CREATE TABLE stack.Accounts
            (
                row_id INT GENERATED ALWAYS AS IDENTITY,
                parent_id INT,
                number INT,
                type INT,
                CONSTRAINT PK_Accounts PRIMARY KEY (row_id),
                CONSTRAINT FK_Accounts_Folder FOREIGN KEY (parent_id) REFERENCES stack.Accounts(row_id)
            )
        ''')

        cur.execute('''
            CREATE TABLE stack.Counters
            (
                row_id INT GENERATED ALWAYS AS IDENTITY,
                name TEXT NOT NULL,
                acc_id INT,
                service INT NOT NULL,
                tarif INT NOT NULL,
                CONSTRAINT PK_Counters PRIMARY KEY (row_id),
                CONSTRAINT FK_Counters FOREIGN KEY (acc_id) REFERENCES stack.Accounts(row_id)
            )
        ''')

        cur.execute('''
            CREATE TABLE stack.Meter_Pok
            (
                row_id INT GENERATED ALWAYS AS IDENTITY,
                acc_id INT,
                counter_id INT,
                value INT NOT NULL,
                date DATE NOT NULL,
                month DATE NOT NULL,
                tarif INT NOT NULL,
                CONSTRAINT PK_Meter_Pok PRIMARY KEY (row_id),
                CONSTRAINT FK_Meter_Acc FOREIGN KEY (acc_id) REFERENCES stack.Accounts(row_id),
                CONSTRAINT FK_Meter_Counters FOREIGN KEY (counter_id) REFERENCES stack.Counters(row_id)
            )
        ''')

        conn.commit()
    conn.close()


def save_data_to_database(database_name: str, params: dict):
    """Сохранение данных о каналах и видео в базу данных."""

    conn = psycopg2.connect(dbname=database_name, **params)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            '''insert into stack.Accounts(parent_id,number,type)                                                            -- 1
                values(null,1,1);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 2
                values(1,1,2);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 3
                values(1,2,2);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 4
                values(1,3,2);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 5
                values(1,4,2);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 6
                values(2,111,3);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 7
                values(3,122,3);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 8
                values(4,133,3);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 9
                values(5,144,3);
                
                insert into stack.Accounts(parent_id,number,type)                                                            -- 10
                values(null,2,1);
                
                insert into stack.Accounts(parent_id,number,type)                                                            -- 11
                values(10,1,2);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 12
                values(10,2,2);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 13
                values(10,3,2);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 14
                values(10,4,2);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 15
                values(10,5,2);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 16
                values(10,6,2);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 17
                values(10,7,2);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 18
                values(10,8,2);
                
                insert into stack.Accounts(parent_id,number,type)                                                            -- 19
                values(11,211,3);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 20
                values(12,222,3);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 21
                values(13,233,3);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 22
                values(14,244,3);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 23
                values(15,255,3);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 24
                values(16,266,3);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 25
                values(17,277,3);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 26
                values(18,288,3);
                
                insert into stack.Accounts(parent_id,number,type)                                                            -- 27
                values(null,3,1);
                
                insert into stack.Accounts(parent_id,number,type)                                                            -- 28
                values(27,301,3);
                
                insert into stack.Accounts(parent_id,number,type)                                                            -- 29
                values(null,4,1);
                
                insert into stack.Accounts(parent_id,number,type)                                                            -- 30
                values(29,401,3);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 31
                values(30,402,3);
                
                insert into stack.Accounts(parent_id,number,type)                                                            -- 32
                values(null,5,1);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 33
                values(32,501,3);
                insert into stack.Accounts(parent_id,number,type)                                                            -- 34
                values(32,502,3);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 1 6,1
                values ('Счетчик на воду',6,100,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 2 6,2
                values ('Счетчик на воду',6,200,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 3 6,3
                values ('Счетчик на электричество',6,300,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 4 6,4
                values ('Счетчик на отопление',6,400,1);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 5 7,5
                values ('Счетчик на воду',7,100,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 6 7,6
                values ('Счетчик на воду',7,200,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 7 7,7
                values ('Счетчик на электричество',7,300,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 8 7,8
                values ('Счетчик на отопление',7,400,1);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 9 8,9
                values ('Счетчик на воду',8,100,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 10 8,10
                values ('Счетчик на воду',8,200,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 11 8,11
                values ('Счетчик на электричество',8,300,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 12 8,12
                values ('Счетчик на отопление',8,400,1);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 13 9,13
                values ('Счетчик на воду',9,100,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 14 9,14
                values ('Счетчик на воду',9,200,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 15 9,15
                values ('Счетчик на электричество',9,300,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 16 9,16
                values ('Счетчик на отопление',9,400,1);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 17 19,17
                values ('Счетчик на электричество',19,300,2);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 18 20,18
                values ('Счетчик на электричество',20,300,2);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 19 21,19
                values ('Счетчик на электричество',21,300,2);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 20 22,20
                values ('Счетчик на электричество',22,300,2);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 21 23,21
                values ('Счетчик на электричество',23,300,3);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 22 24,22
                values ('Счетчик на электричество',24,300,3);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 23 25,23
                values ('Счетчик на электричество',25,300,3);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 24 26,24
                values ('Счетчик на электричество',26,300,3);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 25 28,25
                values ('Счетчик на воду',28,100,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 26 28,26
                values ('Счетчик на воду',28,200,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 27 28,27
                values ('Счетчик на электричество',28,300,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 28 28,28
                values ('Счетчик на отопление',28,400,1);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 29 30,29
                values ('Счетчик на воду',30,100,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 30 30,30
                values ('Счетчик на воду',30,200,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 31 30,31
                values ('Счетчик на электричество',30,300,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 32 30,32
                values ('Счетчик на отопление',30,400,1);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 33 31,33
                values ('Счетчик на воду',31,100,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 34 31,34
                values ('Счетчик на воду',31,200,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 35 31,35
                values ('Счетчик на электричество',31,300,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 36 31,36
                values ('Счетчик на отопление',31,400,1);
                
                insert into stack.Counters (name,acc_id,service,tarif) -- 37 33,37
                values ('Счетчик на воду',33,100,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 38 33,38
                values ('Счетчик на воду',33,200,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 39 33,39
                values ('Счетчик на электричество',33,300,1);
                insert into stack.Counters (name,acc_id,service,tarif) -- 40 33,40
                values ('Счетчик на отопление',33,400,1);
                
                insert into stack.Counters (name,acc_id,service,tarif)  -- 41 34,41
                values ('Счетчик на воду',34,100,1);
                insert into stack.Counters (name,acc_id,service,tarif)  -- 42 34,42
                values ('Счетчик на воду',34,200,1);
                insert into stack.Counters (name,acc_id,service,tarif)  -- 43 34,43
                values ('Счетчик на электричество',34,300,1);
                insert into stack.Counters (name,acc_id,service,tarif)  -- 44 34,44
                values ('Счетчик на отопление',34,400,1);
                
                -----
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (6,1,100,'20230130','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (6,1,100,'20230225','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (6,2,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (6,2,50,'20230227','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (6,3,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (6,3,70,'20230227','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (6,3,10,'20230228','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (6,4,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (6,4,-50,'20230227','20230201',1);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (7,5,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (7,5,50,'20230227','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (7,6,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (7,6,55,'20230227','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (7,7,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (7,8,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (7,8,0,'20230227','20230201',1);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (8,9,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (8,9,900,'20230227','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (8,12,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (8,12,-1,'20230227','20230201',1);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (9,13,0,'20230221','20230201',1);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (9,14,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (9,14,0,'20230227','20230201',1);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (9,15,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (9,15,100,'20230228','20230201',1);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (9,16,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (9,16,10,'20230226','20230201',1);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (19,17,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (19,17,10,'20230227','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (19,17,50,'20230125','20230101',2);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (19,17,60,'20230227','20230201',2);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (20,18,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (20,18,10,'20230227','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (20,18,50,'20230125','20230101',2);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (20,18,0,'20230227','20230201',2);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (21,19,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (21,19,50,'20230227','20230201',2);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (22,20,100,'20230125','20230101',2);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (22,20,10,'20230227','20230201',2);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (23,21,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (23,21,10,'20230227','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (23,21,50,'20230125','20230101',2);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (23,21,0,'20230227','20230201',2);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (23,21,30,'20230125','20230101',3);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (23,21,3,'20230227','20230201',3);
                
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (24,22,200,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (24,22,-90,'20230227','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (24,22,50,'20230125','20230101',2);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (24,22,0,'20230227','20230201',2);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (24,22,30,'20230125','20230101',3);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (24,22,13,'20230227','20230201',3);
                
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (25,23,110,'20230227','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (25,23,70,'20230125','20230101',2);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (25,23,30,'20230125','20230101',3);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (25,23,15,'20230227','20230201',3);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (26,24,200,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (26,24,-90,'20230227','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (26,24,50,'20230125','20230101',2);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (26,24,50,'20230227','20230201',2);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (26,24,30,'20230125','20230101',3);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (26,24,13,'20230227','20230201',3);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (26,24,55,'20230228','20230201',3);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (28,25,200,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (28,25,40,'20230227','20230201',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (28,25,-90,'20230227','20230201',1);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (28,26,200,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (28,26,0,'20230227','20230201',1);
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (28,27,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (28,27,50,'20230227','20230201',1);
                
                
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (33,37,100,'20230125','20230101',1);
                insert into stack.Meter_pok (acc_id,counter_id,value,date,month,tarif)
                values (33,37,50,'20230227','20230201',1);
                '''
        )
    conn.commit()
    conn.close()


def select_count_pok_by_service(database_name: str, params: dict, service_id: int, month: str):
    conn = psycopg2.connect(dbname=database_name, **params)
    with conn.cursor() as cur:
        '''выбор лицевого счета и название его: "acc", выбор услуги и название его: 'serv' 
        и количество строк в таблице: 'stack.Meter_Pok',
        далее запрос к таблице 'stack.Meter_Pok' с псевдонимом 'mp',
        далее соединение таблиц для получения информации о счетчиках и лицевых счетах,
        далее фильтр результатов с переданными значениями
        после группировка результатов
        '''
        query = """
            SELECT a.number AS acc, c.service AS serv, COUNT(mp.row_id) AS count 
            FROM stack.Meter_Pok mp
            JOIN stack.Counters c ON mp.counter_id = c.row_id
            JOIN stack.Accounts a ON mp.acc_id = a.row_id
            WHERE c.service = %s AND mp.month = %s::DATE
            GROUP BY a.number, c.service
        """
        cur.execute(query, (service_id, month))
        results = cur.fetchall()
    conn.close()
    return results


def select_value_by_house_and_month(database_name: str, params: dict, house_number: int, month: str):
    conn = psycopg2.connect(dbname=database_name, **params)
    '''
    Номер лицевого счета из таблицы stack.Accounts, которая будет называться: a.number AS acc,
    Имя счетчика из таблицы stack.Counters,
    Суммирование значений из таблицы Meter_Pok которая будет называться: value,
    Соединение с таблицей stack.Accounts с таблицей stack.Meter_Pok по идентификатору счета (acc_id),
    Соединение c таблицей stack.Meter_Pok с таблицей stack.Counters по идентификатору счетчика (counter_id)
    Фильтр с подзапросом
    Группировка данных.
    '''
    with conn.cursor() as cur:
        query = '''
            SELECT a.number AS acc, c.name, SUM(mp.value) AS value
            FROM stack.Accounts a
            JOIN stack.Meter_Pok mp ON a.row_id = mp.acc_id
            JOIN stack.Counters c ON mp.counter_id = c.row_id
            WHERE a.parent_id IN (
                SELECT row_id
                FROM stack.Accounts
                WHERE number = %s 
            ) AND mp.month = %s::DATE
            GROUP BY a.number, c.name;
        '''
        cur.execute(query, (house_number, month))
        results = cur.fetchall()
    conn.close()
    return results


def stack_select_last_pok_by_acc(database_name: str, params: dict, number: int):
    conn = psycopg2.connect(dbname=database_name, **params)
    '''Номер лицевого счета из таблицы: stack.Accounts, которая будет называться: a.number AS acc,
       Услуга из таблицы stack.Counters, которая, будет называться: c.service AS serv,
       Дата показания из таблицы stack.Meter_Pok, которая будет называться: mp.date,
       Тариф показания из таблицы stack.Meter_Pok, которая будет называться: mp.tarif,
       Объем показания из таблицы stack.Meter_Pok, которая будет называться: mp.value,
       Соединение с таблицей stack.Meter_Pok (называемой mp) по полю acc_id, которое связывает лицевой счет (a.row_id) 
       с показаниями счетчиков (mp.acc_id),
       Соединение с таблицей stack.Counters (называемой c) по полю counter_id, которое связывает показания счетчиков 
       (mp.counter_id) с информацией о счетчиках (c.row_id),
       Фильтрация по номеру лицевого счета,
       Выбирает только те записи, у которых дата показания является последней для каждого счетчика и лицевого счета (подзапрос)
       Сортировка результатов по услуге
    '''
    with conn.cursor() as cur:
        query = '''
            SELECT a.number AS acc, c.service AS serv, mp.date, mp.tarif, mp.value
            FROM stack.Accounts a
            JOIN stack.Meter_Pok mp ON a.row_id = mp.acc_id
            JOIN stack.Counters c ON mp.counter_id = c.row_id
            WHERE a.number = %s
            AND mp.date = (
                SELECT MAX(mp2.date)
                FROM stack.Meter_Pok mp2
                WHERE mp2.acc_id = a.row_id AND mp2.counter_id = mp.counter_id
            )
            ORDER BY c.service;
        '''
        cur.execute(query, (number,))
        results = cur.fetchall()
    conn.close()
    return results


params = config()

while True:
    a = int(input(''' Выберите тип услуги:
    1) select_count_pok_by_service
    2) select_value_by_house_and_month
    3) stack_select_last_pok_by_acc
    4) Выход
    '''))

    if a == 1:
        service_id = int(input("Введите ID услуги: "))
        month = input("Введите месяц в формате YYYY-MM-DD: ")
        results = select_count_pok_by_service('database', params, service_id, month)
        for row in results:
            print(f"{row[0]}\t{row[1]}\t{row[2]}")
    elif a == 2:
        house_number = int(input("Введите номер дома: "))
        month = input("Введите месяц в формате YYYY-MM-DD: ")
        results2 = select_value_by_house_and_month('database', params, house_number, month)
        for row in results2:
            print(f"{row[0]}\t{row[1]}\t{row[2]}")
    elif a == 3:
        number = int(input('Введите номер лицевого счета: '))
        results3 = stack_select_last_pok_by_acc('database', params, number)
        for row in results3:
            print(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}")
    elif a == 4:
        break
    else:
        print("Неверный выбор. Попробуйте снова.")

params = config()
create_database('database', params)
save_data_to_database('database', params)
