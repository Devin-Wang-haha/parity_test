import requests
import datetime
import pytz
import pymysql
from multiprocessing import Pool



db_config = {
    'host' : "127.0.0.1",
    'port' : 8306,
    'user' : 'test',
    'password' : '123456',
    'db' : 'test'
}

######################################################################
######################## Operate the database #######################
######################################################################

def exeSQL(sql, commit=False):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
        if commit:
            connection.commit()
        else:
            result = cursor.fetchall()
            return result
    finally:
        connection.close()
    

    
def create_action_table():
    create_action_sql = """CREATE TABLE {} ( `act_id` int(11) NOT NULL AUTO_INCREMENT,
                `directive` enum('call','create','reward-block','reward-uncle','suicide') NOT NULL, 
                `block_num` int(11) NOT NULL, PRIMARY KEY (`act_id`), 
                ENGINE=InnoDB""".format(table_name)
    exeSQL(create_action_sql, True)
    
def drop_table(table_name):
    drop_table_sql = """DROP TABLE {}""".format(table_name)
    exeSQL(drop_table_sql, True)
    
def insert_table(act_id, directive, block_num):
    insert_act_sql = """INSERT INTO {} (act_id, directive, block_num) 
            VALUES ({}, '{}', {})""".format(table_name, act_id, directive, block_num)
#     print(insert_act_sql)
    exeSQL(insert_act_sql, True)
    
def fetchAddressSet():
    fetch_addrs_sql = ("SELECT * FROM {}").format(table_name)
    addrs = exeSQL(fetch_addrs_sql)
    for sublist in addrs:
        print(sublist)

table_name = test
create_action_table();
insert_table(1, 'create', 404):
insert_table();