import requests
import datetime
import pytz
import pymysql
from multiprocessing import Pool


rpc_ip = "http://localhost"
rpc_port = 8545

'''
db_config = {
    'host' : "127.0.0.1",
    'port' : 8306,
    'user' : 'eth',
    'password' : '123456',
    'db' : 'eth'
}
'''
lowest_bn = 1

def hex2wei(s):
    return int(s, 16)

def wei2eth(w):
    return w/10**18
'''
# return the 1st timestamp of day
def date_to_timestamp(year, month, day, tz_str='utc'):
    dt = datetime.datetime(year, month, day)
    tz = None
    if tz_str=='utc':
        tz = pytz.utc
    else:
        tz = pytz.timezone(tz_str)
    dt_tz = tz.localize(dt)
    return dt_tz.timestamp()

def timestamp_to_date(ts, tz_str='utc'):
    tz = None
    if tz_str=='utc':
        tz = pytz.utc
    else:
        tz = pytz.timezone(tz_str)
    dt = datetime.datetime.fromtimestamp(ts, tz)
    return dt
'''

def rpc_to_parity(method, params):
    rpc_ip_port = rpc_ip + ":" + str(rpc_port)
    payload = {"jsonrpc":"2.0",
              "method":method,
              "params":params,
              "id":1}
    headers = {'Content-type': 'application/json'}
    session = requests.Session()
    response = session.post(rpc_ip_port, json=payload, headers=headers)
    return response

'''
def query_timestamp_of_block(block_num):
    method = 'eth_getBlockByNumber'
    params = [hex(block_num), True]
    rpc_resp = rpc_to_parity(method, params)
    ts_hex = rpc_resp.json()['result']['timestamp']
    return int(ts_hex, 16)


def query_highest_blocknumber():
    method = 'eth_blockNumber'
    params = []
    rpc_resp = rpc_to_parity(method, params)
    bn_hex = rpc_resp.json()['result']
    return int(bn_hex, 16)

def check_date_reasonable(year, month, day):
    highest_bn = query_highest_blocknumber()
    highest_ts = query_timestamp_of_block(highest_bn)
    highest_dt = timestamp_to_date(highest_ts)
    lowest_ts = query_timestamp_of_block(lowest_bn)
    ts = date_to_timestamp(year, month, day)
    if ts < lowest_ts:
        print("Date {}/{}/{} is too early!".format(year, month, day))
        return False
    if ts > highest_ts:
        print("""Date {}/{}/{} is too new, the newest date synchronized is
                {}/{}/{} !""".format(year, month, day, highest_dt.year, highest_dt.month, highest_dt.day))
        return False
    return True

# Two steps
# Step 1: Shorten the possible range
# Step 2: Iterate from the lower bound of range

def first_block_of_day(year, month, day):
    if not check_date_reasonable(year, month, day):
        return
    highest_bn = query_highest_blocknumber()
    highest_ts = query_timestamp_of_block(highest_bn)
    lowest_bn = 1
    lowest_ts = query_timestamp_of_block(lowest_bn)
    target_ts = date_to_timestamp(year, month, day)
    # Step 1: Shorten the possible range
    while(True):
        tmp_bn = int(lowest_bn + (highest_bn-lowest_bn)*(target_ts-lowest_ts)/(highest_ts-lowest_ts))
        tmp_ts = query_timestamp_of_block(tmp_bn)
        if (tmp_ts > target_ts):
            highest_ts = tmp_ts
            highest_bn = tmp_bn
        elif (tmp_ts < target_ts):
            lowest_ts = tmp_ts
            lowest_bn = tmp_bn
        else:
            return tmp_bn
        if (tmp_bn == int(lowest_bn + (highest_bn-lowest_bn)*(target_ts-lowest_ts)/(highest_ts-lowest_ts))):
            break
    # Step 2: Iterate from the lower bound of range
    tmp_ts = lowest_ts
    tmp_bn = lowest_bn
    while(tmp_ts < target_ts):
        tmp_bn += 1
        tmp_ts = query_timestamp_of_block(tmp_bn)
    return tmp_bn
        
# Two steps
# Step 1: Shorten the possible range
# Step 2: Iterate from the upper bound of range
def last_block_of_day(year, month, day):
    if not check_date_reasonable(year, month, day):
        return
    highest_bn = query_highest_blocknumber()
    highest_ts = query_timestamp_of_block(highest_bn)
    lowest_bn = 1
    lowest_ts = query_timestamp_of_block(lowest_bn)
    
    dt = datetime.datetime(year, month, day)
    dt_next = dt + datetime.timedelta(days=1)
    target_ts = date_to_timestamp(dt_next.year, dt_next.month, dt_next.day)-1
    # Step 1: Shorten the possible range
    while(True):
        tmp_bn = int(lowest_bn + (highest_bn-lowest_bn)*(target_ts-lowest_ts)/(highest_ts-lowest_ts))
        tmp_ts = query_timestamp_of_block(tmp_bn)
        if (tmp_ts > target_ts):
            highest_ts = tmp_ts
            highest_bn = tmp_bn
        elif (tmp_ts < target_ts):
            lowest_ts = tmp_ts
            lowest_bn = tmp_bn
        else:
            return tmp_bn
        if (tmp_bn == int(lowest_bn + (highest_bn-lowest_bn)*(target_ts-lowest_ts)/(highest_ts-lowest_ts))):
            break
    # Step 2: Iterate from the upper bound of range
    tmp_ts = lowest_ts
    tmp_bn = lowest_bn
    while(tmp_ts <= target_ts):
        tmp_bn += 1
        tmp_ts = query_timestamp_of_block(tmp_bn)
    tmp_bn -= 1
    return tmp_bn
    

# Split a large list into several smaller sub-lists and then run the function with each sub_list as argument
# arg1: list object
# arg2: number of parallel progresses
# arg3: a function with list as argument
def map_list(large_list, par, func):
    sublist_len = len(large_list) // par
    sublists = []
    for i in range(par-1):
        tmp_list = large_list[i*sublist_len:(i+1)*sublist_len]
        sublists.append(tmp_list)
    tmp_list = large_list[(par-1)*sublist_len:]
    sublists.append(tmp_list)
    p = Pool(par)
    results = p.map(func, sublists)
    p.close()
    return results    


# There are many groups of arguments and each group contains multiple arguments
# arg1: list of args group
# arg2: a function with multiple arguments
def map_args_group(args_group_list, func):
    par = len(args_group_list)
    p = Pool(par)
    results = p.starmap(func, args_group_list)
    p.close()
#     print ("{} has been finished".format(args_group_list))
    return results

    
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
    
    
def exeMultipleSQL(sqls, commit=False):
    connection = pymysql.connect(**db_config)
    try:
        with connection.cursor() as cursor:
            for sql in sqls:
                cursor.execute(sql)
        if commit:
            connection.commit()
        else:
            result = cursor.fetchall()
            return result
    finally:
        connection.close()    
    

def insert_action(directive, source, target, amount, tx, block_num, tx_seq, act_seq, table_name='action_20161001_20161231'):
    insert_act_sql = """INSERT INTO {} (directive, source, target, amount, tx, block_num, tx_seq, act_seq) 
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(table_name, directive, source,
                                                  target, amount, tx, block_num, tx_seq, act_seq)
    print(insert_act_sql)
    exeSQL(insert_act_sql, True)
    

def insert_multiple_actions(parsed_entries, table_name):
    insert_act_sqls = []
    for en in parsed_entries:
        sql = """INSERT INTO {} (directive, source, target, amount, tx, block_num, tx_seq, act_seq) 
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(table_name, *en)
        insert_act_sqls.append(sql)
    #exeMultipleSQL(insert_act_sqls, True)
    

def insert_multiple_accounts(parsed_entries, table_name):
    insert_acc_sqls = []
    for en in parsed_entries:
        sql = """INSERT INTO {} (address, kind) VALUES ('{}', '{}')""".format(table_name, *en)
        insert_acc_sqls.append(sql)
    exeMultipleSQL(insert_acc_sqls, True)
    
def create_action_table(table_name):
    create_action_sql = """CREATE TABLE {} ( `act_id` int(11) NOT NULL AUTO_INCREMENT,
                `directive` enum('call','create','reward-block','reward-uncle','suicide') NOT NULL, 
                `source` char(42) NOT NULL, `target` char(42) NOT NULL, `amount` varchar(32) NOT NULL, 
                `tx` char(66) NOT NULL, `block_num` int(11) NOT NULL, `tx_seq` int(11) NOT NULL, 
                `act_seq` int(11) NOT NULL, PRIMARY KEY (`act_id`), 
                UNIQUE KEY `unique_action` (`block_num`,`tx_seq`,`act_seq`), KEY `block_num_index` (`block_num`), 
                FULLTEXT `target_index` (`target`), FULLTEXT `source_index` (`source`), 
                FULLTEXT `tx_index` (`tx`) ) ENGINE=InnoDB""".format(table_name)
    exeSQL(create_action_sql, True)
    
def drop_table(table_name):
    drop_table_sql = """DROP TABLE {}""".format(table_name)
    exeSQL(drop_table_sql, True)
    
def insert_account(address, account_type, table_name):
    insert_act_sql = """INSERT INTO {} (address, kind) 
            VALUES ('{}', '{}')""".format(table_name, address, account_type)
#     print(insert_act_sql)
    exeSQL(insert_act_sql, True)
    
def fetchAddressSet(account_type, action_table_name):
    fetch_addrs_sql = ("SELECT {} FROM {}").format(account_type, action_table_name)
    addrs = exeSQL(fetch_addrs_sql)
    flat_addrs = [item for sublist in addrs for item in sublist]
    return set((flat_addrs))
	
'''
def modigy_insert_multiple_actions(parsed_entries):
    insert_act_sqls = []
    for en in parsed_entries:
        print(en)
    #exeMultipleSQL(insert_act_sqls, True)
    
#---------
'''
# coding: utf-8

# In[1]:


from utils import first_block_of_day, last_block_of_day, rpc_to_parity
from utils import drop_table, create_action_table
from utils import exeSQL, exeMultipleSQL, insert_action, insert_multiple_actions
from utils import map_list, map_args_group
from tqdm import tqdm_notebook as tqdm

date_start = '2016-10-01'
date_end = '2016-12-31'
block_start = first_block_of_day(int(date_start[:4]), int(date_start[5:7]), int(date_start[8:10]))
block_end = last_block_of_day(int(date_end[:4]), int(date_end[5:7]), int(date_end[8:10]))

table_name = "action_20161001_20161231"

par = 20


# In[2]:


print (block_start)
print (block_end)


# In[3]:


recreate = True
if recreate:
    drop_table('action_20161001_20161231')
    create_action_table('action_20161001_20161231')


# In[4]:

'''
def parse_action_create(dict_obj):
    if 'error' in dict_obj:
        return None
    directive = dict_obj['type']
    tx = dict_obj['transactionHash']
    block_num = dict_obj['blockNumber']
    tx_seq = dict_obj['transactionPosition']
    act_seq = 0
    dict_action = dict_obj['action']
    dict_result = dict_obj['result']
    source = dict_action['from']
    target = dict_result['address']
    amount = dict_action['value']
    parsed_entry = [directive, source, target, amount, tx, block_num, tx_seq, act_seq]
    return parsed_entry


# In[5]:


def parse_action_call(dict_obj):
    if 'error' in dict_obj:
        return None
    directive = dict_obj['type']
    tx = dict_obj['transactionHash']
    block_num = dict_obj['blockNumber']
    tx_seq = dict_obj['transactionPosition']
    act_seq = 0
    dict_action = dict_obj['action']
    source = dict_action['from']
    target = dict_action['to']
    amount = dict_action['value']
    parsed_entry = [directive, source, target, amount, tx, block_num, tx_seq, act_seq]
    return parsed_entry


# In[6]:


def parse_action_reward(dict_obj):
    dict_action = dict_obj['action']
    directive = dict_obj['type'] + "-" + dict_action['rewardType']
    tx = dict_obj['transactionHash']
    block_num = dict_obj['blockNumber']
    tx_seq = -1
    act_seq = 0
    source = 'None'
    target = dict_action['author']
    amount = dict_action['value']
    parsed_entry = [directive, source, target, amount, tx, block_num, tx_seq, act_seq]
    return parsed_entry


def parse_action_suicide(dict_obj):
    directive = dict_obj['type']
    tx = dict_obj['transactionHash']
    block_num = dict_obj['blockNumber']
    tx_seq = dict_obj['transactionPosition']
    act_seq = 0
    dict_action = dict_obj['action']
    source = dict_action['refundAddress']
    target = dict_action['address']
    amount = dict_action['balance']
    parsed_entry = [directive, source, target, amount, tx, block_num, tx_seq, act_seq]
    return parsed_entry

# In[7]:


def fetch_entries_from_actions(actions):
    parsed_entries = []
    for act in actions:
        if act['type'] == 'call':
            parsed_entry = parse_action_call(act)
        elif act['type'] == 'create':
            parsed_entry = parse_action_create(act)
        elif act['type'] == 'reward':
            parsed_entry = parse_action_reward(act)
        elif act['type'] == 'suicide':
            print(act)
            parsed_entry = parse_action_suicide(act)
        else:
            print(act)
        if parsed_entry != None:
            parsed_entries.append(parsed_entry)
    last_tx_hash = ''
    act_seq = -1
    for en in parsed_entries:
        if en[4] != last_tx_hash:
            last_tx_hash=  en[4]
            act_seq = 0
        else:
            act_seq += 1
        en[7] = act_seq
    return parsed_entries


# In[8]:


def delete_error_actions(actions):
    err_txs_list = []
    for act in actions:
        if 'error' in act:
            err_txs_list.append(act['transactionHash'])
    tailored_actions_list = []
    for act in actions:
        if not act['transactionHash'] in err_txs_list:
            tailored_actions_list.append(act)
    return tailored_actions_list


'''
# In[9]:


def parse_blocks(block_start, block_end, table_name):
#    print (block_start, block_end, table_name)
#    return
    for bn in tqdm(range(block_start, block_end)):
#         print ("######### Start to process block {} #########".format(bn))
        method = 'trace_block'
        params = [hex(bn)]
        actions_per_block = rpc_to_parity(method, params).json()['result']
#         print (actions_per_block)
        tailored_actions = delete_error_actions(actions_per_block)
        parsed_entries = fetch_entries_from_actions(tailored_actions)
        insert_multiple_actions(parsed_entries, table_name = table_name)
'''

# In[11]:

'''
interval_start = block_start
flag = True

while(flag):
    if interval_start + 600 >= block_end + 1:
        interval_end = block_end + 1
        flag = False
    else:
        interval_end = interval_start + 600
    print ("Blocks from {} to {} are in process!".format(interval_start, interval_end))
    args_groups_list = []
    sub_range_len = (interval_end - interval_start) // par
    for i in range(par-1):
        tmp_group = [i*sub_range_len+interval_start, (i+1)*sub_range_len+interval_start, table_name]
        args_groups_list.append(tmp_group)
    tmp_group = [(par-1)*sub_range_len+interval_start, interval_end, table_name]
    args_groups_list.append(tmp_group)
    print (map_args_group(args_groups_list, parse_blocks))
    interval_start += 600
'''
actions_per_block = rpc_to_parity("trace_block", ["0xe600"]).json()['result']
print (actions_per_block)
tailored_actions = delete_error_actions(actions_per_block)
parsed_entries = fetch_entries_from_actions(tailored_actions)
modify_insert_multiple_actions(parsed_entries)
