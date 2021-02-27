

# -*- coding: utf-8 -*-
 
import pymysql
import datetime
import re
import time
import os
import traceback
import asyncio
import logging
from elasticsearch import helpers
from elasticsearch import Elasticsearch

def get_local_mysql():
    try:
        mysql_conn = pymysql.connect(
            # host='127.0.0.1',
            host='172.31.1.234',            
            port=3306,
            # user='root',
            # password='123456',
            user='hechanglong',
            password='rCq)QfqNFQI^',            
            # db='train',
            db='yc',
            charset='utf8'
        )
        return mysql_conn
    except Exception as e:
        print(e)
 

es = Elasticsearch(hosts="10.66.66.206", port=9200, http_auth=('elastic', '3eMK4t1AbtPtvSfSgiiS'),timeout=200)   ##账号/用户名：elastic  密码：3eMK4t1AbtPtvSfSgiiS
 
def es_main():
    
    mysql_conn = get_local_mysql()
    
    cursor0 = mysql_conn.cursor()
    # sql='SELECT * FROM `order` t ORDER BY t.id limit  100'
    # sql='SELECT * FROM `order` t where date(FROM_UNIXTIME(t.pay_time))>"2020-10-25" ORDER BY t.id limit  100'

    # 读取 sql 文件文本内容
    sql = open(r'/Users/hexuren/Desktop/MySql代码/代理商动销.sql', 'r', encoding = 'utf8')  # sql文件名， .sql后缀的
    sqltxt = sql.readlines() # 此时 sqltxt 为 list 类型
    # 读取之后关闭文件
    sql.close()
    # list 转 str
    sql = "".join(sqltxt)

    cursor0.execute(sql)
    res0 = cursor0.fetchall()
    
    list1 = []
    for row in cursor0.description:
        list1.append(row[0])
    tuple(list1)
    # print(len(list1))   ##看看有多少列字段
    actions = []
    tb_name = "agent_order_slq_to_es_prod"
    cnt = 0
    for r in res0:
        cnt += 1
        
        data = dict(zip(list1, r))
        
        print(cnt)
        
        try:                                                
            action = {"_index":tb_name, '_id': data["id"], "_source":data} 
            actions.append(action)
               
            # 每1000个一组批量导入
            if len(actions) == 100:
                print("execute 100 action")
                success,errors = helpers.bulk(es, actions, raise_on_error = True)
                actions = []
        except:        
            print('traceback.format_exc():\n%s' % traceback.format_exc())
    if len(actions) > 0:
        try:
            success,errors = helpers.bulk(es, actions, raise_on_error = True)
        except Exception as e:
            print(e)
 
if __name__ == '__main__':
    """程序定时循环"""
    t = 0
    t_last = 0
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(es_main())
    while True:
        time_now = datetime.datetime.now().replace(microsecond=0).hour     ##改为day则每天同步
        print(time_now)
        if t == 0:
            es_main()
            # asyncio.run(es_main().start())
            t = 1
            t_last = time_now
        else:
            if t_last == time_now:   ##相同 即处于同一时段则不处理
                pass
            else:
                t = 0
        logging.info('schedule_main ending!!!! time_sleep 1day')
        time.sleep(60*2)


# nohup python3 xxx.py & 

# ps -ef |grep python
# kill -9 10004












