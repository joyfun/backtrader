#!/usr/bin/python3
# -*- coding: utf-8 -*-
# @Time    : 2020/9/15 13:34
# @Author  : joyfun
# @File    : WADBUtils.py
# @Software: PyCharm
import os
import pandas as pd
import yaml
import pymysql
from sqlalchemy.orm import sessionmaker
from pangres import upsert
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
from sqlalchemy.orm import scoped_session
allpool = {}


class WADBUtils:
    """
    通用数据库API 兼容pymysql mysqlClient
    """
    def __init__(self, filename="data.yml"):
        self.init_database(self.get_yaml_data(filename))

    def init_database(config):
        """
        :param config：配置文件路径 默认data.yml
        :return:void
        """
        # print(config["database"]["dev"])
        for (db, val) in config["database"].items():
            if "mysql" == val["type"]:
                try:
                    print("initDB:" + db)
                    #allpool[db] = PooledDB(pymysql, 20, host=val["host"], user=val["user"], passwd=val["password"],
                    #                       db=val["db"], port=val["port"],)
                    dburl = "mysql+pymysql://" + val["user"] + ":" + val["password"] + "@" + val["host"] + ":" + str(val[
                        "port"]) + "/" + val["db"]
                    print(dburl)
                    engine = create_engine(
                        dburl,
                        max_overflow=0,  # 超过连接池大小外最多创建的连接
                        pool_size=5,  # 连接池大小
                        pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
                        pool_recycle=-1  # 多久之后对线程池中的线程进行一次连接的回收（重置）
                    )
                    allpool[db] = engine
                except Exception as e:
                    print('Error msg: ')
                    print( e)

                # finally:
    def get_pool(self):
        return allpool

    def get_conn(dbname):
        """
        从连接池中返回对应的参数
        ：:param dbname：连接名称
        :return: conn 数据库连接
        """
        conn = allpool[dbname].raw_connection()
        return conn
    def get_engine(dbname):
        return allpool[dbname]

    def select_data_with(conn, sql,param):
        """
        根据sql返回查询结果 使用带参数的preparestatement 优先使用此方法
        :param sql: 查询sql
        :return:
        """
        try:
            cur = conn.cursor()
            cur.execute(sql, args=param)
            alldata = cur.fetchall()
        except Exception as e:
            print(e)
        finally:
            cur.close()
        return alldata

    def select_factor_by_date(conn,factorname,tradeday):
        fsql="select tradeday,stockcode,factornum from "+factorname +" where tradeday=%s"
        result=WADBUtils.select_data_with(conn,fsql,tradeday)
        df = pd.DataFrame(list(result),columns=['tradeday','stockcode','factornum'])
        return df

    def select_data(conn, sql):
        """
        根据sql返回查询结果 采用变参数形式的查询应该使用@select_data_with
        :param sql: 查询sql
        :return:查询结果集
        """
        try:
            cur = conn.cursor()
            if isinstance(sql, str):
                cur.execute(sql)
            else:
                cur.execute(sql[0])
            alldata = cur.fetchall()
        except Exception as e:
            print('Error msg: ' + e)
        finally:
            cur.close()
        return alldata

    def select_databypandas(conn, sql, index_col):
        """
        查询返回pandas
        :param sql:查询sql
        :param index_col:索引列
        :return:
        """
        try:
            # cur = conn.cursor()
            # cur.execute(sql)
            # alldata = cur.fetchall()
            if isinstance(sql, str):
                findsql = sql
            else:
                findsql = sql[0]
            if index_col != '':
                df = pd.read_sql(findsql, conn, index_col=index_col)
            else:
                df = pd.read_sql(findsql, conn)
        except Exception as e:
            print('Error msg: ' + e)
        #finally:
            # cur.close()
            #conn.close()
        return df

    def select_databypandas_with(conn, sql, param, index_col):
        """
        查询返回pandas 使用preparestament 优先使用此方法
        :param sql:查询sql
        :param param: 查询参数
        :param index_col:索引列
        :return:
        """
        df = None
        try:
            # cur = conn.cursor()
            # cur.execute(sql)
            # alldata = cur.fetchall()
            if isinstance(sql, str):
                findsql = sql
            else:
                findsql = sql[0]
            if index_col != '':
                df = pd.read_sql(sql, conn, params=param, index_col=index_col)
            else:
                df = pd.read_sql(findsql, conn, params=param)
        except Exception as e:
            print( e)
       # finally:
            #cur.close()
            #conn.close()
        return df

    def save_data(conn, insertsql, datalist):
        """
        保存数据进入数据库
        :param insertsql:插入语句
        :param datalist:结果集
        :return:
        """
        try:
            cur = conn.cursor()
            cur.executemany(insertsql, datalist)
            conn.commit()

        except Exception as e:
            print( e)
        finally:
            cur.close()
            # conn.close()

    def update_data(conn, updatesql):
        """
        更新数据库数据
        :param updatesql:更新sql
        :return:
        """
        try:
            cur = conn.cursor()
            cur.execute(updatesql)
            conn.commit()

        except Exception as e:
            print('Error msg: ' + e)
        finally:
            cur.close()

    def save_pandas(con_name, pd,tablename,if_exists="append",dtype=''):
        """
        保存pandas到数据库表
        :param updatesql:更新sql
        :return:
        """
        try:
            if dtype!='':
                dtype = {'name':VARCHAR(20)}
            pd.to_sql(tablename,WADBUtils.get_engine(con_name), index=True, dtype=dtype, if_exists=if_exists)

        except Exception as e:
            print('Error msg: ' + e)

    def update_pandas(conn_name,pd, table,  dtype = {'name':VARCHAR(20)},if_row_exists='update',limit=10 ** 5):

        upsert(WADBUtils.get_engine(conn_name),
               df=pd,
               table_name=table,
               if_row_exists=if_row_exists,
               dtype=dtype)
        return True

    def update_data_param(conn, updatesql,param):
        """
        更新数据库数据
        :param updatesql:更新sql
        :return:
        """
        try:
            cur = conn.cursor()
            cur.execute(updatesql, args=param)
            conn.commit()

        except Exception as e:
            print( e)
        finally:
            cur.close()

    def update_data_param_batch(conn, updatesql,param):
        """
        更新数据库数据
        :param updatesql:更新sql
        :return:
        """
        try:
            cur = conn.cursor()
            cur.executemany(updatesql, args=param)
            conn.commit()

        except Exception as e:
            print( e)
        finally:
            cur.close()

    def create_table(conn,createsql):
        try:
            cur = conn.cursor()
            cur.execute(createsql)
        except Exception as e:
            print('Error msg: ' + e)
        finally:
            cur.close()

    def drop_table(conn,dropsql):
        try:
            cur = conn.cursor()
            cur.execute(dropsql)
        except Exception as e:
            print('Error msg: ' + e)
        finally:
            cur.close()
            conn.close()

    def get_result(conn, sql, filename):
        """
        兼容旧接口
        :param sql:导出数据sql
        :param filename: 保存文件名
        :return:
        """

        print(sql)
        results = conn.select_data(sql)
        print('The amount of datas: %d' % (len(results)))
        with open(filename, 'w') as f:
            for result in results:
                f.write(str(result) + '\n')
        print('Data write is over!')
        return results

    def get_factorname_parameter(self):
        findfactornumsql = "SELECT distinct factorname,basename,parameter1,parameter2,parameter3,parameter4,\
        parameter5,parameter6,parameter7 FROM factorname_parameter "
        conn = WADBUtils.get_conn(dbname='jq')
        parameter = WADBUtils.select_databypandas(conn, findfactornumsql, "factorname")
        return parameter

    def get_yaml_data(yaml_file):
        # 打开yaml文件
        # print("***获取yam文件数据***")
        file = open(yaml_file, 'r', encoding='utf-8')
        file_data = file.read()
        file.close()
        #
        # print(file_data)
        # print("类型", type(file_data))
        #
        # # 将字符串转化为字典或列表
        # print("***转化yaml数据为字典或列表***")
        data = yaml.safe_load(file_data)  # safe_load，safe_load,unsafe_load
        # print(data)
        # print("类型", type(data))
        return data

    current_path = os.path.abspath(".")
    yaml_path = os.path.join(current_path, "data.yml")
    print('--------------------', yaml_path)
    init_database(get_yaml_data(yaml_path))
    ##pool = PooledDB(pymysql, 20, host=host, user=username, passwd=password, db=db,port=3306)
