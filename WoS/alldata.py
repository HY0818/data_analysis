import warnings

warnings.simplefilter(action='ignore', category=UserWarning)

import pymysql
import pandas as pd
from sqlalchemy import create_engine

host = "localhost"
port = 3306
user = "root"
db = "data"
password = "123456"
charset = "utf8"


# 建立数据库链接
def get_connection():
    conn = pymysql.connect(host=host, port=port, user=user, password=password, charset=charset, db=db)
    return conn


# 读取数据并进行NA处理,处理后写回数据库
def pro1():
    conn = get_connection()
    sql = 'select * from alldata'
    df = pd.read_sql(sql, conn)

    df.dropna(axis=1, how='all', inplace=True)
    engine = create_engine("mysql+pymysql://root:123456@localhost:3306/data?charset=utf8mb4")
    df.to_sql(name="alldata_a", con=engine, if_exists='replace', index=False, index_label=False)
    df.info()

    print(df)


if __name__ == '__main__':
    pro1()


# 对无意义数据进行处理
def pro2():
    conn = get_connection()
    sql = 'select * from alldata_a'
    df1 = pd.read_sql(sql, conn)

    df1 = df1.drop(
        columns=['Web of Science Record', 'Date of Export', 'Early Access Date', 'DOI Link', 'End Page', 'Start Page',
                 'Book Authors', 'Authors'])
    engine = create_engine("mysql+pymysql://root:123456@localhost:3306/data?charset=utf8mb4")
    df1.to_sql(name="alldata_b", con=engine, if_exists='replace', index=False, index_label=False)
    df1.info()

    print(df1.info())


if __name__ == '__main__':
    pro2()

# def check_it():
#     conn = get_connection()
#     cursor = conn.cursor(pymysql.cursors.DictCursor)
#     cursor.execute("select * from alldata")
#     data = cursor.fetchone()
#     print(data)
#
#     cursor.close()
#     conn.close()


# if __name__ == '__main__':
#     check_it()
