import psycopg2
import requests
from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime


dag_second_assignment = DAG(
	dag_id = 'second_assignment',
	start_date = datetime(2020,8,14), # 적당히 조절
	schedule_interval = '0 2 * * *')  # 적당히 조절

def get_Redshift_connection():
    host = "grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "hankyoul0919"
    redshift_pass = "Hankyoul09191!"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
        )
                            )
    conn.set_session(autocommit=True)
    return conn.cursor()


def etl():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    cur = get_Redshift_connection()
    extract = requests.get(link).text
    transform = extract.split('\n')[1:]
    truncate = "BEGIN; TRUNCATE TABLE hankyoul0919.name_gender; END;"
    cur.execute(truncate)
    for row in transform :
        if row != '' :
            name, gender = row.split(',')
            load = "BEGIN; INSERT INTO hankyoul0919.name_gender VALUES ('{name}', '{gender}'); END;".format(name=name, gender=gender)
            cur.execute(load)
            # print(cur.statusmessage) --> COMMIT
   

task = PythonOperator(
	task_id = 'perform_etl',
	python_callable = etl,
	dag = dag_second_assignment)
