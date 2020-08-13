import psycopg2
import requests

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
    
def ETL() :
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
