import psycopg2
import os

inFolder = r'/home/jonah/Desktop/GlobalFishingWatch/fishing_effort/daily_csvs'

lat_min = -60
lat_max = 20
lon_min = -120
lon_max = 90

# create a list of files
file_list = []
for f in os.listdir(inFolder):
    file_list.append(os.path.join(inFolder, f))

# posgresql connection parameters
conn = psycopg2.connect(host='localhost',
                        database='gfw',
                        user='postgres',
                        password='postgres')

# test the connection
cur = conn.cursor()
cur.execute('SELECT version()')
print(cur.fetchone())
cur.close()

# create table
create_table_command = """
CREATE TABLE public.effort
(
    date date,
    lat_bin character varying(10) COLLATE pg_catalog."default",
    lon_bin character varying(10) COLLATE pg_catalog."default",
    flag character varying(3) COLLATE pg_catalog."default",
    geartype character varying(15) COLLATE pg_catalog."default",
    vessel_hours double precision,
    fishing_hours double precision,
    mmsi_present integer
);"""

cur = conn.cursor()
cur.execute(create_table_command)
cur.close()
conn.commit()

# loop through csvs adding data
