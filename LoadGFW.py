import psycopg2
import os
import csv

inFolder = r'/home/jonah/Desktop/fishing_effort/daily_csvs'

lat_min1 = -60
lat_max1 = 20
lon_min1 = 60
lon_max1 = 180

lat_min2 = -60
lat_max2 = 20
lon_min2 = -180
lon_max2 = -120

create_table = False

# create a list of files
file_list = []
for f in os.listdir(inFolder):
    file_list.append(os.path.join(inFolder, f))

# posgresql connection parameters
conn = psycopg2.connect(host='localhost',
                        database='gfw',
                        user='jonah',
                        password='kewpie')

# test the connection
cur = conn.cursor()
cur.execute('SELECT version()')
print(cur.fetchone())
cur.close()

# create table
if create_table:

    create_table_command = """
    CREATE TABLE public.effort
    (
        date date NOT NULL,
        flag character varying(3) COLLATE pg_catalog."default",
        geartype character varying(25) COLLATE pg_catalog."default",
        vessel_hours double precision,
        fishing_hours double precision,
        mmsi_present integer,
        geom geometry NOT NULL,
        CONSTRAINT effort_pkey PRIMARY KEY (date, flag, geartype, geom)
    );"""

    cur = conn.cursor()
    cur.execute(create_table_command)
    cur.close()
    conn.commit()

# loop through csvs adding data
cur = conn.cursor()
for f in file_list:
    print(f)
    entries = []
    with open(f, 'r') as csvfile:
        csvfile.readline()  # skip header
        reader = csv.reader(csvfile)
        for row in reader:
            lat = float(row[1]) / 100
            if lat_max1 > lat > lat_min1 or lat_max2 > lat > lat_min2:
                lon = float(row[2]) / 100
                if lon_max1 > lon > lon_min1 or lon_max2 > lon > lon_min2:
                    entries.append(row)
    print("found " + str(len(entries)) + " rows")
    for entry in entries:
        lon = float(entry[2]) / 100
        lat = float(entry[1]) / 100
        del entry[1]
        del entry[1]
        coordinates = "POINT(%s %s)" % (lon, lat)
        entry.append(coordinates)
        try:
            cur.execute("""
                        INSERT INTO effort (date, flag, geartype, vessel_hours, fishing_hours, mmsi_present, geom)
                        VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326));
                        """,
                        entry)
        except psycopg2.IntegrityError:
            conn.rollback()
        else:
            conn.commit()
cur.close()
conn.close()
