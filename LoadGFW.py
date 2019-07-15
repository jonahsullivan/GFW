import psycopg2
import os
import csv

inFolder = r'/home/jonah/Desktop/GlobalFishingWatch/fishing_effort/daily_csvs'

lat_min = -60
lat_max = 20
lon_min = 60
lon_max = 180

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

# # create table
# create_table_command = """
# CREATE TABLE public.effort
# (
#     date date,
#     lat_bin character varying(10) COLLATE pg_catalog."default",
#     lon_bin character varying(10) COLLATE pg_catalog."default",
#     flag character varying(3) COLLATE pg_catalog."default",
#     geartype character varying(15) COLLATE pg_catalog."default",
#     vessel_hours double precision,
#     fishing_hours double precision,
#     mmsi_present integer
# );"""
#
# cur = conn.cursor()
# cur.execute(create_table_command)
# cur.close()
# conn.commit()

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
            if lat_max > lat > lat_min:
                lon = float(row[2]) / 100
                if lon_max > lon > lon_min:
                    entries.append(row)
    print("found " + str(len(entries)) + " rows")
    for entry in entries:
        lon = float(entry[2]) / 100
        lat = float(entry[1]) / 100
        del entry[1]
        del entry[1]
        coordinates = "POINT(%s %s)" % (lon, lat)
        entry.append(coordinates)
        cur.execute("""
                    INSERT INTO effort (date, flag, geartype, vessel_hours, fishing_hours, mmsi_present, geom)
                    VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326));
                    """,
                    entry)
    conn.commit()
cur.close()
conn.close()
