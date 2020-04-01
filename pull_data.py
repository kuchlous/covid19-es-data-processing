import mysql.connector
from mysql.connector import Error
from datetime import datetime
from elasticsearch_dsl import Document, Date, Float, Integer, Keyword, Text, GeoPoint
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

PUT_PATH = 'reapbenefit_locations/_doc/'

class Entity(Document):
    id = Integer()
    cityId = Keyword()
    wardId = Integer()
    level = Integer()
    menuId = Keyword()
    rb_pin = GeoPoint()
    lat = Float()
    lng = Float()
    total = Integer()
    type = Keyword()
    data = Text()
    wardName = Keyword()
    cityName = Keyword()
    icon = Keyword()

    class Index:
        name = 'rb_locations'
        settings = {
          "number_of_shards": 2
        }

    def mprint(self):
        print (self.meta.id, self.lat, self.lng, self.total, self.type, self.data, self.wardName, self.cityName, self.icon, self.menuId)


try:
    connection = mysql.connector.connect(host='devlp.solveninja.org',
                                         database='theapp',
                                         password='S0lvesm@lld3ntbig',
                                         user='curiouscat')
    table_name = "public_data_place_org_table"
    sql_select_query = "select * from " + table_name
    cursor = connection.cursor()
    cursor.execute(sql_select_query)
    entity_records = cursor.fetchall()
    print("Total number of entities: ", cursor.rowcount)

    table_name = "public_data_places_master"
    sql_select_query = "select * from " + table_name
    cursor = connection.cursor()
    cursor.execute(sql_select_query)
    area_records = cursor.fetchall()
    print("Total number of rows in Laptop is: ", cursor.rowcount)
    areas_hash = {}
    for row in area_records:
        areas_hash[row[0]] = row

    table_name = "nd_menuList"
    sql_select_query = "select * from " + table_name
    cursor = connection.cursor()
    cursor.execute(sql_select_query)
    menu_records = cursor.fetchall()
    print("Total number of rows in Laptop is: ", cursor.rowcount)
    menu_hash = {}
    for row in menu_records:
        menu_hash[row[1]] = row 


except Error as e:
    print("Error reading data from MySQL table", e)

finally:
    if (connection.is_connected()):
        connection.close()
        cursor.close()
        print("MySQL connection is closed")

try:
    # Define a default Elasticsearch client
    connections.create_connection(hosts=['localhost'], timeout=20)
    Entity.init()
    id = 0
    for row in entity_records:
        print(row)
        e = Entity()
        e.id = row[0]
        e.meta.id = row[0]
        e.place_org_lat = row[3]
        e.place_org_long = row[4]
        e.cityId = row[10]
        if e.cityId and e.cityId in areas_hash:
            e.cityName = areas_hash[e.cityId][3]
        if row[7] and row[7] in menu_hash:
            if menu_hash[row[7]][4]:
                e.icon = './assets/Icons/'+menu_hash[row[7]][4]

        if row[3]:
            e.lat = row[3]
        else:
            e.lat = float(0)
        if row[4]:
            e.lng = row[4]
        else:
            e.lng = float(0)
        e.rb_pin = {"lat": e.lat, "lon": e.lng}

        e.total = 1
        e.type = "12"
        if row[7] and row[7] in menu_hash:
            e.menuId = menu_hash[row[7]][0]
        e.wardId = row[8]
        if row[8] and row[8] in areas_hash:
            e.wardName = areas_hash[row[8]][3]
        e.mprint()
        e.save()

    results = Entity.search().filter(
            'geo_bounding_box', 
            rb_pin={
                "top_left": {
                    "lat": 12.98,
                    "lon": 77.50
                    },
                "bottom_right": {
                    "lat": 12.95,
                    "lon": 77.75
                    }
                }).execute()
    for e in results:
        e.mprint()

except Error as e:
    print("Error connecting to ES", e)

