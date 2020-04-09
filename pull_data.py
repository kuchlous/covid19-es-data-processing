import mysql.connector
from mysql.connector import Error
from datetime import datetime
from elasticsearch_dsl import Document, Date, Float, Integer, Keyword, Text, GeoPoint
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from datetime import datetime

PUT_PATH = 'rb_locations/_doc/'

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
    print("Logged at: ", datetime.now())
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
    print("Total number of rows in public_data_place_org_table is: ", cursor.rowcount)
    areas_hash = {}
    for row in area_records:
        areas_hash[row[0]] = row

    table_name = "nd_menuList"
    sql_select_query = "select * from " + table_name
    cursor = connection.cursor()
    cursor.execute(sql_select_query)
    menu_records = cursor.fetchall()
    print("Total number of rows in nd_menuList is: ", cursor.rowcount)

    menu_id_hash = {}
    for row in menu_records:
        menu_id_hash[row[0]] = row
    menu_name_hash = {}
    for row in menu_records:
        subcategory = row[1]
        parent_id = row[2]
        category = subcategory
        # For top level categories the parent category is empty, we probably
        # do not need to save it as the entities should all have only 
        # subcategories but save it anyway
        if parent_id:
            parent_category_row = menu_id_hash[parent_id]
            category = parent_category_row[1]
        menu_name_hash[(category, subcategory)] = row 

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
    n = 0
    for row in entity_records:
        n = n + 1
        if n % 1000 == 0:
            print ("Done: ", n)
        # print(row)
        e = Entity()
        e.id = row[0]
        e.meta.id = row[0]
        e.place_org_lat = row[3]
        e.place_org_long = row[4]
        e.cityId = row[10]
        if e.cityId and e.cityId in areas_hash:
            e.cityName = areas_hash[e.cityId][3]
        category = row[6]
        subcategory = row[7]
        if category and subcategory and (category, subcategory) in menu_name_hash:
            subcategory_row = menu_name_hash[(category, subcategory)]
            if subcategory_row[4]:
                e.icon = subcategory_row[4]
            e.menuId = subcategory_row[0]

        if row[3] and row[3] > -90.0 and row[3] < 90.0:
            e.lat = row[3]
        else:
            e.lat = float(0)
        if row[4] and row[4] > -90.0 and row[4] < 90.0:
            e.lng = row[4]
        else:
            e.lng = float(0)
        e.rb_pin = {"lat": e.lat, "lon": e.lng}

        e.total = 1
        e.type = "12"
        e.wardId = row[8]
        if row[8] and row[8] in areas_hash:
            e.wardName = areas_hash[row[8]][3]
        # e.mprint()
        e.save()
    print("Finished at: ", datetime.now())

except Error as e:
    print("Error connecting to ES", e)

