import mysql.connector
from mysql.connector import Error
from datetime import datetime
from elasticsearch_dsl import Document, InnerDoc, Nested, Date, Float, Integer, Keyword, Text, GeoPoint
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from datetime import datetime
from os import getenv
from sys import exit
import math

class CreatorOrg(Document):
    creator_org_name = Keyword()
    creator_id = Integer()
    created_by = Keyword()

    class Index:
        name = 'rb_creator_orgs'
        settings = {
          "number_of_shards": 2
        }


class Person(InnerDoc):
    name = Keyword()
    designation = Keyword()
    mobile = Keyword()
    email = Keyword()

    def mprint(self):
        print("Person: ", self.name, self.designation, self.mobile, self.email)

    def __eq__(self, other):
        if self.name != other.name or self.designation != other.designation or self.mobile != other.mobile or self.email != other.email:
            return False

        return True


class Entity(Document):
    id = Integer()
    name = Keyword()
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
    address = Keyword()
    jurisdiction = Text()
    icon = Keyword()
    impact = Integer()
    category = Keyword()
    subcategory = Keyword()
    closed_at = Date()
    closed_by = Keyword()
    phone = Keyword()
    created_by = Keyword()
    creator_org = Keyword()

    persons = Nested(Person)

    class Index:
        name = 'rb_locations'
        settings = {
          "number_of_shards": 2
        }

    def mprint(self):
        print ("Org: ", self.meta.id, self.name, self.cityId, self.wardId, self.level, self.menuId, self.lat, self.lng, self.total, self.type, self.data, self.wardName, self.cityName, self.address, self.jurisdiction, self.impact, self.category, self.subcategory, self.closed_at, self.closed_by, self.phone, self.created_by, self.creator_org)
        for person in self.persons:
            person.mprint();

    def __eq__(self, other):
        if self.id != other.id or self.name != other.name or self.cityId != other.cityId or self.wardId != other.wardId or self.level != other.level or self.menuId != other.menuId or self.total != other.total or self.type != other.type or self.data != other.data or self.wardName != other.wardName or self.cityName != other.cityName or self.address != other.address or self.jurisdiction != other.jurisdiction or self.icon != other.icon or self.impact != other.impact or self.category != other.category or self.subcategory != other.subcategory or self.closed_by != other.closed_by or self.closed_at != other.closed_at or self.phone != other.phone or self.created_by != other.created_by or self.creator_org != other.creator_org:
            return False

        if abs(self.lat - float(other.lat)) > math.pow(10, -8) or abs(self.lng - float(other.lng)) > math.pow(10, -8):
            return False

        if len(self.persons) != len(other.persons):
            return False

        for i, person in enumerate(self.persons):
            if not person == other.persons[i] :
                return False
        return True

def getFromES(id):
    e = None
    try:
        e = Entity.get(id=id)
    except Error as e:
        e = None
    finally:
        return e


try:
    print("Logged at: ", datetime.now())
    host = getenv("MYSQL_HOST")
    password = getenv("MYSQL_PASSWORD")
    user = getenv("MYSQL_USER")
    if not host or not password or not user:
        print("Need MYSQL_HOST, MYSQL_USER and MYSQL_PASSWORD, please set in environment")
        exit(1)
    connection = mysql.connector.connect(host=host,
                                         database='theapp',
                                         password=password,
                                         user=user)
    table_name = "public_data_place_org_table"
    self_solve_table_name = "self_solve"
    sql_select_query = "select public_data_place_org_table.*, self_solve.closed_at, self_solve.closed_by, public_place_data_creator_table.creator_org_name from public_data_place_org_table LEFT JOIN self_solve ON public_data_place_org_table.place_org_id = self_solve.place_org_id LEFT JOIN public_place_data_creator_table ON public_data_place_org_table.creator_org_id = public_place_data_creator_table.creator_id"
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


    person_id_hash = {}
    table_name = "public_data_person"
    sql_select_query = "select * from " + table_name
    cursor = connection.cursor()
    cursor.execute(sql_select_query)
    person_records = cursor.fetchall()
    for row in person_records:
        person_id_hash[row[0]] = row
    print("Total number of rows in person_records is: ", cursor.rowcount)

    org_id_hash = {}
    table_name = "person_org_affiliation"
    sql_select_query = "select * from " + table_name
    cursor = connection.cursor()
    cursor.execute(sql_select_query)
    affiliation_records = cursor.fetchall()
    for row in affiliation_records:
        org_id = row[1]
        person_id = row[2]
        if not org_id in org_id_hash:
            org_id_hash[org_id] = []
        org_id_hash[org_id].append(row)

    print("Total number of rows in affiliation_records is: ", cursor.rowcount)

    table_name = "public_place_data_creator_table"
    sql_select_query = "select * from " + table_name
    cursor = connection.cursor()
    cursor.execute(sql_select_query)
    creator_records = cursor.fetchall()
    print("Total number of rows in public_place_data_creator_table is: ", cursor.rowcount)

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
            print ("Done: ", n, flush=True)
        # print(row)
        e = Entity()
        e.id = row[0]
        e.name = row[1]
        e.meta.id = row[0]
        e.address = row[2]
        e.place_org_lat = row[3]
        e.place_org_long = row[4]
        category = row[6]
        subcategory = row[7]
        e.category = category
        e.subcategory = subcategory
        e.wardId = row[8]
        e.cityId = row[10]
        e.phone = row[12]
        e.jurisdiction = row[13]
        e.data = row[15]
        e.impact = row[16]
        # sms_sent = row[17]
        # flagged_as_erroneous = row[18]
        # logical_delete = row[19]
        e.created_by = row[20]
        # creator_org_id = row[21]
        e.closed_at = row[22]
        e.closed_by = row[23]
        e.creator_org = row[24]
        if e.cityId and e.cityId in areas_hash:
            e.cityName = areas_hash[e.cityId][3]
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
        if row[8] and row[8] in areas_hash:
            e.wardName = areas_hash[row[8]][3]
        if e.id in org_id_hash:
            affiliations = org_id_hash[e.id]
            for row in affiliations:
                person_id = row[2]
                name = ""
                person_phone = ""
                person_email = ""
                if person_id in person_id_hash:
                    person_row = person_id_hash[person_id]
                    name = person_row[2]
                    person_phone = person_row[10]
                    person_email = person_row[11]

                designation = row[4]
                mobile = row[5] 
                email = row[6]
                p = Person()
                p.name = name
                p.designation = designation
                p.mobile = mobile if mobile else person_phone
                p.email = email if email else person_email
                e.persons.append(p)

        saved = getFromES(e.id)
        if not (saved is None) and saved == e:
            continue
        print ("Trying to save id", e.id)
        e.mprint()
        if not(saved is None):
            saved.mprint()
        else:
            print("saved is None")

        e.save()

    print("Finished at: ", datetime.now())

    CreatorOrg.init()
    for row in creator_records:
        c = CreatorOrg()
        c.id = row[0]
        c.meta.id = row[0]
        c.creator_org_name = row[1]
        c.created_by = row[3]
        print("Saving One Creator")
        c.save()


except Error as e:
    print("Error connecting to ES", e)

