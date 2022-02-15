import logging
import sys

import mysql.connector
from interface import implements

from ExtendedObjectStorageInterface import ExtendedObjectStorageInterface
from S3ObjectStorageImpl import S3ObjectStorageImpl

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

DB_NAME = "storage"
TABLE_NAME = "OBJECTS"
USE_STORAGE = "USE " + DB_NAME
CREATE_DB = "CREATE DATABASE IF NOT EXISTS " + DB_NAME + " CHARACTER SET ucs2 COLLATE ucs2_general_ci"
CREATE_BUCKET_TABLE_SQL_FMT = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + \
                              "(bucket_name VARCHAR(64) NOT NULL, " \
                              "obj_key VARCHAR(1024) NOT NULL, " \
                              "alt_obj_key VARCHAR(1024)," \
                              "PRIMARY KEY (bucket_name, obj_key))"
DELETE_BUCKET_TABLE_SQL_FMT = "DROP TABLE " + TABLE_NAME

CREATE_OBJECT_SQL = "REPLACE INTO " + TABLE_NAME + " (bucket_name, obj_key) VALUES (%s, %s)"
SELECT_OBJECT_SQL = "SELECT * FROM " + DB_NAME + "." + TABLE_NAME + " WHERE bucket_name = %s and obj_key = %s"
UPDATE_ALT_OBJECT_SQL = "UPDATE " + DB_NAME + "." + TABLE_NAME + " " + \
                          "SET alt_obj_key = %s " + \
                          "WHERE bucket_name = %s AND obj_key = %s"

DELETE_OBJECT_SQL = "DELETE FROM " + DB_NAME + "." + TABLE_NAME + " " + \
                    "WHERE bucket_name = %s AND obj_key = %s"

SELECT_PREFIX_SQL = "SELECT * " + \
                    "FROM " + DB_NAME + "." + TABLE_NAME + " " + \
                    "WHERE bucket_name = %(bucket_name)s AND obj_key LIKE CONCAT(%(object_name)s , '_%')"


class ExtendedObjectStorage(implements(ExtendedObjectStorageInterface)):

    def __init__(self):
        self.storage = S3ObjectStorageImpl()
        self.db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="administrator")

        if self.db.is_connected():
            cursor = self.db.cursor()
            cursor.execute(CREATE_DB)
            cursor.execute(USE_STORAGE)
            cursor.execute(CREATE_BUCKET_TABLE_SQL_FMT)
        logging.info("Done initializing connection with DB")

    def create_object(self, bucket_name, key, path_to_file):
        if self.db.is_connected():
            self.storage.create_object(bucket_name, key, path_to_file)

            val = (bucket_name, key)
            cursor = self.db.cursor()
            cursor.execute(CREATE_OBJECT_SQL, val)
            self.db.commit()
            logging.info("Done creating a new object")

    def get_object(self, bucket_name, key, path_to_file):
        if self.db.is_connected():
            val = (bucket_name, key)
            cursor = self.db.cursor()
            cursor.execute(SELECT_OBJECT_SQL, val)
            result = cursor.fetchone()

            if result is not None:
                alt_obj_key = result[2]
                if alt_obj_key is not None:
                    key = alt_obj_key

            self.storage.get_object(bucket_name, key, path_to_file)

            logging.info("Done retrieving an object")

    def rename_object(self, source_bucket, source_key, dest_bucket, dest_key):
        if self.db.is_connected():
            val = (dest_key, source_bucket, source_key)
            cursor = self.db.cursor()
            cursor.execute(UPDATE_ALT_OBJECT_SQL, val)
            self.db.commit()

            val = (dest_bucket, dest_key)
            cursor = self.db.cursor()
            cursor.execute(CREATE_OBJECT_SQL, val)
            self.db.commit()

            self.storage.rename_object(source_bucket, source_key, dest_bucket, dest_key)

            val = (source_bucket, source_key)
            cursor = self.db.cursor()
            cursor.execute(DELETE_OBJECT_SQL, val)
            self.db.commit()

            logging.info("Done renaming object")

    def delete_object(self, bucket_name, key):
        if self.db.is_connected():
            val = (bucket_name, key)
            cursor = self.db.cursor()
            cursor.execute(DELETE_OBJECT_SQL, val)
            self.db.commit()

            self.storage.delete_object(bucket_name, key)

            logging.info("Done deleting object")

    def create_directory(self, bucket_name, key):
        if self.db.is_connected():
            self.storage.create_directory(bucket_name, key)

            val = (bucket_name, key)
            cursor = self.db.cursor()
            cursor.execute(CREATE_OBJECT_SQL, val)
            self.db.commit()
            logging.info("Done creating a new directory")

    def list_directory(self, bucket_name, source_dir):
        if self.db.is_connected() and source_dir.endswith('/'):

            val = {'bucket_name': bucket_name, 'object_name': source_dir}
            cursor = self.db.cursor()
            cursor.execute(SELECT_PREFIX_SQL, val)
            result = cursor.fetchall()

            if result is not None:
                ret_val = []
                for record in result:
                    ret_val.append(record[1])
                print(ret_val)
                logging.info("Done listing a directory")
                return ret_val

    def rename_directory(self, bucket_name, source_dir, dest_dir):
        if self.db.is_connected() and source_dir.endswith('/') and dest_dir.endswith('/'):
            val = {'bucket_name': bucket_name, 'object_name': source_dir}
            cursor = self.db.cursor()
            cursor.execute(SELECT_PREFIX_SQL, val)
            result = cursor.fetchall()

            if result is not None:

                create_dir_val = (bucket_name, dest_dir)
                cursor.execute(CREATE_OBJECT_SQL, create_dir_val)
                self.db.commit()

                for record in result:
                    alt_key = record[1].replace(source_dir, dest_dir)
                    update_val = (alt_key, bucket_name, record[1])
                    cursor.execute(UPDATE_ALT_OBJECT_SQL, update_val)
                    self.db.commit()

                    create_val = (bucket_name, alt_key)
                    cursor.execute(CREATE_OBJECT_SQL, create_val)
                    self.db.commit()

                self.storage.rename_directory(bucket_name=bucket_name,
                                              source_dir_name=source_dir,
                                              dest_dir_name=dest_dir)
                for record in result:
                    delete_val = (bucket_name, record[1])
                    cursor.execute(DELETE_OBJECT_SQL, delete_val)
                    self.db.commit()

                delete_dir_val = (bucket_name, source_dir)
                cursor.execute(DELETE_OBJECT_SQL, delete_dir_val)
                self.db.commit()

            logging.info("Done renaming directory")


if __name__ == "__main__":
    extended_storage = ExtendedObjectStorage()
    extended_storage.create_directory(bucket_name="private-rludan-preprod", key="A/")
    extended_storage.create_object(bucket_name="private-rludan-preprod", key="A/kolshehu", path_to_file="dummy.txt")
    extended_storage.create_directory(bucket_name="private-rludan-preprod", key="A/B/")
    extended_storage.create_object(bucket_name="private-rludan-preprod", key="A/B/kolshehu", path_to_file="dummy.txt")
    extended_storage.create_object(bucket_name="private-rludan-preprod", key="kolshehu", path_to_file="dummy.txt")
    extended_storage.rename_object(source_bucket="private-rludan-preprod", source_key="A/B/kolshehu",
                                   dest_bucket="private-rludan-preprod", dest_key="A/B/sheker")
    extended_storage.get_object(bucket_name="private-rludan-preprod", key="A/B/sheker", path_to_file="dummy2.txt")
    extended_storage.list_directory(bucket_name="private-rludan-preprod", source_dir="A/")
    extended_storage.rename_directory(bucket_name="private-rludan-preprod", source_dir="A/B/", dest_dir="A/C/")
