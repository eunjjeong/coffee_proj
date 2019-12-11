import os
import shutil
from configparser import ConfigParser
from mysql.connector import Error, errorcode
from db_connection.db_connection import ExplicitlyConnectionPool


class DBInitService:
    OPTION = """
        CHARACTER SET 'UTF8'
        FIELDS TERMINATED by ','
        LINES TERMINATED by '\r\n'
        """

    def __init__(self, source_dir='data/', data_dir='/home/jjeong/data'):
        self._db = self.read_ddl_file()
        self.source_dir = os.path.abspath(source_dir) + "/"
        self.data_dir = os.path.abspath(data_dir) + "/"


    def read_ddl_file(self, filename='database_setting/coffee_ddl.ini'):
        parser = ConfigParser()
        parser.read(filename, encoding='UTF8')

        db = {}
        for sec in parser.sections():
            items = parser.items(sec)
            if sec == 'name':
                for key, value in items:
                    db[key] = value
            if sec == 'sql':
                sql = {}
                for key, value in items:
                    sql[key] = " ".join(value.splitlines())
                db['sql'] = sql
            if sec == 'user':
                for key, value in items:
                    db[key] = value
        return db


    def __create_database(self):
        try:
            sql = self.read_ddl_file()
            conn = ExplicitlyConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self._db['database_name']))
            print("CREATE DATABASE {}".format(self._db['database_name']))
        except Error as err:
            if err.errno == errorcode.ER_DB_CREATE_EXISTS:
                cursor.execute("DROP DATABASE {}".format(self._db['database_name']))
                print("DROP DATABASE {}".format(self._db['database_name']))
                cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self._db['database_name']))
                print("CREATE DATABASE {}".format(self._db['database_name']))
            else:
                print(err.msg)
        finally:
            cursor.close()
            conn.close()

    def __create_table(self):
        try:
            conn = ExplicitlyConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()
            cursor.execute("USE {}".format(self._db['database_name']))
            for table_name, table_sql in self._db['sql'].items():
                try:
                    print("Creating table {}: ".format(table_name), end='')
                    cursor.execute(table_sql)
                except Error as err:
                    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                        print("already exists.")
                    else:
                        print(err.msg)
                else:
                    print("OK")
        except Error as err:
            print(err)
        finally:
            cursor.close()
            conn.close()

    def __create_user(self):
        try:
            conn = ExplicitlyConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()
            print("Creating user: ", end='')
            cursor.execute(self._db['user_sql'])
            print("OK")
        except Error as err:
            print(err)
        finally:
            cursor.close()
            conn.close()

    def data_backup(self, table_name):
        filename = table_name + '.txt'
        try:
            conn = ExplicitlyConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()
            cursor.execute("USE {}".format(self._db['database_name']))
            source_path = self.source_dir + filename
            if os.path.exists(source_path):
                os.remove(source_path)

            backup_sql = "SELECT * FROM {} INTO OUTFILE '{}' {}".format(table_name, source_path, DBInitService.OPTION)
            cursor.execute(backup_sql)

            # if not os.path.exists(self.data_dir):
            #     os.makedirs(os.path.join('data'))
            # shutil.move(source_path, self.data_dir + '/' + filename)  # 파일이 존재하면 overwrite
            # shutil.copy(source_path, self.data_dir + '/' + filename)
            print(table_name, "backup complete!")
        except Error as err:
            print(err)
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    def data_restore(self, table_name):
        filename = table_name + '.txt'
        try:
            conn = ExplicitlyConnectionPool.get_instance().get_connection()
            cursor = conn.cursor()

            data_path = os.path.abspath(self.data_dir + filename).replace('\\', '/')
            if not os.path.exists(data_path):
                print("파일 '{}' 이 존재하지 않음".format(data_path))
                return

            restore_sql = "LOAD DATA INFILE '{}' INTO TABLE {} {}".format(data_path, table_name, DBInitService.OPTION)
            cursor.execute(restore_sql)
            conn.commit()
            print(table_name, "restore complete!")
        except Error as err:
            print(err)
            print(table_name, "restore fail!")
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

    def service(self):
        self.__create_database()
        self.__create_table()
        self.__create_user()
