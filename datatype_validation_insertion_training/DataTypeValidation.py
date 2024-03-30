from application_logging.logger import App_Logger
import sqlite3
from os import listdir
import csv
import shutil
import os

class Db_operation:
    """
    This class shall be used for handling all the SQL operations.

    Written By: Fariz Ali
    Version: 1.0
    Revisions: None
    """

    def __init__(self) -> None:
        self.path = "training_database"
        self.bad_file_path = "training_raw_files_validated/bad_raw"
        self.good_file_path = "training_raw_files_validated/good_raw"
        self.logger = App_Logger()

    def database_connection(self,database_name):
        """
        Method Name: database_connection
        Description: This method creates the database with the given name and if database already exists then opens the connection to the DB.
        Output: Connection to the DB
        On Failur: Raise ConnectionError

        Written By: Fariz Ali
        Version: 1.0
        Revisions: None
        """
        try:
            conn = sqlite3.connect(self.path+database_name+".db")

            file = open("training_logs/database_connection_log.txt","a+")
            self.logger.log(file,"Opened %s database successfully" % database_name)
            file.close()
        except ConnectionError:
            file = open("training_logs/database_connection_log.txt","a+")
            self.logger.log(file,"Error while connecting to database: %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return conn
    
    def create_table_db(self,database_name,column_names):
        """
        Method Name: create_table_db
        Description: This method creates a table in the given database which will be used to insert the good data after raw data has been validated.
        Output: None
        On Failure: Raise Exception

        Written By: Fariz Ali
        Versions: 1.0
        Revisions: None
        """

        try:
            conn = self.database_connection(database_name)
            c = conn.cursor
            c.execute("SELECT count(name) FROM sqlite_master WHERE type = 'table' AND name = 'good_raw_data'")
            if c.fetchone()[0] == 1:
                conn.close()
                file = open("training_logs/db_table_create_log.txt","a+")
                self.logger.log(file,"Tables created successfully!")
                file.close()

                file = open("training_logs/database_connection_log.txt",'a+')
                self.logger.log(file,"Closed %s database successfully" %database_name)
                file.close()
        
            else:
                for key in column_names.keys():
                    type = column_names[key]

                    # in try block we check if the table exists, if yes then add columns to the table
                    # else in catch block we will create the table
                    try:
                        conn.execute("ALTER TABLE good_raw_data ADD COLUMN'{column_name}'{data_type}".format(column_name=key, data_type = type))
                    except:
                        conn.execute('CREATE TABLE good_raw_data ({column_name} {data_type})'.format(column_name = key, data_type = type))
                
                conn.close()

                file = open("training_logs/db_table_create_log.txt","a+")
                self.logger.log(file,"Tables created successfully")
                file.close()

                file = open("training_logs/database_connection_log.txt","a+")
                self.logger.log(file,"Closed %s database connection successfully"%database_name)
                file.close()

        except Exception as e:
            file = open("training_logs/db_table_create_log.txt","a+")
            self.logger.log(file, "Error while creating tables: %s" %e)
            file.close()
            
            conn.close()
            file = open("training_logs/database_connection_log.txt","a+")
            self.logger.log(file, "Closed %s database connection successfully"%database_name)
            file.close()
            raise e
        
    def insert_into_table_good_data(self,database):
        """
        Method Name: insert_into_table_good_data
        Description: This method inserts the good data files from the good_raw_folder into the above created table.
        Output: None
        On Failure: Raise Exception

        Written By: Fariz Ali
        Versions: 1.0
        Revisions: None
        """

        conn = self.database_connection(database)
        good_file_path = self.good_file_path
        bad_file_path = self.bad_file_path
        onlyfiles = [f for f in listdir(good_file_path)]
        log_file = open("training_logs/db_inset_log.txt","a+")

        for file in onlyfiles:
            try:
                with open(good_file_path+"/"+file,"r") as f:
                    next(f)
                    reader = csv.reader(f,delimeter = "\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute("INSERT INTO good_raw_data values ({values})".format(values = list_))
                                self.logger.log(log_file, " %s: File loaded successfully!!" % file)
                                conn.commit()
                            except Exception as e:
                                raise e
            except Exception as e:

                conn.rollback()
                self.logger.log(log_file, "Error while creating table: %s" % e)
                shutil.move(good_file_path+"/"+file, bad_file_path)
                self.logger.log(log_file,"File moved successfully %s" %file)
                log_file.close()
                conn.close()
            
        log_file.close()
        conn.close()
    
    def selecting_data_from_table_into_csv(self,database):
            """
            Method Name: selecting_data_from_table_into_csv
            Description: This method exports the data in good_raw_data table as a CSV file. In the given location above created.
            Output: None
            On Failure: Raise Exception

            Written By: Fariz Ali
            Version: 1.0
            Revisions: None
            """

            self.file_from_db = "training_file_from_db/"
            self.file_name = "input_file.csv"
            log_file = open("training_logs/export_to_csv.txt","a+")
            try:
                conn = self.database_connection(database)
                sql_select = "SELECT * FROM good_raw_data"
                cursor = conn.cursor

                cursor.execute(sql_select)

                results = cursor.fetchall()

                # Get the headers of the csv file
                headers = [i[0] for i in cursor.description]

                # Make the CSV output directory
                if not os.path.isdir(self.file_from_db):
                    os.makedirs(self.file_from_db)
                
                # Open CSV file for writing
                csv_file = csv.writer(open(self.file_from_db + self.file_name, 'w', newline=''),delimiter=",", lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar= '\\') 

                # Add the headers and data to the CSV file
                csv_file.writerow(headers)
                csv_file.writerows(results)

                self.logger.log(log_file,"File exported successfully!!!")
                log_file.close()
            
            except Exception as e:
                self.logger.log(log_file, "File exporting failed. Error: %s" %e)
                log_file.close()


