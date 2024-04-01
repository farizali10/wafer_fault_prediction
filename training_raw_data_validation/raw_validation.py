import json
from application_logging.logger import App_Logger
import os
import datetime
import shutil

class Raw_data_validation:
    """
    This class shall be used for handling all the validation done on the Raw Training Data.

    Written By: Fariz Ali
    Version: 1.0
    Revisions: None
    """

    def __init__(self,path):
        self.batch_directory = path
        self.schema_path = "schema_training.json"
        self.logger = App_Logger()
    
    def valuesfromschema(self):
        """
        Method Name: valuesfromschema
        Description: This method extracts all the relevant information from the pre-defined "Schema" field.
        Output: LengthofDateStampInFile, LengthOfTimeStampInFile, NumberofColumns, ColumnName
        On Failure: Raise ValueError,KeyError,Exception

        Written By: Fariz Ali
        Version: 1.0
        Revisions: None
        """

        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            
            pattern = dic["SampleFileName"]
            LengthOfTimeStampInFile = dic["LengthOfTimeStampInFile"]
            LengthOfDateStampInFile = dic["LengthOfDateStampInFile"]
            column_names = dic["ColName"]
            NumberofColumns = dic["NumberofColumns"]

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt","a+")
            message = "LengthofDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" %LengthOfTimeStampInFile + "\t" + "NumberofColumns:: %s" %NumberofColumns + "\n"
            self.logger.log(file,message)

            file.close()

        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt","a+")
            self.logger.log(file,"ValueError: Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt","a+")
            self.logger.log(file,"KeyError: Key Value Error, incorrect key passed")
            file.close()
            raise KeyError
        
        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt","a+")
            self.logger.log(file,str(e))
            file.close()
            raise e
        
        return LengthOfDateStampInFile, LengthOfTimeStampInFile, NumberofColumns, column_names
    
    def manual_regex_creation(self):
        """
        Method Name: manual_regex_creation
        Description: This method contains a manually defined regex based on the "file_name" given in "Schema" file.
                     This Regex is used to validate the filename of the training data.
        Output: Regex Pattern
        On Failure: None

        Written By: Fariz Ali
        Version: 1.0
        Revisions: None
        """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def create_directory_for_good_bad_raw_data(self):
        try:
            path = os.path.join("training_raw_files_validated/","good_raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("training_raw_files_validated/", "bad_raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("training_logs/general_log.txt","a+")
            self.logger.log(file,"Error while creating directory %s:" %ex)
            file.close()
            raise OSError
        
    def delete_existing_good_data_training_folder(self):
        """
        Method Name: delete_existing_good_data_training_folder
        Description: This method deletes the directory made to store the Good Data
                     after loading the data in the table. Once the good files are
                     loaded in the DB, deleting the directory ensures space optimization.
        Ouput: None
        On Failure: OS Error

        Written By: Fariz Ali
        Version: 1.0
        Revisions: None
        """

        try:
            path = 'training_raw_files_validated/'
            if os.path.isdir(path + "good_raw/"):
                shutil.rmtree(path + "good_raw/")
                file = open("training_logs/general_log.txt","a+")
                self.logger.log(file,"Good Raw directory deleted successfully")
                file.close()
        except OSError as s:
            file = open("training_logs/general_log.txt","a+")
            self.logger.log(file,"Error while deleting directory: %s" %s)
            file.close()
            raise OSError
        
    def delete_existing_bad_data_training_folder(self):
        """
        Method Name: delete_existing_bad_data_training_folder
        Description: This method deletes the directory made to store the bad data

        Ouput: None
        On Failure: OS Error

        Written By: Fariz Ali
        Version: 1.0
        Revisions: None
        """

        try:
            path = 'training_raw_files_validated/'
            if os.path.isdir(path + "bad_raw/"):
                shutil.rmtree(path + "bad_raw/")
                file = open("training_logs/general_log.txt","a+")
                self.logger.log(file,"Bad Raw directory deleted successfully")
                file.close()
        except OSError as s:
            file = open("training_logs/general_log.txt","a+")
            self.logger.log(file,"Error while deleting directory: %s" %s)
            file.close()
            raise OSError
    
    def move_bad_files_to_archived(self):
        """
        Method Name: move_bad_files_to_archived
        Description: This method deletes the directory made to store the bad data after moving the data in an archive folder. 
                     We archive the bad files to send them back to the client for invalid data isuse.
        
        Output: None
        On Failure: OS Error

        Written By: Fariz Ali
        Version: 1.0
        Revisions: None
        """

        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")

        try:
            source = 'training_raw_files_validated/bad_raw/'
            if os.path.isdir(source):
                path = "training_archive_bad_data"
                
                if not os.path.isdir(path):
                    os.makedirs(path)
                
                dest = "training_archive_bad_data/bad_data_" + str(date) + "_" + str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source+f,dest)

                file = open("training_logs/general_log.txt","a+")
                self.logger.log(file,"Bad files moved to archive")
                
                path = 'training_raw_files_validated/'
                if os.path.isdir(path + 'bad_raw/'):
                    shutil.rmtree(path+"bad_raw/")
                self.logger.log(file,"Bad Raw Data folder deleted successfully!")
                file.close()
        
        except Exception as e:
            file = open("training_logs/general_log.txt","a+")
            self.logger.log(file,"Error while moving bad files to archive:: %s" %e)
            file.close()
            raise e
        
    def validation_file_name_raw(self,regex,length_of_date_stamp_in_file,length_of_time_stamp_in_file):
        """
        Method Name: validation_file_name_raw
        Description: This function validates the name of the training csv files as per given name in the schema.
                     Regex pattern is used to do the validation. If name format do not match the file is moved to bad raw data folder else in good raw data.
        Output: None
        On Failure: Exception

        Written By: Fariz Ali
        Version: 1.0
        Revisions: None
        """

        # Delete the directories for good and bad data in case last run was unsuccessfull and folders were not deleted.
        self.delete_existing_bad_data_training_folder()
        self.delete_existing_good_data_training_folder()

        # Create new directories
        self.create_directory_for_good_bad_raw_data()
        onlyfiles = [for f in listdir(self.batch_directory)]


