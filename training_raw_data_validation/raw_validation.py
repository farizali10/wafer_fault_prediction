import json
from application_logging.logger import App_Logger

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