from datetime import datetime
from application_loggin.logger import App_Logger
from os import listdir
import pandas as pd

class Data_transform:
    """
    This class shall be used for transforming the Good Raw Training Data before loading the data into the daabse.

    Written By: iNeuron Intellingece
    Version: 1.0
    Revision: None
    """

    def __init__(self):
        self.good_data_path = "H:/Data-Science-F-drive/waferFaultDetection/fariz_wafer_fault_detection/training_raw_files_validated/good_raw"
        self.logger = App_Logger()

    def replace_missing_with_null(self):
        """
        Method Name: replace_missing_with_null
        Description: This method replaces the missing values in columns with "NULL" to store in the table.
                     We are using substring in the first column to keep only "Integer" data for ease up the loading.
                     This column will anyways going to be removed during training.
        
        Written By: Fariz Ali
        Version: 1.0
        Revisions: None             
        """

        log_file = open("H:/Data-Science-F-drive/waferFaultDetection/fariz_wafer_fault_detection/training_logs/Data_transform_log.txt", "a+")
        try:
            onlyfiles = [f for f in listdir(self.good_data_path)]
            for file in onlyfiles:
                csv = pd.read_csv(self.good_data_path + "/" + file)
                csv.fillna("NULL",inplace=True)
                csv["Wafer"] = csv["Wafer"].str[6:]
                csv.to_csv(self.good_data_path + "/" + file, index=None,header=True)
                self.logger.log(log_file, " %s: File Transformed Successfully!! " % file)
        
        except Exception as e:
            self.logger.log(log_file, "Data Transformation failed because: %s" %e)
            log_file.close()
        log_file.close()