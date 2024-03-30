from datetime import datetime

class App_Logger:
    def __init__(self):
        pass

    def log(self,file_object,log_message):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S") # The strftime() method returns a string representing date and time using date, time or datetime object.
        file_object.write(str(self.date)+ "/" + self.current_time + "\t\t" + log_message + "\n")
