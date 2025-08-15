from Conversions import DataConversions
from Fetch import FetchData
from LineData import MetroData

import numpy as np
from datetime import datetime
import time
import configparser
import os
import codecs
import sys


config = configparser.ConfigParser()
config_dir = os.path.dirname(os.path.realpath(__file__)) + '\\Config.ini'
config_parsed_files = config.read(config_dir)##read config file (must be in same ordner)

LINES =  ["U1","U2","U3","U4","U6"]
wait_time = 60
path = "C:\\github\\DIY-Electronic-Projects\\WienerLinienMonitor\\neu\\new_debug\\"

if __name__=="__main__":
    Co = DataConversions(LINES)
    file_date = open(path+"meassured_at.txt","w")
    
    
    ref_time = datetime.now() 
    Me = MetroData(LINES,Co,ref_time,config,False)
    Fe = FetchData(LINES,Me,Co,config)
    meass_st = Fe.meass_stations
    time.sleep(10)
    print("now")
    for line in LINES:
        data = Fe.fetch(meass_st[line])
        while(data==None):
            print("bad response")
            time.sleep(wait_time)
            data = Fe.fetch(meass_st[line])
        print("good resp")
        file_date.write(line  + ": " + str(datetime.now()) + "\n")
        
        with codecs.open(f"C:\\github\\DIY-Electronic-Projects\\WienerLinienMonitor\\neu\\new_debug\\{line}.json",'x',"ISO-8859-1") as file:
            file.write(data.decode("ISO-8859-1"))
            file.close()
        time.sleep(wait_time)
    initial_data = Fe.fetch(Fe.INITIAL_MEASSURE_divas)
    with codecs.open(f"C:\\github\\DIY-Electronic-Projects\\WienerLinienMonitor\\neu\\new_debug\\init.json",'x',"ISO-8859-1") as file:
        file.write(initial_data.decode("ISO-8859-1"))
        file.close()
    file_date.close()