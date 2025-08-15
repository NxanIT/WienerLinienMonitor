from datetime import datetime
import urllib.error
import numpy as np
import urllib.request,json
from http import client
import logging
import time
from configparser import ConfigParser

from Conversions import DataConversions
from LineData import MetroData

import sys
import io

logger = logging.getLogger(__name__)

def seconds_since(since_time:datetime):
        return (datetime.now()-since_time).total_seconds()

def millis_since(since_time:datetime):
    return int((datetime.now()-since_time).total_seconds()*1000)

def delta_millis(time1:datetime, time2:datetime):
    return int((time2-time1).total_seconds()*1000)

def dateTimeFromString(string):
          """Takes the formatet departure time string and returns a datetime object
          """
          String_Time = time.strptime(string,'%Y-%m-%dT%H:%M:%S.000%z')
          return datetime.fromtimestamp(time.mktime(String_Time))

class FetchData:
    """This class fetches data from the API and provides the interface for monitor to the Departures object, calls to update the raw meassured data.
    """
    def __init__(self,LINES:list[str],Me:MetroData,Co:DataConversions,config:ConfigParser) -> None:
        self.LINES = LINES
        self.MIN_REFRESH_INTERVALL = config.getint('FETCH','MIN_REFRESH_INTERVALL')
        self.MAX_REFRESH_INTERVALL = config.getint('FETCH','MAX_REFRESH_INTERVALL')
        self.Me = Me
        self.Co = Co
        self.MEASS_STATIONS_PER_LINE = config.getint('FETCH','MEASS_STATIONS_PER_LINE')
        INITIAL_MEASSURE = json.loads(config.get('DEFAULT','INITIAL_MEASSURE'))
        self.INITIAL_MEASSURE_divas = [Co.getDiva(str_stop) for str_stop in INITIAL_MEASSURE]
        self.flag_debug_mode = config.getboolean('DEFAULT','FLAG_DEBUG')
        self.url_start = config.get('FETCH','url_start')
        self.url_inbetween = config.get('FETCH','url_inbetween')
        #fetch initial meassure data
        if(self.flag_debug_mode):
            self.file_path_debug = config.get('PATH','path') + '\\debug_files'
            logger.info(f"debug_mode enabled. File path for debug-files is: {self.file_path_debug}")
        self.last_update_tried = None
        initial_data = self.__fetch(self.INITIAL_MEASSURE_divas)
        while(initial_data==None and not self.flag_debug_mode):
            time.sleep(10*len(self.INITIAL_MEASSURE_divas))
            initial_data = self.__fetch(self.INITIAL_MEASSURE_divas)
        self.last_update_tried = datetime.now()
        # compute service operations and which stations are to be meassured
        self.Co.init_bounds_and_service(initial_data)
        self.meass_stations_ind = self.Co.createMeassureStations(self.MEASS_STATIONS_PER_LINE)
        
        self.meass_stations = {}
        for key in self.meass_stations_ind.keys():
            self.meass_stations[key] = [Co.getDivafromIndex(key,index) for index in self.meass_stations_ind[key]]
        self.Lines_meass = list(self.meass_stations_ind.keys())
        self.ref_time = np.zeros(len(self.meass_stations_ind),dtype=object)
        
        
        est_time_forallupdates = len(self.meass_stations_ind)*(self.MIN_REFRESH_INTERVALL+1)
        if(est_time_forallupdates>self.MAX_REFRESH_INTERVALL):
            logger.warning(f"With current configuration, the estimated refresh rate for each lines is {est_time_forallupdates}s")
        pass

    def check_for_updates(self):#TODO:remove/review old code - start
        """ if the last update is no longer than self.MIN_REFRESH_INTERVALL seconds ago, no update is 
        performed if any element of self.meass_stations has never been loaded it loads the first 
        of them otherwise it loads the element of self.meass_stations that is the most outdated, 
        if the update was more then self.MAX_REFRESH_INTERVALL seconds ago

        returns: True, if all elements of self.meass_stations have been updated once, i.e. if the displaydata has entries for every meassured station.
        """
        # if(np.all(self.ref_time==0)): #TODO: should not be needed anymore
        #     #no data fetched so far
        #     self.update_index(0)
        #     return False
        if(seconds_since(self.last_update_tried)<self.MIN_REFRESH_INTERVALL and not self.flag_debug_mode):
            # last update was less then MIN_REFRESH_INTERVALL ago, no update
            return False
        never_been_updated = np.where(self.ref_time==0)[0]
        if(np.any(never_been_updated)):
            # one of the meassured list elements has never been upddated, update this element
            updated_index = never_been_updated[0]
            self.update_index(int(updated_index))
            return not np.any(self.ref_time==0)
        #it is now ensured that every element of ref_time is of type datetime
        longest_without_update = np.min(self.ref_time)
        if(seconds_since(longest_without_update)>self.MAX_REFRESH_INTERVALL):
            updated_index = np.argmin(self.ref_time)
            self.update_index(int(updated_index))
        return True
    
    def update_index(self,update_index):#TODO:remove/review old code - start
        self.last_update_tried = datetime.now()
        update_line = self.Lines_meass[update_index]
        data = self.__fetch(self.meass_stations[update_line]) if not self.flag_debug_mode else self.__debug_fetch(update_index)
        if(data==None): return False
        self.ref_time[update_index] = self.last_update_tried
        time1 = datetime.now()
        converted_data = self.convertData(update_line,data)
        time2 = datetime.now()
        self.Me.updateDepartures(update_line,converted_data)
        logger.info('updated index %s of meass_stations, took %sms for fetching, %sms for conversion, %sms for updating departures.',update_index,delta_millis(self.last_update_tried,time1),delta_millis(time1,time2),millis_since(time2))
        return True
    
    def __debug_fetch(self,update_index):
        line = self.LINES[update_index].lower()
        path = self.file_path_debug + '\\' + line + '.json'
        print(path)
        with io.open(path,encoding="UTF-8") as file:
            data = json.loads(file.read())
            return data
        
    def fetch(self,meass):
        try:
            URL = self.generateAPI_URL(meass)
            logger.debug("sending API request.")
            with urllib.request.urlopen(URL) as url:
                data = url.read()
                return data
        except urllib.error.URLError as err:
            logger.error("Error while attempting to open API data. Message: %s",err)
            return None
        return self.__fetch(meass)
    def __fetch(self,meassured_stations:list[int]):
        """ input: meassured_stations - list of stationnames to be meassured
            loads json response from API, 
                unless debug_mode = 1. then it will try to read json data from file in same relative path.
                if unable to read files, this method will throw an exception
            returns: the json data, or None if a URLError was raised durring the api request
        """
        if(self.flag_debug_mode and self.last_update_tried == None):

            with open(self.file_path_debug + '\\init.json') as file:
                data = json.loads(file.read())
                return data
            #json.load()
            

        try:
            URL = self.generateAPI_URL(meassured_stations)
            logger.debug("sending API request.")
            with urllib.request.urlopen(URL) as url:
                data = json.loads(url.read().decode())
                return data
        except urllib.error.URLError or client.IncompleteRead as err:
            logger.error("Error while attempting to open API data. Message: %s",err)
            return None

    def generateAPI_URL(self,station_name_List:list[int]):
        """ input: list of station-numbers
            returns: url for api request of the stations given
        """
        assert len(station_name_List)>0
        string = self.url_start + str(station_name_List[0])
        for name in station_name_List[1:]:
            string += self.url_inbetween + str(name)
        return string

    
    def convertData(self,line_select,data):
        """input: 
            - line : str
            - data fetched for this line
            returns:
            a dictionary, the keys corresponding to the indices of stations the data is, the elements are numpy arrays of next departures
        """
        Dictionary:dict[int,dict[str,list[list]]] = {}
        Stops = data["data"]["monitors"]
        for Stop in Stops:
            stop_diva = int(Stop["locationStop"]["properties"]["name"])
            stop_index = self.Co.getStationIndex(line_select,stop_diva) #assertion: only stations on this line are meassured
            Dictionary[stop_index] = {"trains":[[],[]],"terminal":[[],[]]}

        for Stop in Stops:
            stop_diva = int(Stop["locationStop"]["properties"]["name"])
            stop_index = self.Co.getStationIndex(line_select,stop_diva) #assertion: only stations on this line are meassured
            for line_data in Stop["lines"]:
                line = line_data["name"]
                if(line == line_select):
                    trains = Dictionary[stop_index]["trains"]
                    terminal_ind = Dictionary[stop_index]["terminal"]
                    defaulting_towards = str(line_data["towards"]).strip().upper()
                    defaulting_towards_ind = self.Co.getStationIndex_from_str(line_select,defaulting_towards)
                    
                    dep_data = line_data["departures"]["departure"]
                    self.convertLineData(line_select,stop_index,defaulting_towards_ind,dep_data,trains,terminal_ind)
        return Dictionary

    def convertLineData(self,line,index,defaulting_towards_ind,data,trains_datetime:list[list],terminal_ind:list[list]):
        default_dir = 0 if defaulting_towards_ind>index else 1
        for train_data in data:
            towards_index = defaulting_towards_ind
            direction = default_dir #computed below, equals index of terminal station of train / platform if train is missing these data
            if(not "departureTime" in train_data):
                #no departing time -> no meassurement can be taken, abort
                #TODO: uncomment
                #logger.warning(f"train departure data missing required argument(s): \"departureTime\", this train can not be tracked. \nstation is {self.Co.stationNamefromIndex(line,index)}, defaulting towards index: {defaulting_towards_ind}. Full train_data:\n{train_data}")
                continue
            
            if(not "vehicle" in train_data):
                #assuming that direction equals station direction and foldingRamp defaults to "unknown"
                #assumption: vehicle_towards_index = station_towards_index
                
                logger.debug(f"train departure data missing required argument(s): \"vehicle\", station is {self.Co.stationNamefromIndex(line,index)}, defaulting towards index: {defaulting_towards_ind}. Full train_data is :\n{train_data}\n")
            else:
                vehicle_d = train_data["vehicle"] #contains towards, vehicle direction, ...
                vehicle_endstation_str = str(vehicle_d["towards"]).strip().upper()
                towards_index = self.Co.getStationIndex_from_str(line,vehicle_endstation_str)
                direction = 0 if index<towards_index else 1
                if(direction!=default_dir):
                    logger.warning(f"departing Train from {line}: index {self.Co.stationNamefromIndex(line,index)}-> {vehicle_endstation_str} does not match \
                    station_direction {default_dir}, indicating 'Gleiswechselbetrieb'. This train will still be tracked.")
                    

            #departure at meassured station:
            departure_data = train_data["departureTime"]
            planned_or_real = "timeReal" if "timeReal" in departure_data else "timePlanned"
            train_datetime = dateTimeFromString(departure_data[planned_or_real])

            trains_datetime[direction].append(train_datetime)
            terminal_ind[direction].append(towards_index)
        pass