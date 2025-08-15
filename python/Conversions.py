from datetime import datetime
import os
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def seconds_since(since_time:datetime):
        return (datetime.now()-since_time).total_seconds()

def millis_since(since_time:datetime):
    return int((datetime.now()-since_time).total_seconds()*1000)

class DataConversions:
    def init_STATIONS(self):
        self.STATIONS_OF_LINE:dict[str,list[int]] = {}#keys: line, val: array of DIVA numbers
        logger.info('current import path is set to: %s',self.import_path)
        stations_pd = pd.read_csv(self.import_path + "/Coordinates.csv",sep=";")
        def stations_of_line(line):
            stations_of_line = []
            for station_diva in stations_pd[line]:
                if(station_diva>0):
                    stations_of_line.append(int(station_diva))
            return stations_of_line
        
        for line in self.LINES:
            self.STATIONS_OF_LINE[line] = np.array(stations_of_line(line))

    def init_STATION_NAME_DICT(self):
        self.STATION_NAME_DICT = {}
        stations_pd = pd.read_csv(self.import_path + "/Coordinates.csv",sep=";")
        for line in self.LINES:
            for index in range(len(stations_pd[line])):
                diva = stations_pd[line].iloc[index]
                if diva>0: #diva is not nan
                    text = stations_pd["Text"+line].iloc[index]
                    self.STATION_NAME_DICT[int(diva)] = text.upper()
    
    def getIndexBounds(self,line:str,meass_index:int,direction,preceeding_index = None,succseeding_index=None):
        """if(line=='U4' and meass_index == 18 and direction==1):
                    print("here")
        """
        fun = [self.getIndexBoundsDirection0,self.getIndexBoundsDirection1]
        return fun[direction](line,meass_index,preceeding_index,succseeding_index)
          
    def getIndexBoundsDirection0(self,line,meass_index,preceeding_index = None,succseeding_index=None):#TODO:check function
        #assert(preceeding_index>meass_index>succseeding_index)
        mini,maxi = self.getServiceBounds(line,meass_index)
        if(None!=preceeding_index):
            on_same_service_with_prec = self.on_same_service(line,meass_index, preceeding_index)
            prec_was_terminal_station = self.is_terminal_station_for_dir(line,preceeding_index,0)
            maxi = min(maxi,meass_index) if on_same_service_with_prec and not prec_was_terminal_station else maxi
        if(None!=succseeding_index):
            mini = max(mini,succseeding_index)
        #assuming not called at endstation for all trains, we now have mini<=meass_index<maxi
        weight_func = np.zeros(self.lenOfLine(line))
        
        weight_func[meass_index:maxi+1] = np.linspace(1,0,maxi-meass_index+1,endpoint=False)
        weight_func[mini:meass_index+1] = 1
        return mini, maxi, weight_func

    def getIndexBoundsDirection1(self,line,meass_index,preceeding_index = None,succseeding_index=None):#TODO:check function
        #assert(preceeding_index<meass_index<succseeding_index)
        mini,maxi = self.getServiceBounds(line,meass_index)
        if(None!=succseeding_index):
            maxi = min(maxi,succseeding_index)
        if(None!=preceeding_index):
            on_same_service_with_prec = self.on_same_service(line,meass_index, preceeding_index)
            prec_was_terminal_station = self.is_terminal_station_for_dir(line,preceeding_index,1)
            mini = max(mini,meass_index) if on_same_service_with_prec and not prec_was_terminal_station else mini
        #assuming not called at endstation for all trains, we now have mini<=meass_index<maxi
        weight_func = np.zeros(self.lenOfLine(line))
        
        weight_func[meass_index:maxi+1] = 1
        weight_func[mini:meass_index+1] = np.linspace(0,1,meass_index-mini+1,endpoint=False)
        return mini, maxi, weight_func

    def init_travel_times(self):
        """Computes for each line a travel time matrix, for a travel time matrix A, 
        the entry A[i][j] corresponds with the traveltime from the station with 
        index i to the station with index j.
        """
        travel_pd = pd.read_csv(self.import_path + "/UbahnFahrtzeitenZwischenStationen.csv",sep=";")
        self.TravelTimes:dict[str,np.ndarray] = {} #keys=lines, val=arrays with length of stations
        for line in self.LINES:
            length_of_line = self.lenOfLine(line)
            journey_vector = travel_pd[line+"Data"].values[:length_of_line-1]
            diva_vector = travel_pd[line+"Diva"].values[:length_of_line]
            A = np.zeros((length_of_line,length_of_line))
            for i in range(length_of_line-1):
                A[i,i+1] = journey_vector[i]
            for j in range(2,length_of_line):
                for i in range(0,length_of_line-1):
                    if(j>i+1):
                        A[i,j] = A[i,j-1] + journey_vector[j-1]

            A = A - A.T 
            self.TravelTimes[line] = A 
        np.set_printoptions(linewidth=400)
        logger.debug(f"Travel times for each line have been created.")
        for key in self.TravelTimes.keys():
            logger.debug(f"Line = {key}, Travel_times = \n{self.TravelTimes[key]}")
        
        
    def __init__(self,Lines,import_path=None):
        self.LINES = Lines
        self.import_path = os.path.dirname(os.path.realpath(__file__)) if import_path==None else import_path
        self.STATION_NAME_DICT = None
        self.STATIONS_OF_LINE:dict[str,list[int]] = None
        self.init_STATIONS()
        self.init_STATION_NAME_DICT()
        #self.init_ALL_MEASSURED_INDICES(All_meassured_stations)
        self.init_travel_times()

    def getStationName(self,diva:int):
        """ Input: diva - int, diva-number of Station.
            returns: String of Station with corresponding diva number.
        """
        return self.STATION_NAME_DICT[diva]
    
    def lenOfLine(self,line:str):
        """ Input: line - str, line-name
            returns: length of the corresponding line.
        """
        return np.size(self.STATIONS_OF_LINE[line])
    
    def getLineDivas(self,line):
        """ Input: line - str, line-name
            returns: a list of all the diva-numbers of stations on that line, ordered by station-index
        """
        return self.STATIONS_OF_LINE[line]
    
    def getDivafromIndex(self,line,index):
        """ Input: line - str, line-name; index - int, station-index
            returns: the diva number of the station with index 'index' on line 'line'
        """
        
        return self.STATIONS_OF_LINE[line][int(index)] #TODO: add casts of indices

    def getDiva(self,station_name:str,line=None):
        """ Input: station_name - str, name of the station, line - str, optional. 
            returns: diva number of specified station, the key in the dictionary STATION_NAME_DICT with value = station_name.
            
            If station_name is not a value of STATION_NAME_DICT, an attempt will be made to shorten the string inductively by looking at the substring from the start until the last instance of a space symbol in the station_name. If no key can be found, a keyerror is raised.
        """
        string = station_name.upper()
        if(string in self.STATION_NAME_DICT.values()):
            return self.__getStationDiva(string)
        
        new_str = string
        while(" " in new_str):
            new_str = new_str[:new_str.rfind(" ")]
            if(new_str in self.STATION_NAME_DICT.values()):
                logger.warning(f"name {string} not in STATION_NAME_DICT, will use {new_str} as key instead")
                return self.__getStationDiva(new_str)
        logger.warning(f"name {string} not in STATION_NAME_DICT, will use the initial segment {string[:9]} for searching for a key on line {line}.")
        #Uniqueness of station names per line is given for any correct initial segment of a station name with length 9, TODO: need line for that uniqueness
        if(line!=None):
            if(string[:9]=='HAUPTBAHN'):
                return self.__getStationDiva_fromInitialSegment('SÜDTIROLE',line)
            return self.__getStationDiva_fromInitialSegment(string[:9],line)
        logger.critical(f"name {string} not in STATION_NAME_DICT and line not specified. Cannot uniquely determine the station diva.")
        raise KeyError(f"name {string} not in STATION_NAME_DICT and line not specified. Cannot uniquely determine the station diva.")
        
    
    def __getStationDiva(self,string:str):
        index = list(self.STATION_NAME_DICT.values()).index(string)
        return int(list(self.STATION_NAME_DICT.keys())[index])
    
    def __getStationDiva_fromInitialSegment(self,substring:str,line):
        
        divas_on_line = self.STATIONS_OF_LINE[line]
        initial_values = [self.getStationName(diva_num)[:len(substring)] for diva_num in divas_on_line]
        if(substring[3:]== 'TTELDO'): #TODO: fix hotfix, encoding of file
            return self.getDiva('HÜTTELDORF',line='U4')
        index = initial_values.index(substring) #corresponds to the station index on this line
        return self.stationDivafromIndex(line,index)
    
    def getStationIndex(self,line:int,station_diva:int):
        """ Input: line - str, 
                station_diva - int,
            returns: index of the value station_diva in the numpy array STATIONS[line].
        """
        #TODO: experienced index out of range on late night service shortly before the ubahn service holds service for the night. Test if this exception can be catched.
        return np.where(self.STATIONS_OF_LINE[line] == station_diva)[0][0] 
    
    def getStationIndex_from_str(self,line,station_str):
        """ Input: line - str, station_str - str, station-name
            returns: index of station on line 'line' named 'station_str'
        """
        station_diva = self.getDiva(station_str,line)
        return self.getStationIndex(line,station_diva)

    def stationNamefromIndex(self,line:str,index:int):
        """ Input: line - str, 
                index - int,
            returns: the name of the station in STATIONS[line] at the index index.
        """
        return self.getStationName(self.stationDivafromIndex(line,index))
    
    def stationDivafromIndex(self,line:str,index:int):
        """ Input: line - str, 
                index - int,
            returns: the diva-number of the station in STATIONS[line] at the index index.
        """
        return self.STATIONS_OF_LINE[line][index]
    

    def init_bounds_and_service(self,data):
        """ input: initially fetched data for all lines to be meassured
            initializes a tuple of dictionaries
            - first dictionary, for each line containing the indices of terminal stations of this line
            - second dictionary, for each line containing a list of sets, each set corresponds to the maximal interval where there is a continuous service at this line
                e.g. if a line does not operate fully, but as two segments of a line, each operating on one of the two intervalls
        """
        temp_service:dict[str,dict[str,set]] = {}
        for line in self.LINES:
            temp_service[line] = {}

        self.bounds:dict[str,set] = {}
        for line in self.LINES:
            self.bounds[line] = set()

        Stops = data["data"]["monitors"]
        for Stop in Stops:
            stop_diva = int(Stop["locationStop"]["properties"]["name"])
            for line_data in Stop["lines"]:
                line = line_data["name"]
                if(line in self.LINES):
                    if(not stop_diva in temp_service[line]):
                        temp_service[line][stop_diva] = set()
                    data = line_data["departures"]["departure"]
                    for train_data in data:
                        if("vehicle" in train_data):    
                            vehicle_d = train_data["vehicle"] #contains towards, vehicle direction, ...
                            vehicle_endstation_str = str(vehicle_d["towards"]).strip().upper()
                            towards_index = self.getStationIndex_from_str(line,vehicle_endstation_str)
                            temp_service[line][stop_diva].add(towards_index)
                            self.bounds[line].add(towards_index)

        self.service:dict[str,list[set[int]]] = {}
        for key in temp_service.keys():
            List = []
            for diva_key in temp_service[key].keys():
                this_set = temp_service[key][diva_key]
                this_service = set([min(this_set),max(this_set)])
                if not this_service in List:
                    List.append(this_service)
            self.service[key] = List
    
    def on_same_service(self,line,index1,index2):
        """returns True, if index1 and index2 are in the same service interval, False otherwise
        """
        lower,upper = self.getServiceBounds(line,index1)
        return bool(lower<=index2<=upper)
    
    def is_terminal_station_for_dir(self,line,index,direction):
        if not index in self.bounds[line]: return False
        lower,upper = self.getServiceBounds(line,index)
        #assertion made here that terminal station of one direction is closer to the corresponding latest terminal station, 
        # #TODO change this in the future so it also works on closing times of the subway, i.e. when this can not be ensured i.g.
        index_closer_to_upper = bool(upper-index<index-lower) 
        return index_closer_to_upper if direction==0 else not index_closer_to_upper

    def getServiceBounds(self,line,station_index):
        """returns the service bounds of a meassured station of the specified line as an ordered tuple.
        """
        this_service = self.service[line]
        assert(any([min(s)<=station_index<=max(s) for s in this_service])) # station is in one service interval
        index_of_service = [min(s)<=station_index<=max(s) for s in this_service].index(True)
        service_interval = this_service[index_of_service]
        lower = min(service_interval)
        upper = max(service_interval)
        return (lower,upper)
    
    def getLineServiceSets(self,line):
        """returns: the services on that line as a list of sets
        """
        return self.service[line]
    
    def createMeassureStations(self,MEASS_STATIONS_PER_LINE):
        """computes for each line the optimal stations to meassure.
            it considers:
            1. line operation interruptions: 
                - all meassure stations are stations currently in operation
                - if not the whole line is in service it creates at least one meassure station for each part of the line operating
            2. trains terminating before the last station:
                - if there are trains only traveling to a station not beeing the endstation, this station is also meassured
            3. equidistant spacing between stations

            returns: a dictionary, meassured lines are keys.
                For each meassured line the entry is a orderd list of integers corresponding to the indices that are to be meassured for this line.
        """
        assert(self.service)
        L = {}
        for line in self.service.keys():
            line_meass = []
            line_services = self.service[line]
            line_destinations = self.bounds[line]
            for line_service in line_services:
                part_mess = []
                lower = min(line_service)
                upper = max(line_service)
                partition_destinations = [desti for desti in line_destinations if lower<desti and desti<upper]
                part_mess = part_mess + partition_destinations
                for part_desti in partition_destinations:
                    part_mess.append(lower) if part_desti-lower<upper-part_desti else part_mess.append(upper)
            
            for j in range(len(line_services)):
                line_service = line_services[j]
                lower = min(line_service)
                upper = max(line_service)
                left_to_append = MEASS_STATIONS_PER_LINE - len(part_mess)# TODO: check if needed: if partition in two or more then below can not be equal to left to append - (len(line_services) - j - 1) #endnumber - already added - might added in future
                left_on_partition = int(np.max([1,np.floor((upper-lower+1)/self.lenOfLine(line)*left_to_append)]))
                conserv_optimal_distance = (upper-lower)/left_on_partition

                start_interpolation = lower
                end_interpolation = upper
                if(len(part_mess)>1):
                    part_mess.sort()
                    part_mess_np = np.array(part_mess)
                    meass_diff = part_mess_np[1:] - part_mess_np[:-1]
                    start_index = 0
                    for i in range(len(meass_diff)):
                        diff = meass_diff[i] #part_mess[i+1]-part_mess[i]
                        if diff<1.5*conserv_optimal_distance:
                            start_index = i+1
                            continue
                        if diff<3*conserv_optimal_distance:
                            part_mess.append(np.round((part_mess[i+1]+part_mess[i])/2))
                            left_on_partition = left_on_partition - 1
                            start_index = i+1
                            continue
                        start_index = i+1
                        break
                    start_interpolation = part_mess[start_index]
                    
                    for i in reversed(range(start_index+1,len(meass_diff))):
                        diff = meass_diff[i] #part_mess[i+1]-part_mess[i]
                        if diff<1.5*conserv_optimal_distance:
                            end_interpolation = part_mess[i]
                            continue
                        if diff<3*conserv_optimal_distance:
                            part_mess.append(np.round((part_mess[i+1]+part_mess[i])/2))
                            left_on_partition = left_on_partition - 1
                            end_interpolation = part_mess[i]
                            continue
                        end_interpolation = part_mess[i]
                        break
                optimal_dist = (end_interpolation-start_interpolation)/(left_on_partition+1)
                Array = np.round(start_interpolation + optimal_dist*np.arange(1,left_on_partition+1)).astype(dtype=int,casting='unsafe')
                line_meass = line_meass + part_mess + list(Array)
            line_meass.sort()
            L[line] = line_meass.copy()
        self.meass_stations_ind:dict[str,list[int]] = L
        self.log_meass_stations()
        return L
    
    def getMeassuredLines(self):
        """returns: the lines that are meassured
        """
        return list(self.meass_stations_ind.keys())
    
    def getLineMeassureStations(self,line:str):
        """returns list of indices of the stations meassured on this line
        """
        return self.meass_stations_ind[line]
    
    def log_meass_stations(self):
        """ assertion made: self.bounds, self.service and self.meass_stations have the same keys!
        """
        string = "\nLine | Service intervals | Terminal stations | Computated meassurestations:\n"
        for line in self.meass_stations_ind.keys():
            string = string + '{: <7}'.format(line)
            serv_int = ''
            for service in self.service[line]:
                serv_int = serv_int + f"{min(service)}<->{max(service)}, "
            string = string + '{: <20}'.format(serv_int)[:20]
            terminal_st = ''
            for terminal_station in self.bounds[line]:
                terminal_st = terminal_st + str(terminal_station) + ", "
            string = string + '{: <20}'.format(terminal_st)[:20]
            for i in self.meass_stations_ind[line]:
                string = string + str(i) + ", "
            string = string + "\n"
        logger.info(string)
        pass