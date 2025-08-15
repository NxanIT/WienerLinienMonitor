import numpy as np
from datetime import datetime
import time
from configparser import ConfigParser
import logging
logger = logging.getLogger(__name__)
from threading import Lock

from Conversions import DataConversions

LockDepartureData = Lock()
LockStationData = Lock()

logger = logging.getLogger(__name__)

def seconds_since_now(since_time:datetime):
        return seconds_since(since_time,datetime.now())

def seconds_since(time1:datetime,time2:datetime):
     return int((time2-time1).total_seconds())

class LineData:
     def __init__(self,line:str, refTime:datetime,Co:DataConversions,config:ConfigParser): 
          self.LINE = line
          self.LineLenght = Co.lenOfLine(line)
          #self.direction = direction 
          self.LineDivas = Co.getLineDivas(line)
          self.refTime = refTime
          self.TravelTimes = Co.TravelTimes[line]
          self.max_size = config.getint('METRO','max_trains_on_line')
          self.Co = Co
          self.TRAIN_SIMULATION_DELAY = config.getint('METRO','TRAIN_DEPARTURE_DELAY_TIME_OFFSET')
          self.TRAIN_IN_STATION_TIME = config.getint('METRO','TRAIN_IN_STATION_TIME')
          self.MAX_TRAINS_ON_LINE = config.getint('METRO','MAX_TRAINS_ON_LINE')
          self.TRAIN_DEP_CUTOFF_TIME = config.getint('METRO','TRAIN_DEP_CUTOFF_TIME')
          self.threshold_time_between_departures = config.getint('METRO','threshold_time_between_departures')
          self.DepartureDataDir0 = np.full((self.max_size,self.LineLenght), 0) #TODO: is this nec? ,dtype=np.float32
          self.DepartureDataDir1 = np.full((self.max_size,self.LineLenght), 0)
          self.DepartureData:list[np.ndarray] = [self.DepartureDataDir0,self.DepartureDataDir1]# index 0 <-> stationindex increasing with time, 1 else
          
          pass

     def __repr__(self):
          meass_ind = self.Co.getLineMeassureStations(self.LINE)
          this_service = self.Co.service[self.LINE]
          info = str()
          for i in range(self.LineLenght):
               if any([min(s)<=i<=max(s) for s in this_service]):
                    info += '{:>5}'.format(i if i in meass_ind else "")
                    continue
               info += '{:>5}'.format("x")
               self.DepartureDataDir0.__repr__()
          return "direction ->: \n "+ info +f"\n{self.DepartureDataDir0}\n {info} \ndirection <-:\n {info}\n{self.DepartureDataDir1}\n {info}"
    
     def updateData(self,data:dict[int,dict[str,list[list]]]):
          for direction in range(2):
               self.updateDataOneDirection(data,direction)
          pass
     
     def updateDataOneDirection(self,data:dict[int,dict[str,list[list]]],direction:int):
          indices = list(data.keys())
          indices.sort(reverse=(direction==0))
          service:list[set[int]] = self.Co.service[self.LINE]
          if len(indices)==1: # case only one station meassured, bounds are service interval bounds, weight_function == 1, TODO: test this case
               index = indices[0]
               train_data = data[index]["trains"][direction]
               terminal_data = data[index]["terminal"][direction]
               lower, upper = self.Co.getServiceBounds(self.LINE,index)
               weight_function = np.zeros((self.LineLenght))
               weight_function[lower:upper+1] = 1
               self.append_at_best_location(train_data,terminal_data,direction,index,lower,upper,weight_function)
               return
          Flag_prec_index_was_origin = False
          for i in range(len(indices)):
               index = indices[i]
               
               train_data = data[index]["trains"][direction]
               terminal_data = data[index]["terminal"][direction]
               
               flag_is_endstation_for_all = index in [max(s) for s in service] if direction==0 else index in [min(s) for s in service]
               if(flag_is_endstation_for_all): continue #no data to meassure here
               
               #lower, upper, weight_function  = None
               if(i==0): # test if no preceeding update-index exists, if so then a succseeding one exists (len>=2)
                    lower, upper, weight_function = self.Co.getIndexBounds(self.LINE,index,direction,preceeding_index=None,succseeding_index=indices[i+1])
                    self.append_at_best_location(train_data,terminal_data,direction,index,lower,upper,weight_function)
                    Flag_prec_index_was_origin = self.Co.is_terminal_station_for_dir(self.LINE,index,1-direction)
                    continue
               if(i+1==len(indices)): # test if no succseeding update-index exists, if so then a preceeding one exists
                    lower, upper, weight_function = self.Co.getIndexBounds(self.LINE,index,direction,preceeding_index=indices[i-1])
                    self.append_at_best_location(train_data,terminal_data,direction,index,lower,upper,weight_function,flag_prev_was_origin_station=Flag_prec_index_was_origin,prev_index=indices[i-1])
                    Flag_prec_index_was_origin = self.Co.is_terminal_station_for_dir(self.LINE,index,1-direction)
                    continue
               
               lower, upper, weight_function = self.Co.getIndexBounds(self.LINE,index,direction,succseeding_index=indices[i+1],preceeding_index=indices[i-1])
               self.append_at_best_location(train_data,terminal_data,direction,index,lower,upper,weight_function,flag_prev_was_origin_station=Flag_prec_index_was_origin,prev_index=indices[i-1])
               Flag_prec_index_was_origin = self.Co.is_terminal_station_for_dir(self.LINE,index,1-direction)
          pass
     
     def append_at_best_location(self,train_data,terminal_data,direction,index,lower,upper,weight_function,flag_prev_was_origin_station=False,prev_index = None):##TODO: check code
          """if flag_prev_was_origin_station==True then it deletes the train departures that have no matching with train_data
          """
          prev_calc_departures = self.DepartureData[direction][:,index]
          assert(len(train_data)==len(terminal_data))
          start = 0
          appended_at = []
          dep_mess_sec = np.zeros(len(train_data))
          for j in range(len(train_data)):
               dep_mess_sec[j] = seconds_since(self.refTime,train_data[j])

          for j in range(len(train_data)):
               if(self.LINE=='U7' and index == 0 and direction==0):
                    print("here")
               mini = lower if direction==0 else max(lower,terminal_data[j])
               maxi = min(upper,terminal_data[j]) if direction==0 else upper
               
               # TODO: change initial entries of DepData to zeros, only then the following will work
               best_match, flag_overwrite = self.get_best_first_match(prev_calc_departures,dep_mess_sec,start=start,start_new = j)
               if(best_match==-1 and flag_overwrite):
                    continue
               if(best_match==-1 or best_match>=self.max_size):
                    break
               A = self.TravelTimes if direction==0 else self.TravelTimes.T
               travel_vector = np.zeros(self.LineLenght)
               travel_vector[mini:maxi+1] = A[index,mini:maxi+1] + dep_mess_sec[j]
               self.__appendToDepData(travel_vector,direction,best_match,index,overwrite=flag_overwrite,weight_func=weight_function)
               appended_at = [i+1 if i>=best_match and not flag_overwrite else i for i in appended_at]
               appended_at.append(best_match)
               start = best_match + 1
          appended_at.sort()
          if(flag_prev_was_origin_station):
               #non-matched results need to be trimmed
               assert(prev_index!=None)
               for vert_index in range(self.max_size):
                    if(vert_index not in appended_at): 
                         self.__deleteDepDataRow(vert_index,prev_index,direction)
     
     def get_best_first_match(self,prev_array:np.ndarray,dep_mess_sec,start=0,allow_after_prev = True,start_new = 0):
          """  
               prev_array is meant to be the full list of indices
               If allows_after_prev is True, new entry after entries are allowed, else returns -1,False instead
               returns: int index of best match and bool flag_overwrite
               -if no best match is found:
               returns -1, True if next departure should be looked at
                         -1, False if self.max_size is reached
          """
          first_zero_occurence = 0 if np.all(prev_array==0) else np.size(prev_array)
          if(start>=self.max_size):#TODO:recently removed start+1 -> start, check if it runs
               return -1,False
          new_dep_sec = dep_mess_sec[start_new]
          if(np.any(prev_array==0) and not np.all(prev_array==0)):
               last_non_zero_occurence = np.argwhere(prev_array!=0)[-1][0]
               zero_bevore_data = np.argwhere(prev_array[:last_non_zero_occurence]==0)
               if(np.size(zero_bevore_data)>0):
                    start = max(zero_bevore_data[-1][0]+1,start)
               first_zero_occurence = last_non_zero_occurence +1
               prev_array = prev_array[:first_zero_occurence]
          if(first_zero_occurence==start and allow_after_prev):
               return start,False
          #prev_array now contains no zeros
          norms = np.abs(prev_array-new_dep_sec)[start:]
          if(np.size(norms)==0):#case no entry has been made, or start is beyond data
               return start,False #TODO: check if this solves problem on like u1 where when new trains are added at endstation this could leed to scrambling of ordering of departures or if other meassures are needed like caping the departures of previous stations to only departures registered in previous stations
          opt_index = np.argmin(norms) + start

          if(np.size(np.where(norms<=2*self.threshold_time_between_departures))==0):
               if(np.all(prev_array[start:]>=new_dep_sec)):
                    #this should not happen in correct operation. the previously updated index has departures before this and if index would be first in sequence to update then there would be a positive time difference to last data
                    logger.warning(f"unusual matching of traindata. line:{self.LINE}, new-data:{new_dep_sec}s, old:{prev_array}")
                    return start,False
               # if(np.size(dep_mess_sec)<start_new+2):#optimal index appers not to be close, but dep_mess contains no next 
               #      return -1,False
               if(prev_array[-1]-new_dep_sec>0):
                    #optimal index somewhere inbetween, but not close
                    if(prev_array[opt_index]-new_dep_sec>0):#departure before opt_index
                         return opt_index,False
                    #TODO: check if works correct
                    return opt_index+1,False
               if(allow_after_prev and first_zero_occurence<np.size(prev_array)):
                    return first_zero_occurence,False #TODO: check if this solves problem on like u1.....
               return -1,False
          
          if(norms[opt_index-start]<=self.threshold_time_between_departures):
               return opt_index,True
          if(norms[opt_index-start]<=2*self.threshold_time_between_departures):
               logger.warning(f"close call made when searching for match. match assumed, line:{self.LINE}, new-data:{new_dep_sec}s, old:{prev_array}\n{norms}\n{opt_index}\n{np.size(norms<=2*self.threshold_time_between_departures)}")
               return opt_index,True
          return opt_index,False
     
     def __deleteDepDataRow(self,vert_index,until,direction):
          """removes the vert_index-row of DepData[direction] from 
               0 to until-1                  if direction == 0
               until+1 to self.LineLength    if direction == 1
          """
          start = 0 if direction==0 else until+1
          end = until if direction==0 else self.max_size

          LockDepartureData.acquire()
          self.DepartureData[direction][vert_index,start:end] = 0
          LockDepartureData.release()

          

     def __appendToDepData(self,train,direction,train_index,station_index,overwrite=True,weight_func = None):
          assert(np.size(train)==self.LineLenght)
          min_ind, max_ind = self.Co.getServiceBounds(self.LINE,station_index)
          LockDepartureData.acquire()
          if(not overwrite):
               self.DepartureData[direction][train_index+1:,min_ind:max_ind+1] = self.DepartureData[direction][train_index:-1,min_ind:max_ind+1]
               
          if(np.any(weight_func!=None) and overwrite):
               assert(np.size(weight_func)==self.LineLenght)
               self.DepartureData[direction][train_index,min_ind:max_ind+1] = (1-weight_func[min_ind:max_ind+1])*self.DepartureData[direction][train_index,min_ind:max_ind+1] + weight_func[min_ind:max_ind+1]*train[min_ind:max_ind+1]
          else:
               self.DepartureData[direction][train_index,min_ind:max_ind+1] = train[min_ind:max_ind+1]
          LockDepartureData.release()
          

          
     
     def removeOldData(self):#TODO: unused.
          """deletes lines of DepartureData, where all values are below TRAIN_DEP_CUTOFF_TIME, 
               and shifts the remaining data forward.
          """
          for i in range(2):

               first_to_keep = np.where(np.any(self.DepartureData[i]>self.TRAIN_DEP_CUTOFF_TIME,axis=1))[0][0]
               
               LockDepartureData.acquire()
               self.DepartureData[i][0:self.max_size-first_to_keep,:] = self.DepartureData[i][first_to_keep:,:]
               self.DepartureData[i][self.max_size-first_to_keep:,:] = np.full((first_to_keep,self.LineLenght),np.inf)
               LockDepartureData.release()
          pass
     
     def copyDepartureData(self):
          LockDepartureData.acquire()
          CopyDepartureDataDir0 = self.DepartureDataDir0.copy()
          CopyDepartureDataDir1 = self.DepartureDataDir1.copy()
          LockDepartureData.release()
          return [CopyDepartureDataDir0,CopyDepartureDataDir1]
     
     def copyDepartureData_direction(self,direction):
          func = [self.copyDepartureDataDir0,self.copyDepartureDataDir1]
          return func[direction]()
     
     def copyDepartureDataDir0(self):
          LockDepartureData.acquire()
          CopyDepartureDataDir0 = self.DepartureDataDir0.copy()
          LockDepartureData.release()
          return CopyDepartureDataDir0
     
     def copyDepartureDataDir1(self):
          LockDepartureData.acquire()
          CopyDepartureDataDir1= self.DepartureDataDir1.copy()
          LockDepartureData.release()
          return CopyDepartureDataDir1
     
     def getLEDstates(self,display_type,direction,epoch = None,debug_speed = 1):
          """input: display_type
               * 0: light should be lit, iff train is in station
               * 1: light should be lit at the station the train is nearest to
          returns: list of length (length_of_line) of type int, 
          """
          seconds_since_ref = seconds_since_now(self.refTime) if epoch==None else debug_speed*seconds_since(epoch,datetime.now())
          
          CopyDepartureData = np.zeros((self.max_size,self.LineLenght),dtype=np.float64)
          CopyDepartureData[:] = self.copyDepartureData_direction(direction)[:]
          CopyDepartureData[CopyDepartureData==0] = np.inf
          CopyDepartureData -= seconds_since_ref +self.TRAIN_SIMULATION_DELAY
          service_intervals = self.Co.getLineServiceSets(self.LINE)
          LED_states = np.zeros(self.LineLenght)
          LED_state_func = [self.__LED_state0,self.__LED_state1]
          
          for service in service_intervals:
               
               lower = min(service)
               upper = max(service)
               Data_on_service = CopyDepartureData[:,lower:upper+1]
               # if(self.LINE=='U4' and lower == 17 and direction==0 and seconds_since(epoch,datetime.now())>=26):
               #      print("here")
               LED_states[lower:upper+1] = LED_state_func[display_type](Data_on_service,direction)
          if(epoch!=None and self.LINE=='U4' and direction==0):
               np.set_printoptions(linewidth=400)
               logger.info(f"\n{CopyDepartureData}")
          return LED_states
     
     def __LED_state0(self,CopyDepartureData,dir):
          """input: DataOnService - nd.array 0-axis: trains, 1-axis stations
          """
          states = np.zeros(np.shape(CopyDepartureData)[1])
          for i in range(np.shape(CopyDepartureData)[1]):
               states[i] = 1 if np.any(np.abs(CopyDepartureData[:,i])<self.TRAIN_IN_STATION_TIME/2) else 0
          return states

     def __LED_state1(self,CopyDepartureData,dir):
          states = np.zeros(np.shape(CopyDepartureData)[1])
          indices = np.argmin(np.abs(CopyDepartureData),axis=1)
          # if(self.LINE=="U1" and dir==0):
          #      print("ereh")
          for j in range(len(indices)):
               index = indices[j]
               train = CopyDepartureData[j,:]
               if np.all(train==np.inf): #test if train j has been initialized
                    break #assuming after first occurence of not initialized are no more trains
               
               # #unwanted?
               # if abs(CopyDepartureData(j,index))>self.TRAIN_IN_STATION_TIME/2:
               #      continue
               train_start_index = np.where(train<np.inf)[0][0 if dir == 0 else -1]
               train_end_index = np.where(train<np.inf)[0][-1 if dir == 0 else 0]
               if index==train_start_index and states[index] == 0: #condition: train starting journey
                    states[index] = 1 if train[index] <self.TRAIN_IN_STATION_TIME else 0
                    continue
               if index==train_end_index and states[index] == 0: #condition: train ending journey
                    states[index] = 1 if train[index] >-self.TRAIN_IN_STATION_TIME else 0
                    continue
               states[index] = 1
          return states
     


class MetroData:
     def __init__(self,Lines:list[str],Co:DataConversions,ref_time:datetime,config:ConfigParser,flag_debug=False):
          self.Lines:list[str] = Lines
          self.Co = Co
          self.LData:dict[str,LineData] = {}
          self.LineLowerDirections = {}
          self.displaymode = config.getint('METRO','display_mode')   
          for line in self.Lines:
               self.LData[line] = LineData(line,ref_time,Co,config)
          self.flag_debug = flag_debug
          self.debug_epoch = datetime.now()
          if(flag_debug):
               self.debug_speed = config.getint('MONITOR','debug_speed')
          pass

     def updateDepartures(self,line,data:dict): #TODO: start here rewriting code for new format of data recieved
        logger.info("attempting to update departures...")
        self.LData[line].updateData(data)
        logger.info("update succsessful.")
        logger.info(f"{line}:\n{self.LData[line]}")

     def getStationData(self):
          """called by monitor, should return data for all trains
          returns: dictionary of 2d-np.arrays 0-axis corresponding to station, 1-axis corresponding to direction
          """
          D = {}
          for line in self.LData.keys():
               LED_on_line = np.zeros((self.Co.lenOfLine(line),2))
               for direction in range(2):
                    LED_on_line[:,direction] = self.LData[line].getLEDstates(self.displaymode,direction) if not self.flag_debug else \
                         self.LData[line].getLEDstates(self.displaymode,direction,epoch=self.debug_epoch,debug_speed=self.debug_speed)
               D[line] = LED_on_line
          return D

     