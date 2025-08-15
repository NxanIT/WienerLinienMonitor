import threading
import time
from datetime import datetime
import logging
import numpy as np
import configparser
import os
import json
from Conversions import DataConversions
from Fetch import FetchData
from LineData import LineData, MetroData
#TODO:For use on Raspberry Pi: remove unused imports and import GPIO, uncomment all single commented lines
import sys
from matplotlib import pyplot as plt
#import GPIO


config = configparser.ConfigParser()
config_dir = os.path.dirname(os.path.realpath(__file__)) + '\\Config.ini'
config_parsed_files = config.read(config_dir)##read config file (must be in same ordner)
if(len(config_parsed_files)==0):
    raise ImportError(f"Unable to read config-file. expected file at: {config_dir}")

config['PATH'] = {'path':os.path.dirname(os.path.realpath(__file__))}
with open(os.path.dirname(os.path.realpath(__file__)) + '\\Config.ini','w') as configfile:##write current path to config file
    config.write(configfile)

##debug mode:
FLAG_DEBUG = config.getint('DEFAULT','FLAG_DEBUG')
debug_time_list = json.loads(config.get('DEFAULT','debug_time'))
debug_time = datetime(*debug_time_list)

logging_levels = [logging.DEBUG,logging.INFO]
logging_level = logging_levels[config.getint('DEFAULT','logging_level')]

##displaying variables:
LINES = json.loads(config.get('DEFAULT','LINES')) ##LINES which are meassured

##for displaying
flag_monitor_debug = config.getboolean('MONITOR','flag_monitor_debug')
frame_rate = config.getint('MONITOR','frame_rate') ## no. of times per second the led is turned on and off
duty_cycle = config.getfloat('MONITOR','duty_cycle') ## ratio of on time * numer of lines to be displayed
blink_half_period = config.getint('MONITOR','blink_half_period') ##seconds, time the led stays on (or off) in blinking mode


## GPIO - input
PIN_DISPLAYMODE = config.getint('MONITOR','PIN_DISPLAYMODE')
PIN_MONITOR_ON = config.getint('MONITOR','PIN_MONITOR_ON') ##TODO: if high, turn on monitor
PIN_EXIT = config.getint('MONITOR','PIN_EXIT') ##TODO: if high, stop execution and terminate program
## GPIO - output
PINS_LINE_SELECT = json.loads(config.get('MONITOR','PINS_LINE_SELECT'))

PIN_SDO = config.getint('MONITOR','PIN_SDO')
PIN_CLK = config.getint('MONITOR','PIN_CLK')
PIN_OE_NOT = config.getint('MONITOR','PIN_OE_NOT')
PIN_LE = config.getint('MONITOR','PIN_LE')

#TODO: uncomment on machine: remove all single '#' characters
#GPIO.setmode(GPIO.BCM) ##sets indexing mode of ports to the GPIO numbers
#for pin in PINS_LINE_SELECT.values():
#    GPIO.setup(pin, GPIO.OUT)
#GPIO.setup(PIN_SDO, GPIO.OUT)
#GPIO.setup(PIN_CLK, GPIO.OUT)
#GPIO.setup(PIN_OE_NOT, GPIO.OUT)
#GPIO.setup(PIN_LE, GPIO.OUT)



SHIFT_REGISTER_SIZE = config.getint('MONITOR','SHIFT_REGISTER_SIZE')

logging.basicConfig(filename="WienerLinienMonitor/python/monitor.log", filemode='w', level=logging.INFO,format='%(asctime)s - %(name)s\t- %(levelname)s: %(message)s')

logger = logging.getLogger(__name__)

console = logging.StreamHandler()
logger.addHandler(console)
console.setLevel(logging.INFO)
## TODO: (general todo) change logging method to write logs in diffrent files, depending on severity

class Monitor:
    logger = logging.getLogger("Monitor")
    def __init__(self,Lines,Me:MetroData) -> None:
        global frame_rate,duty_cycle
        self.Lines = Lines
        self.init_display()
        self.ref_time = datetime.now()
        self.MetroData = Me
        self.Time_ON_per_frame = 1/(frame_rate*len(Lines))*duty_cycle
    ##------------GPIO-----------------------------------------------------------------------

    def init_display(self):
    ##TODO: set all transistors off
    ##output enable = high (no display)
    ##latch = false
    ##...
        return

    def lightDisplay(self):
        ##TODO add interruption to terminate method
        while True:
            
            StationData = self.MetroData.getStationData()
            t1 = time.time_ns()
            logger.debug("updates display.")
            while(time.time_ns()-t1<10**9):##loops for a second
                self.__updateDisplay(StationData)
        
        pass

    def __updateDisplay(self,StationData:dict[str,np.array]):

        for line in self.Lines:
            t1 = time.time_ns()
            ##myb a timefunction for controlling on time of leds
            LinePin = PINS_LINE_SELECT[line]
            #GPIO.output(LinePin,0) ##set transistor on 
            ##TODO: check if that means seting transistor on or off, it should be off

            ## Transmit signal

            Number_of_stations = np.shape(StationData[line])[0]
            for i in range(SHIFT_REGISTER_SIZE-Number_of_stations+1): ##padding ##TODO check if the +1 is needed
                self.push_shiftregister(0)

            for i in range(Number_of_stations-1,1,-1): ##reverse order
                for j in range(2):
                    self.push_shiftregister(StationData[line][i,j])

            ##latch_data at end of last transmission
            self.push_shiftregister(StationData[line][0,0])
            self.push_shiftregister(StationData[line][0,1],latch=True)

            ## light leds
            #GPIO.output(PIN_OE_NOT,0)

            ##keep lights on for calc duration
            delta_t = self.Time_ON_per_frame - (time.time_ns()-t1)/10**9
            if(delta_t<=0):
                logger.warning("Framerate to high or duty_cycle to low. Can not keep up.")
            else: time.sleep(delta_t)
            #GPIO.output(PIN_OE_NOT,1)
            #GPIO.output(LinePin,1) ##transistor off

    def push_shiftregister(self,led_state,latch = False):
        Led_turned_on = self.Led_state(led_state)
        ##push to shift register
        #GPIO.output(PIN_SDO,int(Led_turned_on))
        #if(latch):
            #GPIO.output(PIN_LE,1)
        #GPIO.output(PIN_CLK,1)
        #GPIO.output(PIN_CLK,0)
        #GPIO.output(PIN_LE,0)
        #GPIO.output(PIN_SDO,0)
        return

    def Led_state(self,led_state:int):
        """returns wheather or not the led should be on or off at the current time
            if led_state is 0 or 1, the led_state is returned.
            if the led_state is 2 (blinking mode), the state of the led is obtained by the seconds_since_ref_time modulus 2
        """
        if(led_state<=1):
            return led_state
        return (1 + self.seconds_since_ref_time()//blink_half_period) % 2

    def seconds_since_ref_time(self):
        return int((datetime.now()-self.ref_time).total_seconds())


def __DemoUpdateDisplay(Data:dict[str,np.array],ax,meassured_stations,lenOfLine):
        for i in range(len(LINES)):
            if(LINES[i] in Data.keys()):
                index_row = i//2
                index_col = i % 2
                ax[index_row,index_col].cla()
                ax[index_row,index_col].imshow(Data[LINES[i]].T)
                ax[index_row,index_col].set_title(f'Line{LINES[i]}')
                sec = ax[index_row,index_col].secondary_xaxis(location=1)
                sec.set_xticks(meassured_stations)
                ax[index_row,index_col].set_xticks(np.arange(0,lenOfLine[i],2))
        
        


def seconds_since(time1:datetime,time2:datetime):
     return int((time2-time1).total_seconds())

def main():
    ##init
    logger.info('starting...')
    Co = DataConversions(LINES)
    
    ref_time = datetime.now() if not FLAG_DEBUG else debug_time
    Me = MetroData(LINES,Co,ref_time,config,FLAG_DEBUG)
    Fe = FetchData(LINES,Me,Co,config)
    Mo = Monitor(LINES,Me)
    logger.info("initialization completed. Starting to fetch more meassure data...")

    while(True):
        if Fe.check_for_updates():
            break ##breaks, if all meassure_stations have been meassured
        time.sleep(1)
    logger.info("all lines have been meassured at least once. starting Monitor...")

    ##in display-debug-mode: start window with plot
    if(flag_monitor_debug):
        lines = Co.getMeassuredLines()
        no_cols = 2
        fig,ax = plt.subplots(int(np.ceil(len(lines)/2)),no_cols)
        print(ax)
        for i in range(len(lines)):
            meassured_stations = Co.getLineMeassureStations(lines[i])
        debug_speed = config.getint('MONITOR','debug_speed') if FLAG_DEBUG else 1
        while True:
            StationData = Me.getStationData()
            t1 = time.time_ns()
            ##fig.clear()
            logger.debug("updates display.")
            ax[2,1].cla()
            ax[2,1].text(0,2,f"first meassured: {ref_time}")
            ax[2,1].text(0,1,f"time elapsed: {debug_speed*seconds_since(Me.debug_epoch,datetime.now())}s")
            ax[2,1].set_axis_off()
            while(time.time_ns()-t1<10**9):##loops for a second
                line_lens = [Co.lenOfLine(line) for line in LINES]
                __DemoUpdateDisplay(StationData,ax,meassured_stations,line_lens)
                fig.tight_layout()
                plt.pause(0.2)

    ## start monitor
    Thread_monitor = threading.Thread(target=Mo.lightDisplay)
    Thread_monitor.start() ##monitor is now displaying

    ##station data gets now updated once per second, check for updates on departure data every 10 seconds
    while(True): ##TODO: add interrupt condition for a selected pin
    # #     in_key, timed_out = timedKey("In<< ",10,allowCharacters="e012357")
    # #     if(not timed_out):
    # #         if(in_key=='e'):
    # #             print("Out>> program terminates.")
    # #             exit(0)
    # #         print("Out>> changes displaymode to:",in_key)
    # #         De.updateDisplaymode(int(in_key))
            
        Fe.check_for_updates()
        time.sleep(10)

    pass

if __name__ == '__main__':
    main()

