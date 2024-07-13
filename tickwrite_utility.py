import sys
import os
import datetime as dt
import datetime as timedelta
from time import sleep
from pathlib import Path

import numpy as np
import h5py
import tables



bar_type = np.dtype([('date', 'M8[D]'), ('time', 'm8[us]'), # store date in months units, with day resolution
                     ('open_p', 'f8'), ('high_p', 'f8'), # float (decimal) 
                     ('low_p', 'f8'), ('close_p', 'f8'), 
                     ('per_vlm', 'u8') ])

h5_bar_type = np.dtype([('date', 'i8'), ('time', 'i8'), # store date as just a long decimal number.
                     ('open_p', 'f8'), ('high_p', 'f8'), # float (decimal) 
                     ('low_p', 'f8'), ('close_p', 'f8'), 
                     ('per_vlm', 'u8') ])

# set up the types and files for the pytables/h5/numpy data
#tick_type = np.dtype([('date', 'M8[D]'), ('time', 'm8[us]'),# store date in months units, with day resolution
                        #('price', 'f8'), ('volume', 'u8')])# float (decimal) 
                        

tick_type = np.dtype({'names':['symbol', 'date', 'time', 'price', 'volume'],
                    'formats':[('S2'),('M8[D]'),('m8[us]'),('f8'), ('u8')]})

h5_tick_type = np.dtype([('symbol','S2'),('date', 'i8'), ('time', 'i8'), # store date as just a long decimal number.
                            ('price', 'f8'), ('volume', 'u8')]) # float (decimal)                            
                            
                                          
def ticks_to_bars(tick_data, interval):
    # takes in a numpy array of tick_type, collates the data into bars and return
    # a numpy array of bar_type.
    tick_data = tick_data.astype(tick_type)    
    bar_data = np.empty(0, dtype=bar_type)
    
    tick_index = 0
    # get the difference between the first tick time and the closest even beginning
    # of the specified interval length. 
    tick_us = tick_data[0]['time'].astype('i8')
    interval_us = np.timedelta64(interval, 's').astype('i8')
    
    start_us = tick_us - (tick_us % interval_us)
    
    begin_time = tick_data[0]['date'] + start_us.astype('m8[us]') # interval start at first tick
    end_time = begin_time + np.timedelta64(interval, 's')
    #print("tick_data[0]['date'] is a {}".format(tick_data[0]['date'].dtype))
    #print("begin_time is a {}".format(begin_time.dtype))
    #print("end_time is a {}".format(begin_time.dtype))
    while(tick_index < tick_data.size):
        # find all the ticks that fall within the next range
        interval_end = tick_index
        while(interval_end < tick_data.size and ( tick_data[interval_end]['date'] + tick_data[interval_end]['time']) < end_time):
            interval_end += 1
            
        # interval_end is the first index that should NOT be included in the bar. 
        
        # find all the features of the bar based on those ticks. (e.g. open, close, high, low)
        next_entry = np.empty(1,bar_type)
        np_date = end_time      
        next_entry[0]['date'] = np_date
        next_entry[0]['time'] = np_date - np_date.astype('M8[D]')
        if(interval_end is tick_index):
            next_entry[0]['high_p'] = bar_data[-1]['close_p']
            next_entry[0]['low_p'] = bar_data[-1]['close_p']
            next_entry[0]['open_p'] = bar_data[-1]['close_p']
            next_entry[0]['close_p'] = bar_data[-1]['close_p']
            next_entry[0]['per_vlm'] = 0
        else:
            next_entry[0]['high_p'] = tick_data[tick_index:interval_end]['last_p'].max()
            next_entry[0]['low_p'] = tick_data[tick_index:interval_end]['last_p'].min()
            next_entry[0]['open_p'] = tick_data[tick_index]['last_p']
            next_entry[0]['close_p'] = tick_data[interval_end - 1]['last_p']
            next_entry[0]['per_vlm'] = np.sum(tick_data[tick_index:interval_end]['last_v'])
        #print(next_entry)
        bar_data = np.append(bar_data, next_entry)        
        
        tick_index = interval_end
        begin_time = end_time
        end_time = begin_time + np.timedelta64(interval, 's')
    return bar_data
                            
                            
class CommodityBars:
    
    def __init__(self, sym, name, h5file, h5path, is_live):
        self.symbol = sym
        self.name = name       
        self.nparray = np.empty(0, bar_type)
        self.h5file = h5file # shared with other instances
        if (h5path + '/' + self.name not in self.h5file):
            self.h5table = h5file.create_table(h5path, self.name,  h5_bar_type, "{} table".format(self.name))
        else:
            
            self.h5table = self.h5file.get_node(h5path , self.name)
        self.is_live = is_live
        
    def read_data(self, filepath):
        #filepath = Path(input("Please enter the filepath for your data:\n"))
        #filepath = "C:\Users\eric\Desktop\BO.txt"
        count = 0
        
        with open(filepath, 'r' ) as fp_object: 
            for line in fp_object:              
                line = line.split(',')
                #date = dt.datetime.strptime(line[1] + ' ' + line[2], "%m/%d/%Y %H:%M:%S.%f" )               
                #count+=1
                #next_entry = np.empty(1,tick_type)
                #np_date = np.datetime64(date)
                #next_entry[0]['symbol'] = np.string_(line[0])
                #next_entry[0]['date'] = np_date
                #next_entry[0]['time'] = np_date - np_date.astype('M8[D]')
                #next_entry[0]['price'] = np.float64(line[3])
                #next_entry[0]['volume'] = np.float64(line[4])
                
                date = dt.datetime.strptime(line[0] + ' ' + line[1], "%m/%d/%Y %H:%M" )               
                count+=1
                next_entry = np.empty(1,bar_type)
                np_date = np.datetime64(date)
                #next_entry[0]['symbol'] = np.string_(line[0])
                next_entry[0]['date'] = np_date
                next_entry[0]['time'] = np_date - np_date.astype('M8[D]')
                next_entry[0]['open_p'] = np.float64(line[2])
                next_entry[0]['high_p'] = np.float64(line[3])
                next_entry[0]['low_p'] = np.float64(line[4])
                next_entry[0]['close_p'] = np.float64(line[5])
                next_entry[0]['per_vlm'] = np.float64(line[6])
                
                self.nparray = np.append(self.nparray, next_entry)       
                #if(self.nparray.size > 100):
                self.save_h5()
                   
    def save_h5(self):
        self.h5table.append(self.nparray.astype(h5_bar_type))
        self.nparray = np.empty(0, bar_type)
        
