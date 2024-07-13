import tickwrite_utility
import tables
import numpy as np
import seaborn
import matplotlib
import matplotlib.pyplot as plt
matplotlib.style.use('ggplot')

from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score

class com_Regression:
    def __init__(self,comm1=None,comm2=None, time_in_seconds=None):
        # anything that we want to be on the "class bulletin board", we should
        # make inside this function.
        if(comm1 is not None):
            self.comm1 = comm1
        else:
            self.comm1 = input("Enter the first commodity you want to analyze: ")
            
        if(comm2 is not None):
            self.comm2 = comm2
        else:
            self.comm2 = input("Enter the second commodity you want to analyze: ")
        
        self.data = tables.open_file("HistoricalTicks.h5")
        if(time_in_seconds is not None):
            self.time_in_seconds = time_in_seconds
        else:
            self.time_in_seconds = input("Enter the length of time, in seconds, to include in each bar: ")
            
        self.bars_comm1, self.bars_comm2 = self.make_bars()
        
    def make_bars(self):            
        arr_comm1 = self.data.get_node(("/HistTicks/{}").format(self.comm1)).read()
        arr_comm2 = self.data.get_node(("/HistTicks/{}").format(self.comm2)).read()
        bars_comm1 = ticks_to_bars(arr_comm1,self.time_in_seconds)
        bars_comm2 = ticks_to_bars(arr_comm2,self.time_in_seconds)
        #I thought that since I returned needed variables that I can call them in def regression??
        return bars_comm1, bars_comm2
