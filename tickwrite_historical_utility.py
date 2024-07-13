import tickwrite_tility 
import sys
import pandas as pd
import datetime as dt
from time import sleep
import numpy as np
import tables

def make_h5(h5_filename = "TD_HistoricalBars.h5"):
    h5file = tables.open_file(h5_filename, mode="w", title = "Historical Ticks for Wheat, Corn, Soy, SoyMeal, and Bean Oil")
    # arguments: location (/ means top level), group name, description.
    group = h5file.create_group("/", "TD_HistBars", "Bar data") 
    return h5file
    
def make_commodity_data(h5file):
    # I want to access all the info for a commodity, and control the data, based on 
    # its symbol. So, I'm going to store them in a dictionary
    # keys: symbols, values: instances of CommodityTicks
    commodities = {}
    commodities["WC"] = CommodityBars("WC", "Wheat", h5file, "/TD_HistBars", False)
    commodities["CN"] = CommodityBars("CN", "Corn", h5file, "/TD_HistBars", False)
    commodities["SY"] = CommodityBars("SY", "Soy", h5file, "/TD_HistBars", False)
    commodities["SM"] = CommodityBars("SM","SoyMeal", h5file, "/TD_HistBars", False)
    commodities["BO"] = CommodityBars("BO", "BeanOil", h5file, "/TD_HistBars", False)
    return commodities
#Hack for begin date and end date
#def begin_end_date():
    #startDate = input('What is the startDate and time?(YYYYmmdd HHMMSS)\n')
    #endDate = input('What is the endDate and time?(YYYYmmdd HHMMSS)\n')
    #return startDate, endDate 
    
if __name__ == "__main__":
    h5file = make_h5()
    commodities = make_commodity_data(h5file)
    #filepath = Path(input("Please enter the filepath for your data:\n"))
    filepath = "C:/Users/eric/Desktop/SM.txt"
    for sym in commodities.values():
        sym.read_data(filepath)
        #sym.save_h5()
        h5file.close()
