"""
Code needed to read ticks from
locally stored ticks in h5 file.
This takes the tick data and
makes them into time bars
"""

import tables
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from shared_types import tick_type, bar_type

class H5BarType(tables.IsDescription):
    """
    Description of the HDF5 table structure for bar data.
    """
    date = tables.Int64Col()
    time = tables.Int64Col()
    open_price = tables.Float64Col()
    high_price = tables.Float64Col()
    low_price = tables.Float64Col()
    close_price = tables.Float64Col()
    volume = tables.Int64Col()

def ticks_to_bars(tick_data, bar_interval):
    """
    collect ticks gathering them
    into appropriate time
    buckets.
    """

    tick_data = tick_data.astype(tick_type)    
    bar_data = np.empty(0, dtype=bar_type)
    
    tick_index = 0
    tick_us = tick_data[0]['time'].astype('i8')
    interval_us = np.timedelta64(bar_interval, 's').astype('i8')
    
    start_us = tick_us - (tick_us % interval_us)
    begin_time = tick_data[0]['date'] + start_us.astype('m8[us]')
    end_time = begin_time + np.timedelta64(bar_interval, 's')
    
    while tick_index < tick_data.size:
        interval_end = tick_index
        while interval_end < tick_data.size and (tick_data[interval_end]['date'] + tick_data[interval_end]['time']) < end_time:
            interval_end += 1
        
        new_bar = np.empty(1, dtype=bar_type)
        np_date = end_time
        new_bar[0]['date'] = np_date
        new_bar[0]['time'] = np_date - np_date.astype('M8[D]')
        if interval_end == tick_index:
            new_bar[0]['high_price'] = bar_data[-1]['close_price']
            new_bar[0]['low_price'] = bar_data[-1]['close_price']
            new_bar[0]['open_price'] = bar_data[-1]['close_price']
            new_bar[0]['close_price'] = bar_data[-1]['close_price']
            new_bar[0]['volume'] = 0
        else:
            new_bar[0]['high_price'] = tick_data[tick_index:interval_end]['last_p'].max()
            new_bar[0]['low_price'] = tick_data[tick_index:interval_end]['last_p'].min()
            new_bar[0]['open_price'] = tick_data[tick_index]['last_p']
            new_bar[0]['close_price'] = tick_data[interval_end - 1]['last_p']
            new_bar[0]['volume'] = np.sum(tick_data[tick_index:interval_end]['last_v'])
        
        bar_data = np.append(bar_data, new_bar)        
        tick_index = interval_end
        begin_time = end_time
        end_time = begin_time + np.timedelta64(bar_interval, 's')
    
    return bar_data

def process_ticks_to_bars(hdf5_file, hdf5_path, symbol, bar_interval, start_dt, end_dt):
    """
    take the gathered ticks
    and turn them into bars.
    """
    node = hdf5_file.get_node(hdf5_path, symbol)
    tick_data = node.read_where(f"date >= {start_dt} & date <= {end_dt}")
    bars = ticks_to_bars(tick_data, bar_interval)
    
    bar_table = hdf5_file.create_table(hdf5_path, f"{symbol}_bars", H5BarType, f"{symbol} bars table")
    for bar in bars:
        bar_table.append([bar])
    hdf5_file.flush()

def plot_bars(hdf5_file, symbol, start_dt, end_dt):
    """
    plot the newly created time bars.
    """
    
    node = hdf5_file.get_node("/", f"{symbol}_bars")
    bars = node.read_where(f"date >= {start_dt} & date <= {end_dt}")

    dates = [datetime.utcfromtimestamp(bar['date'].astype('M8[s]').astype(int)) for bar in bars]
    plt.figure(figsize=(12, 6))
    plt.plot(dates, bars['open_price'], label='Open')
    plt.plot(dates, bars['close_price'], label='Close')
    plt.fill_between(dates, bars['low_price'], bars['high_price'], alpha=0.3)
    plt.legend()
    plt.show()

if __name__ == "__main__":
    hdf5_filename = "tick_data.h5"
    hdf5_file = tables.open_file(hdf5_filename, mode="a")
    symbol = input("Enter the commodity symbol: ")
    bar_interval = int(input("Enter the bar interval in seconds: "))
    start_dt = np.datetime64(input("Enter the start date (YYYY-MM-DD): "))
    end_dt = np.datetime64(input("Enter the end date (YYYY-MM-DD): "))
    process_ticks_to_bars(hdf5_file, "/", symbol, bar_interval, start_dt, end_dt)
    plot_bars(hdf5_file, symbol, start_dt, end_dt)
    hdf5_file.close()
