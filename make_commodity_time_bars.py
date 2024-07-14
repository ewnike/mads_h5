"""
Code needed to read ticks from
locally stored ticks in h5 file.
This takes the tick data and
makes them into time bars
"""

from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tables

from shared_types import bar_type, tick_type


class H5BarType(tables.IsDescription):
    """
    Description of the HDF5 table structure for bar data.
    """

    date = tables.Int64Col()
    time = tables.Int64Col()
    open_p = tables.Float64Col()
    high_p = tables.Float64Col()
    low_p = tables.Float64Col()
    close_p = tables.Float64Col()
    per_vlm = tables.Int64Col()


def ticks_to_bars(tick_data, interval):
    """
    collect ticks gathering them
    into appropriate time
    buckets.
    """

    tick_data = tick_data.astype(tick_type)
    bar_data = np.empty(0, dtype=bar_type)

    tick_index = 0
    tick_us = tick_data[0]["time"].astype("i8")
    interval_us = np.timedelta64(interval, "s").astype("i8")

    start_us = tick_us - (tick_us % interval_us)
    begin_time = tick_data[0]["date"] + start_us.astype("m8[us]")
    end_time = begin_time + np.timedelta64(interval, "s")

    while tick_index < tick_data.size:
        interval_end = tick_index
        while (
            interval_end < tick_data.size
            and (tick_data[interval_end]["date"] + tick_data[interval_end]["time"])
            < end_time
        ):
            interval_end += 1

        next_entry = np.empty(1, dtype=bar_type)
        np_date = end_time
        next_entry[0]["date"] = np_date
        next_entry[0]["time"] = np_date - np_date.astype("M8[D]")
        if interval_end == tick_index:
            next_entry[0]["high_p"] = bar_data[-1]["close_p"]
            next_entry[0]["low_p"] = bar_data[-1]["close_p"]
            next_entry[0]["open_p"] = bar_data[-1]["close_p"]
            next_entry[0]["close_p"] = bar_data[-1]["close_p"]
            next_entry[0]["per_vlm"] = 0
        else:
            next_entry[0]["high_p"] = tick_data[tick_index:interval_end]["last_p"].max()
            next_entry[0]["low_p"] = tick_data[tick_index:interval_end]["last_p"].min()
            next_entry[0]["open_p"] = tick_data[tick_index]["last_p"]
            next_entry[0]["close_p"] = tick_data[interval_end - 1]["last_p"]
            next_entry[0]["per_vlm"] = np.sum(
                tick_data[tick_index:interval_end]["last_v"]
            )

        bar_data = np.append(bar_data, next_entry)
        tick_index = interval_end
        begin_time = end_time
        end_time = begin_time + np.timedelta64(interval, "s")

    return bar_data


def process_ticks_to_bars(h5file, h5path, sym, interval, start_date, end_date):
    """
    take the gathered ticks
    and turn them into bars.
    """

    node = h5file.get_node(h5path, sym)
    tick_data = node.read_where(f"date >= {start_date} & date <= {end_date}")
    bars = ticks_to_bars(tick_data, interval)

    bar_table = h5file.create_table(
        h5path, f"{sym}_bars", H5BarType, f"{sym} bars table"
    )
    for bar in bars:
        bar_table.append([bar])
    h5file.flush()


def plot_bars(h5file, sym, start_date, end_date):
    """
    plot the newly created time bars.
    """
    node = h5file.get_node("/", f"{sym}_bars")
    bars = node.read_where(f"date >= {start_date} & date <= {end_date}")

    dates = [
        datetime.utcfromtimestamp(bar["date"].astype("M8[s]").astype(int))
        for bar in bars
    ]
    plt.figure(figsize=(12, 6))
    plt.plot(dates, bars["open_p"], label="Open")
    plt.plot(dates, bars["close_p"], label="Close")
    plt.fill_between(dates, bars["low_p"], bars["high_p"], alpha=0.3)
    plt.legend()
    plt.show()


if __name__ == "__main__":
    h5_filename = "tick_data.h5"
    h5file = tables.open_file(h5_filename, mode="a")
    sym = input("Enter the commodity symbol: ")
    interval = int(input("Enter the bar interval in seconds: "))
    start_date = np.datetime64(input("Enter the start date (YYYY-MM-DD): "))
    end_date = np.datetime64(input("Enter the end date (YYYY-MM-DD): "))
    process_ticks_to_bars(h5file, "/", sym, interval, start_date, end_date)
    plot_bars(h5file, sym, start_date, end_date)
    h5file.close()
