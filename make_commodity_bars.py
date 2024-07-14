"""
Code rewritten to make commodity_data
bars from the tick files created with
tickwrite_process_ticks.py and
stored locally as /HistTicks.h5.
"""

import numpy as np
import pandas as pd
import tables
import os
import datetime as dt

# Define the data type for bar data
bar_type = np.dtype(
    [
        ("date", "M8[D]"),
        ("time", "m8[us]"),
        ("open_p", "f8"),
        ("high_p", "f8"),
        ("low_p", "f8"),
        ("close_p", "f8"),
        ("per_vlm", "u8"),
    ]
)

h5_bar_type = np.dtype(
    [
        ("date", "i8"),
        ("time", "i8"),
        ("open_p", "f8"),
        ("high_p", "f8"),
        ("low_p", "f8"),
        ("close_p", "f8"),
        ("per_vlm", "u8"),
    ]
)


def ticks_to_bars(tick_data, interval):
    tick_data = tick_data.astype(bar_type)
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

        next_entry = np.empty(1, bar_type)
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
            next_entry[0]["high_p"] = tick_data[tick_index:interval_end]["price"].max()
            next_entry[0]["low_p"] = tick_data[tick_index:interval_end]["price"].min()
            next_entry[0]["open_p"] = tick_data[tick_index]["price"]
            next_entry[0]["close_p"] = tick_data[interval_end - 1]["price"]
            next_entry[0]["per_vlm"] = np.sum(
                tick_data[tick_index:interval_end]["volume"]
            )

        bar_data = np.append(bar_data, next_entry)
        tick_index = interval_end
        begin_time = end_time
        end_time = begin_time + np.timedelta64(interval, "s")

    return bar_data


class CommodityBars:
    def __init__(self, sym, name, h5file, h5path, is_live):
        self.symbol = sym
        self.name = name
        self.nparray = np.empty(0, bar_type)
        self.h5file = h5file

        if h5path + "/" + self.name not in self.h5file:
            self.h5table = h5file.create_table(
                h5path, self.name, h5_bar_type, "{} table".format(self.name)
            )
        else:
            self.h5table = self.h5file.get_node(h5path, self.name)
        self.is_live = is_live

    def read_data(self, filepath):
        filepath = Path(filepath)
        count = 0

        with open(filepath, "r") as fp_object:
            for line in fp_object:
                line = line.split(",")
                date = dt.datetime.strptime(line[0] + " " + line[1], "%m/%d/%Y %H:%M")
                count += 1
                next_entry = np.empty(1, bar_type)
                np_date = np.datetime64(date)
                next_entry[0]["date"] = np_date
                next_entry[0]["time"] = np_date - np_date.astype("M8[D]")
                next_entry[0]["open_p"] = np.float64(line[2])
                next_entry[0]["high_p"] = np.float64(line[3])
                next_entry[0]["low_p"] = np.float64(line[4])
                next_entry[0]["close_p"] = np.float64(line[5])
                next_entry[0]["per_vlm"] = np.float64(line[6])

                self.nparray = np.append(self.nparray, next_entry)
                self.save_h5()

    def save_h5(self):
        self.h5table.append(self.nparray.astype(h5_bar_type))
        self.h5file.flush()
        self.nparray = np.empty(0, bar_type)


def make_h5(h5_filename="TW_HistoricalBars.h5"):
    h5file = tables.open_file(
        h5_filename,
        mode="w",
        title="Historical Ticks for Wheat, Corn, Soy, SoyMeal, and Bean Oil",
    )
    group = h5file.create_group("/", "TW_HistBars", "Bar data")
    return h5file


def make_commodity_data(h5file):
    commodities = {}
    commodities["WC"] = CommodityBars("WC", "Wheat", h5file, "/TW_HistBars", False)
    commodities["CN"] = CommodityBars("CN", "Corn", h5file, "/TW_HistBars", False)
    commodities["SY"] = CommodityBars("SY", "Soy", h5file, "/TW_HistBars", False)
    commodities["SM"] = CommodityBars("SM", "SoyMeal", h5file, "/TW_HistBars", False)
    commodities["BO"] = CommodityBars("BO", "BeanOil", h5file, "/TW_HistBars", False)
    return commodities


def create_commodity_bars(
    h5_file_path, start_date, end_date, duration, start_time, end_time
):
    with tables.open_file(h5_file_path, mode="r") as h5file:
        for node in h5file.walk_nodes("/", classname="Table"):
            print(f"Processing {node._v_pathname}...")
            df = pd.DataFrame.from_records(node.read())
            df["datetime"] = df["date"] + df["time"]
            df = df[(df["datetime"] >= start_date) & (df["datetime"] <= end_date)]
            df = df.set_index("datetime")
            df = df.between_time(start_time, end_time)

            resampled = df.resample(duration).agg({"price": "ohlc", "volume": "sum"})

            # Save or process the resampled data as needed
            print(resampled.head())


def main():
    # Path to the local HDF5 file
    local_dir = os.path.expanduser("~/Desktop/commodity_data")
    h5_file_path = os.path.join(local_dir, "TD_HistoricalTicks.h5")

    start_date = input("Enter the start date (YYYY-MM-DD): ")
    end_date = input("Enter the end date (YYYY-MM-DD): ")
    duration = input("Enter bar duration in seconds (e.g., 3600 for 1 hour): ")
    start_time = input("Enter start time of trading day (HH:MM:SS): ")
    end_time = input("Enter end time of trading day (HH:MM:SS): ")

    create_commodity_bars(
        h5_file_path, start_date, end_date, duration, start_time, end_time
    )


if __name__ == "__main__":
    main()
