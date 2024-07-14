import datetime as dt
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import tables
from sklearn import linear_model

from shared_types import bar_type, h5_bar_type, ticks_to_bars

matplotlib.style.use("ggplot")


class H5TickType(tables.IsDescription):
    """
    Description of the HDF5 table structure for tick data.

    Attributes:
        symbol (str): Symbol representing the commodity (2 characters).
        date (int): Date as an integer timestamp.
        time (int): Time as an integer timestamp.
        last_p (float): Last price.
        last_v (int): Last volume.
    """

    symbol = tables.StringCol(2)
    date = tables.Int64Col()
    time = tables.Int64Col()
    last_p = tables.Float64Col()
    last_v = tables.Int64Col()


class CommodityBars:
    """
    Handle operations related to commodity bars, including reading bar data and saving it.
    """

    def __init__(self, sym, name, h5file, h5path, is_live):
        self.symbol = sym
        self.name = name
        self.nparray = np.empty(0, bar_type)
        self.h5file = h5file

        if f"{h5path}/{self.name}" not in self.h5file:
            self.h5table = h5file.create_table(
                h5path, self.name, h5_bar_type, f"{self.name} table"
            )
        else:
            self.h5table = self.h5file.get_node(h5path, self.name)
        self.is_live = is_live

    def read_data(self, filepath):
        count = 0
        with open(filepath, "r", encoding="utf-8") as fp_object:
            for line in fp_object:
                line = line.split(",")
                date = dt.datetime.strptime(line[0] + " " + line[1], "%m/%d/%Y %H:%M")
                count += 1
                next_entry = np.empty(1, bar_type)
                np_date = np.datetime64(date)
                next_entry[0]["date"] = np_date
                next_entry[0]["time"] = np_date - np_date.astype("M8[D]")
