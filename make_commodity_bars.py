import datetime as dt
from pathlib import Path

import numpy as np
import tables

from shared_types import bar_type, h5_bar_type


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
                next_entry[0]["open_p"] = np.float64(line[2])
                next_entry[0]["high_p"] = np.float64(line[3])
                next_entry[0]["low_p"] = np.float64(line[4])
                next_entry[0]["close_p"] = np.float64(line[5])
                next_entry[0]["per_vlm"] = np.float64(line[6])

                self.nparray = np.append(self.nparray, next_entry)
                if self.nparray.size > 100:
                    self.save_h5()

    def save_h5(self):
        self.h5table.append(self.nparray.astype(h5_bar_type))
        self.h5file.flush()
        self.nparray = np.empty(0, bar_type)
        print(f"Data for {self.name} saved to HDF5!")


def make_h5(h5_filename="TD_HistoricalBars.h5"):
    h5file = tables.open_file(
        h5_filename,
        mode="w",
        title="Historical Ticks for Wheat, Corn, Soy, SoyMeal, and Bean Oil",
    )
    _ = h5file.create_group("/", "TW_HistBars", "Bar data")
    return h5file


def make_commodity_data(h5file):
    commodities = {
        "Wheat": CommodityBars("WC", "Wheat", h5file, "/TD_HistBars", False),
        "Corn": CommodityBars("CN", "Corn", h5file, "/TD_HistBars", False),
        "Soy": CommodityBars("SY", "Soy", h5file, "/TD_HistBars", False),
        "SoyMeal": CommodityBars("SM", "SoyMeal", h5file, "/TD_HistBars", False),
        "BeanOil": CommodityBars("BO", "BeanOil", h5file, "/TD_HistBars", False),
    }
    return commodities


if __name__ == "__main__":
    h5file = make_h5()
    commodities = make_commodity_data(h5file)
    filepath = input("Please enter the filepath for your data:\n")
    for sym in commodities.values():
        sym.read_data(filepath)
        sym.save_h5()
    h5file.close()
