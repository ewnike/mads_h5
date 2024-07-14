"""
Code rewritten by Eric Winiecke on 2024-07-13
to update and modularize reading
commodity futures tick data from
an csv file on aws s3 converted
to a numpy array stored locally
as an h5_tick_file.
"""

import numpy as np
import pandas as pd
import tables
import boto3
import os

# Define the data type for numpy arrays and HDF5 tables
tick_type = np.dtype(
    [
        ("date", "datetime64[ns]"),
        ("time", "timedelta64[ns]"),
        ("last_p", "float64"),
        ("last_v", "int64"),
    ]
)


class H5TickType(tables.IsDescription):
    date = tables.Time64Col()
    time = tables.Time64Col()
    last_p = tables.Float64Col()
    last_v = tables.Int64Col()


def extract_letters(sym):
    return sym[:2] + sym[3:]


class CommodityTicks:
    def __init__(self, sym, name, h5file, h5path, is_live):
        self.symbol = sym
        self.name = name
        self.nparray = np.empty(0, tick_type)
        self.h5file = h5file  # shared with other instances
        self.h5path = h5path

        if h5path + "/" + self.name not in self.h5file:
            self.h5table = h5file.create_table(
                h5path, self.name, H5TickType, "{} table".format(self.name)
            )
        else:
            self.h5table = self.h5file.get_node(h5path, self.name)
        self.is_live = is_live

    def process_file(self, path):
        data = pd.read_csv(path)
        data["sym"] = data["sym"].apply(extract_letters)

        for row in data.itertuples(index=False, name="Commodity"):
            date_str = f"{row.Date} {row.Time}"
            np_date = np.datetime64(pd.to_datetime(date_str))
            next_entry = np.array(
                [(np_date, np_date - np_date.astype("M8[D]"), row.LastP, row.LastV)],
                dtype=tick_type,
            )
            self.nparray = np.append(self.nparray, next_entry)

            if self.nparray.size >= 100:
                self.save_h5()

        if self.nparray.size > 0:
            self.save_h5()

    def save_h5(self):
        self.h5table.append(self.nparray)
        self.h5file.flush()
        print(f"Data for {self.name} saved to HDF5!")
        self.nparray = np.empty(0, tick_type)


def download_from_s3(bucket_name, file_key, local_path):
    s3 = boto3.client("s3")
    s3.download_file(bucket_name, file_key, local_path)
    print(f"Downloaded {file_key} from S3 to {local_path}")


def make_commodity_data(h5file):
    commodities = {}
    commodities["WC"] = CommodityTicks(
        "WC", "soft_red_wheat", h5file, "/HistTicks", False
    )
    commodities["CN"] = CommodityTicks("CN", "corn", h5file, "/HistTicks", False)
    commodities["SY"] = CommodityTicks("SY", "soybeans", h5file, "/HistTicks", False)
    commodities["SM"] = CommodityTicks(
        "SM", "soybean_meal", h5file, "/HistTicks", False
    )
    commodities["BO"] = CommodityTicks("BO", "beanoil", h5file, "/HistTicks", False)
    return commodities


def main():
    # S3 bucket details
    bucket_name = "your-s3-bucket-name"
    s3_file_keys = {
        "WC": "s3://ewnike-commodity-data/WC.csv",
        "CN": "s3://ewnike-commodity-data/CN.csv",
        "SY": "s3://ewnike-commodity-data/SY.csv",
        "SM": "s3://ewnike-commodity-data/SM.csv",
        "BO": "s3://ewnike-commodity-data/BO.csv",
    }

    # Local directory to store downloaded CSVs
    local_dir = os.path.expanduser("~/Desktop/commodity_data")
    os.makedirs(local_dir, exist_ok=True)

    # Path to the local HDF5 file
    h5_file_path = os.path.join(local_dir, "data.h5")

    # Open or create an HDF5 file
    with tables.open_file(h5_file_path, mode="a") as h5file:
        commodities = make_commodity_data(h5file)

        # Download and process each CSV file from S3
        for sym, s3_file_key in s3_file_keys.items():
            local_csv_path = os.path.join(local_dir, os.path.basename(s3_file_key))
            download_from_s3(bucket_name, s3_file_key, local_csv_path)
            commodities[sym].process_file(local_csv_path)


if __name__ == "__main__":
    main()
