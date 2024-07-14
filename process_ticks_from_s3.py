"""
Code that processes futures
trade data that is stored in 
aws s3 bucket. Step 1 of
the process. 
"""

import os

import boto3
import numpy as np
import pandas as pd
import tables

from shared_types import tick_type


class H5TickType(tables.IsDescription):
    """
    Description of the HDF5 table structure for tick data.
    """

    symbol = tables.StringCol(2)
    date = tables.Int64Col()
    time = tables.Int64Col()
    last_p = tables.Float64Col()
    last_v = tables.Int64Col()


def download_from_s3(bucket_name, s3_file_key, local_file_path):
    s3 = boto3.client("s3")
    s3.download_file(bucket_name, s3_file_key, local_file_path)


def process_csv_to_hdf5(csv_file, h5file, h5path, sym):
    data = pd.read_csv(csv_file)
    h5table = h5file.create_table(h5path, sym, H5TickType, f"{sym} table")
    for row in data.itertuples(index=False):
        date = np.datetime64(row.date + " " + row.time)
        next_entry = np.array(
            [
                (
                    sym,
                    date.astype("M8[D]"),
                    date - date.astype("M8[D]"),
                    row.last_p,
                    row.last_v,
                )
            ],
            dtype=tick_type,
        )
        h5table.append(next_entry)
    h5file.flush()


def make_h5(h5_filename="tick_data.h5"):
    """
    write data to table.
    """
    return tables.open_file(h5_filename, mode="w", title="Tick Data")


if __name__ == "__main__":
    bucket_name = input("Enter your S3 bucket name: ")
    s3_file_keys = {
        "Wheat": "path/to/wheat.csv",
        "Corn": "path/to/corn.csv",
        "Soy": "path/to/soy.csv",
        "SoyMeal": "path/to/soymeal.csv",
        "BeanOil": "path/to/beanoil.csv",
    }

    h5file = make_h5()

    for sym, s3_file_key in s3_file_keys.items():
        local_file_path = f"{sym}.csv"
        download_from_s3(bucket_name, s3_file_key, local_file_path)
        process_csv_to_hdf5(local_file_path, h5file, "/", sym)
        os.remove(local_file_path)

    h5file.close()
