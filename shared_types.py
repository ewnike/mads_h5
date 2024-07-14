import numpy as np

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

# Define the data type for HDF5 bar data
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
    """
    Collecting ticks and preparing to
    make ticks into commodity bars.
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
