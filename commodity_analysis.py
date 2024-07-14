import matplotlib.pyplot as plt
import numpy as np
import tables
from sklearn import linear_model

from shared_types import h5_bar_type

matplotlib.style.use("ggplot")


class ComRegression:
    """
    Turn commodity regression into a class so that it can be used
    in an application to make working with data more user friendly.
    The intent is to use this class with a Flask application.
    """

    def __init__(
        self,
        comm1=None,
        comm2=None,
        time_in_seconds=None,
        h5file_path="TD_HistoricalBars.h5",
    ):
        self.comm1 = comm1 or input("Enter the first commodity you want to analyze: ")
        self.comm2 = comm2 or input("Enter the second commodity you want to analyze: ")
        self.time_in_seconds = time_in_seconds or int(
            input("Enter the length of time, in seconds, to include in each bar: ")
        )
        self.data = tables.open_file(h5file_path, mode="r")
        self.bars_comm1, self.bars_comm2 = self.read_bars()

    def read_bars(self):
        arr_comm1 = self.data.get_node(f"/TD_HistBars/{self.comm1}").read()
        arr_comm2 = self.data.get_node(f"/TD_HistBars/{self.comm2}").read()
        return arr_comm1, arr_comm2

    def scatter(self):
        comm1_open = self.bars_comm1["open_p"]
        comm2_open = self.bars_comm2["open_p"]
        plt.scatter(comm1_open, comm2_open)
        plt.show()

    def regression(self):
        comm1_open = self.bars_comm1["open_p"]
        comm2_open = self.bars_comm2["open_p"]

        comm1_time = self.bars_comm1["time"]

        regr = linear_model.LinearRegression()
        regr.fit(comm1_time.reshape((-1, 1)), comm1_open.reshape((-1, 1)))

        line_y = regr.predict(comm1_time.reshape((-1, 1)))

        fig, ax1 = plt.subplots()
        ax1.scatter(comm1_time, comm1_open, color="black")
        ax1.plot(comm1_time, line_y, color="blue", linewidth=3)

        comm2_time = self.bars_comm2["time"]
        regr = linear_model.LinearRegression()
        regr.fit(comm2_time.reshape((-1, 1)), comm2_open.reshape((-1, 1)))

        line_y = regr.predict(comm2_time.reshape((-1, 1)))
        print("Coefficients: \n", regr.coef_)

        ax2 = ax1.twinx()
        ax2.scatter(comm2_time, comm2_open, color="green")
        ax2.plot(comm2_time, line_y, color="red", linewidth=3)

        fig.tight_layout()
        plt.show()

    def get_stats(self):
        comm1_sum = 0
        comm2_sum = 0
        cov_sum = 0

        comm1_open = self.bars_comm1["open_p"]
        comm2_open = self.bars_comm2["open_p"]
        comm1_mean = np.mean(comm1_open)
        comm2_mean = np.mean(comm2_open)

        for comm1_open_value, comm2_open_value in zip(comm1_open, comm2_open):
            comm1_diff = comm1_open_value - comm1_mean
            comm2_diff = comm2_open_value - comm2_mean
            cov_sum += comm1_diff * comm2_diff
            comm1_sum += comm1_diff**2
            comm2_sum += comm2_diff**2

        covariance = cov_sum / (len(comm1_open) - 1)
        sigma_comm1 = np.sqrt(comm1_sum / (len(comm1_open) - 1))
        sigma_comm2 = np.sqrt(comm2_sum / (len(comm1_open) - 1))
        correlation = covariance / (sigma_comm1 * sigma_comm2)
        r_coef = np.corrcoef(comm1_open, comm2_open)

        return covariance, sigma_comm1, sigma_comm2, correlation, r_coef


if __name__ == "__main__":
    regression_analysis = ComRegression()
    regression_analysis.scatter()
    regression_analysis.regression()
    stats = regression_analysis.get_stats()
    print(
        f"Covariance: {stats[0]}, Sigma Comm1: {stats[1]}, Sigma Comm2: {stats[2]}, Correlation: {stats[3]}, R Coef: {stats[4]}"
    )
