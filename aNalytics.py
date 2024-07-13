#Code for getting nodes from the H5 files
from DTN_Utility import *
import tables
import numpy as np
import seaborn
import matplotlib
import matplotlib.pyplot as plt
matplotlib.style.use('ggplot')

from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score

comm1 = "Wheat"
comm2 = "Corn"
time_in_seconds = 5
class com_Regression:
    """
    Turn commodity regression into a class so that it can be used
    in an application to make working with data more user friendly.
    The intent is to use this class with a Flask application.
    """

 
        
    def __init__(self,comm1=None,comm2=None, time_in_seconds=None):
        # anything that we want to be on the "class bulletin board", we should
        # make inside this function.
        if(comm1 is not None):
            self.comm1 = comm1
        else:
            self.comm1 = input("Enter the first commodity you want to analyze: ")
            
        if(comm2 is not None):
            self.comm2 = comm2
        else:
            self.comm2 = input("Enter the second commodity you want to analyze: ")
        
        self.data = tables.open_file("HistoricalTicks.h5")
        if(time_in_seconds is not None):
            self.time_in_seconds = time_in_seconds
        else:
            self.time_in_seconds = input("Enter the length of time, in seconds, to include in each bar: ")
            
        self.bars_comm1, self.bars_comm2 = self.make_bars()
        
    def make_bars(self):            
        arr_comm1 = self.data.get_node(("/HistTicks/{}").format(self.comm1)).read()
        arr_comm2 = self.data.get_node(("/HistTicks/{}").format(self.comm2)).read()
        bars_comm1 = ticks_to_bars(arr_comm1,self.time_in_seconds)
        bars_comm2 = ticks_to_bars(arr_comm2,self.time_in_seconds)
        #I thought that since I returned needed variables that I can call them in def regression??
        return bars_comm1, bars_comm2

    def scatter(self):  

        #Trying to make def regression work by passing in make_bars 
        # select different data for different graphs/analysis 
        comm1_open = self.bars_comm1['open_p']
        comm2_open = self.bars_comm2['open_p']
        comm1_mean = np.mean(comm1_open)
        comm2_mean = np.mean(comm2_open)
        #A = self.make_bars()
        # com_Regression.make_bars(self) 

        plt.scatter(comm1_open, comm2_open)
        plt.show()
        
    def regression(self):
        #Trying to make def regression work by passing in make_bars 
        # select different data for different graphs/analysis 
        comm1_open = self.bars_comm1['open_p']
        comm2_open = self.bars_comm2['open_p']
        #A = self.make_bars()
        # com_Regression.make_bars(self) 
        # regression for commodity 1
        # based on: http://scikit-learn.org/stable/auto_examples/linear_model/plot_ols.html
        comm1_time = self.bars_comm1.astype(h5_bar_type)['time']
        
        regr = linear_model.LinearRegression()
        regr.fit(comm1_time.reshape((-1,1)), comm1_open.reshape((-1,1))) # makes the regression
        
        line_y = regr.predict(comm1_time.reshape((-1,1))) # makes the datapoints along the best fit line
        #print('Coefficients: \n', regr.coef_)
        #print("Mean squared error: %.2f" % mean_squared_error(comm1_open.reshape((-1,1)), line_y.reshape((-1,1))))
        #print('Variance score: %.2f' % r2_score(comm1_open.reshape((-1,1)), line_y.reshape((-1,1))))
        #print(comm1_time.shape, comm1_time.dtype)
        #print(comm1_open.shape, comm1_open.dtype)
        fig, ax1 = plt.subplots() # we want different things, not just one simple plot.
        # ax1 is the first graph. 
        ax1.scatter(comm1_time, comm1_open,  color='black')
        ax1.plot(comm1_time, line_y, color='blue', linewidth=3)
       
        
        # regression for commodity 2
        comm2_time = self.bars_comm1.astype(h5_bar_type)['time']
        regr = linear_model.LinearRegression()
        regr.fit(comm2_time.reshape((-1,1)), comm2_open.reshape((-1,1))) # makes the regression
        
        line_y = regr.predict(comm2_time.reshape((-1,1))) # makes the datapoints along the best fit line
        print('Coefficients: \n', regr.coef_)
        #print("Mean squared error: %.2f" % mean_squared_error(comm2_open.reshape((-1,1)), line_y.reshape((-1,1))))
        #print('Variance score: %.2f' % r2_score(comm2_open.reshape((-1,1)), line_y.reshape((-1,1))))
        ax2 = ax1.twinx()
        ax2.scatter(comm2_time, comm2_open,  color='green')
        ax2.plot(comm2_time, line_y, color='red', linewidth=3)
        
        fig.tight_layout()  
        
        #plt.xticks(())
        #plt.yticks(())

        plt.show()
        
    def get_stats(self):
        comm1_sum = 0
        comm2_sum = 0
        cov_sum = 0
        
        comm1_open = self.bars_comm1['open_p']
        comm2_open = self.bars_comm2['open_p']
        comm1_mean = np.mean(comm1_open)
        comm2_mean = np.mean(comm2_open)
        #print(A.comm1_open)
        #getting error for Line 39:NameError: name 'comm1_open' is not defined
        for i in range(len(comm1_open)):
            comm1_diff = comm1_open[i] - comm1_mean
            comm2_diff = comm2_open[i] - comm2_mean
            cov_sum += comm1_diff*comm2_diff
            comm1_sum += comm1_diff**2
            comm2_sum += comm2_diff**2
   
        cov_entries = (comm1_open - comm1_mean)*(comm2_open - comm2_mean)
        covariance = cov_sum / (len(comm1_open)-1) # np.sum(cov_entries) / (len(soy_open)-1)
        sigma_comm1 =  np.sqrt(comm1_sum/(len(comm1_open)-1)) # or you could just do np.std(soy_open) 
        sigma_comm2 = np.sqrt(comm2_sum/(len(comm1_open)-1))
        correlation = covariance / (sigma_comm1 * sigma_comm2)
        r_coef= np.corrcoef(comm1_open, comm2_open)
        return covariance, sigma_comm1, sigma_comm2, correlation, r_coef
    
if __name__ == "__main__":
    #comm1 = "Wheat"
    #comm2 = "Corn"
    #time_in_seconds = 5
    
    #a = com_Regression("Wheat", "Corn", 5)
    #print(a.get_stats())
    #a.scatter()
    #a.regression()
    a = com_Regression()  # no info provided, so it will ask the user. 
    #print(a.make_bars)
    print(a.get_stats())
    a.scatter()
    a.regression()