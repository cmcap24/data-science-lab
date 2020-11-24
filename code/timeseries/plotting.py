from matplotlib import pyplot as plt
import seaborn as sns

import pandas as pd


class TimeSeriesPlotter:

    def __init__(self, series):
        self.series = series

    def time_plot(self, title="", x_label="", y_label="", style="line"):
        if style == "line":
            sns.lineplot(data=self.series)
        if style == "dotted":
            sns.scatterplot(data=self.series)
        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.show()

    def seasonal_plot(self, freq="A", title="", x_label="", y_label=""):
        names = []
        groups = self.series.groupby(pd.Grouper(freq=freq))

        for name, group in groups:
            names.append(name.year)
            sns.lineplot(data=group.values)

        plt.legend(labels=names, loc=(1.05, 0))
        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.show()

    def lag_plot(self, lag=1):
        pd.plotting.lag_plot(self.series, lag)
        plt.show()

    def acf_plot(self):
        pd.plotting.autocorrelation_plot(self.series)
        plt.show()


class MultivariateTimeSeriesPlotter:

    def __init__(self, dataframe):
        self.dataframe = dataframe

    def pair_plot(self, title="", x_label="", y_label=""):
        sns.pairplot(self.dataframe, corner=True)
        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.show()
