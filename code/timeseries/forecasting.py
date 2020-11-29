import numpy as np


class BaseForecaster:
    def fit(self, series):
        return NotImplemented

    def predict(self, *args, **kwargs):
        return NotImplemented


class NaiveForecaster(BaseForecaster):
    def __init__(self):
        self.y_hat = None

    def fit(self, series):
        self.y_hat = series[-1]
        return self

    def predict(self, n):
        return self.y_hat


class NaiveSeasonalForecaster(BaseForecaster):
    def __init__(self, period=1):
        self.period = period
        self.series = None
        self.T = None

    def fit(self, series):
        self.T = len(series)
        self.series = series
        return self

    def predict(self, n=1):
        k = int((n - 1) / self.period)
        last_seasonal_index = (self.T + n) - self.period * (k + 1)
        y_hat = self.series[last_seasonal_index - 1]
        return y_hat


class DriftForecaster(BaseForecaster):
    def __init__(self, window_size=0):
        self.window_size = window_size
        self.series = None
        self.T = None

    def fit(self, series):
        self.T = len(series)
        self.series = series
        return self

    def predict(self, n):
        window_index = self.T - (self.window_size + 1)
        y_t = self.series[-1]
        y_w = self.series[window_index]
        if self.window_size > 0:
            drift = n * ((y_t - y_w) / self.window_size)
        else:
            drift = n * ((y_t - self.series[0]) / (self.T - 1))
        return y_t + drift


class SimpleAverageForecaster(BaseForecaster):
    def __init__(self, window_size=0):
        self.window_size = window_size
        self.y_bar = None

    def fit(self, series):
        if self.window_size < 1:
            self.y_bar = np.mean(series)
        else:
            self.y_bar = np.mean(series[-self.window_size:])
        return self

    def predict(self, n):
        return self.y_bar
