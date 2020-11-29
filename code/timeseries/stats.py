import numpy as np
from scipy.stats import chi2
from statsmodels.stats.diagnostic import acorr_ljungbox


# Helper Functions
def auto_covariance(series, h):
    n = len(series)
    x_bar = np.mean(series)
    series_centered = series - x_bar
    return np.dot(series_centered[:(n - h)], series_centered[h:]) / (n - h)


# INDEPENDENCE TESTING
def acorr_chi2_test(series, lags, alpha=0.05):
    n = len(series)
    quantile = chi2.ppf(q=1-alpha, df=lags)
    sum_of_autocov = 0

    for lag in range(1, lags + 1):
        sum_of_autocov += auto_covariance(series, lag)

    test_statistic = n * sum_of_autocov / (auto_covariance(series, 0) ** 2)

    if test_statistic > quantile:
        reject_h0 = True
    else:
        reject_h0 = False

    return quantile, test_statistic, reject_h0


def acorr_ljungbox_test(series, **kwargs):
    results = acorr_ljungbox(series, **kwargs)
    return results


# STATIONARITY TESTING

