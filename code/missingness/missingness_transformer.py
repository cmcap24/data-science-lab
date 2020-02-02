import pandas as pd

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import SimpleImputer, IterativeImputer
from sklearn.linear_model import BayesianRidge
from sklearn.neighbors import KNeighborsRegressor
from sklearn.pipeline import Pipeline

from missingness.utils import drop_dataframe_values_by_threshold


class MissingnessTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, config=None, return_missing_indicators=False):
        if config:
            self.config = config
        else:
            self.config = {}
        self.random_state = 0
        self.return_missing_indicators = return_missing_indicators
        self.estimator_dict = {
            'linear': BayesianRidge(),
            'tree': ExtraTreesRegressor(n_estimators=50, random_state=self.random_state),
            'knn': KNeighborsRegressor(n_neighbors=15)
        }
        self.missing_data_transformer = None

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        if 'delete_threshold_list' in self.config:
            if 'delete_column_threshold' in self.config:
                X = drop_dataframe_values_by_threshold(
                    X,
                    axis=1,
                    threshold=self.config['delete_column_threshold']
                )

            if 'delete_row_threshold' in self.config:
                X = drop_dataframe_values_by_threshold(
                    X,
                    axis=0,
                    threshold=self.config['delete_row_threshold']
                )

        impute_keys_list = [key for key in self.config if key.startswith('impute_')]

        if len(impute_keys_list) > 0:
            column_names = []
            transformer_list = []
            for impute_key in impute_keys_list:
                impute_type = impute_key.split('_')[1]
                impute_strategy = impute_key.split('_')[2]
                fill_value = None

                if impute_type == 'univariate':
                    # following strategies are admissable [“mean”, “median”, “most_frequent”, or “constant”]
                    if impute_strategy == 'constant':
                        try:
                            fill_value = float(impute_key.split('_')[3])
                        except:
                            fill_value = impute_key.split('_')[3]
                    imputer = SimpleImputer(strategy=impute_strategy, fill_value=fill_value)

                if impute_type == 'multivariate':
                    # following estimators are admissable ['linear', 'tree', '']
                    impute_estimator = self.estimator_dict[impute_strategy]
                    imputer = IterativeImputer(estimator=impute_estimator, random_state=self.random_state)

                transformer = Pipeline(steps=[('imputer', imputer)])
                transformer_list.append(
                    (impute_key, transformer, self.config[impute_key])
                )
                column_names += self.config[impute_key]
            # combine all imputers
            missing_data_transformer = ColumnTransformer(transformers=transformer_list)
            missing_data_transformer.fit(X)
            self.missing_data_transformer = missing_data_transformer
            X_transformed = pd.concat(
                [
                    pd.DataFrame(missing_data_transformer.transform(X), columns=column_names),
                    X[[column for column in X.columns if column not in column_names]]
                ],
                axis=1
            )
            return X_transformed

        return X