import logging
logging.basicConfig(level=logging.INFO)

import pandas as pd

from data_validation.base import Validator


class PandasDataFrameValidator(Validator):

    def __init__(self):
        self.admissable_data_types = {
            'id': None,
            'boolean': pd.api.types.is_bool_dtype,
            'datetime': pd.api.types.is_datetime64_any_dtype,
            'categorical': pd.api.types.is_categorical_dtype,
            'ordinal': pd.api.types.is_integer_dtype,
            'interval': pd.api.types.is_float_dtype,
            'text': pd.api.types.is_string_dtype
        }
        self.validation_result = {}

    def validate(self, schema, data):
        # ensure appropriate schema structure
        self._check_schema_structure(schema)
        # get specified columns and check for any missing
        required_columns = list(schema.keys())
        self._check_missing_columns(required_columns, data)
        self._check_unspecified_columns(required_columns, data)
        self._check_data_types(schema, data)
        return self.validation_result

    def coerce_data_types(self, schema, data):
        type_mapping_dict = {
            'boolean': 'bool',
            'categorical': 'category',
            'ordinal': 'Int64',
            'interval': 'Float64',
            'text': 'string'
        }
        column_name_intersection = list(set(schema.keys()) & set(data.columns))
        schema_mapping = {
            column: type_mapping_dict[schema[column]['type']]
            for column in column_name_intersection
            if schema[column]['type'] not in ['id', 'datetime']
        }

        df_corrected_types = data.astype(schema_mapping)

        for column in column_name_intersection:
            if schema[column]['type'] == 'datetime':
                df_corrected_types[column] = pd.to_datetime(df_corrected_types[column], utc=True)
        return df_corrected_types

    def _check_data_types(self, schema, data):
        actual_dtypes = {
            column_name: dtype.type for column_name, dtype in data.dtypes.to_dict().items()
        }
        column_name_intersection = list(set(schema.keys()) & set(data.columns))
        incorrect_dtype_dict = {}
        for column in column_name_intersection:
            if self.admissable_data_types[schema[column]['type']]:
                if not self.admissable_data_types[schema[column]['type']](data[column]):
                    incorrect_dtype_dict[column] = {
                        'specified_column_type': schema[column]['type'],
                        'actual_column_type': actual_dtypes[column]
                    }
        if incorrect_dtype_dict:
            logging.info(f'incorrect column types: {list(incorrect_dtype_dict.keys())}')
            self.validation_result['incorrect_dtype_dict'] = incorrect_dtype_dict

    def _check_missing_columns(self, required_columns, dataframe):
        missing_columns_list = list(set(required_columns) - set(dataframe.columns))
        if missing_columns_list:
            logging.info(f'missing columns: {missing_columns_list}')
            self.validation_result['missing_column_list'] = missing_columns_list

    def _check_unspecified_columns(self, required_columns, dataframe):
        unspecified_columns_list = list(set(dataframe.columns) - set(required_columns))
        if unspecified_columns_list:
            logging.info(f'unspecified columns: {unspecified_columns_list}')
            self.validation_result['unspecified_column_list'] = unspecified_columns_list

    def _check_schema_structure(self, schema):
        for column_name, specification in schema.items():
            # check types are specified and admissable
            if 'type' not in specification.keys():
                raise KeyError('schema must contain type for each column name')
            elif specification['type'] not in self.admissable_data_types.keys():
                _type = specification['type']
                raise ValueError(
                    f'{_type} not in list of admissable fields: {list(self.admissable_data_types.keys())}'
                )
            else:
                pass
    # TODO: add in constraint validation for each admissable data type -  ex. nullable or not, range of values, ect.
