

def drop_dataframe_values_by_threshold(dataframe, axis=1, threshold=0.75):
    # select axis to calculate percent of missing values
    mask_axis = 1 if axis == 0 else 0
    # boolean mask to filter dataframe based on threshold
    mask = (
            (dataframe.isnull().sum(axis=mask_axis) / dataframe.shape[mask_axis]) < threshold
    ).values

    if axis == 1:
        # get columns to drop for logging
        columns_to_drop = dataframe.columns[~mask].values.tolist()
        if len(columns_to_drop) > 0:
            print(f'dropping columns: {columns_to_drop}')
        # filter dataframe and reset index
        dataframe = dataframe.loc[:, mask].reset_index(drop=True)
    else:
        num_rows_original = dataframe.shape[0]
        # filter dataframe and reset index
        dataframe = dataframe.loc[mask, :].reset_index(drop=True)
        # calculate number of rows that are being dropped for logging
        num_rows_to_drop = num_rows_original - dataframe.shape[0]
        pct_dropped = round((num_rows_to_drop / num_rows_original) * 100, 1)
        print(f'dropping rows: {num_rows_to_drop} ({pct_dropped}%)')

    return dataframe
