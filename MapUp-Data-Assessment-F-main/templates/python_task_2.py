iimport datetime
import pandas as pd


def calculate_distance_matrix(df)->pd.DataFrame():
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """
    # Write your logic here
    unique_ids = sorted(set(df['id_start'].unique()) | set(df['id_end'].unique()))
    distance_matrix = pd.DataFrame(index=unique_ids, columns=unique_ids)

    # Fill the distance matrix with cumulative distances along known routes
    for _, row in df.iterrows():
        start_id, end_id, distance = row['id_start'], row['id_end'], row['distance']
        # Set the distances in both directions (symmetric)
        distance_matrix.at[start_id, end_id] = distance_matrix.at[end_id, start_id] = distance

    # Fill diagonal values with 0
    distance_matrix = distance_matrix.fillna(0)

    # Update the matrix with cumulative distances
    for col in distance_matrix.columns:
        for idx in distance_matrix.index:
            if distance_matrix.at[idx, col] == 0 and idx != col:
                # Find intermediate points for cumulative distances
                intermediates = distance_matrix.index[distance_matrix[idx] != 0]
                distances_to_intermediates = distance_matrix.at[idx, intermediates]
                distances_from_intermediates = distance_matrix.loc[intermediates, col]
                cumulative_distance = distances_to_intermediates + distances_from_intermediates
                # Set the cumulative distance
                distance_matrix.at[idx, col] = distance_matrix.at[col, idx] = cumulative_distance.max()

    return distance_matrix


def unroll_distance_matrix(df)->pd.DataFrame():
    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    # Write your logic here
    unrolled_df = pd.DataFrame(columns=['id_start', 'id_end', 'distance'])

    # Iterate over rows of the distance matrix
    for idx_start in df.index:
        for idx_end in df.columns:
            if idx_start != idx_end:
                # Append a new row to the unrolled DataFrame
                unrolled_df = unrolled_df.append({
                    'id_start': idx_start,
                    'id_end': idx_end,
                    'distance': df.at[idx_start, idx_end]
                }, ignore_index=True)

    return unrolled_df



def find_ids_within_ten_percentage_threshold(df, reference_id)->pd.DataFrame():
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """
    # Write your logic here
    reference_rows = df[df['id_start'] == reference_id]

    # Calculate the average distance for the reference value
    average_distance = reference_rows['distance'].mean()

    # Calculate the threshold range (within 10% of the average distance)
    lower_threshold = average_distance - (0.1 * average_distance)
    upper_threshold = average_distance + (0.1 * average_distance)

    # Filter rows where distances are within the threshold range
    within_threshold = df[(df['distance'] >= lower_threshold) & (df['distance'] <= upper_threshold)]

    # Get unique values from the 'id_start' column and sort them
    result_ids = sorted(within_threshold['id_start'].unique())

    return result_ids


def calculate_toll_rate(df)->pd.DataFrame():
    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Wrie your logic here
    rate_coefficients = {'moto': 0.8, 'car': 1.2, 'rv': 1.5, 'bus': 2.2, 'truck': 3.6}

    # Add new columns for toll rates based on vehicle types
    for vehicle_type, rate_coefficient in rate_coefficients.items():
        df[vehicle_type] = df['distance'] * rate_coefficient

    return df


def calculate_time_based_toll_rates(df)->pd.DataFrame():
    """
    Calculate time-based toll rates for different time intervals within a day.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    # Write your logic here
    time_ranges = [
        ((0, 0, 0), (10, 0, 0), 0.8),  # From 00:00:00 to 10:00:00 (Weekdays)
        ((10, 0, 0), (18, 0, 0), 1.2),  # From 10:00:00 to 18:00:00 (Weekdays)
        ((18, 0, 0), (23, 59, 59), 0.8)  # From 18:00:00 to 23:59:59 (Weekdays)
    ]

    # Apply a constant discount factor of 0.7 for all times during weekends
    weekend_discount_factor = 0.7

    # Add new columns for start_day, start_time, end_day, end_time, and time_based_toll
    df['start_day'] = df['startDay'].apply(lambda x: datetime.datetime.strptime(x, '%A').strftime('%A'))
    df['end_day'] = df['endDay'].apply(lambda x: datetime.datetime.strptime(x, '%A').strftime('%A'))

    # Convert time columns to datetime.time type
    df['start_time'] = pd.to_datetime(df['startTime']).dt.time
    df['end_time'] = pd.to_datetime(df['endTime']).dt.time

    # Initialize the time_based_toll column
    df['time_based_toll'] = 0.0

    # Iterate over time ranges and apply discount factors
    for start_range, end_range, discount_factor in time_ranges:
        weekday_mask = (df['start_time'] >= datetime.time(*start_range)) & (df['end_time'] <= datetime.time(*end_range)) & (df['start_day'].isin(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']))
        weekend_mask = df['start_day'].isin(['Saturday', 'Sunday'])

        df.loc[weekday_mask, 'time_based_toll'] += discount_factor * df.loc[weekday_mask, 'distance']
        df.loc[weekend_mask, 'time_based_toll'] += weekend_discount_factor * df.loc[weekend_mask, 'distance']

    return df
