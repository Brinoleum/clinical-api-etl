# TODO: Implement actual ETL processing
# This is where the candidate would implement:
# 1. File extraction
# 2. Data transformation 
# 3. Quality validation
# 4. Database loading

"""
according to the above outline I'm going to break down the pipeline into 
modular functions that can be called in sequence to perform the ETL process.

if the functions fail we can catch the exceptions and update the job status accordingly in the calling code
"""

import pandas as pd
import asyncio
import psycopg2 as pg
from typing import Dict, Any

async def extract_data(filename: str) -> pd.DataFrame:
    """
    File extraction from provided CSV files

    Input: filename

    Output: DataFrame containing raw CSV data

    Possibly throws:
    - FileNotFoundError: If the file does not exist
    - pd.errors.EmptyDataError: If the file is empty
    - pd.errors.ParserError: If the file is malformed
    """
    DATA_DIR = "/app/data/" # derived from docker config
    filename = DATA_DIR + filename

    await asyncio.sleep(1)  # Simulate I/O delay
    data = pd.read_csv(filename)
    return data

async def transform_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Data transformation to clean and standardize the data:
    Missing values handling, normalization, type conversion

    - Any rows with missing values should be skipped:
        - depending on company policy there could be processes for recovering missing fields but for demo purposes just drop rows
    - Repeated participant_id/measurement combos should be handled by keeping the highest quality_score entry
    - Convert data types from generic object (e.g. timestamps to datetime objects, numeric fields to appropriate types, etc)

    Columns: (type inferred from sample data)
    study_id: str
    participant_id: str
    measurement_type: str
    value: float
    unit: str
    timestamp: str (parsable datetime)
    site_id: str
    quality_score: float

    Input: data (DataFrame)

    Output: Transformed DataFrame

    Possibly throws:
    - ValueError/AssertionError: If data types cannot be converted as expected
    - KeyError: If expected columns are missing
    """

    await asyncio.sleep(1)  # Simulate processing delay
    data = data.dropna()  # Drop rows with any missing values

    # convert timestamps
    data['timestamp'] = pd.to_datetime(data['timestamp'], errors='coerce')
    data = data.dropna(subset=['timestamp'])  # Drop rows where timestamp conversion failed

    # convert data types
    data = data.convert_dtypes()
    assert data['study_id'].dtype           == 'string[python]'
    assert data['participant_id'].dtype     == 'string[python]'
    assert data['measurement_type'].dtype   == 'string[python]'
    assert data['value'].dtype              == 'Float64'
    assert data['unit'].dtype               == 'string[python]'
    assert data['timestamp'].dtype          == 'datetime64[ns, UTC]'
    assert data['site_id'].dtype            == 'string[python]'
    assert data['quality_score'].dtype      == 'Float64'
    
    # drop duplicate participant_id/measurement_type keeping highest quality_score
    data = data.sort_values(by='quality_score', ascending=False).drop_duplicates(subset=['participant_id', 'measurement_type'], keep='first')
    return data

async def validate_data(data: pd.DataFrame) -> bool:
    """
    Quality validation to ensure data integrity:
    - Assert that for a given measurement type that the unit is consistent
    (e.g. all 'blood_pressure' measurements use 'mmHg')

    - Numeric values are within 'realistic' ranges
    (I'm not a medical professional so for demo purposes just make sure values are non-negative)

    - Timestamps are within realistic ranges (e.g., not in the future)
    - Quality_score is between 0 and 1

    Input: DataFrame with cleaned data

    Output: Boolean indicating if data passed validation
    """
    await asyncio.sleep(1)  # Simulate processing delay
    try:
        assert all(data['value'] >= 0)
        assert all(data['quality_score'].between(0, 1))
        assert all(data['timestamp'] <= pd.Timestamp.now(tz='UTC'))
        for _, group in data.groupby('measurement_type'):
            assert len(group['unit'].unique()) == 1  # All units for a measurement type should be the same
        return True
    except AssertionError:
        return False

async def load_data(data: pd.DataFrame) -> None:
    """
    Database loading to insert the processed data into the Postgres database
    """
    await asyncio.sleep(1)  # Simulate I/O delay
    pass

async def etl_pipeline(filename: str, jobs: Dict[str, Dict[str, Any]], job_id: str) -> None:
    """
    Orchestrates the ETL process for a given file and updates job status accordingly
    """
    try:
        jobs[job_id]['message'] = "Extracting data"
        data = await extract_data(filename)
        jobs[job_id]['progress'] = 25

        jobs[job_id]['message'] = "Transforming data"
        data = await transform_data(data)
        jobs[job_id]['progress'] = 50

        jobs[job_id]['message'] = "Validating data"
        is_valid = await validate_data(data)
        if not is_valid:
            jobs[job_id]['status'] = 'failed'
            jobs[job_id]['message'] = 'Data validation failed'
            jobs[job_id]['progress'] = 100
            return
        jobs[job_id]['progress'] = 75

        jobs[job_id]['message'] = "Loading data into database"
        await load_data(data)
        jobs[job_id]['progress'] = 100

        jobs[job_id]['status'] = 'completed'
        jobs[job_id]['message'] = 'ETL process completed successfully'
    except Exception as e:
        jobs[job_id]['status'] = 'failed'
        jobs[job_id]['message'] = f'ETL process failed: {str(e)}'
        jobs[job_id]['progress'] = 100
    