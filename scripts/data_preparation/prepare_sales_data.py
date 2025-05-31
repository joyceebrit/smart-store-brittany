"""
scripts/data_preparation/prepare_sales_data.py

This script reads data from the data/raw folder, cleans the data, 
and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Remove outliers
- Ensure consistent formatting

"""

#####################################
# Import Modules at the Top
#####################################

# Import from Python Standard Library
import pathlib
import sys

# Import from external packages (requires a virtual environment)
import pandas as pd

# Ensure project root is in sys.path for local imports (now 3 parents are needed)
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent.parent))

# Import local modules (e.g. utils/logger.py)
from utils.logger import logger  

# Optional: Use a data_scrubber module for common data cleaning tasks
from utils.data_scrubber import DataScrubber  


# Constants
SCRIPTS_DATA_PREP_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent  # Directory of the current script
SCRIPTS_DIR: pathlib.Path = SCRIPTS_DATA_PREP_DIR.parent 
PROJECT_ROOT: pathlib.Path = SCRIPTS_DIR.parent 
DATA_DIR: pathlib.Path = PROJECT_ROOT/ "data" 
RAW_DATA_DIR: pathlib.Path = DATA_DIR / "raw"  
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR / "prepared"  # place to store prepared data


# Ensure the directories exist or create them
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)

#####################################
# Define Functions - Reusable blocks of code / instructions
#####################################

# TODO: Complete this by implementing functions based on the logic in the other scripts

def read_raw_data(file_name: str) -> pd.DataFrame:
    """
    Read raw data from CSV.

    Args:
        file_name (str): Name of the CSV file to read.
    
    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    logger.info(f"FUNCTION START: read_raw_data with file_name={file_name}")
    file_path = RAW_DATA_DIR.joinpath(file_name)
    logger.info(f"Reading data from {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Loaded dataframe with {len(df)} rows and {len(df.columns)} columns")
    
    # TODO: OPTIONAL Add data profiling here to understand the dataset
    # Suggestion: Log the datatypes of each column and the number of unique values
    # Example:
    # logger.info(f"Column datatypes: \n{df.dtypes}")
    # logger.info(f"Number of unique values: \n{df.nunique()}")
    
    return df

def remove_duplicates(df: pd.DataFrame, scrubber: DataScrubber) -> pd.DataFrame:
    """
    Remove duplicate rows from the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.
    
    Returns:
        pd.DataFrame: DataFrame with duplicates removed.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    initial_count = len(df)
    
    # Consider which columns should be used to identify duplicates
    df = scrubber.remove_duplicates(subset=["transaction_id"])
    
    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} duplicate rows")
    logger.info(f"{len(df)} records remaining after removing duplicates.")
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values by filling or dropping.
    This logic is specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.
    
    Returns:
        pd.DataFrame: DataFrame with missing values handled.
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")
    
    # Log missing values by column before handling
    # NA means missing or "not a number" - ask your AI for details
    missing_by_col = df.isna().sum()
    logger.info(f"Missing values by column before handling:\n{missing_by_col}")
    
    # missing value handling specific to our data.
    df.fillna({'BonusPoints':0}, inplace=True)
    
    # Log missing values by column after handling
    missing_after = df.isna().sum()
    logger.info(f"Missing values by column after handling:\n{missing_after}")
    logger.info(f"{len(df)} records remaining after handling missing values.")
    return df

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """
    Save cleaned data to CSV.

    Args:
        df (pd.DataFrame): Cleaned DataFrame.
        file_name (str): Name of the output file.
    """
    logger.info(f"FUNCTION START: save_prepared_data with file_name={file_name}, dataframe shape={df.shape}")
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove outliers based on thresholds.
    This logic is very specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.
    
    Returns:
        pd.DataFrame: DataFrame with outliers removed.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial_count = len(df)
    
    # campaign_id should not be below 0. 
    # Count how many are invalid (NaN or <= 0)
    invalid_count = df[(df['campaign_id'].isna()) | (df['campaign_id'] <= 0)].shape[0]
    logger.info(f"Found {invalid_count} rows with invalid campaign_id")

    # Remove those rows
    df = df[df['campaign_id'] > 0]
    
    # OPTIONAL ADVANCED: Use IQR method to identify outliers in numeric columns
    # Example:
    # for col in ['price', 'weight', 'length', 'width', 'height']:
    #     if col in df.columns and df[col].dtype in ['int64', 'float64']:
    #         Q1 = df[col].quantile(0.25)
    #         Q3 = df[col].quantile(0.75)
    #         IQR = Q3 - Q1
    #         lower_bound = Q1 - 1.5 * IQR
    #         upper_bound = Q3 + 1.5 * IQR
    #         df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
    #         logger.info(f"Applied outlier removal to {col}: bounds [{lower_bound}, {upper_bound}]")
    
    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} outlier rows")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df


#####################################
# Define Main Function - The main entry point of the script
#####################################

def main() -> None:
    """
    Main function for processing data.
    """
    logger.info("==================================")
    logger.info("STARTING prepare_sales_data.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "sales_data.csv"
    output_file = "sales_data_prepared.csv"
    
    df = read_raw_data(input_file)
    original_shape = df.shape
    original_columns = df.columns.tolist()

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(original_columns)}")
    logger.info(f"Initial dataframe shape: {original_shape}")
    
    # --- Clean using DataScrubber ---
    scrubber = DataScrubber(df)
    scrubber.normalize_column_names()
    scrubber.format_string_columns()
    df = scrubber.get_dataframe()

    # Log column name changes
    cleaned_columns = df.columns.tolist()
    changed_columns = [f"{old} -> {new}" for old, new in zip(original_columns, cleaned_columns) if old != new]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Remove duplicates
    df = remove_duplicates(df, scrubber)

    # Handle missing values
    df = handle_missing_values(df)

    # Remove outliers
    df = remove_outliers(df)

    df = scrubber.rename_columns(
        {
            "transaction_id" : "sale_id"
        }
    )

    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info(f"Original shape: {df.shape}")
    logger.info(f"Cleaned shape:  {original_shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_sales_data.py")
    logger.info("==================================")

#####################################
# Conditional Execution Block 
# Ensures the script runs only when executed directly
# This is a common Python convention.
#####################################

if __name__ == "__main__":
    main()