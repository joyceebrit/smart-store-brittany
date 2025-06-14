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

# Paths
SCRIPTS_DATA_PREP_DIR = pathlib.Path(__file__).resolve().parent
SCRIPTS_DIR = SCRIPTS_DATA_PREP_DIR.parent 
PROJECT_ROOT = SCRIPTS_DIR.parent 
DATA_DIR = PROJECT_ROOT / "data" 
RAW_DATA_DIR = DATA_DIR / "raw"  
PREPARED_DATA_DIR = DATA_DIR / "prepared"

DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)


class PrepareSalesData:
    def __init__(self, df: pd.DataFrame):
        self.scrubber = DataScrubber(df)

    def remove_duplicates(self):
        logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={self.scrubber.df.shape}")
        initial_count = len(self.scrubber.df)
        self.scrubber.remove_duplicates(subset=["TransactionID"])
        removed_count = initial_count - len(self.scrubber.df)
        logger.info(f"Removed {removed_count} duplicate rows")
        logger.info(f"{len(self.scrubber.df)} records remaining after removing duplicates.")

    def handle_missing_values(self):
        df = self.scrubber.df.copy()

        logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")

        missing_by_col = df.isna().sum()
        logger.info(f"Missing values by column before handling:\n{missing_by_col}")

        df = self.scrubber.handle_missing_data(fill_value={'BonusPoints': 0})

        missing_after = df.isna().sum()
        logger.info(f"Missing values by column after handling:\n{missing_after}")
        logger.info(f"{len(df)} records remaining after handling missing values.")

        self.scrubber.df = df  # update scrubber's df after filling

    def remove_outliers(self):
        """
        Remove outliers and invalid data rows.
        Delegates column-specific cleanup to private methods.
        """
        logger.info(f"FUNCTION START: remove_outliers with dataframe shape={self.scrubber.df.shape}")
        initial_count = len(self.scrubber.df)

        self._remove_invalid_campaign_ids()

        removed = initial_count - len(self.scrubber.df)
        logger.info(f"Removed {removed} total outlier/invalid rows")
        logger.info(f"{len(self.scrubber.df)} records remaining after removing outliers.")

    def clean_columns(self):
        logger.info(f"FUNCTION START: clean columns")
        self._clean_sale_amount()
        self._clean_sale_date()

    def finalize_cleaning(self):
        self.scrubber.rename_columns({"TransactionID": "sale_id"})
        self.scrubber.normalize_column_names()
        self.scrubber.format_string_columns()

    def get_dataframe(self):
        return self.scrubber.get_dataframe()
    
    def _remove_invalid_campaign_ids(self):
        df = self.scrubber.df

        invalid = df[(df["CampaignID"].isna()) | (df["CampaignID"] <= 0)].shape[0]
        logger.info(f"Found {invalid} rows with invalid CampaignID")

        df = df[df["CampaignID"].notna() & (df["CampaignID"] > 0)].copy()
        df["CampaignID"] = df["CampaignID"].astype("Int64")

        self.scrubber.df = df

    def _clean_sale_date(self):
        logger.info("Cleaning 'SaleDate' column values")
        invalid_dates_count = self.scrubber.clean_date('SaleDate', '%m/%d/%Y')
        logger.info(f"Found {invalid_dates_count} invalid SaleDate entries")
    
    def _clean_sale_amount(self):
        df = self.scrubber.df.copy()

        logger.info("Cleaning 'SaleAmount' column of non-monetary values")

        # Strip out unwanted characters (like $ or letters), keep digits and dot
        df["SaleAmount"] = (
            df["SaleAmount"]
                .astype(str)
                .str.replace(r"[^\d.]", "", regex=True)
                .replace("", "0")  # replace empty strings after cleaning with "0"
                .astype(float)
        )
        self.scrubber.df = df


def read_raw_data(file_name: str) -> pd.DataFrame:
    logger.info(f"FUNCTION START: read_raw_data with file_name={file_name}")
    file_path = RAW_DATA_DIR.joinpath(file_name)
    logger.info(f"Reading data from {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Loaded dataframe with {len(df)} rows and {len(df.columns)} columns")
    return df

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    logger.info(f"FUNCTION START: save_prepared_data with file_name={file_name}, dataframe shape={df.shape}")
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def main() -> None:
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

    logger.info(f"Initial dataframe columns: {', '.join(original_columns)}")
    logger.info(f"Initial dataframe shape: {original_shape}")

    preparer = PrepareSalesData(df)

    preparer.remove_duplicates()
    preparer.handle_missing_values()
    preparer.clean_columns()
    preparer.remove_outliers()
    preparer.finalize_cleaning()

    df_cleaned = preparer.get_dataframe()

    cleaned_columns = df_cleaned.columns.tolist()
    changed_columns = [f"{old} -> {new}" for old, new in zip(original_columns, cleaned_columns) if old != new]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    save_prepared_data(df_cleaned, output_file)

    logger.info("==================================")
    logger.info(f"Original shape: {original_shape}")
    logger.info(f"Cleaned shape:  {df_cleaned.shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_sales_data.py")
    logger.info("==================================")


if __name__ == "__main__":
    main()
