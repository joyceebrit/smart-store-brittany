import pandas as pd
import sqlite3
import pathlib
import sys

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from utils.logger import logger  # noqa: E402

# Constants
DW_DIR: pathlib.Path = pathlib.Path("data").joinpath("dw")
DB_PATH: pathlib.Path = DW_DIR.joinpath("smart_sales.db")
OLAP_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("olap_cubing_outputs")

OLAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def ingest_sales_data_from_dw() -> pd.DataFrame:
    """Ingest sales data from SQLite data warehouse."""
    try:
        conn = sqlite3.connect(DB_PATH)
        sales_df = pd.read_sql_query("SELECT * FROM sales", conn)
        conn.close()
        logger.info("Sales data successfully loaded from SQLite data warehouse.")
        return sales_df
    except Exception as e:
        logger.error(f"Error loading sales data from data warehouse: {e}")
        raise


def ingest_customers_from_dw() -> pd.DataFrame:
    """Ingest customer data (customer_id and name) from SQLite data warehouse."""
    try:
        conn = sqlite3.connect(DB_PATH)
        customers_df = pd.read_sql_query("SELECT customer_id, name FROM customers", conn)
        conn.close()
        logger.info("Customers data successfully loaded from SQLite data warehouse.")
        return customers_df
    except Exception as e:
        logger.error(f"Error loading customers data from data warehouse: {e}")
        raise


def generate_column_names(dimensions: list, metrics: dict) -> list:
    """Generate explicit column names for OLAP cube, ensuring no trailing underscores."""
    column_names = dimensions.copy()
    for column, agg_funcs in metrics.items():
        if isinstance(agg_funcs, list):
            for func in agg_funcs:
                column_names.append(f"{column}_{func}")
        else:
            column_names.append(f"{column}_{agg_funcs}")
    column_names = [col.rstrip("_") for col in column_names]
    return column_names


def create_olap_cube(
    sales_df: pd.DataFrame, dimensions: list, metrics: dict
) -> pd.DataFrame:
    """
    Create an OLAP cube by aggregating data across multiple dimensions.
    """
    try:
        # Group by the specified dimensions
        grouped = sales_df.groupby(dimensions)

        # Aggregate metrics
        cube = grouped.agg(metrics).reset_index()

        # Add list of sale IDs for traceability
        cube["sale_ids"] = grouped["sale_id"].apply(list).reset_index(drop=True)

        # Rename columns explicitly
        explicit_columns = generate_column_names(dimensions, metrics)
        explicit_columns.append("sale_ids")
        cube.columns = explicit_columns

        # Compute average transaction size
        cube["avg_transaction_size"] = cube["sale_amount_sum"] / cube["sale_id_count"]

        logger.info(f"OLAP cube created with dimensions: {dimensions}")
        return cube
    except Exception as e:
        logger.error(f"Error creating OLAP cube: {e}")
        raise


def write_cube_to_csv(cube: pd.DataFrame, filename: str) -> None:
    """Write the OLAP cube to a CSV file."""
    try:
        output_path = OLAP_OUTPUT_DIR.joinpath(filename)
        cube.to_csv(output_path, index=False)
        logger.info(f"OLAP cube saved to {output_path}.")
    except Exception as e:
        logger.error(f"Error saving OLAP cube to CSV file: {e}")
        raise


def main():
    logger.info("Starting OLAP Cubing process...")

    # 1) Load sales data
    sales_df = ingest_sales_data_from_dw()

    # 2) Define cube dimensions and metrics
    dimensions = ["customer_id"]
    metrics = {
        "sale_amount": ["sum"],
        "sale_id": "count"
    }

    # 3) Create OLAP cube
    olap_cube = create_olap_cube(sales_df, dimensions, metrics)

    # 4) Load customers to get names
    customers_df = ingest_customers_from_dw()

    # 5) Merge to add customer names
    olap_cube = olap_cube.merge(customers_df, on="customer_id", how="left")

    # 6) Save cube with customer names
    write_cube_to_csv(olap_cube, "multidimensional_olap_cube.csv")

    logger.info("OLAP Cubing process completed successfully.")
    logger.info(f"Please see outputs in {OLAP_OUTPUT_DIR}")


if __name__ == "__main__":
    main()
