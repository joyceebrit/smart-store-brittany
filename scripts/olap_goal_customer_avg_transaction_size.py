import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import pathlib
import sys

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from utils.logger import logger

OLAP_OUTPUT_DIR = pathlib.Path("data").joinpath("olap_cubing_outputs")
CUBE_WITH_NAMES_FILE = OLAP_OUTPUT_DIR.joinpath("multidimensional_olap_cube.csv")
GRAPHS_DIR = pathlib.Path("graphs")
GRAPHS_DIR.mkdir(parents=True, exist_ok=True)


def load_olap_cube(file_path: pathlib.Path) -> pd.DataFrame:
    """Load the OLAP cube with customer names."""
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded OLAP cube data from {file_path}.")
        return df
    except Exception as e:
        logger.error(f"Failed to load OLAP cube data: {e}")
        raise


def plot_avg_transaction_size(cube_df: pd.DataFrame, top_n=20):
    """Plot top N customers by average transaction size, aggregating others."""

    # Sort descending by avg_transaction_size
    df_sorted = cube_df.sort_values(by="avg_transaction_size", ascending=False)

    if "name" in df_sorted.columns:
        label_col = "name"
    else:
        label_col = "customer_id"

    if len(df_sorted) > top_n:
        top_customers = df_sorted.head(top_n)
        others = df_sorted.iloc[top_n:]

        others_summary = {
            label_col: "Others",
            "sale_amount_sum": others["sale_amount_sum"].sum(),
            "sale_id_count": others["sale_id_count"].sum(),
            "avg_transaction_size": others["sale_amount_sum"].sum() / others["sale_id_count"].sum(),
            "sale_ids": None  # can be left None or aggregate as needed
        }

        plot_df = pd.concat([top_customers, pd.DataFrame([others_summary])], ignore_index=True)
    else:
        plot_df = df_sorted.copy()

    plt.figure(figsize=(14, 7))
    sns.set_theme(style="whitegrid")

    barplot = sns.barplot(data=plot_df, x=label_col, y="avg_transaction_size", palette="Blues_d")
    barplot.set_xticklabels(barplot.get_xticklabels(), rotation=45, ha="right")

    plt.title(f"Top {top_n} Customers by Average Transaction Size (Others Aggregated)")
    plt.xlabel("Customer Name" if label_col == "name" else "Customer ID")
    plt.ylabel("Average Transaction Size ($)")
    plt.tight_layout()

    # Annotate bars with values
    for p in barplot.patches:
        height = p.get_height()
        barplot.annotate(f'{height:,.2f}',
                         (p.get_x() + p.get_width() / 2, height),
                         ha='center', va='bottom', fontsize=9, rotation=90,
                         xytext=(0, 5), textcoords='offset points')

    # Save the figure before showing it
    output_file = GRAPHS_DIR.joinpath(f"avg_transaction_size_top_{top_n}.png")
    plt.savefig(output_file, bbox_inches='tight', dpi=300)
    logger.info(f"Saved plot image to {output_file}")

    plt.show()


def main():
    cube_df = load_olap_cube(CUBE_WITH_NAMES_FILE)
    plot_avg_transaction_size(cube_df, top_n=20)


if __name__ == "__main__":
    main()
