import pandas as pd
import seaborn as sns

from pyspark.sql import types as t
import daipe as dp


def plot_drift(now_df, day_before_df, feature: str):
    plot_data = pd.concat([now_df.assign(period="Now"), day_before_df.assign(period="Day before")], axis=0, ignore_index=True)

    sns.set(
        rc={
            "figure.figsize": (11.7 * 1.5, 8.27 * 1.5),
            "font.size": 8,
            "axes.titlesize": 8,
            "axes.labelsize": 20,
            "legend.fontsize": 20,
            "legend.title_fontsize": 20,
        },
    )
    sns.set_style("whitegrid")
    sns.kdeplot(data=plot_data, hue="period", x=feature, fill=True, common_norm=False)


def get_drift_table_schema():
    """Schema for logging table"""

    return dp.TableSchema(
        [
            t.StructField("Date", t.DateType()),
            t.StructField("Entity_name", t.StringType()),
            t.StructField("Threshold", t.DoubleType()),
            t.StructField("Is_Drift", t.BooleanType()),
        ],
        primary_key=["Date", "Entity_name"],
    )
