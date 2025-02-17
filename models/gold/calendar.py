import pandas as pd
import polars as pl
import typing as t

from datetime import datetime
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

def extract_year_range(df: pl.DataFrame, date_column: str) -> tuple[int, int]:
    """
    Extract the minimum and maximum years from the date strings in the DataFrame.
    
    Args:
        df: Polars DataFrame containing the date strings
        date_column: Name of the column containing the date strings
    
    Returns:
        Tuple of (min_year, max_year)
    """
    years = df.select(
        pl.col(date_column)
    ).with_columns(
        year=pl.col(date_column).str.extract(r'(\d{4})', 1)
    ).select([
        pl.col("year").min().alias("min_year"),
        pl.col("year").max().alias("max_year")
    ])
        
    # Get min and max year    
    min_year = int(years.item(0, "min_year"))
    max_year = int(years.item(0, "max_year"))
    
    if min_year is None or max_year is None:
        raise ValueError("No valid years found in the dataset")
        
    return min_year, max_year

def detect_grains(df: pl.DataFrame, date_column: str) -> t.Set[str]:
    """
    Detect the calendar grains present in the DataFrame.
    
    Args:
        df: Polars DataFrame containing the date strings
        date_column: Name of the column containing the date strings
    
    Returns:
        Set of grain types ('daily', 'monthly', 'quarterly', 'weekly', 'yearly')
    """
    grains = set()
    
    # Using Polars string pattern matching
    if df.filter(pl.col(date_column).str.contains(r'\d{4}-\d{2}-\d{2}')).height > 0:
        grains.add('daily')
    if df.filter(pl.col(date_column).str.contains(r'\d{4}-\d{2}$')).height > 0:
        grains.add('monthly')
    if df.filter(pl.col(date_column).str.contains(r'\d{4}-Q[1-4]')).height > 0:
        grains.add('quarterly')
    if df.filter(pl.col(date_column).str.contains(r'\d{4}-W\d{2}')).height > 0:
        grains.add('weekly')
    if df.filter(pl.col(date_column).str.contains(r'\d{4}$')).height > 0:
        grains.add('yearly')
    
    return grains

def generate_base_calendar(start_year: int, end_year: int) -> pl.DataFrame:
    """
    Generate a base calendar with daily dates and all possible grain fields.
    Uses Polars for efficient date operations.
    """

    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    
    # Create base calendar using Polars date range
    base = pl.DataFrame({
        'date': pl.date_range(start_date, end_date, '1d', eager=True)
    })
    
    # Extract components using Polars expressions
    calendar = base.with_columns([
        pl.col("date"),
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.iso_year().alias("iso_year"),
        pl.col("date").dt.quarter().alias("quarter"),
        pl.col("date").dt.month().alias("month"),
        pl.col("date").dt.strftime("%b").alias("month__name"),
        pl.col("date").dt.week().alias("week"),
        pl.col("date").dt.weekday().alias("weekday"),
        pl.col("date").dt.strftime("%a").alias("weekday__name"),
        pl.col("date").dt.strftime("%Y-%m").alias("year_month"),
        pl.col("date").dt.strftime("%Y-%b").alias("year_month__name"),
        pl.col("date").dt.strftime("%G-W%V").alias("year_week"),
        pl.col("date").dt.strftime("%G-W%V-%u").alias("iso_week_date"),
        pl.col("date").dt.strftime("%Y-%j").alias("ordinal_date"),
        pl.col("date").dt.is_leap_year().alias("is_leap_year"),
    ]).with_columns(
        # Quarter format needs special handling
        (pl.col('year').cast(str) + '-Q' + pl.col('quarter').cast(str)).alias('year_quarter')
    )
    
    return calendar

def generate_calendar(df: pl.DataFrame, date_column: str) -> pl.DataFrame:
    """
    Generate calendar based on detected grains using Polars operations.
    
    Args:
        df: Input Polars DataFrame containing date strings
        date_column: Name of the column containing the date strings
    
    Returns:
        Polars DataFrame containing the calendar with detected grains
    """
    
    start_year, end_year = extract_year_range(df, date_column)
    base_calendar = generate_base_calendar(start_year, end_year)
    grains = detect_grains(df, date_column)
    
    # Create separate DataFrames for each detected grain
    calendar_parts = []
    
    if 'yearly' in grains:
        yearly = base_calendar.select(
            pl.col('year').cast(pl.String).alias("date_key"),
            pl.lit('yearly').alias("calendar_grain"),
            
            pl.col("year"),
            pl.col("is_leap_year")
        ).unique()

        calendar_parts.append(yearly)
        
    if 'quarterly' in grains:
        quarterly = base_calendar.select(
            pl.col('year_quarter').alias("date_key"),
            pl.lit('quarterly').alias("calendar_grain"),
            pl.col("year"),
            pl.col("quarter"),
            pl.col('year_quarter'),
            pl.col("is_leap_year")
        ).unique()
        
        calendar_parts.append(quarterly)
    
    if 'monthly' in grains:
        monthly = base_calendar.select(
            pl.col('year_month').alias("date_key"),
            pl.lit('monthly').alias("calendar_grain"),
            pl.col("year"),
            pl.col("quarter"),
            pl.col("month"),
            pl.col("month__name"),
            pl.col('year_quarter'),
            pl.col("year_month"),
            pl.col("year_month__name"),
            pl.col("is_leap_year")
        ).unique()
        
        calendar_parts.append(monthly)
    
    if 'weekly' in grains:
        weekly = base_calendar.select(
            pl.col('year_week').alias("date_key"),
            pl.lit('weekly').alias("calendar_grain"),
            pl.col("year"),
            pl.col("iso_year"),
            pl.col("week"),
            pl.col("year_week"),
            pl.col("is_leap_year")
        ).unique()
        
        calendar_parts.append(weekly)
    
    if 'daily' in grains:
        daily = base_calendar.select(
            pl.col('date').cast(pl.String).alias("date_key"),
            pl.lit('daily').alias("calendar_grain"),
            pl.col("year"),
            pl.col("iso_year"),
            pl.col("quarter"),
            pl.col("month"),
            pl.col("month__name"),
            pl.col("week"),
            pl.col("weekday"),
            pl.col("weekday__name"),
            pl.col('year_quarter'),
            pl.col("year_month"),
            pl.col("year_month__name"),
            pl.col("year_week"),
            pl.col("date"),
            pl.col("iso_week_date"),
            pl.col("ordinal_date"),
            pl.col("is_leap_year")
        ).unique()
        
        calendar_parts.append(daily)
    
    # Combine all calendar parts
    if not calendar_parts:
        raise ValueError("No valid calendar grains detected")
    
    calendar = pl.concat(calendar_parts, how="diagonal_relaxed").sort(["calendar_grain", "date_key"])
    
    return calendar

@model(
    name='gold.calendar',
    kind=dict(
        name=ModelKindName.FULL,
    ),
    columns={
        "_hook__calendar__date": "binary",
        "calendar_grain": "string",
        "date": "date",
        "year": "int",
        "iso_year": "int",
        "quarter": "int",
        "month": "int",
        "month__name": "string",
        "week": "int",
        "weekday": "int",
        "weekday__name": "string",
        "year_quarter": "string",
        "year_month": "string",
        "year_month__name": "string",
        "year_week": "string",
        "iso_week_date": "string",
        "ordinal_date": "string",
        "is_leap_year": "bool",
    }
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> t.Generator[pd.DataFrame, None, None]:
    
    date_key = "_hook__calendar__date"
    
    # Load data
    table = context.resolve_table("lakehouse.silver.uss_bridge")
    source_df = pl.from_pandas(context.fetchdf(f"SELECT DISTINCT {date_key} FROM {table}"))
    
    # Early exit if empty, e.g., during first plan
    if source_df.height == 0:
        return None
    
    # Extract the prefix and value from the date key
    source_df = source_df.with_columns(
        pl.col(date_key).cast(pl.String()).str.split("|").alias("list")
    ).with_columns(
        pl.col("list").list.len().alias("length"),
        pl.col("list").list.last().alias("value")
    ).with_columns(
        pl.col("list").list.head(pl.col("length") - 1).cast(pl.List(pl.String)).list.join("|").alias("prefix")
    )
    
    prefix = source_df.item(0, "prefix")
    
    # Generate the calendar(s)
    calendar_df = generate_calendar(source_df, "value").with_columns(
        (pl.lit(prefix + "|") + pl.col("date_key")).cast(pl.Binary).alias(date_key)
    ).select(
        pl.exclude("date_key")
    )
    
    yield calendar_df.to_pandas()