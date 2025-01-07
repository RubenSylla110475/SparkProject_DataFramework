 # Spark Data Framework / Streamlit Project / Ruben SYLLA ING 4 DATA IA Grp 03

import streamlit as st
import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import seaborn as sns

from itertools import combinations

# Spark Initialization
#os.environ["PYSPARK_PYTHON"] = r"C:\Users\solix\AppData\Local\Programs\Python\Python310\python.exe"
#os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\solix\AppData\Local\Programs\Python\Python310\python.exe"

#un comment the lignes for the os and change the directory if an about python occurs

spark_application_name = "DataFramework_Project"
spark = SparkSession.builder.appName(spark_application_name).getOrCreate()

def Importing_Data(Ticker_name, Start_date, End_date):
    data_Yahoo = yf.download(Ticker_name, Start_date, End_date)
    data_Yahoo.reset_index(inplace=True)
    data_Yahoo.columns = ["Date", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]
    data_Yahoo["Stock"] = Ticker_name
    spark_df_Yahoo = spark.createDataFrame(data_Yahoo)
    spark_df_Yahoo = spark_df_Yahoo.dropDuplicates(["Date"])
    return spark_df_Yahoo

def Observation_count(df):
    return df.count()

def Get_Period(df):
    df = df.withColumn("DateDiff", F.datediff(F.lead("Date").over(Window.orderBy("Date")), "Date"))
    interval = df.groupBy("DateDiff").count().orderBy(F.desc("count")).first()[0]
    return "daily" if interval == 1 else "weekly" if interval == 7 else f"{interval}-day"

def descriptive_stats(df):
    return df.describe().toPandas()

def missing_value(df):
    pdf = df.toPandas()
    total_missing = pdf.isnull().sum()
    return total_missing

def plot_price_distribution_combined(price_distributions):
    plt.figure(figsize=(10, 6))
    for stock in price_distributions["Stock"].unique():
        stock_data = price_distributions[price_distributions["Stock"] == stock]
        plt.plot(stock_data["Date"], stock_data["Close"], label=stock)
    plt.title("Price Distributions for Selected Stocks")
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.legend()
    st.pyplot(plt)

def correlation(df, c1, c2):
    return df.stat.corr(c1, c2)

def av_open_close(df):
    weekly_avg = df.groupBy(F.date_trunc('week', 'Date').alias('Week')).agg(
        F.avg("Open").alias("Avg_Open"), 
        F.avg("Close").alias("Avg_Close")
    )
    monthly_avg = df.groupBy(F.date_trunc('month', 'Date').alias('Month')).agg(
        F.avg("Open").alias("Avg_Open"), 
        F.avg("Close").alias("Avg_Close")
    )
    yearly_avg = df.groupBy(F.date_trunc('year', 'Date').alias('Year')).agg(
        F.avg("Open").alias("Avg_Open"), 
        F.avg("Close").alias("Avg_Close")
    )
    return weekly_avg, monthly_avg, yearly_avg

def price_changes(df):
    day_window = Window.orderBy("Date")
    df = df.withColumn("Day_Change", F.col("Close") - F.lag("Close", 1).over(day_window))
    month_window = Window.partitionBy(F.date_trunc('month', 'Date')).orderBy("Date")
    df = df.withColumn("Month_Change", F.col("Close") - F.lag("Close", 1).over(month_window))
    return df

def daily_return(df):
    df = df.withColumn("Daily_Return", (F.col("Close") - F.col("Open")) / F.col("Open") * 100)
    return df

def highest_daily_return(df):
    df = daily_return(df)
    max_daily_return = df.orderBy(F.desc("Daily_Return")).select("Date", "Daily_Return").first()
    return max_daily_return

def average_daily_return(df):
    df = daily_return(df)
    weekly_avg_return = df.groupBy(F.date_trunc('week', 'Date').alias('Week')).agg(
        F.avg("Daily_Return").alias("Weekly_Avg_Return")
    )
    monthly_avg_return = df.groupBy(F.date_trunc('month', 'Date').alias('Month')).agg(
        F.avg("Daily_Return").alias("Monthly_Avg_Return")
    )
    yearly_avg_return = df.groupBy(F.date_trunc('year', 'Date')).agg(
        F.avg("Daily_Return").alias("Yearly_Avg_Return")
    )
    return weekly_avg_return, monthly_avg_return, yearly_avg_return

def calculate_moving_average(df, column, window_size):
    
    window_spec = Window.orderBy("Date").rowsBetween(-(window_size - 1), 0)
    df = df.withColumn(f"{column}_Moving_Avg_{window_size}", F.avg(column).over(window_spec))

    return df

def plot_moving_average(df, stock, column, window_size):
    stock_df = df.filter(F.col("Stock") == stock)
    stock_df_with_ma = calculate_moving_average(stock_df, column, window_size)

    pandas_df = stock_df_with_ma.select(
        F.col("Date").cast("string").alias("Date"),
        column,
        f"{column}_Moving_Avg_{window_size}",
    ).toPandas()


    pandas_df["Date"] = pandas_df["Date"].astype("datetime64[ns]")
    plt.figure(figsize=(12, 6))
    plt.plot(pandas_df["Date"], pandas_df[column], label=f"{column} Price", color="blue", alpha=0.7)
    plt.plot(
        pandas_df["Date"],
        pandas_df[f"{column}_Moving_Avg_{window_size}"],
        label=f"{window_size}-Day Moving Average",
        color="orange",
        linestyle="--",
    )
    plt.title(f"{column} Prices and Moving Average for {stock}")
    plt.xlabel("Date")
    plt.ylabel(f"{column} Price")
    plt.legend()
    plt.grid()
    st.pyplot(plt)

def Align_Dataframes(df1, df2):
    df1 = df1.select("Date", "Close").withColumnRenamed("Close", "Stock1_Close")
    df2 = df2.select("Date", "Close").withColumnRenamed("Close", "Stock2_Close")
    aligned_df = df1.join(df2, on="Date", how="inner")
    return aligned_df

def Analyze_Correlation(stock1, stock2, start_date, end_date):
    st.write(f"Importing data for {stock1}...")
    df1 = Importing_Data(stock1, start_date, end_date)

    st.write(f"Importing data for {stock2}...")
    df2 = Importing_Data(stock2, start_date, end_date)

    st.write("Aligning data on the same dates...")
    aligned_df = Align_Dataframes(df1, df2)

    st.write(f"Calculating correlation between {stock1} and {stock2}...")
    corr = correlation(aligned_df, "Stock1_Close", "Stock2_Close")
    st.write(f"Correlation between {stock1} and {stock2}: {corr}")

    return aligned_df

def analyze_all_correlations(df):
    stocks = df.select("Stock").distinct().rdd.flatMap(lambda x: x).collect()
    stock_pairs = list(combinations(stocks, 2))

    correlation_results = []
    for stock1, stock2 in stock_pairs:
        try:
            df1 = df.filter(F.col("Stock") == stock1).select("Date", "Close").withColumnRenamed("Close", f"{stock1}_Close")
            df2 = df.filter(F.col("Stock") == stock2).select("Date", "Close").withColumnRenamed("Close", f"{stock2}_Close")
            combined = df1.join(df2, on="Date", how="inner")

            corr = combined.stat.corr(f"{stock1}_Close", f"{stock2}_Close")
            correlation_results.append((stock1, stock2, corr))
        except Exception as e:
            print(f"Error calculating correlation between {stock1} and {stock2}: {str(e)}")
            correlation_results.append((stock1, stock2, None))

    correlation_df = spark.createDataFrame(correlation_results, ["Stock1", "Stock2", "Correlation"])
    print("All Stock Pair Correlations:")
    correlation_df.show()
    return correlation_df

def plot_correlation_matrix(df):
    columns = df.columns[1:]
    pandas_df = df.select(columns).toPandas()
    corr_matrix = pandas_df.corr()
    sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", xticklabels=columns, yticklabels=columns)
    plt.title("Stock Correlation Matrix")
    st.pyplot(plt)


def calculate_return_rate(df, period):
    period_col = F.date_trunc(period, "Date").alias("Period")
    return_rate = df.groupBy(period_col, "Stock").agg(
        ((F.last("Close") - F.first("Close")) / F.first("Close") * 100).alias("Return_Rate")
    )
    return return_rate

def best_return_rate(df, period, start_date):
    filtered_df = df.filter(F.col("Date") >= F.lit(start_date))
    period_col = F.date_trunc(period, "Date").alias(period.capitalize())
    return_rate = filtered_df.groupBy(period_col, "Stock").agg(
        ((F.last("Close") - F.first("Close")) / F.first("Close") * 100).alias("Return_Rate")
    ).distinct()
    print("Return Rate DataFrame:")
    return_rate.show(truncate=False)
    best_stock = return_rate.orderBy(F.desc("Return_Rate")).limit(1).collect()[0]
    
    print(f"The best performing stock for the {period} starting from {start_date} is:")
    print(best_stock)
    return best_stock

def calculate_volatility(df):
    daily_returns = df.withColumn("Daily_Return", ((F.col("Close") - F.col("Open")) / F.col("Open")) * 100)
    volatility = daily_returns.groupBy("Stock").agg(F.stddev("Daily_Return").alias("Volatility"))
    print("Volatility by Stock:")
    volatility.show()
    return volatility

def sector_performance(df):
    monthly_returns = df.groupBy(F.date_trunc("month", "Date").alias("Month"), "Sector").agg(
        ((F.last("Close") - F.first("Close")) / F.first("Close") * 100).alias("Monthly_Return")
    )
    print("Sector Performance:")
    monthly_returns.show()
    return monthly_returns

def calculate_momentum(df, period):
    window_spec = Window.partitionBy("Stock").orderBy("Date")
    momentum = df.withColumn(
        f"Momentum_{period}_Days",
        ((F.col("Close") - F.lag("Close", period).over(window_spec)) / F.lag("Close", period).over(window_spec)) * 100
    )
    print(f"Momentum ({period} Days) calculated:")
    momentum.select("Date", "Stock", f"Momentum_{period}_Days").show()
    return momentum

def calculate_dividend_yield(df, dividends):
    dividend_df = spark.createDataFrame(dividends.items(), ["Stock", "Annual_Dividend"])
    df_with_yield = df.join(dividend_df, on="Stock", how="left").withColumn(
        "Dividend_Yield", (F.col("Annual_Dividend") / F.col("Close")) * 100
    )
    print("Dividend Yield by Stock:")
    df_with_yield.select("Stock", "Close", "Annual_Dividend", "Dividend_Yield").distinct().show()
    return df_with_yield

def correlation_with_index(df, index_df, index_name):
    df = df.withColumn("Daily_Return", ((F.col("Close") - F.col("Open")) / F.col("Open")) * 100)
    index_df = index_df.withColumn("Index_Return", ((F.col("Close") - F.col("Open")) / F.col("Open")) * 100)

    correlation_results = {}
    for stock in df.select("Stock").distinct().rdd.flatMap(lambda x: x).collect():
        stock_df = df.filter(F.col("Stock") == stock)
        aligned_df = stock_df.join(index_df, on="Date", how="inner")
        correlation = aligned_df.stat.corr("Daily_Return", "Index_Return")
        correlation_results[stock] = correlation

    print(f"Correlation with {index_name}:")
    for stock, corr in correlation_results.items():
        print(f"{stock}: {corr}")
    return correlation_results

def analyze_earnings_surprises(df, earnings_dates):
    results = []
    for stock, dates in earnings_dates.items():
        stock_df = df.filter(F.col("Stock") == stock)
        for date in dates:
            try:
                before = stock_df.filter(F.col("Date") == F.lit(date).cast("date") - 1).select("Close").first()
                after = stock_df.filter(F.col("Date") == F.lit(date).cast("date") + 1).select("Close").first()

                if before is not None and after is not None:
                    change = ((after["Close"] - before["Close"]) / before["Close"]) * 100
                else:
                    change = None
                results.append((stock, date, change))
            except Exception as e:
                print(f"Error processing earnings surprise for {stock} on {date}: {str(e)}")
                results.append((stock, date, None))

    spark_results = spark.createDataFrame(results, ["Stock", "Earnings_Date", "Price_Change"])
    print("Earnings Surprise Analysis:")
    spark_results.show()
    return spark_results

def calculate_sharpe_ratio(df, risk_free_rate=2.0):
    daily_returns = df.withColumn("Daily_Return", ((F.col("Close") - F.col("Open")) / F.col("Open")) * 100)
    sharpe_ratios = daily_returns.groupBy("Stock").agg(
        (F.avg("Daily_Return") - F.lit(risk_free_rate) / 252) / F.stddev("Daily_Return")
    ).alias("Sharpe_Ratio")

    print("Sharpe Ratios:")
    sharpe_ratios.show()
    return sharpe_ratios

def calculate_rsi(df, period=14):
    window_spec = Window.partitionBy("Stock").orderBy("Date")
    df = df.withColumn("Change", F.col("Close") - F.lag("Close").over(window_spec))
    df = df.withColumn("Gain", F.when(F.col("Change") > 0, F.col("Change")).otherwise(0))
    df = df.withColumn("Loss", F.when(F.col("Change") < 0, -F.col("Change")).otherwise(0))

    rolling_avg = df.groupBy("Stock").agg(
        F.avg(F.col("Gain")).alias("Avg_Gain"),
        F.avg(F.col("Loss")).alias("Avg_Loss")
    )
    rolling_avg = rolling_avg.withColumn(
        "RSI", 100 - (100 / (1 + F.col("Avg_Gain") / F.col("Avg_Loss")))
    )
    print("RSI Calculated:")
    rolling_avg.show()
    return rolling_avg

def add_sector_column(df, sectors):
    sector_df = spark.createDataFrame(sectors.items(), ["Stock", "Sector"])
    df_with_sector = df.join(sector_df, on="Stock", how="left")
    return df_with_sector

sectors = {
    "AAPL": "Technology",
    "MSFT": "Technology",
    "GOOGL": "Technology",
    "AMZN": "Consumer Discretionary",
    "TSLA": "Consumer Discretionary",
    "META": "Technology",
    "NFLX": "Communication Services",
    "NVDA": "Technology",
    "ADBE": "Technology",
    "PYPL": "Financials"
}

earnings_dates = {
    "AAPL": ["2024-01-25", "2024-04-25"],
    "MSFT": ["2024-01-30", "2024-04-30"]
}

# ^^^^^^ Add more earnings dates for other tickets

known_companies = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NFLX", "NVDA", "ADBE", "PYPL",
    "CSCO", "INTC", "PEP", "COST", "AVGO", "TXN", "QCOM", "AMD", "INTU", "AMAT",
    "ADP", "SBUX", "BKNG", "ZM", "MRNA", "DOCU", "SNOW", "CRM", "ORCL", "WMT"
]

st.write("**Ruben SYLLA N° 932152151**")
st.title("BigDataAnalysis - Project")

st.write("""
For this project, we will go through the entire process of analyzing a set of Nasdaq tech stocks using
the spark framework. The main idea of this lab is to use spark to build a python application that will
deliver interesting insights from this data in order to bring value to someone who is wishing to invest
in those stocks.
""")

st.title("Advanced Stock Analysis App")

if "stock_dataframes" not in st.session_state:
    st.session_state.stock_dataframes = {}
if "combined_df" not in st.session_state:
    st.session_state.combined_df = None
if "best_stock" not in st.session_state:
    st.session_state.best_stock = None
if "selected_tickers" not in st.session_state:
    st.session_state.selected_tickers = []
if "data_loaded" not in st.session_state:
    st.session_state.data_loaded = False

st.sidebar.header("Data Selection Parameters")
selected_tickers = st.sidebar.multiselect(
    "Select 2 to 5 stocks for analysis:", known_companies
)

start_date = st.sidebar.date_input(
    "Start Date", pd.to_datetime("2024-01-01")
)
end_date = st.sidebar.date_input(
    "End Date", pd.to_datetime("2024-05-25")
)

st.sidebar.subheader("Return Rate Analysis")
selected_month = st.sidebar.selectbox(
    "Select a month:",
    [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ],
    key="month_selector"
)

selected_period = st.sidebar.radio(
    "Select the period:", ["month", "year"], key="period_selector"
)

if len(selected_tickers) < 2 or len(selected_tickers) > 5:
    st.warning("Please select between 2 to 5 stocks.")
else:
    if st.sidebar.button("Load Data"):
        stock_dataframes = {}
        price_distributions = pd.DataFrame()
        missing_values = {}

        st.header("Data Overview")
        for ticker in selected_tickers:
            df = Importing_Data(ticker, start_date, end_date)
            stock_dataframes[ticker] = df

            st.subheader(f"First 40 Rows of {ticker}")
            st.dataframe(df.toPandas().head(40))

            stock_df = df.toPandas()
            stock_df["Stock"] = ticker
            price_distributions = pd.concat([price_distributions, stock_df], ignore_index=True)

            missing_values[ticker] = missing_value(df)

            st.session_state.data_loaded = True

        st.subheader("Schema of the Data")
        schema = pd.DataFrame([(field.name, str(field.dataType)) for field in df.schema], columns=["Column", "Type"])
        st.dataframe(schema)

        total_observations = sum([Observation_count(df) for df in stock_dataframes.values()])
        st.write(f"**Total Number of Observations**: {total_observations}")
        st.write(f"**Data Period**: {Get_Period(list(stock_dataframes.values())[0])}")

        st.subheader("Missing Values")
        st.write("Missing values for each stock:")
        for ticker, missing in missing_values.items():
            st.write(f"- {ticker}: {missing.sum()} missing values")

        st.subheader("Price Distributions")
        if not price_distributions.empty:
            plot_price_distribution_combined(price_distributions)

        st.header("Advanced Analysis")
        for ticker, df in stock_dataframes.items():
            st.subheader(f"Analysis for {ticker}")

            st.write(f"**Weekly, Monthly, Yearly Averages for {ticker}:**")
            weekly_avg, monthly_avg, yearly_avg = av_open_close(df)
            st.write("**Weekly Averages**")
            st.dataframe(weekly_avg.toPandas())
            st.write("**Monthly Averages**")
            st.dataframe(monthly_avg.toPandas())
            st.write("**Yearly Averages**")
            st.dataframe(yearly_avg.toPandas())
            st.write(f"**Price Changes for {ticker}:**")
            df_changes = price_changes(df)
            st.dataframe(df_changes.select("Date", "Close", "Day_Change", "Month_Change").toPandas())
            st.write(f"**Daily Returns for {ticker}:**")
            df_returns = daily_return(df)
            st.dataframe(df_returns.select("Date", "Open", "Close", "Daily_Return").toPandas())
            st.write(f"**Highest Daily Return for {ticker}:**")
            max_return = highest_daily_return(df)
            st.write(f"Date: {max_return['Date']}, Return: {max_return['Daily_Return']}%")
            st.write(f"**Average Daily Returns for {ticker}:**")
            weekly_avg_ret, monthly_avg_ret, yearly_avg_ret = average_daily_return(df)
            st.write("**Weekly Average Returns**")
            st.dataframe(weekly_avg_ret.toPandas())
            st.write("**Monthly Average Returns**")
            st.dataframe(monthly_avg_ret.toPandas())
            st.write("**Yearly Average Returns**")
            st.dataframe(yearly_avg_ret.toPandas())

            if "selected_columns" not in st.session_state:
                st.session_state["selected_columns"] = {}

        for i, ticker in enumerate(selected_tickers):
            if ticker not in st.session_state["selected_columns"]:
                st.session_state["selected_columns"][ticker] = "Open"
            selectbox_key = f"selectbox_{ticker}_{i}"

            selected_column = st.selectbox(
                f"Select a column for moving average ({ticker}):",
                ["Open", "Close"],
                key=selectbox_key,
                index=["Open", "Close"].index(st.session_state["selected_columns"][ticker])  # Set the default value
            )

            st.session_state["selected_columns"][ticker] = selected_column
            st.subheader(f"Moving Average for {ticker}")
            plot_moving_average(stock_dataframes[ticker], stock=ticker, column=selected_column, window_size=5)

        if len(selected_tickers) >= 2:
            try:
                st.subheader("Correlation Analysis")
                if st.session_state.combined_df is None:
                    combined_df = None
                    for ticker in selected_tickers:
                        if combined_df is None:
                            combined_df = stock_dataframes[ticker]
                        else:
                            combined_df = combined_df.union(stock_dataframes[ticker])
                    st.session_state.combined_df = combined_df
                else:
                    combined_df = st.session_state.combined_df

                correlation_df = analyze_all_correlations(combined_df)
                correlation_pd = correlation_df.toPandas()
                st.dataframe(correlation_pd)

                for _, row in correlation_pd.iterrows():
                    stock1, stock2 = row["Stock1"], row["Stock2"]
                    st.subheader(f"Correlation Matrix: {stock1} and {stock2}")
                    df1 = combined_df.filter(F.col("Stock") == stock1).select("Date", "Close").withColumnRenamed("Close", f"{stock1}_Close")
                    df2 = combined_df.filter(F.col("Stock") == stock2).select("Date", "Close").withColumnRenamed("Close", f"{stock2}_Close")
                    aligned_df = df1.join(df2, on="Date", how="inner")
                    plot_correlation_matrix(aligned_df)
            except Exception as e:
                st.error(f"Error calculating correlations: {str(e)}")
        else:
            st.warning("Please select at least two stocks for correlation analysis.")
        
        st.header("Return Rate Analysis")

        if len(selected_tickers) >= 2:
            combined_df = None
            for ticker in selected_tickers:
                if combined_df is None:
                    combined_df = stock_dataframes[ticker]
                else:
                    combined_df = combined_df.union(stock_dataframes[ticker])

            periods = ["month", "week", "year"]

            for period in periods:
                return_rate_df = calculate_return_rate(combined_df, period)
                st.subheader(f"Return Rates by {period.capitalize()}")

                pandas_return_rate = return_rate_df.toPandas()
                st.dataframe(pandas_return_rate)

                st.subheader(f"Return Rate Visualization for {period.capitalize()}")
                for stock in pandas_return_rate["Stock"].unique():
                    stock_data = pandas_return_rate[pandas_return_rate["Stock"] == stock]
                    plt.plot(stock_data["Period"], stock_data["Return_Rate"], label=stock)

                plt.title(f"Return Rates by {period.capitalize()}")
                plt.xlabel(f"{period.capitalize()}")
                plt.ylabel("Return Rate (%)")
                plt.legend()
                st.pyplot(plt)

            if st.session_state.data_loaded:
                st.header("Best Stock by Return Rate")
                combined_df = None

                for ticker in selected_tickers:
                    if combined_df is None:
                        combined_df = stock_dataframes[ticker]
                    else:
                        combined_df = combined_df.union(stock_dataframes[ticker])

                if combined_df is None:
                    st.error("No data available. Please load the data first.")
                else:
                    month_mapping = {
                        "January": "2024-01-01", "February": "2024-02-01", "March": "2024-03-01",
                        "April": "2024-04-01", "May": "2024-05-01", "June": "2024-06-01",
                        "July": "2024-07-01", "August": "2024-08-01", "September": "2024-09-01",
                        "October": "2024-10-01", "November": "2024-11-01", "December": "2024-12-01"
                    }
                    start_date = month_mapping[selected_month]

                    try:


                        best_result = best_return_rate(combined_df, selected_period, start_date)

                        st.subheader("Return Rate DataFrame:")
                        pandas_return_rate = combined_df.groupBy(F.date_trunc(selected_period, "Date").alias("Period"), "Stock") \
                            .agg(((F.last("Close") - F.first("Close")) / F.first("Close") * 100).alias("Return_Rate")) \
                            .toPandas()
                        st.dataframe(pandas_return_rate)
                        st.subheader(f"The Best Stock for {selected_month} ({selected_period.capitalize()}):")
                        st.write(f"**Stock:** {best_result['Stock']}")
                        st.write(f"**Month:** {best_result['Month']}")
                        st.write(f"**Return Rate:** {best_result['Return_Rate']:.2f}%")
                        
                    except Exception as e:
                        st.error(f"Error calculating best return rate: {str(e)}")

            if st.session_state.data_loaded:
                st.header("Advanced Stock Metrics")

                try:
                    st.subheader("Volatility")
                    volatility = calculate_volatility(combined_df)
                    volatility_pd = volatility.toPandas()
                    st.dataframe(volatility_pd)
                except Exception as e:
                    st.error(f"Error calculating volatility: {str(e)}")

                try:
                    st.subheader("Sector Performance")
                    combined_df_with_sector = add_sector_column(combined_df, sectors)
                    sector_performance_df = sector_performance(combined_df_with_sector)
                    sector_performance_pd = sector_performance_df.toPandas()
                    st.dataframe(sector_performance_pd)
                except Exception as e:
                    st.error(f"Error calculating sector performance: {str(e)}")

                try:
                    st.subheader("Momentum")
                    selected_momentum_period = st.number_input(
                        "Select the period for momentum calculation (days):", min_value=1, max_value=30, value=5
                    )
                    momentum = calculate_momentum(combined_df, selected_momentum_period)
                    momentum_pd = momentum.select("Date", "Stock", f"Momentum_{selected_momentum_period}_Days").toPandas()
                    st.dataframe(momentum_pd)
                except Exception as e:
                    st.error(f"Error calculating momentum: {str(e)}")

                try:
                    st.subheader("Dividend Yield")
                    example_dividends = {"AAPL": 0.88, "MSFT": 2.48, "GOOGL": 0.0, "AMZN": 0.0}
                    dividend_yield = calculate_dividend_yield(combined_df, example_dividends)
                    dividend_yield_pd = dividend_yield.select("Stock", "Close", "Annual_Dividend", "Dividend_Yield").toPandas()
                    st.dataframe(dividend_yield_pd)
                except Exception as e:
                    st.error(f"Error calculating dividend yield: {str(e)}")

                try:
                    st.subheader("Correlation with Index")
                    index_data = Importing_Data("SPY", start_date, end_date)
                    correlation_results = correlation_with_index(combined_df, index_data, "S&P 500")
                    st.write(f"Correlation with S&P 500:")
                    st.json(correlation_results)
                except Exception as e:
                    st.error(f"Error calculating correlation with index: {str(e)}")

                try:
                    st.subheader("Earnings Surprises")
                    earnings_surprises_df = analyze_earnings_surprises(combined_df, earnings_dates)
                    earnings_surprises_pd = earnings_surprises_df.toPandas()
                    st.dataframe(earnings_surprises_pd)
                except Exception as e:
                    st.error(f"Error analyzing earnings surprises: {str(e)}")

                try:
                    st.subheader("Sharpe Ratio")
                    risk_free_rate = st.number_input("Enter the annual risk-free rate (%):", min_value=0.0, value=2.0)
                    sharpe_ratios = calculate_sharpe_ratio(combined_df, risk_free_rate)
                    sharpe_ratios_pd = sharpe_ratios.toPandas()
                    st.dataframe(sharpe_ratios_pd)
                except Exception as e:
                    st.error(f"Error calculating Sharpe Ratio: {str(e)}")

                try:
                    st.subheader("Relative Strength Index (RSI)")
                    rsi_period = st.number_input("Select the period for RSI calculation (days):", min_value=1, max_value=30, value=14)
                    rsi = calculate_rsi(combined_df, rsi_period)
                    rsi_pd = rsi.select("Stock", "Avg_Gain", "Avg_Loss", "RSI").toPandas()
                    st.dataframe(rsi_pd)
                except Exception as e:
                    st.error(f"Error calculating RSI: {str(e)}")

        else:
            st.warning("Please select at least two stocks for return rate analysis.")

