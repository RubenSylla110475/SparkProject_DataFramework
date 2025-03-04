# Spark Data Framework / Streamlit Project  
**Ruben SYLLA ING 4 DATA IA Grp 03**

This project is an advanced stock data analysis application developed using Spark and Streamlit.  
The application allows for the import and analysis of historical data from various Nasdaq stocks, providing valuable insights for investment decision-making.

---

## Project Overview

The main objective of this application is to provide an interactive platform that enables:
- **Data Import:** Retrieving stock market data via Yahoo Finance.
- **Statistical & Financial Analysis:** Calculating indicators such as moving averages, return rates, volatility, momentum, Sharpe ratio, RSI, etc.
- **Interactive Visualization:** Displaying charts and dashboards to explore trends and correlations between stocks.

Leveraging PySpark, the application efficiently processes large volumes of data, while Streamlit offers an intuitive and responsive user interface.

---

## How It's Built

- **Language:** Python

- **Technologies & Libraries:**
  - **Streamlit:** For creating an interactive interface for analysis and visualization.
  - **PySpark:** For distributed data processing to enhance performance.
  - **Pandas:** For data manipulation and transformation.
  - **Matplotlib & Seaborn:** For graphical visualization of indicators and trends.
  - **yfinance:** For extracting stock market data directly from Yahoo Finance.

- **Code Structure:**
  The main script (lab2.py) performs the following steps:
  - **Initialization:** Starts a Spark session and configures the environment.
  - **Data Import:** Downloads data for several stocks and converts it into a Spark DataFrame.
  - **Analysis & Computations:** Executes various functions to:
    - Calculate moving averages.
    - Analyze price distributions.
    - Measure correlations between stocks.
    - Evaluate financial indicators (return rates, volatility, momentum, etc.).
  - **Visualization:** Displays interactive results via Streamlit with dynamic charts and tables.
  - **Parameter Setting:** Uses a sidebar to allow users to select stocks, set analysis dates, and adjust other parameters.

---

## Results

When executed, the application:
- **Provides a Data Overview:** Displays the initial records and data schema for each selected stock.
- **Visualizes the Analysis:**
  - Charts showing price distributions and moving averages.
  - Tables presenting descriptive statistics and financial indicators.
  - Interactive visualizations of correlations between different stocks.
- **Delivers In-Depth Analysis:** Enables exploration of metrics such as return rates by period, volatility, sector performance, momentum, and other key indicators that help identify investment opportunities.

These visualizations and results offer a detailed understanding of market trends and assist in making informed decisions based on quantitative analysis.

---

## How to Launch the Application

1. **Install Dependencies**  
   Install the required libraries using pip:

   ```bash
   pip install streamlit pandas yfinance matplotlib pyspark seaborn
2. **Run the Application**
   Launch the Streamlit application with the following command:

   ```bash
   streamlit run app.py

3. **Usage**

- Select between 2 and 5 stocks from the provided list.
- Choose the start and end dates for the analysis.
- Configure analysis parameters (period, month, etc.) via the sidebar.
- Click on Load Data to import and analyze the data, then explore the interactive results displayed.

![alt text](https://github.com/RubenSylla110475/SparkProject_DataFramework/blob/main/img/StreamLit_MainPage.jpg)
![alt text](https://github.com/RubenSylla110475/SparkProject_DataFramework/blob/main/img/Streamlit2.png)
![alt text](https://github.com/RubenSylla110475/SparkProject_DataFramework/blob/main/img/Streamlit3.png)
![alt text](https://github.com/RubenSylla110475/SparkProject_DataFramework/blob/main/img/Streamlit4.png)
![alt text](https://github.com/RubenSylla110475/SparkProject_DataFramework/blob/main/img/Streamlit5.png)
![alt text](https://github.com/RubenSylla110475/SparkProject_DataFramework/blob/main/img/Streamlit6.png)

