This repo pulls closing price stock data for the s&p500 and shows each stock's change in value.
Additionally, the value of a $10,000 invested into each stock at the beginning period, is calculated at today's share price.
If data has already been pulled, it will compare the latest date and if new data is available it is appended to the main parquet.

**There are limitations to using the Polygon API free tier
 1) Maximum 2 years of historical data is available 
 2) Maximum 5 requests per minute

*As a workaround, I stagger the requests in batches of 5. This means that the after 5 requests, we need to wait 1 minute before making another 5 requests. 
All requests complete ~40 mins

Dependencies:
python3 -m  pipreqs.pipreqs
    Pyspark
    Pandas
    Java 8+ (for pyspark)
    dotenv (for api key only)

How to run:
    1) Install all dependencies
    2) Sign up to Polygon API and get an API key 
    3) Save API key to a .env file in the root directory 
    4) Run stocks.py
    5) outputs are saved to the /outputs folder

OUTPUTS:
    1) polygon_stock_data.parquet -> This file contains all data extracted using polygon api. 
       It is partitioned on date, meaning that even if we rerun the same date, we will not encounter duplicates
    2) polygon_stock_comparison.csv -> This contains only the furtherest in the past date, and the latest date.
       The file shows the value of the Investment at the latest period and the change in share prices
    3) results.txt -> This text file contains the answers to the questions

**there is also a .ipynb notebook with the same code, which I used for testing

-------------- Questions
1. Create a free account with Polygon https://polygon.io/docs/stocks/getting-started, Polygon provides financial market data.

2. Create a mini Python project where you can run PySpark locally.

3. Attached is a CSV file with the top 100 companies in the S&P 500. Using local PySpark, read in the CSV file and save to a dataframe.

4. Query the Polygon API to get their stock price data per day (closing price) from 2019-01-01 to today, for each company in the CSV file.

5. Write some PySpark transforms to answer the below questions, for simplification you can ignore the effect of dividends.

    a. Which stock has had the greatest relative increase in price in this period?

    b. If you had invested $1 million at the beginning of this period by purchasing $10,000 worth of shares in every company in the list equally, how much would you have today? Technical note, you can assume that it is possible to purchase fractional shares.