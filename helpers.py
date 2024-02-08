import time
import requests
import pyspark.sql.functions as psf
from pathlib import Path
from pyspark.sql.window import Window

def round_cols(df, cols):
  for col in cols:
    df = df.withColumn(col, psf.round(col, 2))
  return df

def request_prices_polygon(stocks_dict, date_period, api_key):
  
  modified_dict =stocks_dict.copy()

  for i, stock in enumerate(stocks_dict):
    ticker = stock['symbol'] 
    # issue when requesting data for FB and ANTM - the tickers have changed over time
      #FB changed on June 9, 2022
    if date_period < '2022-06-09' and ticker=='META':
        print('changing META ticker to old version')
        ticker='FB'
        #ANTM changed on June 28, 2022
    if date_period < '2022-06-28' and ticker=='ELV':
        print('changing ELV ticker to old version')
        ticker='ANTM'
        
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{date_period}/{date_period}?apiKey={api_key}'
  
    try:
      response = requests.get(url=url)
      # Polygon only allows 5 requests per min - status 409 means too many requests
      if response.status_code==429:
        print('maximum requests... waiting 1 minute')
        time.sleep(60)
        response = requests.get(url=url)

      stock_data = response.json()
      print(f"response: {response.status_code}, ticker: {ticker}, date: {date_period}")
      
      # Add share price to dict
      closing_price = float(stock_data['results'][0]['c'])
    #   modified_dict[i][f'{date_period}_price'] = round(closing_price, 2)
      modified_dict[i][f'date'] = f'{date_period}'
      modified_dict[i][f'price'] = round(closing_price, 2)

    except Exception as e:
      print(f"error requesting data for {ticker}")
      raise e
  
  return modified_dict


def compare_share_prices(df, current_date, old_date):
    print("Comparing the share prices across dates")
    max_date = df.select(psf.max("date")).collect()[0][0]
    min_date = df.select(psf.min("date")).collect()[0][0]
    max_window = Window.partitionBy("initial_investment").orderBy(psf.col('change_percentage').desc())
    
    # pivot and transform data to produce share price analysis
    df_compare = df.filter(f"date ='{max_date}' or date ='{min_date}'") \
        .groupBy(["company_name", "symbol", "initial_investment"]).pivot("date").sum('price') \
        .withColumn('no_shares', psf.col('initial_investment')/psf.col(f'{min_date}')) \
        .withColumn('current_value', psf.col('no_shares')*psf.col(f'{max_date}')) \
        .withColumn('change_percentage', (psf.try_subtract(f'{max_date}', f'{min_date}'))/psf.col(f'{min_date}')*100) \
        .withColumn("rank", psf.rank().over(max_window)) 
    
    # rounds to 2 decimal places
    df_compare = round_cols(df_compare, cols=[x for x in df_compare.columns if x not in ('company_name', 'symbol', 'date', 'rank')])
    print(max_date, min_date)

    print(f"writing comparison outputs to: ./outputs/polygon_stock_comparison.csv")
    pdf = df_compare.toPandas()
    pdf.to_csv('./outputs/polygon_stock_comparison.csv', sep=',', encoding='utf-8', index=False)

    # sum of all the current investments' value
    sum_of_investments = df_compare.groupBy().sum().collect()
    greatest_relative_inrease = df_compare.filter("rank==1").collect()[0]
    initial_investment_total = sum_of_investments[0]['sum(initial_investment)']
    current_investment_total = round(sum_of_investments[0]['sum(current_value)'], 2)
    
    # create path if doesnt exist
    Path("./outputs").mkdir(parents=True, exist_ok=True)

    
    # write answers to txt file
    print(f"Writing comparison results to: ./outputs/results.txt")
    with open('./outputs/results.txt', 'w') as f:
        print('Stock price comparison\n', file=f)
    
    with open('./outputs/results.txt', 'a') as f:
      print(f'Current date: {current_date}',file=f)
      print(f'Start date: {old_date}\n',file=f)



      print("Greatest relative increase: ", greatest_relative_inrease['company_name'], f"({greatest_relative_inrease['symbol']})",file=f)
      print("Growth %: ", greatest_relative_inrease['change_percentage'],file=f)
      print("Gross profit: $", greatest_relative_inrease['current_value']-greatest_relative_inrease['initial_investment'], '\n',file=f)
      
      print("Initial total investment: ", initial_investment_total, file=f)
      print("Current total investment (growth): ", current_investment_total, file=f)
    
    return df_compare
    # df_compare.show()