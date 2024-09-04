import yfinance as yf
import pandas as pd
from os import path, makedirs, getcwd
from datetime import datetime
from transformers import pipeline
from bs4 import BeautifulSoup
import requests
from airflow import DAG
from airflow.decorators import task
# from airflow.operators.python_operator import PythonOperator

# [ ] add distinguishing EU and US stocks

OUTPUT_DIR = path.join(getcwd(), 'output', 'stocks') # by default cwd is /opt/airflow
RAW_DIR = path.join(OUTPUT_DIR, 'raw')
BRONZE_DIR = path.join(OUTPUT_DIR, 'bronze')
TICKER_STRS = ['AAPL', 'MSFT', 'META', 'GOOGL', 'AMZN',]
STOCKS = [yf.Ticker(ticker_str) for ticker_str in TICKER_STRS]

for dir in [RAW_DIR, BRONZE_DIR]:
    if not path.isdir(dir):
        makedirs(dir)

def get_financial_df(stocks, fin_page_name):
    fin_dfs = []
    
    for stock in stocks:
        fin_df = getattr(stock, fin_page_name)
        fin_df = fin_df.copy().transpose()[::-1]
        fin_df.insert(0, 'Ticker', stock.info['symbol'])
        fin_dfs.append(fin_df)

    fins_df = pd.concat(fin_dfs).reset_index()
    fins_df.to_parquet(path.join(RAW_DIR, f'stocks_{fin_page_name}.parquet'), index=False)
    return fins_df

def get_rel_df(df):
    shifted_df = df.shift(1).fillna(pd.NA) # fillna pd.NA (by default yf dfs have None and not nan)
    return 100 * (df - shifted_df) / shifted_df.abs()

def get_text_from_link(link):
    HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/114.0'}
    page_source = requests.get(link, headers=HEADERS)
    soup = BeautifulSoup(page_source.content, 'html.parser')
    return '\n'.join([paragraph.text for paragraph in soup.article.find_all('p')]) if soup.article else ''

with DAG(dag_id = 'stocks_dag', start_date=datetime(2024, 9, 1), schedule="1 9-22/1 * * MON-FRI") as dag:

    @task
    def get_info_raw_df(stocks):
        # Note: dict + DataFrame once runs 400x faster, than making dfs and concatenating approach (0.0025s x 1s)
        infos = []
        
        for stock in stocks:
            infos.append(stock.info)

        info_df = pd.DataFrame(infos)
        info_df.to_parquet(path.join(RAW_DIR, 'stocks_info.parquet'), index=False)
        return info_df

    @task
    def get_income_stmt_raw_df(stocks):
        return get_financial_df(stocks, 'income_stmt')

    @task
    def get_income_stmt_bronze_df(income_stmt_df):
        # income_stmt_df = pd.read_parquet(path.join(RAW_DIR, 'stocks_income_stmt.parquet'))

        if 'Total Revenue' in income_stmt_df.columns and (income_stmt_df['Total Revenue']!=0).all():
            total_revenue = income_stmt_df['Total Revenue']

            if 'Gross Profit' in income_stmt_df.columns:
                income_stmt_df['Gross Margin %'] = 100 * income_stmt_df['Gross Profit'] / total_revenue

            if 'Operating Income' in income_stmt_df.columns:
                income_stmt_df['Operating Margin % (target>15%)'] = 100 * income_stmt_df['Operating Income'] / total_revenue

            if 'Net Income' in income_stmt_df.columns:
                income_stmt_df['Net Margin % (target>20%[=MOAT])'] = 100 * income_stmt_df['Net Income'] / total_revenue

            income_stmt_df.to_parquet(path.join(BRONZE_DIR, 'stocks_income_stmt.parquet'), index=False)

    @task
    def get_balance_sheets_raw_df(stocks):
        return get_financial_df(stocks, 'balance_sheet')

    @task
    def get_balance_sheets_bronze_df(balance_sheet_df, income_stmt_df):
        # balance_sheet_df = pd.read_parquet(path.join(RAW_DIR, 'stocks_balance_sheet.parquet'))
        # income_stmt_df = pd.read_parquet(path.join(RAW_DIR, 'stocks_income_stmt.parquet'))

        if 'Total Liabilities Net Minority Interest' in balance_sheet_df.columns and 'Total Assets' in balance_sheet_df.columns and (balance_sheet_df['Total Assets']!=0).all():
            balance_sheet_df['Total Debtness %'] = 100 * balance_sheet_df['Total Liabilities Net Minority Interest'] / balance_sheet_df ['Total Assets']
        
        if 'Current Liabilities' in balance_sheet_df.columns and 'Current Assets' in balance_sheet_df.columns and (balance_sheet_df['Current Assets']!=0).all():
            balance_sheet_df['Current Debtness %'] = 100 * balance_sheet_df['Current Liabilities'] / balance_sheet_df['Current Assets']

        if 'Net Income' in income_stmt_df.columns:
            net_income = income_stmt_df['Net Income']

            if 'Stockholders Equity' in balance_sheet_df.columns and (balance_sheet_df['Stockholders Equity']!=0).all():
                balance_sheet_df['ROE % (target>10%)'] = 100 * net_income / balance_sheet_df['Stockholders Equity']

            if 'Total Assets' in balance_sheet_df.columns and (balance_sheet_df['Total Assets']!=0).all():
                balance_sheet_df['ROA %'] = 100 * net_income / balance_sheet_df['Total Assets']

            if 'Long Term Debt' in balance_sheet_df.columns and (balance_sheet_df['Long Term Debt']!=0).all():
                balance_sheet_df['WB Bonity % (4*NetIncome/LongTermDebt)'] = 4 * net_income / balance_sheet_df['Long Term Debt']

        balance_sheet_df.to_parquet(path.join(BRONZE_DIR, 'stocks_balance_sheet.parquet'), index=False)

    @task
    def get_cashflow_raw_df(stocks):
        return get_financial_df(stocks, 'cashflow')

    @task
    def get_cashflow_bronze_df(cashflow_df, income_stmt_df):
        # cashflow_df = pd.read_parquet(path.join(RAW_DIR, 'stocks_cashflow.parquet'))
        # income_stmt_df = pd.read_parquet(path.join(RAW_DIR, 'stocks_income_stmt.parquet'))

        if 'Capital Expenditure' in cashflow_df.columns and 'Net Income' in income_stmt_df.columns and (income_stmt_df['Net Income']!=0).all():
            cashflow_df['WB CapEx to NetIncome % (target<25%,>50%nebrat)'] = 100 * cashflow_df['Capital Expenditure'] / income_stmt_df['Net Income']

        cashflow_df.to_parquet(path.join(BRONZE_DIR, 'stocks_cashflow.parquet'), index=False)

    @task
    def get_history_raw_df(stocks):
        history_dfs = []
        PERIODS = ['5d', '1mo', 'ytd', '1y', '5y', '10y']
        INTERVALS = ['1h', '1d', '1d', '1wk', '1mo', '1mo']

        for period, interval in zip(PERIODS, INTERVALS):

            for stock in stocks:
                history_df = stock.history(period=period, interval=interval)
                history_df['Relative Close'] = 100 * (history_df['Close'] / history_df['Close'].iloc[0] - 1) # ?? Move to a separate task ??

                history_df.insert(0, 'Ticker', stock.info['symbol'])
                history_df.insert(1, 'Period', period)
                history_df.insert(2, 'Interval', interval)
                history_dfs.append(history_df)

        history_df = pd.concat(history_dfs).reset_index()
        history_df.to_parquet(path.join(RAW_DIR, 'stocks_history.parquet'), index=False)

    @task
    def get_news_raw_df(stocks):
        news = []

        for stock in stocks:
            for new in stock.news:
                news.append(new)

        news_df = pd.DataFrame(news).drop_duplicates(subset='uuid').reset_index()
        news_df.to_parquet(path.join(RAW_DIR, 'stocks_news.parquet'), index=False)
        return news_df

    @task
    def get_news_bronze_df(news_df):
        sentiment_pipeline = pipeline(
            "text-classification",
            model="mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis"
        )

        texts_df = news_df[['uuid', 'link']]
        texts_df['text'] = texts_df['link'].apply(get_text_from_link)
        texts_df['text_len'] = texts_df['text'].str.len()
        texts_df = texts_df.drop(columns='link')
        # news_df = news_df.merge(texts_df, how='left', on='uuid') # add text and len columns

        sentiment_df = texts_df.query('text_len < 514') # token window of our txt-classification model used
        sentiment_df['sentiment'] = sentiment_df['text'].apply(sentiment_pipeline).explode()
        # sentiment_df.join(sentiment_df.pop('sentiment').apply(pd.Series)).rename(columns={'label': 'sentiment_label', 'score': 'sentiment_score'})
        sentiment_df = sentiment_df.drop(columns=['text', 'text_len'])
        news_df = news_df.merge(sentiment_df, how='left', on='uuid')

        news_df.to_parquet(path.join(BRONZE_DIR, 'stocks_news.parquet'), index=False)

    info_df = get_info_raw_df(STOCKS)

    income_stmt_raw_df = get_income_stmt_raw_df(STOCKS)
    get_income_stmt_bronze_df(income_stmt_raw_df)

    balance_sheet_raw_df = get_balance_sheets_raw_df(STOCKS)
    get_balance_sheets_bronze_df(balance_sheet_raw_df, income_stmt_raw_df)

    cashflow_raw_df = get_cashflow_raw_df(STOCKS)
    get_cashflow_bronze_df(cashflow_raw_df, income_stmt_raw_df)

    get_history_raw_df(STOCKS)

    news_df = get_news_raw_df(STOCKS)
    get_news_bronze_df(news_df)