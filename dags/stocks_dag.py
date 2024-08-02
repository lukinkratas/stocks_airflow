import yfinance as yf
import pandas as pd
from os import path, makedirs, getcwd
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator

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
    fins_df.to_parquet(path.join(RAW_DIR, f'stocks_{fin_page_name}.parquet'))
    return fins_df

def get_rel_df(df):
    shifted_df = df.shift(1).fillna(pd.NA) # fillna pd.NA (by default yf dfs have None and not nan)
    return 100 * (df - shifted_df) / shifted_df.abs()

def get_WACC(info_df, income_stmt_df, balance_sheet_df):
    beta = info_df['beta'] if 'beta' in info_df.keys() else None
    interest_expense = income_stmt_df.loc['Interest Expense',:].values[0] if 'Interest Expense' in income_stmt_df.index else None
    debt = balance_sheet_df.loc['Long Term Debt',:].values[0] if 'Long Term Debt' in balance_sheet_df.index else None
    debt += balance_sheet_df.loc['Other Current Borrowings',:].values[0] if if 'Other Current Borrowings' in balance_sheet_df.index else 0 # current portion of long term debt
    market_cap = info_df['marketCap'] if 'marketCap' in info_df.keys() else None
    eff_tax_rate = income_stmt_df.loc['Tax Rate For Calcs',:].values[0] if 'Tax Rate For Calcs' in income_stmt_df.columns else None # can be calculated alternitevely

    if not all(beta, interest_expense, debt, market_cap, eff_tax_rate):
        return None

    # WACC
    # https://www.wallstreetmojo.com/weighted-average-cost-capital-wacc/

    E = market_cap # equity
    Rf = 0.0402 # risk free rate = https://www.bankrate.com/rates/interest-rates/10-year-treasury-bill/ x yf.Ticker('%5ETNX').info['previousClose'] / 100 # %
    Rm = Rf + 0.046# https://pages.stern.nyu.edu/~adamodar/New_Home_Page/datafile/ctryprem.html
    Ke = Rf + ( Rm - Rf ) * beta # Cost of Equity

    # D = ltd + cpltd # debt
    D = debt
    Kd = interest_expense / D  # Cost of Debt
    # eff_tax_rate = tax_provision / pretax_income
    V = D + E # market value

    return E / V * Ke + D / V * Kd * ( 1 - eff_tax_rate )

def get_dcf(info_df, balance_sheet_df, cashflow_df, growth_rate, discount_rate=0.10):
    last_free_cashflow = cashflow_df.loc['Free Cash Flow',:].values[0] if 'Free Cash Flow' in cashflow_df.columns else None
    nshares = info_df['floatShares'] if 'floatShares' in info_df.keys() else None
    
    if not all(last_free_cashflow, nshares):
        return None, None
    
    cash = balance_sheet_df.loc['Cash And Cash Equivalents',:].values[0] if 'Cash And Cash Equivalents' in balance_sheet_df.columns else None
    debt = info_df['totalDebt'] if 'totalDebt' in info_df.keys() else None

    #DCF
    # https://youtu.be/FT1zntJaP0w?si=JOzjW4zK7jn7I3E9
    # https://youtu.be/FMyOM7IM8k8?si=bzCkaR0GzwLvBsEl

    perpetual_growth_rate = 0.025
    # TODO: try dollar value method, debt/equity method

    future_free_cashflow = last_free_cashflow
    dcf_data = {}

    for yr in range(1, 10):

        future_free_cashflow *= ( 1 + growth_rate )
        present_value = future_free_cashflow /  ( 1 + discount_rate ) ** yr
        dcf_data[yr] = {
            'future_free_cashflow': future_free_cashflow,
            'present_value': present_value
            }

    terminal_future_free_cashflow = future_free_cashflow * ( 1 + perpetual_growth_rate ) / ( discount_rate - perpetual_growth_rate )
    terminal_present_value = terminal_future_free_cashflow /  ( 1 + discount_rate ) ** 10
    dcf_data['terminal'] = {
            'future_free_cashflow': terminal_future_free_cashflow,
            'present_value': terminal_present_value
            }

    dcf_df = pd.DataFrame(dcf_data).transpose()

    enterprise_value = dcf_df['present_value'].sum()
    equity_value = enterprise_value + cash - debt if cash and debt else enterprise_value
    value = equity_value / nshares

    # # porovnat s Enterprise value z Yahoo Statistics
    # print()

    return dcf_df, value

with DAG(dag_id = 'stocks_dag', start_date=datetime(2024, 8, 1), schedule="1 9-22/1 * * MON-FRI") as dag:

    @task
    def get_info_raw_df(stocks):
        # Note: dict + DataFrame once runs 400x faster, than making dfs and concatenating approach (0.0025s x 1s)
        infos = []
        
        for stock in stocks:
            infos.append(stock.info)

        info_df = pd.DataFrame(infos).set_index('symbol')
        info_df.to_parquet(path.join(RAW_DIR, 'stocks_info.parquet'))
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

            income_stmt_df.to_parquet(path.join(BRONZE_DIR, 'stocks_income_stmt.parquet'))

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

        balance_sheet_df.to_parquet(path.join(BRONZE_DIR, 'stocks_balance_sheet.parquet'))

    @task
    def get_cashflow_raw_df(stocks):
        return get_financial_df(stocks, 'cashflow')

    @task
    def get_cashflow_bronze_df(cashflow_df, income_stmt_df):
        # cashflow_df = pd.read_parquet(path.join(RAW_DIR, 'stocks_cashflow.parquet'))
        # income_stmt_df = pd.read_parquet(path.join(RAW_DIR, 'stocks_income_stmt.parquet'))

        if 'Capital Expenditure' in cashflow_df.columns and 'Net Income' in income_stmt_df.columns and (income_stmt_df['Net Income']!=0).all():
            cashflow_df['WB CapEx to NetIncome % (target<25%,>50%nebrat)'] = 100 * cashflow_df['Capital Expenditure'] / income_stmt_df['Net Income']

        cashflow_df.to_parquet(path.join(BRONZE_DIR, 'stocks_cashflow.parquet'))

    @task
    def get_value_raw_df(stocks, info_df, income_stmt_df, balance_sheet_df, cashflow_df):
        values = []

        for stock in stocks:
            # GRMN missing Long Term Debt
            # NVDA missing beta
            free_cashflow_df = cashflow_df.loc[:,'Free Cash Flow']
            free_cashflow_rel_df = get_rel_df(free_cashflow_df).rename(f'Rel {free_cashflow_df.name}')
            free_cashflow_growth_mean = free_cashflow_rel_df.mean()

            if wacc:=get_WACC(info_df, income_stmt_df, balance_sheet_df):
                _, value_mean_gr = get_dcf(info_df, balance_sheet_df, cashflow_df, free_cashflow_growth_mean/100, discount_rate=wacc)
            else:
                _, value_mean_gr = get_dcf(info_df, balance_sheet_df, cashflow_df, free_cashflow_growth_mean/100, discount_rate=0.10)
            _, value_10perc_gr = get_dcf(0.10, 0.10)
            values.append({
                'Ticker': info_df['symbol'],
                'Avg Free Cash Flow Growth [%]': free_cashflow_growth_mean,
                'WACC [%]': 100*wacc if wacc else wacc, # if wacc == None
                'Value_10%_Growth': value_10perc_gr,
                'Value_Mean_Growth': value_mean_gr,
                'Current Price': info_df['currentPrice'],
            })

        values_df = pd.DataFrame(values).set_index('symbol')
        values_df.to_parquet(path.join(RAW_DIR, 'stocks_values.parquet'))

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
        history_df.to_parquet(path.join(RAW_DIR, 'stocks_history.parquet'))

    @task
    def get_news_raw_df(stocks):
        news = []

        for stock in stocks:
            for new in stock.news:
                news.append(new)

        news_df = pd.DataFrame(news).explode('relatedTickers').rename(columns={'relatedTickers': 'Ticker'}).reset_index()
        news_df.to_parquet(path.join(RAW_DIR, 'stocks_news.parquet'))

    info_df = get_info_raw_df(STOCKS)

    income_stmt_raw_df = get_income_stmt_raw_df(STOCKS)
    get_income_stmt_bronze_df(income_stmt_raw_df)

    balance_sheet_raw_df = get_balance_sheets_raw_df(STOCKS)
    get_balance_sheets_bronze_df(balance_sheet_raw_df, income_stmt_raw_df)

    cashflow_raw_df = get_cashflow_raw_df(STOCKS)
    get_cashflow_bronze_df(cashflow_raw_df, income_stmt_raw_df)

    get_value_raw_df(STOCKS, info_df, income_stmt_raw_df, balance_sheet_raw_df, cashflow_raw_df)

    get_history_raw_df(STOCKS)

    get_news_raw_df(STOCKS)