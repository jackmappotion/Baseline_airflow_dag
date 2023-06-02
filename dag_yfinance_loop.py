import datetime

import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain

def _check_yfinance(stock_name):
    _interval = "1m"
    _start_date = (datetime.datetime.today() - datetime.timedelta(days=7)).date().strftime("%Y-%m-%d")
    _end_date = datetime.datetime.today().date().strftime("%Y-%m-%d")

    ticker = yf.Ticker(stock_name)
    df = ticker.history(interval=_interval, start=_start_date, end=_end_date)
    return None

def load_stock_price(stock_name):
    # Params (today-7days ~ today)
    _interval = "1m"
    _start_date = (datetime.datetime.today() - datetime.timedelta(days=7)).date().strftime("%Y-%m-%d")
    _end_date = datetime.datetime.today().date().strftime("%Y-%m-%d")

    ticker = yf.Ticker(stock_name)

    df = ticker.history(interval=_interval, start=_start_date, end=_end_date)

    df.reset_index(inplace=True)
    df.columns = [col.strip().lower() for col in df.columns]

    df["datetime"] = df["datetime"].apply(lambda x: x.tz_localize(None))

    df.index += 1
    df.index.name = "id"
    return df

""" save2db """
def save2db(db_url, df, stock_name):
    db = create_engine(db_url)

    try:
        # if table doesnt exist
        df.to_sql(
            name=stock_name,
            con=db,
            index=True,
            if_exists="fail",
        )
        with db.connect() as con:
            con.execute(text(f"ALTER TABLE `{stock_name}` ADD PRIMARY KEY auto_increment(`id`);"))
            con.execute(text(f"ALTER TABLE `{stock_name}` MODIFY id INT NOT NULL AUTO_INCREMENT;"))

    except ValueError:
        db_df = pd.read_sql(f"SELECT datetime FROM {stock_name}", db)
        df_append = (
            db_df.merge(df, on=["datetime"], how="outer", indicator=True)
            .query('_merge=="right_only"')
            .drop(columns=["_merge"])
        )

        df_append.to_sql(
            name=stock_name,
            con=db,
            index=False,
            if_exists="append",
        )
    return None

def _load_and_save(stock_name, db_url):
    df = load_stock_price(stock_name)
    save2db(db_url,df,stock_name)
    return None

with DAG(
    "y_finance_data_loader_loop",
    schedule_interval=None,
    start_date=datetime.datetime(2023, 6, 1),
    catchup=False,
) as dag:
    check_yfinance = PythonOperator(
        task_id="check_yfinance",
        python_callable=_check_yfinance,
        op_kwargs={"stock_name":"AAPL"}
        )
    
    load_and_save_list = list() 

    for stock_name in ['AAPL','GOOG','GOOGL','NVDA','MBINO']:
        load_and_save_list.append(PythonOperator(
            task_id=f"load_and_save_{stock_name}",
            python_callable=_load_and_save,
            op_kwargs={
                "stock_name":stock_name,
                "db_url":"mysql+pymysql://admin:6569@localhost:3306/mydb",
                }
            ))
    chain(check_yfinance,*load_and_save_list)

