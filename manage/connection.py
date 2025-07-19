from typing import Optional

from pandas import DataFrame
import pandas as pd
import psycopg2

from settings import DATABASE


def fetch(query: str, params: Optional[tuple] = None) -> DataFrame:
    conn = psycopg2.connect(**DATABASE)
    dataframe = pd.read_sql(query, conn, params=params)
    conn.close()
    return dataframe