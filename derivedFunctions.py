
"""
Clean record of derived feature functions
To be used in DataFormatting.py

Each function:
- Takes a DataFrame
- Adds ONE derived column
- Handles divide-by-zero safely
- Does NOT drop rows
"""

import numpy as np
import pandas as pd


def safe_divide(a, b):
    return np.where((b == 0) | (b.isna()), np.nan, a / b)

def monthly_return(df):
    df["monthly_return"] = df["adjusted close"].pct_change()
    return df


def monthly_volatility(df, window=3):
    df["monthly_volatility"] = df["monthly_return"].rolling(window).std()
    return df


def price_range_pct(df):
    df["price_range_pct"] = safe_divide(
        df["high"] - df["low"], df["close"]
    )
    return df


def volume_change(df):
    df["volume_change"] = df["volume"].pct_change()
    return df

def profit_margin(df):
    df["profit_margin"] = safe_divide(
        df["netIncome"], df["totalRevenue"]
    )
    return df


def perating_margin(df):
    df["operating_margin"] = safe_divide(
        df["operatingIncome"], df["totalRevenue"]
    )
    return df


def gross_margin(df):
    df["gross_margin"] = safe_divide(
        df["grossProfit"], df["totalRevenue"]
    )
    return df

def current_ratio(df):
    df["current_ratio"] = safe_divide(
        df["totalCurrentAssets"], df["totalCurrentLiabilities"]
    )
    return df


def debt_to_equity(df):
    df["debt_to_equity"] = safe_divide(
        df["totalLiabilities"], df["totalShareholderEquity"]
    )
    return df

def free_cash_flow(df):
    df["free_cash_flow"] = (
        df["operatingCashflow"] - df["capitalExpenditures"]
    )
    return df


def fcf_margin(df):
    df["fcf_margin"] = safe_divide(
        df["free_cash_flow"], df["totalRevenue"]
    )
    return df

def ebitda_margin(df):
    df["ebitda_margin"] = safe_divide(
        df["ebitda"], df["totalRevenue"]
    )
    return df


def interest_coverage(df):
    df["interest_coverage"] = safe_divide(
        df["ebit"], df["interestExpense"]
    )
    return df




