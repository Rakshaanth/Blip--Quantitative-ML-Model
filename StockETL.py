
from fileinput import filename
import requests
import json
from pathlib import Path
from dotenv import load_dotenv
import os
import time
import pandas as pd
 

load_dotenv()

core_metric = "TIME_SERIES_MONTHLY_ADJUSTED" # different key than fundamental metrics
fundamental_metrics = ["INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW","EARNINGS", "SHARES_OUTSTANDING"] # same URL and key format

date_colCore = None # for TIME_SERIES_MONTHLY_ADJUSTED (index already dates)
date_colFundamentals = "fiscalDateEnding" # for INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW, EARNINGS
date_colShares = "date" # for SHARES_OUTSTANDING


class AlphaVantageExtractor:

    ''' Keys within JSON responses for corresponding function(s)'''
    keyMonth = "Monthly Adjusted Time Series" # for TIME_SERIES_MONTHLY_ADJUSTED
    keyReports = "quarterlyReports" # for INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW
    keyEarnings = "quarterlyEarnings" # for EARNINGS
    keyShares = "data" # for SHARES_OUTSTANDING


    def APIFetch(self, function: str) -> dict:
        API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")  # Replace with your actual API key or load from environment
        SYMBOL = "ORCL"
        
        url = f'https://www.alphavantage.co/query?function={function}&symbol={SYMBOL}&apikey={API_KEY}'
        r = requests.get(url)
        data = r.json()
        
        return data
    
    def StoreJSON(self, function:str):
        SYMBOL = "ORCL"
        OUT_DIR = Path("data/raw/metrics")
        OUT_DIR.mkdir(parents=True, exist_ok=True)

        data = self.APIFetch(function)

        ''' Handle different function naming conventions for file saving '''

        if function == self.core_metric or function in self.fundamental_metrics:
            filename = f"{SYMBOL.replace(':', '_')}_{function}.json"

            try:
                if function == self.core_metric:
                    if self.keyMonth not in data:
                        raise ValueError(f"Expected key '{self.keyMonth}' not found in response.")
                elif function in self.fundamental_metrics:
                    if function in ["INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW"]:
                        if not any(key in data for key in self.keyReports):
                            raise ValueError(f"Expected keys '{self.keyReports}' not found in response.")
                    elif function == "EARNINGS":
                        if not any(key in data for key in self.keyEarnings):
                            raise ValueError(f"Expected keys '{self.keyEarnings}' not found in response.")
                    elif function == "SHARES_OUTSTANDING":
                        if self.keyShares not in data:
                            raise ValueError(f"Expected key '{self.keyShares}' not found in response.")
                with open(OUT_DIR / filename, 'w') as f:
                    json.dump(data, f, indent=4)
                print(f"Saved data to {OUT_DIR / filename}")    
            except Exception as e:
                print(f"Error saving data: {e}")

class AlphaVantageTransformer:
    """Set consistent date index for all JSON data types."""

    def setIndex(self, filename: str, date_col: str = None) -> pd.DataFrame:
        with open(filename, 'r') as f:
            data = json.load(f)

        # Determine the correct key
        if "MONTHLY_ADJUSTED" in filename:
            key = AlphaVantageExtractor.keyMonth
            df = pd.DataFrame(data[key]).T  # transpose to get dates as index
        elif "INCOME_STATEMENT" in filename or "BALANCE_SHEET" in filename or "CASH_FLOW" in filename:
            key = AlphaVantageExtractor.keyReports
            df = pd.DataFrame(data[key])
        elif "EARNINGS" in filename:
            key = AlphaVantageExtractor.keyEarnings
            df = pd.DataFrame(data[key])
        elif "SHARES_OUTSTANDING" in filename:
            key = AlphaVantageExtractor.keyShares
            df = pd.DataFrame(data[key])
        else:
            raise ValueError("Unknown file type for setting index.")

        # Set date index
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col])
            df.set_index(date_col, inplace=True)
        else:
            df.index = pd.to_datetime(df.index)

        return df.sort_index()


    def stripQuarter(self, filename: str) -> pd.DataFrame:
        """
        Load Alpha Vantage fundamentals JSON and keep ONLY quarterly data.
        Annual data is discarded.
        """
        with open(filename, "r") as f:
            data = json.load(f)

        if "INCOME_STATEMENT" in filename or "BALANCE_SHEET" in filename or "CASH_FLOW" in filename:
            rows = data["quarterlyReports"]

        elif "EARNINGS" in filename:
            rows = data["quarterlyEarnings"]

        elif "SHARES_OUTSTANDING" in filename:
            rows = data["data"]  # already quarterly

        else:
            raise ValueError("stripQuarter only valid for fundamentals / shares")

        df = pd.DataFrame(rows)

        date_col = "date" if "date" in df.columns else "fiscalDateEnding"
        df[date_col] = pd.to_datetime(df[date_col])
        df.set_index(date_col, inplace=True)
        df.attrs["source"] = Path(filename).stem


        return df.sort_index()


    def mergeIndex(self, df_monthly: pd.DataFrame, df_quarterly_list: list) -> pd.DataFrame:
        """
        Merge quarterly fundamentals into monthly price data.

        df_monthly:
            DataFrame indexed by monthly dates (e.g., prices).
            This is the master time index.

        df_quarterly_list:
            List of DataFrames indexed by quarterly dates
            (income, balance sheet, cash flow, shares, etc.)

        Returns:
            Single DataFrame with monthly rows and
            quarterly values forward-filled after report dates.
        """

        # Start with a copy of monthly data so original is untouched
        merged = df_monthly.copy()

        # Loop through each quarterly fundamentals DataFrame
        for df_q in df_quarterly_list:
            # Ensure quarterly data is ordered by time
            # (critical before reindexing / forward filling)
            df_q = df_q.sort_index()

            #In case of overlapping/ duplicate columns retain only one set
            overlapping_cols = merged.columns.intersection(df_q.columns)
            df_q = df_q.drop(columns=overlapping_cols)

            # Reindex quarterly data to monthly index:
            # - quarterly dates are matched to monthly dates
            # - values are forward-filled AFTER the quarter date
            # - months before first quarter remain NaN (no leakage)
            aligned_q = df_q.reindex(merged.index, method="ffill")
            # Join the aligned quarterly columns onto the monthly DataFrame
            merged = merged.join(aligned_q)

        # Return the fully merged monthly + quarterly dataset
        return merged

class AlphaVantageLoader:
    def saveFile(self, df: pd.DataFrame, filename: str) -> None:
        out_path = Path(filename)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if out_path.suffix == ".csv":
            df.to_csv(out_path)
        elif out_path.suffix == ".xlsx":
            df.to_excel(out_path)
        else:
            raise ValueError("Unsupported file type")

        print(f"Saved file to {out_path}")


    



if __name__ == "__main__":
    extract = AlphaVantageExtractor()
    transform = AlphaVantageTransformer()
    load = AlphaVantageLoader()
    
    df_monthlyPrice = transform.setIndex("data/raw/metrics/ORCL_TIME_SERIES_MONTHLY_ADJUSTED.json")

    df_quartlerylyList = ["data/raw/metrics/ORCL_BALANCE_SHEET.json",
                       "data/raw/metrics/ORCL_CASH_FLOW.json",
                        "data/raw/metrics/ORCL_INCOME_STATEMENT.json",
                        "data/raw/metrics/ORCL_EARNINGS.json",
                        "data/raw/metrics/ORCL_SHARES_OUTSTANDING.json"] 

    df_quartlerylyData = [transform.stripQuarter(i) for i in df_quartlerylyList]

    df_merged = transform.mergeIndex(df_monthlyPrice, df_quartlerylyData)

    load.saveFile(df_merged, "data/processed/ORCL_merged.csv")
    load.saveFile(df_merged, "data/processed/ORCL_merged.xlsx")