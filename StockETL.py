
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
    """Transform JSON data into a workable DataFrame format.
    Match and Merge Fundamentals on Fiscal Date Ending. Set the index to Reported Date
    Strip off annual data from Fundamentals.
    Then merge with Core Metrics (monthly adjusted) with Quarterly Reported data.
    """
    def setFIndex(self, filename: str) -> pd.DataFrame:
        # Load json data
        with open(filename, 'r') as f:
            data = json.load(f)
        
        #Determine key from filename and set date column
        if  "INCOME_STATEMENT"or  "BALANCE_SHEET" or "CASH_FLOW" in filename:
            rows = data["quarterlyReports"]
            date_col = "fiscalDateEnding"
        elif "EARNINGS" in filename:
            rows = data["quarterlyEarnings"]
            date_col = "fiscalDateEnding"
        elif "SHARES_OUTSTANDING" in filename:
            rows = data["data"]
            date_col = "date"
        else:
            raise ValueError("Unsupported filename for fundamentals")
        
        # Build DataFrame
        df = pd.DataFrame(rows)

        #convert date column to datetime
        df[date_col] = pd.to_datetime(df[date_col])
        df.attrs['date_col'] = date_col # Store date column name in attrs for later merge use
        print(f"Set index for {filename} on {date_col}")

        return df

    def stripQuarter(self, filename: str) -> pd.DataFrame:
        """
        Load Alpha Vantage fundamentals JSON and keep ONLY quarterly data.
        Safely handles missing keys and strips annual data.
        """
        with open(filename, "r") as f:
            data = json.load(f)

        fname = filename.upper()
        if "INCOME_STATEMENT" in fname or "BALANCE_SHEET" in fname or "CASH_FLOW" in fname:
            rows = data.get("quarterlyReports", [])
            date_col = "fiscalDateEnding"
        elif "EARNINGS" in fname:
            rows = data.get("quarterlyEarnings", [])
            date_col = "fiscalDateEnding"
        elif "SHARES_OUTSTANDING" in fname:
            rows = data.get("data", [])
            date_col = "date"
        else:
            raise ValueError(f"Unsupported file type: {filename}")

        if not rows:
            print(f"Warning: No quarterly data found in {filename}")
            return pd.DataFrame()  # return empty DF if missing

        df = pd.DataFrame(rows)
        df[date_col] = pd.to_datetime(df[date_col])
        df.attrs["date_col"] = date_col
        print(f"Processed {filename}, keeping only quarterly data.")
        return df.sort_values(by=date_col)

    def merge_fundamentals(self, fundamental_dfs: list) -> pd.DataFrame:
        """
        Merge multiple fundamental DataFrames based on their fiscal/date column.
        """
        if not fundamental_dfs:
            return pd.DataFrame()

        # Start with first DataFrame
        merged = fundamental_dfs[0].copy()
        date_col = merged.attrs.get("date_col", "fiscalDateEnding")

        for df in fundamental_dfs[1:]:
            df_copy = df.copy()
            df_date_col = df_copy.attrs.get("date_col", "fiscalDateEnding")

            # Drop overlapping columns
            overlapping_cols = merged.columns.intersection(df_copy.columns)
            overlapping_cols = [c for c in overlapping_cols if c != date_col and c != df_date_col]
            df_copy = df_copy.drop(columns=overlapping_cols, errors='ignore')

            # Merge on the date column
            merged = pd.merge(
                merged,
                df_copy,
                left_on=date_col,
                right_on=df_date_col,
                how="outer"
            )

        # Sort by date column
        merged = merged.sort_values(by=date_col)
        mergedFile = merged.to_excel("data/processed/ORCL_Fundamentals_Merged.xlsx")
        print("Fundamentals Merged and Saved as excel")
        return merged

    def setIndex(self, filename: str) -> pd.DataFrame:

        if "Fundamentals_Merged" in filename:

            df = pd.read_excel(filename)
            date_col = "reportedDate"
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.set_index(date_col)
            print("Fundamentals Indexed by Reported Date")
        # Core Metrics
        elif "TIME_SERIES_MONTHLY_ADJUSTED" in filename:
            with open(filename, 'r') as f:
                data = json.load(f)
                rows = data["Monthly Adjusted Time Series"] 
                df = pd.DataFrame.from_dict(rows, orient='index') #
                df.index = pd.to_datetime(df.index)
                df = df.sort_index()

            IndexmonthlyAdjusted = df.to_excel("data/processed/ORCL_Monthly_Adjusted_Index.xlsx")
            print("Core Metrics Indexed by Date")
            return df
        return df
    
    def merge_core_fundamentals(self, core_df: pd.DataFrame, fund_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge core metrics DataFrame with fundamentals DataFrame on date index.
        """
        # Index of core is mothly dates, fundamentals is quarterly dates
        merged_df = pd.merge_asof()
        pass




'''class AlphaVantageLoader:
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
'''

    



if __name__ == "__main__":
    extract = AlphaVantageExtractor()
    transform = AlphaVantageTransformer()
    # load = AlphaVantageLoader()

    FundamentalFiles = [
    "data/raw/metrics/ORCL_BALANCE_SHEET.json",
    "data/raw/metrics/ORCL_CASH_FLOW.json",
    "data/raw/metrics/ORCL_INCOME_STATEMENT.json",
    "data/raw/metrics/ORCL_EARNINGS.json",
    "data/raw/metrics/ORCL_SHARES_OUTSTANDING.json"
]    

    fundamental_dfs = [transform.stripQuarter(f) for f in FundamentalFiles]
    merged_df = transform.merge_fundamentals(fundamental_dfs)
    FundamentalsIndexed = transform.setIndex("data/processed/ORCL_Fundamentals_Merged.xlsx")
    CoreMetricsIndexed = transform.setIndex("data/raw/metrics/ORCL_TIME_SERIES_MONTHLY_ADJUSTED.json")