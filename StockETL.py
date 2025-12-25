
import requests
import json
from pathlib import Path
from dotenv import load_dotenv
import os
import time
import pandas as pd

load_dotenv()

class AlphaVantageExtractor:

    core_metric = "TIME_SERIES_MONTHLY_ADJUSTED" # different key than fundamental metrics
    fundamental_metrics = ["INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW","EARNINGS", "SHARES_OUTSTANDING"] # same URL and key format

    '''' Keys within JSON responses for corresponding function(s)'''
    keyMonth = "Monthly Adjusted Time Series" # for TIME_SERIES_MONTHLY_ADJUSTED
    keyReports = "quarterlyReports", "annualReports" # for INCOME_STATEMENT, BALANCE_SHEET, CASH_FLOW
    keyEarnings = "quarterlyEarnings", "annualEarnings" # for EARNINGS
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
        OUT_DIR = Path("data/raw/prices")
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
                    if function == ["INCOME_STATEMENT", "BALANCE_SHEET", "CASH_FLOW"]:
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
    pass


if __name__ == "__main__":
    fetch = AlphaVantageExtractor()
    # fetch.StoreJSON("TIME_SERIES_MONTHLY_ADJUSTED")
    # time.sleep(2) 
    # fetch.StoreJSON("INCOME_STATEMENT")
    # time.sleep(2)
    # fetch.StoreJSON("BALANCE_SHEET") 
    # time.sleep(2)
    # fetch.StoreJSON("CASH_FLOW")
    # time.sleep(2)
    # fetch.StoreJSON("EARNINGS")
    # time.sleep(2)
    fetch.StoreJSON("SHARES_OUTSTANDING")

