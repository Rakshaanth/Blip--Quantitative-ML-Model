
import pandas as pd
from DerivedFunctions import (
    monthly_return,
    monthly_volatility,
    price_range_pct,
    volume_change,
    profit_margin,
    operating_margin,
    gross_margin,
    current_ratio,
    debt_to_equity,
    free_cash_flow,
    fcf_margin,
    ebitda_margin,
    interest_coverage
)



class formatData:
    def duplicateFile(self, filename: str):
        ''' Creates a duplicate of the specified .xlsx file with '_copy' appended to the filename. '''

        # Read the original Excel file
        df = pd.read_excel(filename)
        # Create the new filename by appending '_copy' before the file extension
        new_filename = filename.replace('.xlsx', '_copy.xlsx')
        # Save the DataFrame to the new file
        df.to_excel(new_filename, index=False)

    def deleteColumns(self, filename: str, columns: list) -> pd.DataFrame:
        """
        Deletes specified columns from a xlsx file and returns the modified DataFrame.

        Parameters:
        filename (str): The path to the xlsx file.
        columns (list): A list of column names to be deleted.

        Returns:
        pd.DataFrame: The DataFrame after deleting the specified columns.
        """
        # Read the xlsx file into a DataFrame
        df = pd.read_excel(filename)

        # Drop the specified columns
        df.drop(columns=columns, inplace=True, errors='ignore')
        print("Unwanted columns deleted.")

        return df

    def addDerivedColumns(self, df: pd.DataFrame, derived_functions: list) -> pd.DataFrame:

        for func in derived_functions:
            df = func(df)

        print("Derived columns added.")
        return df
    
    def cleanColumns(self, df):
        """
        Cleans core market columns:
        """

        # Rename index / unnamed column to DateTime
        df.rename(columns={"Unnamed: 0.1": "DateTime"}, inplace=True)
        

        # Clean numbered column names
        rename_map = {
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. adjusted close": "adjusted_close",
            "6. volume": "volume",
            "7. dividend amount": "dividend_amount"
        }

        df.rename(columns=rename_map, inplace=True)

        df.set_index("DateTime", inplace =True)

        return df


    def saveFileparquet(self, df):

        df.to_parquet("data/final/ORCL_features.parquet", index = False)
        print("Production file saved as '.praquet'.")

        return df

    def saveFileCSV(self, df):

        df.to_csv("data/final/ORCL_features.csv", index = True)
        print("Production file saved as 'CSV'.")

        return df



if __name__ == "__main__":

    Symbol = "ORCL"
    original_file = rf"data\processed\{Symbol}_CoreMonthly_Fundamentals_Merged.xlsx"

    # Initialize the formatData class
    format = formatData()

    # Step 1: Duplicate the file
    format.duplicateFile(original_file)
    # After duplication, work with the copy
    fileURL = rf"data\processed\{Symbol}_CoreMonthly_Fundamentals_Merged_copy.xlsx"

    # Step 2: Columns to remove
    columnsRemove = [
        "Unnamed: 0", "date", "reportedCurrency", "fiscalDateEnding", "reportedDate",
        "reportTime", "accumulatedDepreciationAmortizationPPE", "investments",
        "longTermInvestments", "otherCurrentAssets", "otherNonCurrentAssets",
        "deferredRevenue", "currentDebt", "capitalLeaseObligations", "currentLongTermDebt",
        "longTermDebtNoncurrent", "otherCurrentLiabilities", "otherNonCurrentLiabilities",
        "treasuryStock", "commonStock", "paymentsForOperatingActivities",
        "proceedsFromOperatingActivities", "changeInOperatingLiabilities",
        "changeInOperatingAssets", "changeInReceivables", "changeInInventory",
        "profitLoss", "proceedsFromRepaymentsOfShortTermDebt", "paymentsForRepurchaseOfCommonStock",
        "paymentsForRepurchaseOfEquity", "paymentsForRepurchaseOfPreferredStock",
        "dividendPayoutCommonStock", "dividendPayoutPreferredStock",
        "proceedsFromIssuanceOfCommonStock", "proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet",
        "proceedsFromIssuanceOfPreferredStock", "proceedsFromSaleOfTreasuryStock",
        "changeInExchangeRate", "costOfRevenue", "costofGoodsAndServicesSold",
        "investmentIncomeNet", "netInterestIncome", "interestIncome", "nonInterestIncome",
        "otherNonOperatingIncome", "depreciation", "incomeTaxExpense",
        "interestAndDebtExpense", "comprehensiveIncomeNetOfTax", "surprise"
    ]

    # Step 3: Derived functions to add
    derived_functions = [
        monthly_return,
        monthly_volatility,
        price_range_pct,
        volume_change,
        profit_margin,
        operating_margin,
        gross_margin,
        current_ratio,
        debt_to_equity,
        free_cash_flow,
        fcf_margin,
        ebitda_margin,
        interest_coverage
    ]

    # Step 4: Delete unwanted columns
    df = format.deleteColumns(fileURL, columnsRemove)

    # Step 5: Add derived columns
    df = format.addDerivedColumns(df, derived_functions)

    # Step 6: Clean column names and set DateTime index
    df = format.cleanColumns(df)

    # Step 7: Save final production files (optional)
    format.saveFileCSV(df)
    format.saveFileparquet(df)
