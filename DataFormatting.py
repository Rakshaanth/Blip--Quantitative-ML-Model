
import pandas as pd

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

    def addDerivedColumns(self, filename: str, columns: list):
        """
        Adds derived columns to a .xlsx file based on existing data and returns the modified DataFrame.

        Parameters:
        filename (str): The path to the .xlsx file.

        Returns:
        pd.DataFrame: The DataFrame after adding the derived columns.
        """
        # Read the Excel file into a DataFrame
        df = pd.read_excel(filename)    

        df.add(columns)

        
        
        

        return df


if __name__ == "__main__":

    format = formatData()
    # format.duplicateFile(r"data\processed\ORCL_CoreMonthly_Fundamentals_Merged.xlsx")
    columnsRemove = ["Unnamed: 0", "date", "reportedCurrency", "fiscalDateEnding", "reportedDate", "reportTime", "accumulatedDepreciationAmortizationPPE", "investments", "longTermInvestments", "otherCurrentAssets", "otherNonCurrentAssets", "deferredRevenue", "deferredRevenue", "currentDebt", "capitalLeaseObligations", "currentLongTermDebt", "longTermDebtNoncurrent", "otherCurrentLiabilities", "otherNonCurrentLiabilities", "treasuryStock", "commonStock", "paymentsForOperatingActivities", "proceedsFromOperatingActivities", "changeInOperatingLiabilities", "changeInOperatingAssets", "changeInReceivables", "changeInInventory", "profitLoss", "proceedsFromRepaymentsOfShortTermDebt", "paymentsForRepurchaseOfCommonStock", "paymentsForRepurchaseOfEquity", "paymentsForRepurchaseOfPreferredStock", "dividendPayoutCommonStock", "dividendPayoutPreferredStock", "proceedsFromIssuanceOfCommonStock", "proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet", "proceedsFromIssuanceOfPreferredStock", "proceedsFromSaleOfTreasuryStock", "changeInExchangeRate", "costOfRevenue", "costofGoodsAndServicesSold", "investmentIncomeNet", "netInterestIncome", "interestIncome", "nonInterestIncome", "otherNonOperatingIncome", "depreciation", "incomeTaxExpense", "interestAndDebtExpense", "comprehensiveIncomeNetOfTax", "surprise"]
    
    derivedColumns =[]
    format.deleteColumns(r"data\processed\ORCL_CoreMonthly_Fundamentals_Merged_copy.xlsx", columnsRemove)