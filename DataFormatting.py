import pandas as pd

class DataFormatter:
    def showColumns(self, df):  
        count = 0  
        for col in range(len(df.columns)):  
            print(f"{col}: {df.columns[col]}")

    def detectNANs(self, df):
        ''' Detect column with NaN values and print percentage of NaNs ''' 
        nanColumns = []
        for col in df.columns:
            nanCount = df[col].isna().sum()
            if nanCount > 0:
                nanPercentage = (nanCount / len(df)) * 100
                nanColumns.append((col, nanPercentage))
                print(f"Column: {col}, NaN Percentage: {nanPercentage:.2f}%")
        return nanColumns

    

    def numericToFloat(self, df):  
        ''' Convert numeric columns to float type for consistency '''
        numeric_cols = df.select_dtypes(include=['number']).columns  
        df[numeric_cols] = df[numeric_cols].astype(float)  
        print(f"Converted {len(numeric_cols)} numeric columns to float type.\n out of {len(df.columns)} ")
        nonNumeric_cols = []
        for col in df.columns:
            if df[col].dtype != 'float64':
                nonNumeric_cols.append(col)
        return nonNumeric_cols
    
    def dropSpams(self, df):
        ''' Drop non-quantitative columns, that were not converted to float type '''
        nonNumeric_cols = self.numericToFloat(df)
        for col in nonNumeric_cols:
            if col == "Unnamed: 0":
                continue
            df = df.drop(columns=[col])
            print(f"Dropped column: {col}")







if __name__ == "__main__":
    Format = DataFormatter()
    Format.detectNANs(pd.read_excel(r"C:\Users\Dell\Desktop\Python baby!\Stage 3\data\processed\ORCL_merged.xlsx"))





