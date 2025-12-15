# PDF Data Extractor

Imports
```python
import pandas as pd
import pdfplumber

import warnings
warnings.filterwarnings('ignore')
```

Extract Data
```python
def extract_tables(input_file: str) -> pd.DataFrame:
    dfs = []
    with pdfplumber.open(input_file) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            for table in tables:
                df = pd.DataFrame(table)
    
                table_name = " ".join(df.iloc[0].dropna().astype(str)).strip().split("\n")[0]
                header = " ".join(df.iloc[0].dropna().astype(str)).strip().split("\n")[-1]
                cols = df.iloc[1].astype(str).str.strip().tolist()
    
                df = df[2:].copy()
                df.columns = cols
                df["Chapter"] = table_name
                df["Page"] = page_number
                df["Header"] = header
    
                dfs.append(df)
    merged_data = pd.concat(dfs[1:], ignore_index=True)
    merged_data["Source Description"] = input_file.split("\\")[-1].split(".")[0]
    
    return merged_data    
```
Transform Data
```python
def insert_chapter_separators(df: pd.DataFrame) -> pd.DataFrame:
    all_cols = df.columns.tolist()
    new_rows = []
    prev_chapter = None
    
    for idx, row in df.iterrows():
        current_chapter = row["Chapter"]
        if current_chapter != prev_chapter:
            separator_row = {col: "" for col in all_cols}
            separator_row["Chapter"] = current_chapter
            separator_row["Page"] = row["Page"]
            separator_row["Source Decription"] = row["Source Description"]
            new_rows.append(separator_row)
        new_rows.append(row.to_dict())
        prev_chapter = current_chapter

    separated_data = pd.DataFrame(new_rows, columns=all_cols)
    separated_data["Abstraction Level"] = separated_data["Description"].str.extract(r"^(\.+)").fillna("").map(lambda x: len(x) + 3)

    return separated_data
```

Format Output Data
```python
def write_data(df, farm, output_name, write_to_excel=False):
    cols = ["Site", "Header", "Material", "Material Description", "Quantity", "Unit of Measure", "Position on SPC",
        "Source ID", "Source Description", "Chapter", "Pos", "Page", "Assembly", "Level", "Level Test", "Parent"]
    out_df = pd.DataFrame(columns=cols)
    out_df[["Header", "Material", "Chapter", "Page", "Source Description", "Level"]] = df[["Header", "Part Nbr", "Chapter", "Page", "Source Description", "Abstraction Level"]]
    out_df["Site"] = farm
    out_df["Material Description"] = df["Description"].str.strip(". ")
    out_df[["Quantity", "Unit of Measure"]] = df["QTY"].str.extract(r"([0-9.,]+)\s*(\w+)").fillna("")
    out_df["Pos"] = df["POS"].apply(lambda x: f"Pos. {x}")
    out_df["Position on SPC"] = out_df.apply(lambda row: f"{row["Pos"]} ({row["Chapter"]})", axis=1)
    out_df["Source ID"] = out_df["Source ID"].fillna("")
    out_df["Level Test"] = df["Description"]
    out_df["Assembly"] = out_df["Assembly"].fillna("")
    out_df["Parent"] = out_df["Parent"].fillna("")
    out_df = out_df[~out_df["Chapter"].str.startswith("Document no:", na=False)]

    if write_to_excel:
        out_df.to_excel(output_name, index=False)
    
    return out_df
```

Put all together
```python
files_info = [
    ("file_path", "info"),
    ("file_path", "info")
]

all_final_dfs = []
for file_path, farm in files_info:
    df = extract_tables(file_path)
    df = insert_chapter_separators(df)
    output_file = file_path.split("\\")[-1].split(".")[0] + ".xlsx"
    df_final = write_data(df, farm, output_file)
    all_final_dfs.append(df_final)

merged_data = pd.concat(all_final_dfs, ignore_index=True)
merged_data.to_excel("DT_SB_Catalog_Complete.xlsx", index=False)
```

Data Validation
```python
merged_data2 = merged_data.copy()
merged_data2["Pos_num"] = merged_data2["Pos"].str.extract(r"Pos\. (\d+)")[0].astype(float).fillna(0).astype(int)
max_pos_df = merged_data2.loc[merged_data2.groupby("Page")["Pos_num"].idxmax()]

validation_df = max_pos_df[["Material", "Pos", "Page"]]
validation_df.to_excel("validation_matrix.xlsx", index=False)
```
