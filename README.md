# PDF Data Extractor

Imports
```python
import pandas as pd
import pdfplumber

import warnings
warnings.filterwarnings('ignore')
```

User input
```python
input_file_path = "FILEPATH.pdf"
# MAX_PAGES = 15 # for testing purposes
```

Extract tables into df
```python
dfs = []
with pdfplumber.open(input_file_path) as pdf:
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
    merged_data["Source Description"] = input_file_path.split("\\")[-1].split(".")[0]
```

Insert Chapter Separators
```python
all_cols = merged_data.columns.tolist()
new_rows = []
prev_chapter = None

for idx, row in merged_data.iterrows():
    current_chapter = row["Chapter"]
    if current_chapter != prev_chapter:
        separator_row = {col: "" for col in all_cols}
        separator_row["Chapter"] = current_chapter
        separator_row["Page"] = row["Page"]
        separator_row["Source Description"] = row["Source Description"]
        new_rows.append(separator_row)
    new_rows.append(row.to_dict())
    prev_chapter = current_chapter

separated_data = pd.DataFrame(new_rows, columns=all_cols)
separated_data["Abstraction Level"] = separated_data["Description"].str.extract(r"^(\.+)").fillna("").map(lambda x: len(x) + 3)
```

Format Output Data
```python
farm = "DanTysk"
cols = ["Site", "Header", "Material", "Material Description", "Quantity", "Unit of Measure", "Position on SPC",
        "Source ID", "Source Description", "Chapter", "Pos", "Page", "Assembly", "Level", "Level Test", "Parent"]
out_df = pd.DataFrame(columns=cols)
out_df[["Header", "Material", "Chapter", "Page", "Source Description", "Level"]] = separated_data[["Header", "Part Nbr", "Chapter", "Page", "Source Description", "Abstraction Level"]]
out_df["Site"] = farm
out_df["Material Description"] = separated_data["Description"].str.strip(". ")
out_df[["Quantity", "Unit of Measure"]] = separated_data["QTY"].str.extract(r"([0-9.,]+)\s*(\w+)").fillna("")
out_df["Pos"] = separated_data["POS"].apply(lambda x: f"Pos. {x}")
out_df["Position on SPC"] = out_df.apply(lambda row: f"{row["Pos"]} ({row["Chapter"]})", axis=1)
out_df["Source ID"] = out_df["Source ID"].fillna("")
out_df["Level Test"] = separated_data["Description"]
out_df["Assembly"] = out_df["Assembly"].fillna("")
out_df["Parent"] = out_df["Parent"].fillna("")

out_df.to_excel(input_file_path.split("\\")[-1].split(".")[0] + ".xlsx", index=False)
```
