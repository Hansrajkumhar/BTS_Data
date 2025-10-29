import os
import json
import pandas as pd
import numpy as np
import gspread
from typing import Tuple


gc_json = "C:/Users/Hanshraj/Downloads/PYTHON/gleaming-glass-394303-a32a50e54159.json"

# def google_cred():
#    gc = gspread.service_account(filename=gc_json)
#    return gc

def google_cred() -> gspread.Client:
    cred_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not cred_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env var is missing")
    cred_dict = json.loads(cred_json)
    return gspread.service_account_from_dict(cred_dict)

def df_from_worksheet(ws: gspread.worksheet) -> pd.DataFrame:
    values = ws.get_all_values()

    if not values or len(values) < 2:
        raise RuntimeError(f"No data found in sheet : {ws.title}")
    header, rows = values[0], values[1:]
    df = pd.DataFrame(rows, columns=header)

    df.columns = [ c.strip() for c in df.columns]
    return df

def write_df(ws: gspread.worksheet, df: pd.DataFrame) -> None:
    data = [list(df.columns)] + df.fillna("").astype(str).values.tolist()
    ws.clear()
    ws.update("A1", data, value_input_option="RAW")

def load_data(gc: gspread.Client) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    #source workbook/sheet
    sh = gc.open("BTS_10_NEW")
    fr3 = sh.worksheet("FR3")
    scrd = sh.worksheet("scrdata")

    #destination
    btspt = gc.open("BTSPT")
    fr_sheet = btspt.worksheet("FR_SHEET")

    df = df_from_worksheet(fr3)
    scddf = df_from_worksheet(scrd)
    dffrs = df_from_worksheet(fr_sheet)
    return df, scddf, dffrs

def compute(df: pd.DataFrame, scddf: pd.DataFrame, dffrs: pd.DataFrame) -> pd.DataFrame:
    # Expected columns (adjust if your headers differ slightly)
    col_bts_main = "BTS-ID -Don't Change"
    col_bts_scr = "BTS ID"
    col_bts_frs = "BTS-ID -Don't Change"

    #Safety check
    missing = [c for c in [col_bts_main] if c not in df.columns] +\
              [c for c in [col_bts_scr, "Project"] if c not in scddf.columns ] +\
              [c for c in [col_bts_frs] if c not in dffrs.columns]
    if missing:
        raise KeyError(f"Missing expected columns: {missing}") 
    
    # Filter where main.BTS-ID exists in scrdata["BTS ID"]
    mask = df[col_bts_main].isin(scddf[col_bts_scr])
    filtred = df.loc[mask].copy()

    # Drop columns by index slice 14:21 if present
    drop_idx = [i for i in range(14, 21) if i< filtred.shape[1]]
    if drop_idx:
        filtred.drop(filtred.columns[drop_idx], axis=1)


    # Join Project from scrdata
    merged = filtred.merge(
        scddf[[col_bts_main, "Project"]],
        left_on=col_bts_main, right_on=col_bts_scr, how='inner'
    ).drop(columns=[col_bts_scr])

    # Remove rows already present in FR_SHEET (avoid duplicates)
    result = merged[~merged[col_bts_main].isin(dffrs[col_bts_frs])].copy()
    return result
    





if __name__ == "__main__":
    gc = google_cred()  # Create Google Sheets client
    fr3d, scrd, frsd = load_data(gc)  # Load dataframes
    result = compute(fr3d, scrd, frsd)  # Process data
    print(result)
