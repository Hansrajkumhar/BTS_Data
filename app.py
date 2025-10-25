import os
import json
from typing import Tuple

import pandas as pd
import gspread
from flask import Flask, jsonify

# --------- Google auth (Render-friendly) ----------
# Put your entire service-account JSON into an env var called GOOGLE_CREDENTIALS_JSON
# And share your Google Sheets with the service account's email.
def get_gspread_client() -> gspread.Client:
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON env var is missing")
    creds_dict = json.loads(creds_json)
    return gspread.service_account_from_dict(creds_dict)

# --------- Helpers ----------
def df_from_worksheet(ws: gspread.Worksheet) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values or len(values) < 2:
        raise ValueError(f"No data found in sheet '{ws.title}'")
    header, rows = values[0], values[1:]
    df = pd.DataFrame(rows, columns=header)
    # Normalize column names a bit
    df.columns = [c.strip() for c in df.columns]
    return df

def write_df(ws: gspread.Worksheet, df: pd.DataFrame) -> None:
    data = [list(df.columns)] + df.fillna("").astype(str).values.tolist()
    ws.clear()
    ws.update("A1", data, value_input_option="RAW")

def load_data(gc: gspread.Client) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Source workbook/sheets (adjust names to yours)
    sh = gc.open("BTS_10_NEW")
    worksheet = sh.worksheet("FR3")
    scrd = sh.worksheet("scrdata")

    # Destination / reference workbook
    btspt = gc.open("BTSPT")
    fr_sheet = btspt.worksheet("FR_SHEET")

    df = df_from_worksheet(worksheet)
    scddf = df_from_worksheet(scrd)
    dffrs = df_from_worksheet(fr_sheet)
    return df, scddf, dffrs

def compute(df: pd.DataFrame, scddf: pd.DataFrame, dffrs: pd.DataFrame) -> pd.DataFrame:
    # Expected columns (adjust if your headers differ slightly)
    col_bts_main = "BTS-ID -Don't Change"
    col_bts_scr = "BTS ID"
    col_bts_frs = "BTS-ID -Don't Change"  # in FR_SHEET

    # Safety checks
    missing = [c for c in [col_bts_main] if c not in df.columns] + \
              [c for c in [col_bts_scr, "Project"] if c not in scddf.columns] + \
              [c for c in [col_bts_frs] if c not in dffrs.columns]
    if missing:
        raise KeyError(f"Missing expected columns: {missing}")

    # Filter where main.BTS-ID exists in scrdata["BTS ID"]
    mask = df[col_bts_main].isin(scddf[col_bts_scr])
    filtered = df.loc[mask].copy()

    # Drop columns by index slice 14:21 if present
    drop_idx = [i for i in range(14, 21) if i < filtered.shape[1]]
    if drop_idx:
        filtered = filtered.drop(filtered.columns[drop_idx], axis=1)

    # Join Project from scrdata
    merged = filtered.merge(
        scddf[[col_bts_scr, "Project"]],
        left_on=col_bts_main, right_on=col_bts_scr, how="inner"
    ).drop(columns=[col_bts_scr])

    # Remove rows already present in FR_SHEET (avoid duplicates)
    result = merged[~merged[col_bts_main].isin(dffrs[col_bts_frs])].copy()
    return result

# --------- Flask app ----------
app = Flask(__name__)

@app.get("/healthz")
def healthz():
    return "ok"

@app.post("/api/run")
@app.get("/api/run")  # allow GET for easy testing
def run_job():
    try:
        gc = get_gspread_client()
        df, scddf, dffrs = load_data(gc)
        out = compute(df, scddf, dffrs)

        # Optional: write output into a new sheet in BTSPT
        btspt = gc.open("BTSPT")
        # Create or reuse a sheet named "FR_RESULT"
        try:
            out_ws = btspt.worksheet("FR_RESULT")
        except gspread.WorksheetNotFound:
            out_ws = btspt.add_worksheet(title="FR_RESULT", rows="1000", cols="50")

        write_df(out_ws, out)

        return jsonify({
            "status": "ok",
            "input_rows": len(df),
            "matched_in_scrdata": int((df["BTS-ID -Don't Change"].isin(scddf["BTS ID"])).sum()),
            "existing_in_fr_sheet": int((df["BTS-ID -Don't Change"].isin(dffrs["BTS-ID -Don't Change"])).sum()),
            "new_rows_written": len(out),
            "output_sheet": "BTSPT / FR_RESULT"
        })
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
