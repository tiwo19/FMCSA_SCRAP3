import streamlit as st
import pandas as pd
import json
from datetime import datetime
from io import StringIO

st.set_page_config(page_title="FMCSA Carrier Dashboard", layout="wide")

st.title("FMCSA Carrier Dashboard")

# Load data
def load_data():
    with open("Scraper/fmcsa_register_enriched.json", "r") as f:
        data = json.load(f)
    # Flatten insurance fields for DataFrame
    for d in data:
        ins = d.get("insurance", {})
        # Flatten insurance_types
        if "insurance_types" in ins:
            for t in ins["insurance_types"]:
                d[f"insurance_{t['type'].lower()}_required"] = t["required"]
                d[f"insurance_{t['type'].lower()}_on_file"] = t["on_file"]
        # Flatten authority_types
        if "authority_types" in ins:
            for a in ins["authority_types"]:
                d[f"authority_{a['authority_type'].lower()}_status"] = a["authority_status"]
        # Flatten property_types
        if "property_types" in ins and ins["property_types"]:
            for k, v in ins["property_types"][0].items():
                d[f"property_{k}"] = v
        # Add insurance_status only (do not overwrite effective/cancellation date)
        d["insurance_status"] = ins.get("insurance_status", "")
    df = pd.DataFrame(data)
    if "insurance" in df.columns:
        df = df.drop(columns=["insurance"])
    # Flatten insurance_coverage dict to two columns
    if "insurance_coverage" in df.columns:
        df["insurance_coverage_from"] = df["insurance_coverage"].apply(lambda x: x.get("From") if isinstance(x, dict) else "")
        df["insurance_coverage_to"] = df["insurance_coverage"].apply(lambda x: x.get("To") if isinstance(x, dict) else "")
        df = df.drop(columns=["insurance_coverage"])
    # Add is_new_mc column
    def is_new_mc(row):
        try:
            reg_date = pd.to_datetime(row.get("register_date", ""))
            return (datetime.now() - reg_date).days <= 30
        except:
            return False
    df["is_new_mc"] = df.apply(is_new_mc, axis=1)
    return df

df = load_data()

# --- Sidebar Filters ---
st.sidebar.header("Filters")

# State filter
states = sorted(df["state"].dropna().unique())
selected_states = st.sidebar.multiselect("State", states, default=states)

# Operating status filter
statuses = sorted(df["usdot_status"].dropna().unique())
selected_statuses = st.sidebar.multiselect("Operating Status", statuses, default=statuses)


# Insurance expiration month filter (based on Effective Date)
# Remove insurance_effective_month from dashboard and export
months = []
selected_months = []


# New MC filter (last 30 days)
show_new_mc = st.sidebar.checkbox("New MCs (last 30 days)", value=False)

# --- Filtering ---


filtered = df[
    df["state"].isin(selected_states) &
    df["usdot_status"].isin(selected_statuses)
]
if show_new_mc:
    filtered = filtered[filtered["is_new_mc"] == True]


# Always show data if present, even if filters are too restrictive
if filtered.empty and not df.empty:
    st.warning("No carriers match the selected filters. Showing all carriers.")
    filtered = df.copy()

st.write(f"Showing {len(filtered)} carriers")

# --- Data Table ---


# Remove unwanted columns before display
cols_to_remove = ["insurance_status", "insurance_effective_month"]
filtered = filtered.drop(columns=[c for c in cols_to_remove if c in filtered.columns], errors="ignore")
st.dataframe(filtered, use_container_width=True)


# --- Export Buttons grouped in expander ---
export_df = filtered.copy()
if "insurance" in export_df.columns:
    export_df = export_df.drop(columns=["insurance"])
# Remove unwanted columns from export
export_df = export_df.drop(columns=[c for c in cols_to_remove if c in export_df.columns], errors="ignore")
import io
with st.expander("Export Options", expanded=True):
    col1, col2, col3 = st.columns([1,1,2])
    with col1:
        csv = export_df.to_csv(index=False)
        st.download_button("Export CSV", csv, "carriers.csv", "text/csv")
    with col2:
        excel_buffer = io.BytesIO()
        export_df.to_excel(excel_buffer, index=False, engine='openpyxl')
        st.download_button("Export Excel", excel_buffer.getvalue(), "carriers.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with col3:
        st.markdown('[Export to Google Sheets](https://docs.google.com/spreadsheets/u/0/)', unsafe_allow_html=True)


# --- New MCs Report removed (now a filter only) ---

st.caption("Dashboard auto-refresh: Reload this page after updating the JSON file. For automation, schedule the scraper to run monthly and overwrite the JSON.")
