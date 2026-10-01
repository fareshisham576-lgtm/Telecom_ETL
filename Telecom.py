import os
import shutil
import numpy as np
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, URL

connection_url = URL.create(
    "mssql+pyodbc",
    host=r".\SQLSERVER",
    database="SSIS_Telecom_DB",
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "trusted_connection": "yes"
    }
)
engine = create_engine(connection_url)

conn = pyodbc.connect(
    "driver={ODBC Driver 17 for SQL Server};"
    "server=.\\SQLSERVER;"
    "database=SSIS_Telecom_DB;"
    "trusted_connection=yes;"
)
cursor = conn.cursor()

refrence = pd.read_sql_query('select * from dim_imsi_reference', engine)

source_folder = 'Source Files'
processed_folder = 'Processed Files'

all_valid_data = []
all_rejected_data = []

for index, file_name in enumerate(sorted(os.listdir(source_folder))):
    if not file_name.endswith('.csv'):
        continue

    if index < 3:
        continue

    file_path = os.path.join(source_folder, file_name)
    print(f"Processing: {file_name}")

    df = pd.read_csv(file_path, sep='|')
    df = df.astype(str).replace(['nan', 'None', 'NaT', '<NA>'], np.nan)

    event_ts_check = pd.to_datetime(df['event_ts'], format='mixed', errors='coerce')

    reject_mask = (
        df['imsi'].isna() |
        df['cell'].isna() |
        df['lac'].isna() |
        df['event_ts'].isna() |
        event_ts_check.isna()
    )

    rejected_df = df[reject_mask].copy()
    if not rejected_df.empty:
        rejected_df['file_name'] = file_name
        all_rejected_data.append(rejected_df)

    df = df[~reject_mask].copy()

    if df.empty:
        shutil.move(file_path, os.path.join(processed_folder, file_name))
        print(f"{file_name} Moved to Processed Folder (No valid records)")
        continue

    df['imsi'] = df['imsi'].astype(str).str.split('.').str[0]
    df['cell'] = pd.to_numeric(df['cell'], errors='coerce').fillna(0).astype(int)
    df['lac'] = pd.to_numeric(df['lac'], errors='coerce').fillna(0).astype(int)
    df['event_type'] = df['event_type'].astype(str).str.split('.').str[0]
    df['imei'] = df['imei'].astype(str).str.split('.').str[0]
    df['event_ts'] = pd.to_datetime(df['event_ts'], format='mixed')

    pre_Final = df.merge(refrence, how='left', on='imsi')

    invalid_imei_mask = pre_Final['imei'].isna() | (pre_Final['imei'].str.len() < 14)

    pre_Final['TAC'] = pre_Final['imei'].str[:8]
    pre_Final['SNR'] = pre_Final['imei'].str[-6:]

    pre_Final.loc[invalid_imei_mask, ['TAC', 'SNR', 'imei']] = '-99999'

    pre_Final['TAC'] = pre_Final['TAC'].fillna('-99999')
    pre_Final['SNR'] = pre_Final['SNR'].fillna('-99999')
    pre_Final['imei'] = pre_Final['imei'].fillna('-99999')

    pre_Final['subscriber_id'] = pre_Final['subscriber_id'].fillna(-99999).astype(int)

    if 'id_y' in pre_Final.columns:
        pre_Final.drop(columns=['id_y'], inplace=True)
    if 'id_x' in pre_Final.columns:
        pre_Final.rename(columns={'id_x': 'Transaction_id'}, inplace=True)
    elif 'id' in pre_Final.columns:
        pre_Final.rename(columns={'id': 'Transaction_id'}, inplace=True)

    Final = pd.DataFrame(pre_Final[[
        'Transaction_id', 'imsi', 'subscriber_id',
        'TAC', 'SNR', 'imei', 'cell', 'lac', 'event_type', 'event_ts'
    ]])

    all_valid_data.append(Final)

    shutil.move(file_path, os.path.join(processed_folder, file_name))
    print(f"{file_name} Moved to Processed Folder")

if len(all_valid_data) > 0:
    Final_Valid_df = pd.concat(all_valid_data, ignore_index=True)
    print(f"Number of Valid Data: {len(Final_Valid_df)}")

    for index, row in Final_Valid_df.iterrows():
        cursor.execute(
            "insert into fact_transaction (transaction_id, imsi, subscriber_id, tac, snr, "
            "imei, cell, lac, event_type, event_ts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            int(row['Transaction_id']), str(row['imsi']), int(row['subscriber_id']),
            str(row['TAC']), str(row['SNR']), str(row['imei']), int(row['cell']),
            int(row['lac']), str(row['event_type']), row['event_ts']
        )
    conn.commit()
    print("Valid Data Inserted Successfully")

if len(all_rejected_data) > 0:
    Final_Rejected_df = pd.concat(all_rejected_data, ignore_index=True)
    print(f"Number of Rejected Data: {len(Final_Rejected_df)}")

    def safe_int(val):
        try:
            if pd.isna(val) or val is None or str(val).strip().lower() in ['nan', 'none', 'nat', '']:
                return None
            return int(float(val))
        except (ValueError, TypeError):
            return None

    def safe_str(val):
        if pd.isna(val) or val is None or str(val).strip().lower() in ['nan', 'none', 'nat', '']:
            return None
        return str(val)

    for index, row in Final_Rejected_df.iterrows():
        row_id = safe_int(row.get('id'))
        imsi_val = safe_str(row.get('imsi'))
        imei_val = safe_str(row.get('imei'))
        cell_val = safe_int(row.get('cell'))
        lac_val = safe_int(row.get('lac'))
        event_type_val = safe_str(row.get('event_type'))
        event_ts_val = safe_str(row.get('event_ts'))
        tac_val = safe_str(row.get('tac'))
        snr_val = safe_str(row.get('snr'))
        subscriber_val = safe_int(row.get('subscriber_id'))
        file_name_val = safe_str(row.get('file_name'))

        cursor.execute(
            "INSERT INTO error_destination_output (id, imsi, imei, cell, lac, event_type, "
            "event_ts, tac, snr, subscriber_id, file_name) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            row_id, imsi_val, imei_val,
            cell_val, lac_val, event_type_val,
            event_ts_val, tac_val, snr_val,
            subscriber_val, file_name_val
        )
    conn.commit()
    print("Rejected Data Inserted Successfully")

cursor.close()
conn.close()
engine.dispose()