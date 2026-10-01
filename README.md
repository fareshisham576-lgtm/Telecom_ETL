# Telecom ETL Pipeline (SSIS-Style Data Pipeline in Python)

An automated Python-based ETL (Extract, Transform, Load) pipeline designed to process batch telecom CSV transaction logs, validate metadata, handle errors, perform dimension lookups, and load cleaned data into Microsoft SQL Server.

---

## 📌 Architecture Overview

```
[ Source Files/*.csv ]
          │
          ▼
    ┌───────────┐
    │  Extract  │ ── Reads Pipe-Delimited '|' CSV Files
    └───────────┘
          │
          ▼
    ┌───────────┐
    │ Validate  │ ── Checks mandatory fields (IMSI, Cell, LAC, Event TS)
    └───────────┘
     /         \
   Valid     Rejected
    │           │
    ▼           ▼
┌───────────┐ ┌───────────┐
│ Transform │ │ Data Sink │ ──> [ DB Table: error_destination_output ]
└───────────┘ └───────────┘
    │
    ▼ ── Merges with Dimension table (dim_imsi_reference)
┌───────────┐
│ Data Sink │ ──> [ DB Table: fact_transaction ]
└───────────┘
    │
    ▼
[ Processed Files/ ] ── Moves completed files
```

---

## 📁 Directory Structure

```text
├── Source Files/           # Incoming batch CSV files
├── Processed Files/        # Storage for successfully processed CSV files
├── SQL Queries/            # DDL script files
│   └── schema.sql          # Table creation and schema setup
├── Telecom.py              # Core ETL pipeline code
└── README.md               # Project documentation
```

---

## 🗄️ Database Schema (SQL Server)

Ensure the database `SSIS_Telecom_DB` is created and run the following DDL script prior to executing the ETL script:

```sql
CREATE DATABASE SSIS_Telecom_DB;
GO

USE SSIS_Telecom_DB;
GO

-- Dimension Reference Table
CREATE TABLE dim_imsi_reference (
    id INT IDENTITY(1,1) PRIMARY KEY,
    imsi VARCHAR(15) NOT NULL,
    subscriber_id INT NOT NULL
);

-- Target Fact Table
CREATE TABLE fact_transaction (
    id INT IDENTITY(1,1) PRIMARY KEY,
    transaction_id INT NOT NULL,
    imsi VARCHAR(15) NOT NULL,
    subscriber_id INT NOT NULL,
    tac VARCHAR(8) NULL,
    snr VARCHAR(6) NULL,
    imei VARCHAR(15) NULL,
    cell INT NOT NULL,
    lac INT NOT NULL,
    event_type VARCHAR(10) NULL,
    event_ts DATETIME NOT NULL
);

-- Rejection / Error Audit Destination
CREATE TABLE error_destination_output (
    id INT NULL,
    imsi VARCHAR(50) NULL,
    imei VARCHAR(50) NULL,
    cell INT NULL,
    lac INT NULL,
    event_type VARCHAR(50) NULL,
    event_ts VARCHAR(100) NULL,
    tac VARCHAR(50) NULL,
    snr VARCHAR(50) NULL,
    subscriber_id INT NULL,
    file_name VARCHAR(300) NULL
);
GO
```

---

## ⚙️ Data Pipeline Logic

### 1. Data Extraction & Validation
* Files are fetched sequentially from the `Source Files/` folder.
* Rows are flagged for **Rejection** if any of the following mandatory attributes are missing or malformed:
  * `imsi` is missing/null
  * `cell` is missing/null
  * `lac` is missing/null
  * `event_ts` is missing/null or cannot be parsed into a valid timestamp.

### 2. Transformation Rules
* **IMSI Parsing:** Trailing `.0` floats are stripped off to normalize IMSI strings.
* **IMEI Validation & Extraction:**
  * Valid IMEIs (length ≥ 14) are split into:
    * **TAC (Type Allocation Code):** First 8 characters (`imei[:8]`)
    * **SNR (Serial Number):** Last 6 characters (`imei[-6:]`)
  * Invalid/Short IMEIs are defaulted to `-99999` for `tac`, `snr`, and `imei`.
* **Dimension Lookup:** Left-joins with `dim_imsi_reference` on `imsi` to populate `subscriber_id`. Unmatched records default `subscriber_id` to `-99999`.

### 3. Loading & File Archiving
* **Valid Data:** Inserted into the `fact_transaction` table.
* **Rejected Data:** Inserted into `error_destination_output` along with the source `file_name` for auditability.
* **File Movement:** Source CSV files are moved from `Source Files/` to `Processed Files/` once execution completes.

---

## 🚀 Setup & Execution

### Prerequisites
* Python 3.10+
* Microsoft SQL Server
* ODBC Driver 17 for SQL Server

### Installation

1. Install required Python packages:
```bash
pip install pandas numpy pyodbc sqlalchemy
```

2. Update the SQL Server connection configuration in `Telecom.py` if your instance name differs from `.\SQLSERVER`:
```python
host = r".\SQLSERVER"
database = "SSIS_Telecom_DB"
```

3. Run the pipeline script:
```bash
python Telecom.py
```