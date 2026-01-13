# 📖 Procurement Data Pipeline - Technical Documentation

> **Complete Technical Reference**: Data Flow, File Schemas, SQL Logic, and System Architecture

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Deep Dive](#2-architecture-deep-dive)
3. [Data Generation Layer](#3-data-generation-layer)
4. [File Schemas & Formats](#4-file-schemas--formats)
5. [HDFS Data Organization](#5-hdfs-data-organization)
6. [Database Schemas](#6-database-schemas)
7. [SQL Processing Logic](#7-sql-processing-logic)
8. [Pipeline Execution Flow](#8-pipeline-execution-flow)
9. [Calculation Formulas](#9-calculation-formulas)
10. [Output Files](#10-output-files)
11. [Exception Handling](#11-exception-handling)
12. [Data Flow Diagrams](#12-data-flow-diagrams)

---

## 1. System Overview

### 1.1 Purpose
This pipeline automates the **procurement process** for a retail chain operating across 10 cities in Morocco. It:
- Collects daily sales orders from stores
- Checks warehouse inventory levels
- Calculates net demand (what needs to be ordered)
- Generates supplier order files automatically

### 1.2 Business Context
```
┌─────────────────────────────────────────────────────────────────────┐
│                      DAILY PROCUREMENT CYCLE                        │
├─────────────────────────────────────────────────────────────────────┤
│  Stores sell products → Inventory depletes → System calculates     │
│  what to reorder → Generates orders for 5 suppliers → Suppliers    │
│  deliver goods → Cycle repeats next day                            │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.3 Key Metrics
| Metric | Value |
|--------|-------|
| Number of Stores | 10 |
| Number of Products | 30 |
| Number of Suppliers | 5 |
| Daily Transactions | ~500-1500 |
| Pipeline Run Time | ~10 PM daily |

---

## 2. Architecture Deep Dive

### 2.1 Container Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         DOCKER COMPOSE NETWORK                           │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌─────────────┐     ┌─────────────┐     ┌──────────────────┐          │
│   │  NAMENODE   │────▶│  DATANODE   │     │  HIVE-METASTORE  │          │
│   │  Port: 9870 │     │             │     │    Port: 9083    │          │
│   │  Port: 9000 │     │   (HDFS     │     │                  │          │
│   │             │     │   Storage)  │     │  (Table Schema   │          │
│   │  (HDFS      │     │             │     │   Management)    │          │
│   │   Master)   │     │             │     │                  │          │
│   └─────────────┘     └─────────────┘     └────────┬─────────┘          │
│          │                   │                      │                    │
│          │                   │                      │                    │
│          ▼                   ▼                      ▼                    │
│   ┌──────────────────────────────────────────────────────────┐          │
│   │                         TRINO                             │          │
│   │                      Port: 8080                           │          │
│   │                                                           │          │
│   │    ┌─────────────┐          ┌─────────────────┐          │          │
│   │    │hive.catalog │          │postgres.catalog │          │          │
│   │    │  (HDFS Data)│          │  (Master Data)  │          │          │
│   │    └─────────────┘          └─────────────────┘          │          │
│   └──────────────────────────────────────────────────────────┘          │
│                                         │                                │
│                                         ▼                                │
│                              ┌──────────────────┐                       │
│                              │    POSTGRESQL    │                       │
│                              │    Port: 5432    │                       │
│                              │                  │                       │
│                              │  (Products &     │                       │
│                              │   Suppliers)     │                       │
│                              └──────────────────┘                       │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Container Roles

| Container | Image | Port | Purpose |
|-----------|-------|------|---------|
| `namenode` | bde2020/hadoop-namenode | 9870, 9000 | HDFS Master - manages filesystem namespace |
| `datanode` | bde2020/hadoop-datanode | - | HDFS Worker - stores actual data blocks |
| `hive-metastore` | apache/hive:3.1.3 | 9083 | Stores table metadata (schemas, locations) |
| `trino` | trinodb/trino:460 | 8080 | SQL Query Engine - joins HDFS + Postgres data |
| `postgres` | postgres:13 | 5432 | Master data store (products, suppliers) |

### 2.3 Trino Catalog Configuration

#### Hive Catalog (`trino/catalog/hive.properties`)
```properties
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
fs.hadoop.enabled=true
hive.config.resources=/etc/trino/catalog/core-site.xml
hive.security=allow-all
```
**Purpose**: Connects Trino to HDFS data via Hive Metastore

#### PostgreSQL Catalog (`trino/catalog/postgres.properties`)
```properties
connector.name=postgresql
connection-url=jdbc:postgresql://postgres:5432/procurement_db
connection-user=user
connection-password=password
```
**Purpose**: Connects Trino to PostgreSQL for master data queries

---

## 3. Data Generation Layer

### 3.1 The Generator Script (`generate_data.py`)

This script simulates realistic retail transaction data:

```python
# Key Configuration
DATE = date.today().isoformat()  # e.g., "2026-01-12"

STORES = [
    "Casablanca", "Rabat", "Tangier", "Marrakech", "Fes",
    "Agadir", "Meknes", "Oujda", "Kenitra", "Tetouan"
]

PRODUCTS = [
    # 30 SKUs across 5 categories
    "P_101", "P_102", ... , "P_506"
]
```

### 3.2 Transaction Generation Logic

```
┌────────────────────────────────────────────────────────────────┐
│                  TRANSACTION GENERATION                        │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│   For each store (10 stores):                                  │
│   ├── Generate 50-150 random transactions                      │
│   ├── Each transaction:                                        │
│   │   ├── Unique transaction_id: TXN_CAS_20260112_0001        │
│   │   ├── Timestamp: Between 8:00 AM and 9:00 PM              │
│   │   ├── Product: Weighted random (popular items sell more)  │
│   │   └── Quantity: Random 1-15 units                         │
│   └── Save to: data/raw/orders/{DATE}/orders-{city}.json      │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### 3.3 Product Popularity Weights

Products have different sale frequencies based on real-world patterns:

```python
PRODUCT_WEIGHTS = {
    "P_301": 20,  # Sucre Granule - HIGHEST (essential item)
    "P_101": 15,  # Milk - HIGH
    "P_501": 14,  # Lben - HIGH (popular dairy)
    "P_401": 12,  # Tagger Chocolate - MEDIUM-HIGH
    "P_106": 12,  # Raibi - MEDIUM-HIGH
    ...
    "P_304": 2,   # Miel - LOW (specialty item)
}
```

### 3.4 Inventory Generation

```python
INVENTORY_DATA = {
    # SKU: (available, reserved, safety_stock)
    "P_101": (500, 50, 100),   # Milk - high stock for high demand
    "P_301": (400, 100, 150),  # Sugar - very high safety stock
    "P_205": (50, 5, 15),      # Moutarde - low stock (slow mover)
    ...
}
```

---

## 4. File Schemas & Formats

### 4.1 Order Files (JSON - Line Delimited)

**Location**: `data/raw/orders/{DATE}/orders-{city}.json`

**Format**: JSON Lines (one JSON object per line, NOT an array)

```json
{"transaction_id": "TXN_CAS_20260112_0000", "timestamp": "2026-01-12T21:59:00", "store_id": "STORE_CASABLANCA", "product_sku": "P_505", "quantity": 11}
{"transaction_id": "TXN_CAS_20260112_0001", "timestamp": "2026-01-12T19:00:00", "store_id": "STORE_CASABLANCA", "product_sku": "P_201", "quantity": 2}
```

**Schema Definition**:
| Field | Type | Example | Description |
|-------|------|---------|-------------|
| `transaction_id` | String | `TXN_CAS_20260112_0001` | Unique ID: TXN_{CITY_PREFIX}_{YYYYMMDD}_{SEQUENCE} |
| `timestamp` | ISO 8601 | `2026-01-12T14:30:00` | Transaction time |
| `store_id` | String | `STORE_CASABLANCA` | Store identifier |
| `product_sku` | String | `P_101` | Product Stock Keeping Unit |
| `quantity` | Integer | `5` | Units sold (1-15) |

### 4.2 Inventory File (CSV)

**Location**: `data/raw/stock/{DATE}/inventory.csv`

**Format**: Standard CSV with header

```csv
warehouse_id,product_sku,available_quantity,reserved_quantity,safety_stock
WH_MAIN,P_101,517,52,100
WH_MAIN,P_102,197,30,60
WH_MAIN,P_301,424,104,150
```

**Schema Definition**:
| Field | Type | Example | Description |
|-------|------|---------|-------------|
| `warehouse_id` | String | `WH_MAIN` | Central warehouse ID |
| `product_sku` | String | `P_101` | Product identifier |
| `available_quantity` | Integer | `517` | Total units in warehouse |
| `reserved_quantity` | Integer | `52` | Units reserved for pending orders |
| `safety_stock` | Integer | `100` | Minimum stock level to maintain |

### 4.3 Aggregated Orders (CSV)

**Location**: `data/processed/aggregated_orders/{DATE}/aggregated_orders.csv`

```csv
"product_sku","total_ordered"
"P_104","177"
"P_102","275"
"P_301","716"
```

**Description**: Sum of all quantities ordered for each product across ALL stores.

### 4.4 Net Demand (CSV)

**Location**: `data/processed/net_demand/{DATE}/net_demand.csv`

```csv
"supplier_id","supplier_name","product_sku","product_name","lead_time_days","raw_net_demand","pack_size","min_order_quantity","order_quantity"
"SUP_DANONE","Centrale Danone","P_101","Milk 1L","1","127","12","100","132.0"
"SUP_COSUMAR","Cosumar","P_301","Sucre Granule 1kg","3","546","25","50","550.0"
```

**Schema Definition**:
| Field | Type | Description |
|-------|------|-------------|
| `supplier_id` | String | Supplier code |
| `supplier_name` | String | Full supplier name |
| `product_sku` | String | Product code |
| `product_name` | String | Product description |
| `lead_time_days` | Integer | Days for supplier delivery |
| `raw_net_demand` | Integer | Calculated demand before rounding |
| `pack_size` | Integer | Units per case/pack |
| `min_order_quantity` | Integer | Minimum order threshold |
| `order_quantity` | Float | Final rounded order quantity |

### 4.5 Supplier Order Files (JSON)

**Location**: `data/output/supplier_orders/{DATE}/{SUPPLIER_ID}.json`

```json
{
  "supplier_name": "Centrale Danone",
  "lead_time_days": 1,
  "order_date": "2026-01-12",
  "items": [
    {
      "product_sku": "P_105",
      "product_name": "Activia Natural",
      "raw_demand": 169,
      "pack_size": 6,
      "min_order_quantity": 36,
      "order_quantity": 174
    },
    {
      "product_sku": "P_101",
      "product_name": "Milk 1L",
      "raw_demand": 127,
      "pack_size": 12,
      "min_order_quantity": 100,
      "order_quantity": 132
    }
  ]
}
```

---

## 5. HDFS Data Organization

### 5.1 Directory Structure

```
hdfs://namenode:9000/
├── raw/                              # Input data (immutable)
│   ├── orders/
│   │   └── {DATE}/
│   │       ├── orders-agadir.json
│   │       ├── orders-casablanca.json
│   │       ├── orders-fes.json
│   │       ├── orders-kenitra.json
│   │       ├── orders-marrakech.json
│   │       ├── orders-meknes.json
│   │       ├── orders-oujda.json
│   │       ├── orders-rabat.json
│   │       ├── orders-tangier.json
│   │       └── orders-tetouan.json
│   └── stock/
│       └── {DATE}/
│           └── inventory.csv
│
├── processed/                        # Intermediate results
│   ├── aggregated_orders/
│   │   └── {DATE}/
│   │       └── aggregated_orders.csv
│   └── net_demand/
│       └── {DATE}/
│           └── net_demand.csv
│
├── output/                           # Final outputs
│   └── supplier_orders/
│       └── {DATE}/
│           ├── SUP_BIMO.json
│           ├── SUP_COPAG.json
│           ├── SUP_COSUMAR.json
│           ├── SUP_DANONE.json
│           └── SUP_LESIEUR.json
│
└── logs/                             # Exception logs
    └── exceptions/
        └── {DATE}/
            └── exceptions.csv
```

### 5.2 HDFS Upload Process

```
┌─────────────────────────────────────────────────────────────────┐
│                    HDFS UPLOAD WORKFLOW                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Local File System                                           │
│     data/raw/orders/2026-01-12/orders-casablanca.json          │
│                         │                                       │
│                         ▼                                       │
│  2. docker cp → Namenode Container                              │
│     namenode:/tmp/orders-casablanca.json                       │
│                         │                                       │
│                         ▼                                       │
│  3. hdfs dfs -put → HDFS                                       │
│     hdfs://namenode:9000/raw/orders/2026-01-12/                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. Database Schemas

### 6.1 PostgreSQL Master Data (`sql/init.sql`)

#### Suppliers Table
```sql
CREATE TABLE suppliers (
    supplier_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    lead_time_days INT
);
```

**Data**:
| supplier_id | name | lead_time_days |
|-------------|------|----------------|
| SUP_DANONE | Centrale Danone | 1 |
| SUP_LESIEUR | Lesieur Cristal | 2 |
| SUP_COSUMAR | Cosumar | 3 |
| SUP_BIMO | Bimo (Mondelez) | 2 |
| SUP_COPAG | Copag (Jaouda) | 1 |

#### Products Table
```sql
CREATE TABLE products (
    sku VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    safety_stock INT,
    min_order_quantity INT,
    pack_size INT,
    supplier_id VARCHAR(50)
);
```

**Data Example**:
| sku | name | safety_stock | min_order_quantity | pack_size | supplier_id |
|-----|------|--------------|-------------------|-----------|-------------|
| P_101 | Milk 1L | 50 | 100 | 12 | SUP_DANONE |
| P_201 | Huile Tournesol 1L | 30 | 24 | 12 | SUP_LESIEUR |
| P_301 | Sucre Granule 1kg | 100 | 50 | 25 | SUP_COSUMAR |
| P_401 | Tagger Chocolate | 40 | 48 | 24 | SUP_BIMO |
| P_501 | Lben 500ml | 60 | 48 | 12 | SUP_COPAG |

### 6.2 Hive External Tables (`sql/hive_schema.sql`)

These tables are "external" - they point to HDFS data without copying it:

#### Orders Table
```sql
CREATE TABLE hive.procurement.orders (
    transaction_id VARCHAR,
    timestamp VARCHAR,
    store_id VARCHAR,
    product_sku VARCHAR,
    quantity INT
)
WITH (
    format = 'JSON',
    external_location = 'hdfs://namenode:9000/raw/orders/{DATE}'
);
```

**Key Points**:
- `format = 'JSON'` - Trino reads JSON Lines format
- `external_location` - Points directly to HDFS folder
- `{DATE}` is replaced at runtime (e.g., `2026-01-12`)

#### Inventory Table
```sql
CREATE TABLE hive.procurement.inventory (
    warehouse_id VARCHAR,
    product_sku VARCHAR,
    available_quantity VARCHAR,
    reserved_quantity VARCHAR,
    safety_stock VARCHAR
)
WITH (
    format = 'CSV',
    skip_header_line_count = 1,
    external_location = 'hdfs://namenode:9000/raw/stock/{DATE}'
);
```

**Key Points**:
- `format = 'CSV'` - Standard CSV parsing
- `skip_header_line_count = 1` - Ignores header row
- Quantities stored as VARCHAR (cast to INT in queries)

---

## 7. SQL Processing Logic

### 7.1 Aggregated Orders Query (`sql/aggregated_orders.sql`)

**Purpose**: Calculate total demand per product across all stores

```sql
SELECT 
    product_sku, 
    SUM(quantity) as total_ordered
FROM hive.procurement.orders
GROUP BY product_sku;
```

**Flow**:
```
┌─────────────────────────────────────────────────────────────────┐
│  orders-casablanca.json  ──┐                                    │
│  orders-rabat.json       ──┼──▶ SUM(quantity) ──▶ total_ordered │
│  orders-tangier.json     ──┤       BY product                  │
│  ... (7 more cities)     ──┘                                    │
└─────────────────────────────────────────────────────────────────┘
```

**Output Example**:
```
P_301 (Sucre Granule): 716 units total across all stores
P_501 (Lben):          628 units
P_101 (Milk):          492 units
```

### 7.2 Net Demand Query (`sql/net_demand.sql`)

This is the **core business logic** - a multi-step calculation:

#### Step 1: Aggregate Orders (CTE)
```sql
WITH aggregated_orders AS (
    SELECT 
        product_sku, 
        SUM(quantity) as total_ordered
    FROM hive.procurement.orders
    GROUP BY product_sku
)
```

#### Step 2: Parse Stock Status (CTE)
```sql
stock_status AS (
    SELECT 
        product_sku, 
        CAST(available_quantity AS INTEGER) as available_quantity, 
        CAST(reserved_quantity AS INTEGER) as reserved_quantity, 
        CAST(safety_stock AS INTEGER) as safety_stock
    FROM hive.procurement.inventory
)
```

#### Step 3: Calculate Raw Demand (CTE)
```sql
raw_demand AS (
    SELECT
        p.sku as product_sku,
        p.name as product_name,
        p.supplier_id,
        p.min_order_quantity,
        p.pack_size,
        s.name as supplier_name,
        s.lead_time_days,
        -- Join all data sources
        COALESCE(ao.total_ordered, 0) as total_ordered,
        COALESCE(ss.available_quantity, 0) as available_quantity,
        COALESCE(ss.reserved_quantity, 0) as reserved_quantity,
        COALESCE(ss.safety_stock, 0) as safety_stock,
        
        -- THE FORMULA:
        GREATEST(0, 
            COALESCE(ao.total_ordered, 0) 
            + COALESCE(ss.safety_stock, 0) 
            - (COALESCE(ss.available_quantity, 0) - COALESCE(ss.reserved_quantity, 0))
        ) as raw_net_demand
        
    FROM postgres.public.products p
    LEFT JOIN aggregated_orders ao ON p.sku = ao.product_sku
    LEFT JOIN stock_status ss ON p.sku = ss.product_sku
    LEFT JOIN postgres.public.suppliers s ON p.supplier_id = s.supplier_id
)
```

#### Step 4: Apply Business Rules (CTE)
```sql
final_demand AS (
    SELECT
        *,
        -- Round up to pack size
        CASE 
            WHEN raw_net_demand = 0 THEN 0
            ELSE CEILING(CAST(raw_net_demand AS DOUBLE) / pack_size) * pack_size
        END as rounded_demand,
        
        -- Apply MOQ (Minimum Order Quantity)
        GREATEST(
            CASE 
                WHEN raw_net_demand = 0 THEN 0
                ELSE CEILING(CAST(raw_net_demand AS DOUBLE) / pack_size) * pack_size
            END,
            CASE WHEN raw_net_demand > 0 THEN min_order_quantity ELSE 0 END
        ) as final_order_quantity
    FROM raw_demand
)
```

#### Step 5: Output Only Non-Zero Orders
```sql
SELECT 
    supplier_id, supplier_name, product_sku, product_name,
    lead_time_days, raw_net_demand, pack_size, 
    min_order_quantity, final_order_quantity as order_quantity
FROM final_demand
WHERE final_order_quantity > 0;
```

### 7.3 Exceptions Query (`sql/exceptions.sql`)

**Purpose**: Identify data quality issues

```sql
-- Unknown Products (orders referencing non-existent SKUs)
SELECT 
    'Unknown Product' as issue_type,
    o.product_sku,
    o.transaction_id
FROM hive.procurement.orders o
LEFT JOIN postgres.public.products p ON o.product_sku = p.sku
WHERE p.sku IS NULL

UNION ALL

-- Missing Suppliers (products without suppliers)
SELECT 
    'Missing Supplier' as issue_type,
    p.sku as product_sku,
    NULL as transaction_id
FROM postgres.public.products p
WHERE p.supplier_id IS NULL;
```

---

## 8. Pipeline Execution Flow

### 8.1 Main Pipeline Steps (`main.py`)

```
┌────────────────────────────────────────────────────────────────────────┐
│                      PIPELINE EXECUTION FLOW                           │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  [STEP 1/7] Generate Data                                              │
│  └─▶ python generate_data.py                                           │
│      ├── Creates 10 order JSON files (one per city)                    │
│      └── Creates 1 inventory CSV file                                  │
│                                                                        │
│  [STEP 2/7] Check Docker Containers                                    │
│  └─▶ docker ps                                                         │
│      └── Verify: namenode, trino, hive-metastore, postgres running    │
│                                                                        │
│  [STEP 3/7] Upload Data to HDFS                                        │
│  └─▶ docker cp + hdfs dfs -put                                         │
│      ├── /raw/orders/{DATE}/ ← 10 JSON files                          │
│      └── /raw/stock/{DATE}/  ← 1 CSV file                             │
│                                                                        │
│  [STEP 4/7] Create Hive Tables                                         │
│  └─▶ trino --file hive_schema.sql                                      │
│      └── Creates external tables pointing to HDFS                      │
│                                                                        │
│  [STEP 5/7] Calculate Net Demand & Generate Supplier Orders            │
│  └─▶ trino --file net_demand.sql                                       │
│      ├── Joins: HDFS orders + HDFS inventory + Postgres products      │
│      ├── Calculates demand, applies MOQ, rounds to pack size          │
│      ├── Outputs CSV to /processed/net_demand/{DATE}/                  │
│      └── Generates 5 JSON files to /output/supplier_orders/{DATE}/     │
│                                                                        │
│  [STEP 6/7] Save Aggregated Orders                                     │
│  └─▶ trino --file aggregated_orders.sql                                │
│      └── Saves to /processed/aggregated_orders/{DATE}/                 │
│                                                                        │
│  [STEP 7/7] Check for Exceptions                                       │
│  └─▶ trino --file exceptions.sql                                       │
│      └── Saves report to /logs/exceptions/{DATE}/                      │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### 8.2 Data Joins Visualization

```
                    ┌─────────────────────────────────────┐
                    │            TRINO ENGINE             │
                    │        (Federated Query)            │
                    └───────────────┬─────────────────────┘
                                    │
            ┌───────────────────────┼───────────────────────┐
            │                       │                       │
            ▼                       ▼                       ▼
   ┌────────────────┐     ┌────────────────┐     ┌────────────────┐
   │ HIVE CATALOG   │     │ HIVE CATALOG   │     │POSTGRES CATALOG│
   │                │     │                │     │                │
   │ procurement.   │     │ procurement.   │     │  public.       │
   │ orders         │     │ inventory      │     │  products      │
   │                │     │                │     │  suppliers     │
   │ (HDFS JSON)    │     │ (HDFS CSV)     │     │ (PostgreSQL)   │
   └────────────────┘     └────────────────┘     └────────────────┘
            │                       │                       │
            │                       │                       │
            └───────────────────────┼───────────────────────┘
                                    │
                                    ▼
                    ┌─────────────────────────────────────┐
                    │       JOINED RESULT SET             │
                    │                                     │
                    │  Orders + Inventory + Products +    │
                    │  Suppliers = Net Demand Calculation │
                    └─────────────────────────────────────┘
```

---

## 9. Calculation Formulas

### 9.1 Net Demand Formula

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      NET DEMAND CALCULATION                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   raw_net_demand = MAX(0,                                               │
│       total_ordered                    (demand from all stores)         │
│       + safety_stock                   (buffer stock to maintain)       │
│       - (available_quantity            (what's in warehouse)            │
│          - reserved_quantity)          (already committed)              │
│   )                                                                     │
│                                                                         │
│   ─────────────────────────────────────────────────────────────────     │
│   EXAMPLE: P_101 (Milk 1L)                                              │
│   ─────────────────────────────────────────────────────────────────     │
│   total_ordered        = 492  (sum from 10 stores)                      │
│   safety_stock         = 100  (from postgres.products)                  │
│   available_quantity   = 517  (from inventory.csv)                      │
│   reserved_quantity    = 52   (from inventory.csv)                      │
│                                                                         │
│   raw_net_demand = MAX(0, 492 + 100 - (517 - 52))                       │
│                  = MAX(0, 592 - 465)                                    │
│                  = MAX(0, 127)                                          │
│                  = 127 units                                            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 9.2 Pack Size Rounding

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      PACK SIZE ROUNDING                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   rounded_demand = CEILING(raw_net_demand / pack_size) * pack_size      │
│                                                                         │
│   ─────────────────────────────────────────────────────────────────     │
│   EXAMPLE: P_101 (Milk 1L) - pack_size = 12                             │
│   ─────────────────────────────────────────────────────────────────     │
│   raw_net_demand = 127                                                  │
│   rounded_demand = CEILING(127 / 12) * 12                               │
│                  = CEILING(10.583) * 12                                 │
│                  = 11 * 12                                              │
│                  = 132 units (11 cases of 12)                           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 9.3 Minimum Order Quantity (MOQ)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    MINIMUM ORDER QUANTITY                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   final_order_quantity = MAX(rounded_demand, min_order_quantity)        │
│                          (only if raw_net_demand > 0)                   │
│                                                                         │
│   ─────────────────────────────────────────────────────────────────     │
│   EXAMPLE: P_101 (Milk 1L) - min_order_quantity = 100                   │
│   ─────────────────────────────────────────────────────────────────     │
│   rounded_demand       = 132                                            │
│   min_order_quantity   = 100                                            │
│   final_order_quantity = MAX(132, 100)                                  │
│                        = 132 units ✓                                    │
│                                                                         │
│   ─────────────────────────────────────────────────────────────────     │
│   EXAMPLE: P_105 (Activia) - pack_size=6, min_order_quantity=36         │
│   ─────────────────────────────────────────────────────────────────     │
│   raw_net_demand = 20 (hypothetical low demand)                         │
│   rounded_demand = CEILING(20/6) * 6 = 24                               │
│   final_order    = MAX(24, 36) = 36 units (MOQ applies!) ✓              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 9.4 Complete Calculation Flow

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                        COMPLETE CALCULATION EXAMPLE                           │
│                           P_301 (Sucre Granule 1kg)                           │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│  INPUT DATA:                                                                  │
│  ├── total_ordered (from orders):        716 units                            │
│  ├── safety_stock (from postgres):       150 units                            │
│  ├── available_quantity (from csv):      424 units                            │
│  ├── reserved_quantity (from csv):       104 units                            │
│  ├── pack_size (from postgres):          25 units                             │
│  └── min_order_quantity (from postgres): 50 units                             │
│                                                                               │
│  STEP 1: Calculate Raw Net Demand                                             │
│  ────────────────────────────────────────────────                             │
│  usable_stock = available - reserved = 424 - 104 = 320                        │
│  raw_demand = total_ordered + safety_stock - usable_stock                     │
│             = 716 + 150 - 320                                                 │
│             = 546 units                                                       │
│                                                                               │
│  STEP 2: Round to Pack Size                                                   │
│  ────────────────────────────────────────────────                             │
│  rounded = CEILING(546 / 25) * 25                                             │
│          = CEILING(21.84) * 25                                                │
│          = 22 * 25                                                            │
│          = 550 units (22 bags of 25kg)                                        │
│                                                                               │
│  STEP 3: Apply MOQ                                                            │
│  ────────────────────────────────────────────────                             │
│  final = MAX(550, 50) = 550 units ✓                                           │
│                                                                               │
│  OUTPUT: Order 550 units of Sucre Granule from Cosumar                        │
│                                                                               │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

## 10. Output Files

### 10.1 Supplier Order JSON Structure

Each supplier gets a JSON file ready for electronic transmission:

```json
{
  "supplier_name": "Centrale Danone",
  "lead_time_days": 1,
  "order_date": "2026-01-12",
  "items": [
    {
      "product_sku": "P_105",
      "product_name": "Activia Natural",
      "raw_demand": 169,
      "pack_size": 6,
      "min_order_quantity": 36,
      "order_quantity": 174
    }
  ]
}
```

### 10.2 Supplier Summary

| Supplier | File | Products | Typical Order Items |
|----------|------|----------|---------------------|
| **Centrale Danone** | SUP_DANONE.json | 6 | Milk, Yogurt, Danette, Activia, Raibi |
| **Lesieur Cristal** | SUP_LESIEUR.json | 5 | Oils, Mayonnaise, Ketchup, Mustard |
| **Cosumar** | SUP_COSUMAR.json | 4 | Sugar (Granule, Cubes, Icing), Honey |
| **Bimo (Mondelez)** | SUP_BIMO.json | 6 | Tagger, Cookies, Crackers, Wafers |
| **Copag (Jaouda)** | SUP_COPAG.json | 6 | Lben, Raib, Juices, Fermented Milk |

---

## 11. Exception Handling

### 11.1 Types of Exceptions Detected

| Issue Type | Description | Cause |
|------------|-------------|-------|
| **Unknown Product** | Order references a SKU not in master data | New product not added to PostgreSQL |
| **Missing Supplier** | Product has no supplier assigned | Data entry error in products table |

### 11.2 Exception Report Format

**Location**: `data/logs/exceptions/{DATE}/report.csv`

```csv
"issue_type","product_sku","transaction_id"
"Unknown Product","P_999","TXN_CAS_20260112_0050"
"Missing Supplier","P_105",""
```

### 11.3 No Exceptions Output

When no issues are found:
```
No exceptions detected
```

---

## 12. Data Flow Diagrams

### 12.1 Complete System Flow

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                  COMPLETE DATA FLOW                                  │
└─────────────────────────────────────────────────────────────────────────────────────┘

                                    ┌─────────────────┐
                                    │  generate_data  │
                                    │     .py         │
                                    └────────┬────────┘
                                             │
                   ┌─────────────────────────┴─────────────────────────┐
                   │                                                   │
                   ▼                                                   ▼
        ┌─────────────────────┐                           ┌─────────────────────┐
        │   data/raw/orders/  │                           │   data/raw/stock/   │
        │   {DATE}/           │                           │   {DATE}/           │
        │                     │                           │                     │
        │ ┌─────────────────┐ │                           │ ┌─────────────────┐ │
        │ │ orders-casa.json│ │                           │ │  inventory.csv  │ │
        │ │ orders-rabat... │ │                           │ │                 │ │
        │ │ (10 files)      │ │                           │ │ (1 file)        │ │
        │ └─────────────────┘ │                           │ └─────────────────┘ │
        └──────────┬──────────┘                           └──────────┬──────────┘
                   │                                                  │
                   │              docker cp + hdfs dfs -put           │
                   │                                                  │
                   ▼                                                  ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │                              HDFS                                    │
        │                                                                      │
        │   /raw/orders/{DATE}/              /raw/stock/{DATE}/               │
        │   ├── orders-agadir.json           └── inventory.csv                │
        │   ├── orders-casablanca.json                                        │
        │   └── ... (10 files)                                                │
        └──────────────────────────────────────────────────────────────────────┘
                                             │
                                             │ Hive External Tables
                                             ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │                         HIVE METASTORE                               │
        │                                                                      │
        │   hive.procurement.orders          hive.procurement.inventory       │
        │   (points to /raw/orders)          (points to /raw/stock)           │
        └──────────────────────────────────────────────────────────────────────┘
                                             │
                                             │
                                             ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │                            TRINO                                     │
        │                                                                      │
        │  ┌────────────────────────────────────────────────────────────────┐ │
        │  │                    SQL PROCESSING                              │ │
        │  │                                                                │ │
        │  │  1. aggregated_orders.sql                                      │ │
        │  │     └─▶ SUM(quantity) GROUP BY product_sku                     │ │
        │  │                                                                │ │
        │  │  2. net_demand.sql                                             │ │
        │  │     └─▶ JOIN hive.orders + hive.inventory                     │ │
        │  │              + postgres.products + postgres.suppliers          │ │
        │  │     └─▶ Calculate: demand + safety - (available - reserved)   │ │
        │  │     └─▶ Round to pack_size, apply MOQ                         │ │
        │  │                                                                │ │
        │  │  3. exceptions.sql                                             │ │
        │  │     └─▶ LEFT JOIN to find orphan records                       │ │
        │  └────────────────────────────────────────────────────────────────┘ │
        │                              │                                       │
        │                              │ ALSO USES                            │
        │                              ▼                                       │
        │  ┌────────────────────────────────────────────────────────────────┐ │
        │  │                    POSTGRESQL                                  │ │
        │  │                                                                │ │
        │  │   products table          suppliers table                      │ │
        │  │   ├── sku                 ├── supplier_id                      │ │
        │  │   ├── name                ├── name                             │ │
        │  │   ├── safety_stock        └── lead_time_days                   │ │
        │  │   ├── min_order_quantity                                       │ │
        │  │   ├── pack_size                                                │ │
        │  │   └── supplier_id (FK)                                         │ │
        │  └────────────────────────────────────────────────────────────────┘ │
        └──────────────────────────────────────────────────────────────────────┘
                                             │
                                             │
                   ┌─────────────────────────┼─────────────────────────┐
                   │                         │                         │
                   ▼                         ▼                         ▼
        ┌─────────────────────┐   ┌─────────────────────┐   ┌─────────────────────┐
        │  /processed/        │   │  /output/           │   │  /logs/             │
        │                     │   │                     │   │                     │
        │ aggregated_orders/  │   │ supplier_orders/    │   │ exceptions/         │
        │ └── {DATE}/         │   │ └── {DATE}/         │   │ └── {DATE}/         │
        │     └── .csv        │   │     ├── SUP_BIMO    │   │     └── report.csv  │
        │                     │   │     ├── SUP_COPAG   │   │                     │
        │ net_demand/         │   │     ├── SUP_COSUMAR │   │                     │
        │ └── {DATE}/         │   │     ├── SUP_DANONE  │   │                     │
        │     └── .csv        │   │     └── SUP_LESIEUR │   │                     │
        └─────────────────────┘   └─────────────────────┘   └─────────────────────┘
                │                         │                         │
                │                         │                         │
                ▼                         ▼                         ▼
        ┌─────────────────────────────────────────────────────────────────────────┐
        │                          LOCAL FILE SYSTEM                              │
        │                                                                         │
        │   data/processed/aggregated_orders/{DATE}/aggregated_orders.csv        │
        │   data/processed/net_demand/{DATE}/net_demand.csv                      │
        │   data/output/supplier_orders/{DATE}/SUP_*.json (5 files)              │
        │   data/logs/exceptions/{DATE}/report.csv                               │
        └─────────────────────────────────────────────────────────────────────────┘
```

### 12.2 Daily Timeline

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DAILY TIMELINE                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  08:00 ─────── Stores Open ─────────────────────────────────────────────────   │
│     │                                                                           │
│     │   Transactions happen throughout the day                                  │
│     │   (50-150 per store × 10 stores = 500-1500 total)                        │
│     │                                                                           │
│  21:00 ─────── Stores Close ────────────────────────────────────────────────   │
│     │                                                                           │
│  22:00 ─────── Pipeline Runs (Scheduled via Task Scheduler) ────────────────   │
│     │                                                                           │
│     ├── [1] Generate data (simulated) or collect real POS data                 │
│     ├── [2] Verify Docker containers                                           │
│     ├── [3] Upload to HDFS                                                     │
│     ├── [4] Create Hive tables                                                 │
│     ├── [5] Calculate net demand + generate supplier orders                    │
│     ├── [6] Save aggregated orders                                             │
│     └── [7] Check exceptions                                                   │
│     │                                                                           │
│  22:15 ─────── Pipeline Complete ───────────────────────────────────────────   │
│     │                                                                           │
│     │   Supplier order files ready:                                            │
│     │   ├── SUP_DANONE.json → Email/EDI to Centrale Danone                     │
│     │   ├── SUP_LESIEUR.json → Email/EDI to Lesieur Cristal                    │
│     │   ├── SUP_COSUMAR.json → Email/EDI to Cosumar                            │
│     │   ├── SUP_BIMO.json → Email/EDI to Bimo                                  │
│     │   └── SUP_COPAG.json → Email/EDI to Copag                                │
│     │                                                                           │
│  NEXT DAY ─────────────────────────────────────────────────────────────────    │
│     │                                                                           │
│     │   Suppliers deliver based on lead_time_days:                             │
│     │   ├── Danone (1 day) → Arrives tomorrow                                  │
│     │   ├── Copag (1 day) → Arrives tomorrow                                   │
│     │   ├── Lesieur (2 days) → Arrives day after tomorrow                      │
│     │   ├── Bimo (2 days) → Arrives day after tomorrow                         │
│     │   └── Cosumar (3 days) → Arrives in 3 days                               │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Appendix A: Product Catalog

### A.1 Complete Product List

| SKU | Product Name | Category | Supplier | Pack Size | MOQ | Safety Stock |
|-----|--------------|----------|----------|-----------|-----|--------------|
| **DAIRY (DANONE)** |
| P_101 | Milk 1L | Dairy | SUP_DANONE | 12 | 100 | 50 |
| P_102 | Yogurt Strawberry | Dairy | SUP_DANONE | 24 | 50 | 30 |
| P_103 | Yogurt Vanilla | Dairy | SUP_DANONE | 24 | 50 | 30 |
| P_104 | Danette Chocolate | Dairy | SUP_DANONE | 12 | 48 | 20 |
| P_105 | Activia Natural | Dairy | SUP_DANONE | 6 | 36 | 25 |
| P_106 | Raibi Jamila | Dairy | SUP_DANONE | 12 | 60 | 40 |
| **OILS (LESIEUR)** |
| P_201 | Huile Tournesol 1L | Oils | SUP_LESIEUR | 12 | 24 | 30 |
| P_202 | Huile Olive 500ml | Oils | SUP_LESIEUR | 6 | 12 | 15 |
| P_203 | Mayonnaise 250g | Condiments | SUP_LESIEUR | 12 | 24 | 20 |
| P_204 | Ketchup 340g | Condiments | SUP_LESIEUR | 12 | 24 | 25 |
| P_205 | Moutarde 200g | Condiments | SUP_LESIEUR | 12 | 24 | 15 |
| **SUGAR (COSUMAR)** |
| P_301 | Sucre Granule 1kg | Sugar | SUP_COSUMAR | 25 | 50 | 100 |
| P_302 | Sucre Morceaux 1kg | Sugar | SUP_COSUMAR | 20 | 40 | 60 |
| P_303 | Sucre Glace 500g | Sugar | SUP_COSUMAR | 12 | 24 | 25 |
| P_304 | Miel 500g | Sweeteners | SUP_COSUMAR | 6 | 12 | 15 |
| **BISCUITS (BIMO)** |
| P_401 | Tagger Chocolate | Biscuits | SUP_BIMO | 24 | 48 | 40 |
| P_402 | Tagger Vanille | Biscuits | SUP_BIMO | 24 | 48 | 35 |
| P_403 | Biscuit Petit Dejeuner | Biscuits | SUP_BIMO | 12 | 36 | 30 |
| P_404 | Cookies Chocolat | Biscuits | SUP_BIMO | 12 | 24 | 25 |
| P_405 | Gaufrettes Fraise | Biscuits | SUP_BIMO | 12 | 24 | 20 |
| P_406 | Crackers Sales | Biscuits | SUP_BIMO | 18 | 36 | 30 |
| **JUICES (COPAG)** |
| P_501 | Lben 500ml | Dairy | SUP_COPAG | 12 | 48 | 60 |
| P_502 | Raib 500ml | Dairy | SUP_COPAG | 12 | 48 | 50 |
| P_503 | Jus Orange 1L | Juices | SUP_COPAG | 12 | 24 | 40 |
| P_504 | Jus Pomme 1L | Juices | SUP_COPAG | 12 | 24 | 30 |
| P_505 | Jus Multifruits 1L | Juices | SUP_COPAG | 12 | 24 | 35 |
| P_506 | Lait Fermente 1L | Dairy | SUP_COPAG | 12 | 36 | 45 |

---

## Appendix B: Store List

| Store ID | City | Region |
|----------|------|--------|
| STORE_CASABLANCA | Casablanca | Casablanca-Settat |
| STORE_RABAT | Rabat | Rabat-Salé-Kénitra |
| STORE_TANGIER | Tangier | Tanger-Tétouan-Al Hoceïma |
| STORE_MARRAKECH | Marrakech | Marrakech-Safi |
| STORE_FES | Fès | Fès-Meknès |
| STORE_AGADIR | Agadir | Souss-Massa |
| STORE_MEKNES | Meknès | Fès-Meknès |
| STORE_OUJDA | Oujda | Oriental |
| STORE_KENITRA | Kénitra | Rabat-Salé-Kénitra |
| STORE_TETOUAN | Tétouan | Tanger-Tétouan-Al Hoceïma |

---

## Appendix C: Troubleshooting

### C.1 Common Issues

| Issue | Symptom | Solution |
|-------|---------|----------|
| Containers not running | `docker ps` shows missing containers | Run `docker-compose up -d` |
| HDFS upload fails | "Connection refused" error | Wait for namenode to fully start (~30s) |
| Trino query fails | "Table not found" | Run Step 4 to create Hive tables |
| Empty supplier files | No items in JSON | Check if orders were generated |
| Wrong date data | Pipeline uses old data | Check `DATE` variable in scripts |

### C.2 Useful Commands

```bash
# Check container logs
docker logs namenode
docker logs trino
docker logs hive-metastore

# Manual Trino query
docker exec -it trino trino
trino> SELECT * FROM hive.procurement.orders LIMIT 10;

# Check HDFS content
docker exec namenode hdfs dfs -ls -R /raw

# Restart everything
docker-compose down && docker-compose up -d
```

---

## Appendix D: Glossary

| Term | Definition |
|------|------------|
| **SKU** | Stock Keeping Unit - unique product identifier |
| **MOQ** | Minimum Order Quantity - smallest order a supplier accepts |
| **Pack Size** | Number of units per case/carton |
| **Safety Stock** | Buffer inventory to prevent stockouts |
| **Lead Time** | Days between placing order and receiving goods |
| **Net Demand** | What needs to be ordered after considering current stock |
| **HDFS** | Hadoop Distributed File System - scalable storage |
| **Hive** | Data warehouse system for querying HDFS data |
| **Trino** | Distributed SQL query engine (formerly PrestoSQL) |
| **CTE** | Common Table Expression - SQL subquery defined with WITH |

---

**Document Version**: 1.0  
**Last Updated**: January 12, 2026  
**Author**: Procurement Data Team  
