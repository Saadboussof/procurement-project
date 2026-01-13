import os
import subprocess
import sys
import time
import json
import csv
from datetime import date

DATE = date.today().isoformat()

def run_command(cmd, check=False):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  Error (code {result.returncode}): {result.stderr.strip()}")
        if check:
            sys.exit(1)
    return result.stdout

def main():
    print(f"=== Procurement Pipeline Setup & Test (Date: {DATE}) ===")

    # 1. Generate Data
    print("\n[1/7] Generating Data...")
    if not os.path.exists("generate_data.py"):
        print("Error: generate_data.py not found.")
        return
    run_command(f"{sys.executable} generate_data.py", check=True)

    # 2. Check Containers
    print("\n[2/7] Checking Docker Containers...")
    ps_out = run_command("docker ps")
    required_containers = ["namenode", "trino", "hive-metastore", "postgres"]
    missing = [c for c in required_containers if c not in ps_out]
    
    if missing:
        print(f"  Warning: The following containers seem to be missing or not running: {missing}")
        print("  Please run 'docker-compose up -d' and wait for them to initialize.")
    else:
        print("  All required containers are running.")

    # 3. Upload Data to HDFS
    print("\n[3/7] Uploading Data to HDFS...")
    
    # Create HDFS directories
    hdfs_orders_path = f"/raw/orders/{DATE}"
    hdfs_stock_path = f"/raw/stock/{DATE}"
    
    run_command(f"docker exec namenode hdfs dfs -mkdir -p {hdfs_orders_path}")
    run_command(f"docker exec namenode hdfs dfs -mkdir -p {hdfs_stock_path}")

    # Local paths
    local_orders_path = f"data/raw/orders/{DATE}"
    local_stock_path = f"data/raw/stock/{DATE}"

    # Copy files to namenode container then put to HDFS
    # We iterate over files in the local directories
    
    # Orders
    if os.path.exists(local_orders_path):
        for f in os.listdir(local_orders_path):  
            local_file = os.path.join(local_orders_path, f)
            remote_tmp = f"/tmp/{f}"
            run_command(f"docker cp {local_file} namenode:{remote_tmp}", check=True)
            run_command(f"docker exec namenode hdfs dfs -put -f {remote_tmp} {hdfs_orders_path}/")
    else:
        print(f"Warning: {local_orders_path} does not exist.")

    # Stock
    if os.path.exists(local_stock_path):
        for f in os.listdir(local_stock_path):
            local_file = os.path.join(local_stock_path, f)
            remote_tmp = f"/tmp/{f}"
            run_command(f"docker cp {local_file} namenode:{remote_tmp}", check=True)
            run_command(f"docker exec namenode hdfs dfs -put -f {remote_tmp} {hdfs_stock_path}/")
    else:
        print(f"Warning: {local_stock_path} does not exist.")
    
    # Verify HDFS content
    ls_out = run_command(f"docker exec namenode hdfs dfs -ls -R /raw")
    print("  HDFS Content:\n" + ls_out)

    # 4. Create Hive Tables
    print("\n[4/7] Creating Hive Tables via Trino...")
    
    # Read template and replace DATE
    with open("sql/hive_schema.sql", "r") as f:
        schema_sql = f.read().replace("{DATE}", DATE)
    
    with open("sql/hive_schema_run.sql", "w") as f:
        f.write(schema_sql)

    run_command("docker cp sql/hive_schema_run.sql trino:/tmp/hive_schema_run.sql", check=True)
    
    # Wait for Trino
    print("  Waiting for Trino...")
    for i in range(10):
        check = run_command("docker exec trino trino --execute \"SELECT 1\"")
        if "1" in check:
            break
        time.sleep(5)
    
    ddl_out = run_command("docker exec trino trino --file /tmp/hive_schema_run.sql")
    print("  DDL Output:\n" + ddl_out)

    # 5. Run Net Demand Calculation & Generate Supplier Files
    print("\n[5/7] Calculating Net Demand & Generating Supplier Orders...")
    run_command("docker cp sql/net_demand.sql trino:/tmp/net_demand.sql", check=True)
    
    # Run query and get CSV output
    # We use CSV_HEADER to parse it easily
    query_out = run_command("docker exec trino trino --file /tmp/net_demand.sql --output-format CSV_HEADER")
    
    # Save aggregated orders to /processed/aggregated_orders/{DATE}/
    run_command(f"docker exec namenode hdfs dfs -mkdir -p /processed/aggregated_orders/{DATE}")
    run_command(f"docker exec namenode hdfs dfs -mkdir -p /processed/net_demand/{DATE}")
    
    # Parse CSV output
    # The output might contain logs or other text, so we need to be careful. 
    # Trino CLI output usually goes to stdout.
    
    # We'll save the output to a file locally to parse
    lines = query_out.strip().split('\n')
    # Filter out lines that don't look like CSV (e.g. "SET") if any, but CSV_HEADER usually is clean if no errors.
    # Actually, let's just try to parse it.
    
    # Save net_demand results to processed folder
    net_demand_local = f"data/processed/net_demand/{DATE}"
    os.makedirs(net_demand_local, exist_ok=True)
    with open(f"{net_demand_local}/net_demand.csv", "w") as f:
        f.write(query_out)
    run_command(f"docker cp {net_demand_local}/net_demand.csv namenode:/tmp/net_demand.csv")
    run_command(f"docker exec namenode hdfs dfs -put -f /tmp/net_demand.csv /processed/net_demand/{DATE}/")
    
    reader = csv.DictReader(lines)
    supplier_orders = {}
    
    try:
        for row in reader:
            # row keys now include: supplier_id, supplier_name, product_sku, product_name, lead_time_days, raw_net_demand, pack_size, min_order_quantity, order_quantity
            sup_id = row.get('supplier_id')
            if not sup_id: continue
            
            if sup_id not in supplier_orders:
                supplier_orders[sup_id] = {
                    "supplier_name": row.get('supplier_name', ''),
                    "lead_time_days": int(float(row.get('lead_time_days', 0))),
                    "order_date": DATE,
                    "items": []
                }
            
            supplier_orders[sup_id]["items"].append({
                "product_sku": row['product_sku'],
                "product_name": row['product_name'],
                "raw_demand": int(float(row.get('raw_net_demand', 0))),
                "pack_size": int(float(row.get('pack_size', 1))),
                "min_order_quantity": int(float(row.get('min_order_quantity', 0))),
                "order_quantity": int(float(row['order_quantity']))
            })
    except Exception as e:
        print(f"  Error parsing query output: {e}")
        print("  Raw Output head:", lines[:5])

    # Write Supplier Files
    output_base = f"data/output/supplier_orders/{DATE}"
    os.makedirs(output_base, exist_ok=True)
    
    for sup_id, order_data in supplier_orders.items():
        fname = f"{output_base}/{sup_id}.json"
        with open(fname, "w") as f:
            json.dump(order_data, f, indent=2)
        print(f"  -> Generated order for {sup_id}: {fname}")
        
        # Upload to HDFS /output/supplier_orders/{DATE}/
        hdfs_out_path = f"/output/supplier_orders/{DATE}"
        run_command(f"docker exec namenode hdfs dfs -mkdir -p {hdfs_out_path}")
        run_command(f"docker cp {fname} namenode:/tmp/{sup_id}.json")
        run_command(f"docker exec namenode hdfs dfs -put -f /tmp/{sup_id}.json {hdfs_out_path}/")

    # 6. Save Aggregated Orders to /processed/
    print("\n[6/7] Saving Aggregated Orders to HDFS...")
    run_command("docker cp sql/aggregated_orders.sql trino:/tmp/aggregated_orders.sql", check=True)
    agg_out = run_command("docker exec trino trino --file /tmp/aggregated_orders.sql --output-format CSV_HEADER")
    
    agg_local = f"data/processed/aggregated_orders/{DATE}"
    os.makedirs(agg_local, exist_ok=True)
    with open(f"{agg_local}/aggregated_orders.csv", "w") as f:
        f.write(agg_out)
    run_command(f"docker cp {agg_local}/aggregated_orders.csv namenode:/tmp/aggregated_orders.csv")
    run_command(f"docker exec namenode hdfs dfs -put -f /tmp/aggregated_orders.csv /processed/aggregated_orders/{DATE}/")
    print(f"  -> Saved aggregated orders to HDFS /processed/aggregated_orders/{DATE}/")

    # 7. Exception Management
    print("\n[7/7] Checking for Exceptions...")
    run_command("docker cp sql/exceptions.sql trino:/tmp/exceptions.sql", check=True)
    except_out = run_command("docker exec trino trino --file /tmp/exceptions.sql --output-format CSV_HEADER")
    
    lines = except_out.strip().split('\n')
    
    # Save exceptions to HDFS /logs/exceptions/{DATE}/
    run_command(f"docker exec namenode hdfs dfs -mkdir -p /logs/exceptions/{DATE}")
    
    if len(lines) > 1: # Header + Data
        print("  !! Exceptions Found !!")
        log_dir = f"data/logs/exceptions/{DATE}"
        os.makedirs(log_dir, exist_ok=True)
        log_file = f"{log_dir}/report.csv"
        with open(log_file, "w") as f:
            f.write(except_out)
        print(f"  Exception report saved to {log_file}")
        run_command(f"docker cp {log_file} namenode:/tmp/exceptions.csv")
        run_command(f"docker exec namenode hdfs dfs -put -f /tmp/exceptions.csv /logs/exceptions/{DATE}/")
    else:
        print("  No exceptions found.")
        # Still create an empty log file for auditability
        log_dir = f"data/logs/exceptions/{DATE}"
        os.makedirs(log_dir, exist_ok=True)
        log_file = f"{log_dir}/report.csv"
        with open(log_file, "w") as f:
            f.write("No exceptions detected\n")

    print("\n=== Pipeline Complete ===")

    
    if not query_out.strip() or "Error" in query_out or "failed" in query_out.lower():
        print("\n❌ Test Failed.")
    else:
        print("\n✅ Test Passed! Pipeline is working.")

if __name__ == "__main__":
    main()
