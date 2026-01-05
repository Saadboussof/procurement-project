import json
import csv
import random
import os
from datetime import date, datetime

DATE = date.today().isoformat()

# =============================================
# STORES - 10 Moroccan cities
# =============================================
STORES = [
    "Casablanca", "Rabat", "Tangier", "Marrakech", "Fes",
    "Agadir", "Meknes", "Oujda", "Kenitra", "Tetouan"
]

# =============================================
# PRODUCTS - 30 SKUs matching init.sql
# =============================================
PRODUCTS = [
    # Dairy (Danone)
    "P_101", "P_102", "P_103", "P_104", "P_105", "P_106",
    # Oils (Lesieur)
    "P_201", "P_202", "P_203", "P_204", "P_205",
    # Sugar (Cosumar)
    "P_301", "P_302", "P_303", "P_304",
    # Biscuits (Bimo)
    "P_401", "P_402", "P_403", "P_404", "P_405", "P_406",
    # Juices (Copag)
    "P_501", "P_502", "P_503", "P_504", "P_505", "P_506"
]

# Product popularity weights (some products sell more)
PRODUCT_WEIGHTS = {
    "P_101": 15, "P_102": 10, "P_103": 8, "P_104": 5, "P_105": 6, "P_106": 12,
    "P_201": 10, "P_202": 4, "P_203": 6, "P_204": 8, "P_205": 3,
    "P_301": 20, "P_302": 8, "P_303": 3, "P_304": 2,
    "P_401": 12, "P_402": 10, "P_403": 7, "P_404": 6, "P_405": 4, "P_406": 5,
    "P_501": 14, "P_502": 10, "P_503": 9, "P_504": 6, "P_505": 7, "P_506": 8
}

# Ensure output directory exists
os.makedirs("data", exist_ok=True)

# --- 1. GENERATE ORDERS (JSON) for each store ---
print(f"Generating orders for {DATE}...")
total_transactions = 0

for store in STORES:
    transactions = []
    
    # Simulate 50-150 transactions per store (realistic daily volume)
    num_transactions = random.randint(50, 150)
    
    for i in range(num_transactions):
        # Weighted random product selection
        product = random.choices(
            PRODUCTS, 
            weights=[PRODUCT_WEIGHTS[p] for p in PRODUCTS]
        )[0]
        
        # Random hour between 8:00 and 21:00
        hour = random.randint(8, 21)
        minute = random.randint(0, 59)
        
        tx = {
            "transaction_id": f"TXN_{store[:3].upper()}_{DATE.replace('-','')}_{i:04d}",
            "timestamp": f"{DATE}T{hour:02d}:{minute:02d}:00",
            "store_id": f"STORE_{store.upper()}",
            "product_sku": product,
            "quantity": random.randint(1, 15)
        }
        transactions.append(tx)
    
    total_transactions += len(transactions)
    
    # Write to JSON file
    output_dir = f"data/raw/orders/{DATE}"
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{output_dir}/orders-{store.lower()}.json"
    
    with open(filename, "w") as f:
        for txn in transactions:
            f.write(json.dumps(txn) + "\n")
    print(f" -> Created {filename} ({len(transactions)} transactions)")

print(f"Total transactions generated: {total_transactions}")

# --- 2. GENERATE INVENTORY (CSV) ---
print("\nGenerating warehouse inventory...")
stock_dir = f"data/raw/stock/{DATE}"
os.makedirs(stock_dir, exist_ok=True)
inventory_file = f"{stock_dir}/inventory.csv"

# Warehouse stock levels (simulating realistic inventory)
INVENTORY_DATA = {
    # SKU: (available, reserved, safety_stock)
    # Dairy - high turnover
    "P_101": (500, 50, 100),   # Milk - high stock
    "P_102": (200, 30, 60),    # Yogurt Strawberry
    "P_103": (180, 25, 50),    # Yogurt Vanilla
    "P_104": (100, 10, 30),    # Danette
    "P_105": (120, 15, 40),    # Activia
    "P_106": (250, 40, 80),    # Raibi - popular
    
    # Oils - medium turnover
    "P_201": (150, 20, 50),    # Huile Tournesol
    "P_202": (60, 5, 20),      # Huile Olive
    "P_203": (80, 10, 25),     # Mayonnaise
    "P_204": (100, 15, 30),    # Ketchup
    "P_205": (50, 5, 15),      # Moutarde
    
    # Sugar - high demand
    "P_301": (400, 100, 150),  # Sucre Granule - very high
    "P_302": (150, 30, 60),    # Sucre Morceaux
    "P_303": (70, 10, 25),     # Sucre Glace
    "P_304": (40, 5, 15),      # Miel
    
    # Biscuits - medium-high
    "P_401": (180, 30, 60),    # Tagger Chocolate
    "P_402": (150, 25, 50),    # Tagger Vanille
    "P_403": (100, 15, 35),    # Petit Dejeuner
    "P_404": (80, 10, 25),     # Cookies
    "P_405": (60, 8, 20),      # Gaufrettes
    "P_406": (90, 12, 30),     # Crackers
    
    # Juices - high turnover
    "P_501": (300, 60, 100),   # Lben - very popular
    "P_502": (250, 50, 80),    # Raib
    "P_503": (200, 40, 70),    # Jus Orange
    "P_504": (120, 20, 40),    # Jus Pomme
    "P_505": (150, 25, 50),    # Jus Multifruits
    "P_506": (180, 35, 60),    # Lait Fermente
}

with open(inventory_file, "w", newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["warehouse_id", "product_sku", "available_quantity", "reserved_quantity", "safety_stock"])
    
    for sku, (available, reserved, safety) in INVENTORY_DATA.items():
        # Add some randomness to simulate real conditions
        avail = max(0, available + random.randint(-30, 30))
        res = max(0, reserved + random.randint(-5, 5))
        writer.writerow(["WH_MAIN", sku, avail, res, safety])

print(f" -> Created {inventory_file} ({len(INVENTORY_DATA)} products)")
print(f"\nDone! Data is ready for {DATE}")