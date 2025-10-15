import csv, random
from datetime import datetime, timedelta

NUM_CUSTOMERS = 50
NUM_PRODUCTS = 20
NUM_PURCHASES = 200
NUM_BROWSING = 500

cities = ["Mumbai","Delhi","Bengaluru","Kolkata","Chennai"]
categories = ["electronics","books","home","fashion"]

with open("data/Customers.csv","w",newline='') as f:
    w=csv.writer(f); w.writerow(["customer_id","name","age","city","signup_date"])
    for i in range(1,NUM_CUSTOMERS+1):
        w.writerow([i,f"cust_{i}",random.randint(18,60),random.choice(cities),(datetime.now()-timedelta(days=random.randint(0,1000))).date()])

with open("data/Products.csv","w",newline='') as f:
    w=csv.writer(f); w.writerow(["product_id","product_name","category","price"])
    for i in range(1,NUM_PRODUCTS+1):
        w.writerow([i,f"product_{i}",random.choice(categories),round(random.uniform(10,5000),2)])

with open("data/Purchases.csv","w",newline='') as f:
    w=csv.writer(f); w.writerow(["purchase_id","customer_id","product_id","purchase_date","quantity"])
    for i in range(1,NUM_PURCHASES+1):
        w.writerow([i,random.randint(1,NUM_CUSTOMERS),random.randint(1,NUM_PRODUCTS),(datetime.now()-timedelta(days=random.randint(0,180))).date(),random.randint(1,5)])

with open("data/Browsing.csv","w",newline='') as f:
    w=csv.writer(f); w.writerow(["session_id","customer_id","product_id","view_timestamp"])
    for i in range(1,NUM_BROWSING+1):
        w.writerow([i,random.randint(1,NUM_CUSTOMERS),random.randint(1,NUM_PRODUCTS),(datetime.now()-timedelta(days=random.randint(0,30))).isoformat()])
