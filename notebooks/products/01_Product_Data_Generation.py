# Databricks notebook source
from faker import Faker
from pyspark.sql.functions import *
import random
from datetime import datetime
from pyspark.sql import functions
from pyspark.sql import *

# COMMAND ----------

# DBTITLE 1,Cell 2
fake = Faker()

products = []

product_map = {
    "Electronics": {
        "subcategories": ["Mobile Phones", "Laptops", "Cameras", "Headphones", "Smartwatches"],
        "brands": ["Samsung", "Apple", "Sony", "Dell", "Bose"],
        "price_range": (5000, 100000),
        "cost_factor": (0.7, 0.9)
    },
    "Grocery": {
        "subcategories": ["Fruits", "Vegetables", "Dairy", "Snacks", "Beverages"],
        "brands": ["Nestlé", "Kellogg's", "PepsiCo", "Britannia", "Amul"],
        "price_range": (50, 2000),
        "cost_factor": (0.6, 0.8)
    },
    "Fashion": {
        "subcategories": ["Men's Clothing", "Women's Clothing", "Footwear", "Accessories", "Kidswear"],
        "brands": ["Nike", "Adidas", "Zara", "H&M", "Levi's"],
        "price_range": (500, 10000),
        "cost_factor": (0.5, 0.75)
    },
    "Home": {
        "subcategories": ["Furniture", "Kitchenware", "Decor", "Bedding", "Lighting"],
        "brands": ["IKEA", "Philips", "Prestige", "Havells", "Godrej"],
        "price_range": (1000, 50000),
        "cost_factor": (0.65, 0.85)
    },
    "Beauty": {
        "subcategories": ["Skincare", "Haircare", "Makeup", "Fragrances", "Personal Care"],
        "brands": ["L'Oréal", "Maybelline", "Dove", "Nivea", "Lakmé"],
        "price_range": (200, 5000),
        "cost_factor": (0.55, 0.75)
    }
}

for product in range(101, 601):
    category = random.choice(list(product_map.keys()))
    subcategory = random.choice(product_map[category]["subcategories"])
    brand = random.choice(product_map[category]["brands"])

    price = random.randint(*product_map[category]["price_range"])
    costprice = int(price * random.uniform(*product_map[category]["cost_factor"]))

    product_name = f"{brand} {subcategory} {fake.word().capitalize()}"

    products.append(
        {
            "product_id": product,
            "product_name": product_name,
            "category": category,
            "sub_category": subcategory,
            "brand": brand,
            "price": float(price),
            "cost_price": float(costprice),
            "launch_date": fake.date_between(start_date="-2y", end_date="today"),
            "supplier_name": fake.company(),
            "stock_quantity": random.randint(0, 500),
            "is_active": random.choice([True]),
            "createddate": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    )

products_df = spark.createDataFrame(products)

# COMMAND ----------

display(products_df)

# COMMAND ----------

products_df.printSchema()

# COMMAND ----------

products_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("abfss://bronze@saretailsales.dfs.core.windows.net/products")

# COMMAND ----------

