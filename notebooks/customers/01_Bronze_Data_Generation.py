# Databricks notebook source
from faker import Faker
from pyspark.sql import Row
import random

# COMMAND ----------

fake = Faker("en_IN")

customer = []

for name in range(1, 1001):
    customer.append(
        Row(
            customer_id = name,
            first_name = fake.first_name(),
            last_name = fake.last_name(),
            dob = fake.date_of_birth(),
            email = fake.email(),
            gender = random.choice(["Male", "Female"]),
            address = fake.address(),
            city = fake.city(),
            state = fake.state(),
            pincode = fake.postcode(),
            country = fake.country(),
            phone = "9" + "".join(random.choices("0123456789", k=9)),
            registration_date = str(fake.date_between(start_date='-2y', end_date='today'))
        )
    )

# COMMAND ----------

products = []

for name in range(1, 101):
    products.append(
        Row(
            product_id = name,
            product_name = fake.word(),
            category = random.choice(["Electronics", "Clothing", "Home Appliances", "Sports", "Toys"]),
            subcategory = random.choice(["Laptop", "Shirt", "Refrigerator", "Football", "Teddy Bear"]),
            brand = random.choice(["Apple", "Adidas", "Samsung", "Nike", "Puma"]),
            product_price = random.randint(100, 10000),
            product_quantity = random.randint(1, 10)
        )
    )
products_df = spark.createDataFrame(products

# COMMAND ----------

customer_df = spark.createDataFrame(customer)

# COMMAND ----------

customer_df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("abfss://bronze@saretailsales.dfs.core.windows.net/customers")

# COMMAND ----------

