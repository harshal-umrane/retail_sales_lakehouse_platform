# Databricks notebook source
from faker import Faker
from pyspark.sql import *
import random
from pyspark.sql.functions import *

# COMMAND ----------

fake = Faker("en_IN")

customer= []

for name in range(1, 21):
    customer.append(
        Row(
            customer_id = name + 1000,
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

customer_incremental_df = spark.createDataFrame(customer)

# COMMAND ----------

display(customer_incremental_df)

# COMMAND ----------

customer_incremental_df.write \
    .mode("overwrite") \
        .format("delta") \
            .save("abfss://bronze@saretailsales.dfs.core.windows.net/customer_incremental/")

# COMMAND ----------

check_existence_df = spark.read \
            .format("delta") \
                .load("abfss://silver@saretailsales.dfs.core.windows.net/customers")

# COMMAND ----------

customer_incremental_df.printSchema()

# COMMAND ----------

# Typecasting columns with correct data types
customer_incremental_df = customer_incremental_df.withColumn("phone", col("phone").cast("long"))

customer_incremental_df = customer_incremental_df.withColumn("registration_date", to_date(col("registration_date")))

customer_incremental_df = customer_incremental_df.withColumn("pincode", col("pincode").cast("long")) 

# COMMAND ----------

upsert_data = [
    (1, "Pooja", "Bal", "1992-05-06", "datewilliam@example.org", "Female",
     "H.No. 269 Sem Chowk Tezpur 034239", "Bangalore", "Karnataka", 359409,
     "India", "9476079365", "2025-09-26"),

    (2, "Omisha", "Borra", "1911-03-28", "miteshchad@example.net", "Female",
     "475, Mane Path, Bally 118666", "Patna", "West Bengal", 994997,
     "India", "9157004601", "2024-12-20"),

    (3, "Dayita", "Choudhury", "1985-01-11", "sabharwalekapad@example.org", "Male",
     "H.No. 86, Deshpande Chowk, Ratlam-135380", "Ranchi", "Bihar", 823598,
     "India", "9912835391", "2026-01-28"),

    (4, "Eshana", "Rastogi", "1943-01-23", "eshana@example.org", "Female",
     "353, Subramaniam Road, Pune 633943", "Medininagar", "Assam", 185270,
     "India", "9570340141", "2025-10-29"),

    (5, "Samaksh", "Sastry", "1944-12-16", "samaksh@example.org", "Male",
     "94, Chadha Ganj Panvel-330849", "Gulbarga", "Telangana", 819281,
     "India", "9494511064", "2026-05-02"),
    
    (2, "Omisha", "Borra", "1911-03-28", "miteshchad@example.net", "Female",
     "475, Mane Path, Bally 118666", "Patna", "West Bengal", 994997,
     "India", "9157004601", "2024-12-20"),

    (3, "Dayita", "Choudhury", "1985-01-11", "sabharwalekapad@example.org", "Male",
     "H.No. 86, Deshpande Chowk, Ratlam-135380", "Ranchi", "Bihar", 823598,
     "India", "9912835391", "2026-01-28"),

    (4, "Eshana", "Rastogi", "1943-01-23", "eshana@example.org", "Female",
     "353, Subramaniam Road, Pune 633943", "Medininagar", "Assam", 185270,
     "India", "9570340141", "2025-10-29"),
]

columns = [
    "customer_id", "first_name", "last_name", "dob", "email", "gender",
    "address", "city", "state", "pincode", "country", "phone", "registration_date"
]

upsert_df = spark.createDataFrame(upsert_data, columns)


# COMMAND ----------

upsert_df.printSchema()

upsert_df = upsert_df.withColumn("dob", to_date("dob"))
upsert_df = upsert_df.withColumn("pincode", col("pincode").cast("long"))
upsert_df = upsert_df.withColumn("phone", col("phone").cast("long"))
upsert_df = upsert_df.withColumn("registration_date", to_date("registration_date"))

# COMMAND ----------

upsert_df.write \
    .mode("append") \
        .format("delta") \
            .save("abfss://bronze@saretailsales.dfs.core.windows.net/customer_incremental/")

# COMMAND ----------

new_incremental = spark.read \
    .format("delta") \
    .load("abfss://bronze@saretailsales.dfs.core.windows.net/customer_incremental/")

# COMMAND ----------

display(new_incremental)

# COMMAND ----------

