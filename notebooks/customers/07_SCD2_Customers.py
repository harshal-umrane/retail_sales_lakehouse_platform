# Databricks notebook source
from pyspark.sql.functions import *
from delta import DeltaTable

# COMMAND ----------

bronze_customers = spark.read \
                    .format("delta") \
                        .load("abfss://bronze@saretailsales.dfs.core.windows.net/customers")

# COMMAND ----------

bronze_customers = bronze_customers.withColumn("effective_from", current_timestamp()) \
                                    .withColumn("effective_to", lit(None)) \
                                    .withColumn("is_active", lit(True)) \
                                    .withColumn("last_updated_timestamp", current_timestamp())
display(bronze_customers)

# COMMAND ----------

bronze_customers.write \
                .mode("overwrite") \
                .format("delta") \
                .save("abfss://silver@saretailsales.dfs.core.windows.net/customers_scd")

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
     "India", "9912835391", "2026-01-28")
]

columns = [ "customer_id", "first_name", "last_name", "dob", "email", "gender",
    "address", "city", "state", "pincode", "country", "phone", "registration_date"]

upsert_df = spark.createDataFrame(upsert_data, columns)

# COMMAND ----------

display(upsert_df)

# COMMAND ----------

silver_df = spark.read \
                .format("delta") \
                .load("abfss://silver@saretailsales.dfs.core.windows.net/customers_scd")

# silver_table = DeltaTable.forPath(spark, "abfss://silver@saretailsales.dfs.core.windows.net/customers_scd")


# COMMAND ----------

# DBTITLE 1,Cell 8
# Fix effective_to column type from VOID to TIMESTAMP before merge
spark.sql("""
    ALTER TABLE delta.`abfss://silver@saretailsales.dfs.core.windows.net/customers_scd`
    ALTER COLUMN effective_to TYPE TIMESTAMP
""")

silver_table.alias("target").merge(upsert_df.alias("source"), "target.customer_id = source.customer_id") \
    .whenMatchedUpdate(
        set={
            "is_active": "false",
            "effective_to": "current_timestamp()",
            "last_updated_timestamp": "current_timestamp()"
        }
    ) \
    .execute()

# COMMAND ----------

silver_df.filter(silver_df.customer_id == 1).display()

# COMMAND ----------

silver_table.alias("target").merge(upsert_df.alias("source"), "target.customer_id = source.customer_id AND target.is_active = 'true'") \
    .whenNotMatchedInsert(
        values = {
            "customer_id": "source.customer_id",
            "first_name": "source.first_name",
            "last_name": "source.last_name",
            "dob": "source.dob",
            "email": "source.email",
            "phone": "source.phone",
            "address": "source.address",
            "city": "source.city",
            "state": "source.state",
            "pincode": "source.pincode",            
            "country": "source.country",
            "gender": "source.gender",
            "registration_date": "source.registration_date",
            "effective_from": "current_timestamp()",
            "effective_to": "null",
            "is_active": "true",
            "last_updated_timestamp": "current_timestamp()"
        }
    ) \
    .execute()

# COMMAND ----------

display(silver_df)

# COMMAND ----------

