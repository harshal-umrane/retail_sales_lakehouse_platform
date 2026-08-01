# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime

BATCH_ID = datetime.now().strftime("%Y%m%d%H%M%S")

# COMMAND ----------

silver_df = spark.read \
            .format("delta") \
                .load("abfss://silver@saretailsales.dfs.core.windows.net/customers")

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df.agg(max("customer_id")).display()


# COMMAND ----------

check_df = silver_df.select(col("customer_id"), col("first_name"), col("last_name")).filter((col("first_name") == "Dayita") & (col("last_name") == "Choudhury"))

display(check_df)

# COMMAND ----------

# STATE WISE CUSTOMER COUNT
customer_state_summary = silver_df.groupBy("state") \
                            .count() \
                            .withColumnRenamed("count", "customer_count") \
                            .withColumn("etl_load_timestamp", current_timestamp()) \
                            .withColumn("batch_id", lit(BATCH_ID))

display(customer_state_summary)

# COMMAND ----------

customer_state_summary.write \
    .mode("overwrite") \
        .format("delta") \
            .save("abfss://gold@saretailsales.dfs.core.windows.net/customers/customer_state_summary")

# COMMAND ----------

# GENDER WISE CUSTOMER COUNT
customer_gender_summary = silver_df.groupBy("gender") \
                            .count() \
                            .withColumnRenamed("count", "customer_count") \
                            .withColumn("etl_load_timestamp", current_timestamp()) \
                            .withColumn("batch_id", lit(BATCH_ID))

display(customer_gender_summary)

# COMMAND ----------

customer_state_summary.write \
    .mode("overwrite") \
        .format("delta") \
            .save("abfss://gold@saretailsales.dfs.core.windows.net/customers/customer_gender_summary")

# COMMAND ----------

# CITY WISE CUSTOMER COUNT
customer_city_summary = silver_df.groupBy("city") \
                            .count() \
                            .withColumnRenamed("count", "customer_count") \
                            .withColumn("etl_load_timestamp", current_timestamp()) \
                            .withColumn("batch_id", lit(BATCH_ID))

display(customer_city_summary)

# COMMAND ----------

customer_city_summary.write \
    .mode("overwrite") \
        .format("delta") \
            .save("abfss://gold@saretailsales.dfs.core.windows.net/customers/customer_city_summary")

# COMMAND ----------

# MONTHLY REGISTRATION CUSTOMER COUNT

silver_df = silver_df.withColumn("year_month", date_format(col("registration_date"), "yyyy-MM"))

customer_monthly_registration = silver_df.groupBy("year_month") \
                            .count() \
                            .withColumnRenamed("count", "customer_count") \
                            .withColumn("etl_load_timestamp", current_timestamp()) \
                            .withColumn("batch_id", lit(BATCH_ID))

display(customer_monthly_registration)

# COMMAND ----------

customer_monthly_registration.write \
    .mode("overwrite") \
        .format("delta") \
            .save("abfss://gold@saretailsales.dfs.core.windows.net/customers/customer_monthly_registration")

# COMMAND ----------


silver_df = silver_df.withColumn("age", (datediff(current_date(), col("dob")) / 365).cast("int"))

silver_df = silver_df.withColumn("age_group",
    when((col("age") >= 18) & (col("age") <= 29), "18-29")
    .when((col("age") >= 30) & (col("age") <= 39), "30-39")
    .when((col("age") >= 40) & (col("age") <= 49), "40-49")
    .when((col("age") >= 50) & (col("age") <= 59), "50-59")
    .when((col("age") >= 60) & (col("age") <= 69), "60-69")
    .when((col("age") >= 70) & (col("age") <= 79), "70-79")
    .when((col("age") >= 80) & (col("age") <= 89), "80-89")
    .when((col("age") >= 90) & (col("age") <= 99), "90-99")
    .otherwise("100+")
)

customer_age_group_summary = silver_df.groupBy("age_group") \
                                        .count() \
                                        .withColumnRenamed("count", "customer_count") \
                                        .withColumn("etl_load_timestamp", current_timestamp()) \
                                        .withColumn("batch_id", lit(BATCH_ID)) \
                                        .orderBy(col("age_group").asc())

display(customer_age_group_summary)


# COMMAND ----------

customer_age_group_summary.write \
    .mode("overwrite") \
        .format("delta") \
            .save("abfss://gold@saretailsales.dfs.core.windows.net/customers/customer_age_group_summary")

# COMMAND ----------

