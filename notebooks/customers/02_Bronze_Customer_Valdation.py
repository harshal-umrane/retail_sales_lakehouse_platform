# Databricks notebook source
customer_df = spark.read \
    .format("delta") \
    .load("abfss://bronze@saretailsales.dfs.core.windows.net/customers/")


# COMMAND ----------

customer_df.printSchema()

# COMMAND ----------

duplicate_customers = customer_df \
    .groupBy('customer_id') \
    .count().filter('count(1) > 1')

duplicate_email = customer_df \
    .groupBy('email') \
    .count().filter('count(1) > 1')

display(duplicate_email)

# COMMAND ----------

null_customerid = customer_df.filter("customer_id is null")

null_emails = customer_df.filter("email is null")

gender_validation = customer_df.filter("gender not in ('Male', 'Female')")

display(gender_validation)

# COMMAND ----------

future_date_registeration = customer_df.filter("registration_date > current_date")

display(future_date_registeration)


# COMMAND ----------

phoneno_length = customer_df.filter("length(phone) != 10")

display(phoneno_length)

# COMMAND ----------

