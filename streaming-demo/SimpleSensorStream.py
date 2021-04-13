# Databricks notebook source

TOPIC = "sensor_hub"
BOOTSTRAP_SERVERS = "mpfdemohubs.servicebus.windows.net:9093"
ConnectionString = dbutils.secrets.get("simple-demos", "ehConnString")
EH_SASL = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{ConnectionString}\";"

df = (spark.readStream
    .format("kafka")
    .option("subscribe", TOPIC)
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .load())

# COMMAND ----------


