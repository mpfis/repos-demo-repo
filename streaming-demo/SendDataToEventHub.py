# Databricks notebook source
from azure.servicebus import ServiceBusService
import json
import random

key_name = 'RootManageSharedAccessKey' # SharedAccessKeyName from Azure portal
key_value = dbutils.secrets.get("simple-demos", "keyValue") # SharedAccessKey from Azure portal
EVENT_HUB_NAME = "sensor_hub"
sbs = ServiceBusService("mpfdemohubs.servicebus.windows.net",
                        shared_access_key_name=key_name,
                        shared_access_key_value=key_value)

# COMMAND ----------



# COMMAND ----------

# use json.dumps to convert the dictionary to a JSON format
s = json.dumps({"Sensor1": random.randint(100,1000), "Sensor2": random.randint(100,1000), "Sensor3": random.randint(100,1000), "Sensor4": random.randint(100,1000)})
# send to Azure Event Hub
sbs.send_event(EVENT_HUB_NAME, s)

# COMMAND ----------


