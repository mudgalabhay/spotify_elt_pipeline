import os 
import json 
import time 

from kafka import KafkaProducer
from dotenv import load_dotenv

# ------------------------------
# Load environment variables 
# ------------------------------
load_dotenv() # Used to read key-value pairs from .env file and load them as environment variables.

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP")