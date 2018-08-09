import pymongo
import pandas as pd
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.lmi
collection = db.reedjobs
data = pd.DataFrame(list(collection.find()))

print(data.info())
