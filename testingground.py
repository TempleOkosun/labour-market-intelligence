# Imports
import pymongo
import pandas as pd
from pymongo import MongoClient
import requests
import json

# client = MongoClient('localhost', 27017)
# db = client.lmi
# collection = db.reedjobs
# data = pd.DataFrame(list(collection.find()))# print(data.info())

# response2 = requests.get('https://maps.googleapis.com/maps/api/place/details/json?placeid=ChIJdd4hrwug2EcRmSrV3Vo6llI&fields=address_component,rating,formatted_address&locationbias=ipbias&key=AIzaSyCBP7Q-EOTwz25Q0zkEmTNFPs9TC5ldD-Y')
# print(response2.status_code)

# Get Place_Id:
# base_url = 'https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input=london&inputtype=textquery&fields=formatted_address,place_id&locationbias=ipbias&key=AIzaSyCBP7Q-EOTwz25Q0zkEmTNFPs9TC5ldD-Y'

# Get_Fuller_detail:
# 'https://maps.googleapis.com/maps/api/place/details/json?placeid=ChIJdd4hrwug2EcRmSrV3Vo6llI&fields=address_component,rating,formatted_address&locationbias=ipbias&key=AIzaSyCBP7Q-EOTwz25Q0zkEmTNFPs9TC5ldD-Y'

#Google api key for agvictormedia = 'AIzaSyB7mLXdBzzNmO6r1rFOrR1BYV9eOUCm5zg'
# Set up the parameters we want to pass to the API.

# This is the latitude and longitude of New York City.
API_KEY = 'AIzaSyB7mLXdBzzNmO6r1rFOrR1BYV9eOUCm5zg'
my_parameters = {
    "input": 'Edinburgh',
    "inputtype": 'textquery',
    "fields": 'formatted_address,place_id',
    "locationbias": 'ipbias',
"key": API_KEY,

}
response1 = requests.get('https://maps.googleapis.com/maps/api/place/findplacefromtext/json?', params=my_parameters)
print(response1.status_code)
print(response1.content)
data = response1.json()

mydata= json.dumps(data)
print(mydata)

# Headers is a dictionary
print(response1.headers)
# Get the content-type from the dictionary.
print(response1.headers["content-type"])



# Make a list of fast food chains.
best_food_chains = ["Taco Bell", "Shake Shack", "Chipotle"]
# This is a list.
print(type(best_food_chains))
# Use json.dumps to convert best_food_chains to a string.
best_food_chains_string = json.dumps(best_food_chains)
# We've successfully converted our list to a string.
print(type(best_food_chains_string))
print(best_food_chains_string)