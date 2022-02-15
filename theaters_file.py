from pymongo import MongoClient
import json
from bson import ObjectId
try:
    connection = MongoClient('localhost', 27017)
except:
    print("Error in Connect")
db=connection['Mflix']
theaters=db['theaters']
#question 4.c.(i)
def top_10_cities_with_maximum_theaters(n):
    output=theaters.aggregate([{"$group":{"_id":"$location.address.city","count":{"$sum":1}}},
                               {"$project":{"location.address.city":1,"count":1}},
                              {"$sort":{"count":-1}},{"$limit":n}])
    for show in output:
        print(show)
top_10_cities_with_maximum_theaters(10)
#question 4.c.(ii)

def top10_theaters_near_givenCoordinates(lat, lng):
    output = db.theaters.aggregate([{"$geoNear": {"near": {"type": "Point", "coordinates": [-91.24, 43.85]},
                                    "maxDistance": 10000000, "distanceField": "distance"}},
                                    {"$project": {"location.address.city": 1, "_id": 0, "location.geo.coordinates": 1}},
                                    {"$limit": 10}])
    for show in output:
       print(show)
top10_theaters_near_givenCoordinates(-91.24,43.85)

