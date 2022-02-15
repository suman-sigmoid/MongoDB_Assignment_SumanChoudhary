from pymongo import MongoClient
try:
    connection=MongoClient('localhost',27017)
except:
    print("error in connection")
db=connection['Mflix']
comments=db['comments']
users=db['users']
movies=db['movies']
theaters=db['theaters']

def insertComment(value):
    comments.insert_one(value)
def insertMovie(value):
    users.insert_one(value)
def insertTheater(value):
    theaters.insert_one(value)
def insertUser(value):
    users.insert_one(value)

