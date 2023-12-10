from pymongo import MongoClient


def get_database():
	# Provide the mongodb atlas url to connect python to mongodb using pymongo
	connection_string = "mongodb+srv://sola-dat:IkTd86Kx7MTvBO1Z@live.2nng2.mongodb.net/sola?retryWrites=true&w=majority"

	# Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
	client = MongoClient(connection_string)

	# Create the database for our example (we will use the same database throughout the tutorial
	return client['sola']


# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":
	# Get the database
	dbname = get_database()
