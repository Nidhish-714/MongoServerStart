from flask import Flask, jsonify
from sentence_transformers import SentenceTransformer
from pinecone import Pinecone
from pymongo.mongo_client import MongoClient
import threading

app = Flask(__name__)

# Function that will run the main logic
def main():
    uri = "mongodb+srv://Nidhish:Nidhish@coephackathon.pbuvv.mongodb.net/?retryWrites=true&w=majority&appName=CoepHackathon"
    # Create a new client and connect to the server
    client = MongoClient(uri)
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    pc = Pinecone(api_key="f68b6c67-0cfd-47b3-980b-5c29ea360fbf")
    index = pc.Index("mongo")
    db = client["mytestdb"]
    collection = db["mytestcollection"]
    embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
    cursor = collection.watch(full_document='updateLookup')
    print("Change stream is now open.")
    
    while True:
        change = next(cursor)
        # If a new document is inserted into the collection, replicate its vector in Pinecone
        if change['operationType'] == 'insert':
            document = change['fullDocument']
            vector = embedding_model.encode(document['fullplot'])
            vector = vector.tolist()  # Convert from numpy array to list
            vector = [float(x) for x in vector]  # Ensure elements are float
            upsert_data = (str(document['_id']), vector)
            index.upsert([upsert_data])

        elif change['operationType'] == 'update':
            document = change['fullDocument']
            document_id = document['_id']
            updated_fields = change['updateDescription']['updatedFields']

            if updated_fields.get('fullplot'):
                vector = embedding_model.encode(updated_fields['fullplot'])
                upsert_data = (str(document_id), vector)
                index.upsert([upsert_data])

        elif change['operationType'] == 'delete':
            index.delete(ids=[str(change['documentKey']['_id'])])

# API route to start the server
@app.route('/startServer', methods=['GET'])
def start_server():
    try:
        # Run the main function in a separate thread to avoid blocking the Flask server
        thread = threading.Thread(target=main)
        thread.start()
        return jsonify({"message": "Server started and watching MongoDB changes."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
