from flask import Flask, jsonify
from sentence_transformers import SentenceTransformer
from pinecone import Pinecone
from pymongo.mongo_client import MongoClient
import threading
import os

# Initialize Flask app
app = Flask(__name__)

# Global variable to hold the change stream thread
change_stream_thread = None
stop_thread = False

# Initialize MongoDB and Pinecone inside main()
def initialize_services():
    global client, db, collection, embedding_model, index

    # MongoDB URI
    uri = os.getenv("MONGODB_URI", "your_mongo_uri_here")
    client = MongoClient(uri)
    
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    # Initialize Pinecone
    api_key = os.getenv("PINECONE_API_KEY", "your_pinecone_api_key_here")
    pc = Pinecone(api_key=api_key)
    index = pc.Index("mongo")

    # Access database and collection
    db = client["mytestdb"]
    collection = db["mytestcollection"]

    # Initialize Sentence Transformer
    embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')

# Function to handle the change stream
def watch_change_stream():
    global stop_thread

    cursor = collection.watch(full_document='updateLookup')
    print("Change stream is now open.")

    while not stop_thread:
        try:
            change = next(cursor)
            
            if change['operationType'] == 'insert':
                document = change['fullDocument']
                vector = embedding_model.encode(document['fullplot']).tolist()
                upsert_data = (str(document['_id']), [float(x) for x in vector])
                index.upsert([upsert_data])

            elif change['operationType'] == 'update':
                document = change['fullDocument']
                document_id = document['_id']
                updated_fields = change['updateDescription']['updatedFields']

                if updated_fields.get('fullplot'):
                    vector = embedding_model.encode(updated_fields['fullplot']).tolist()
                    upsert_data = (str(document_id), [float(x) for x in vector])
                    index.upsert([upsert_data])

            elif change['operationType'] == 'delete':
                index.delete(ids=[str(change['documentKey']['_id'])])

        except Exception as e:
            print(f"Error in change stream: {e}")

# Endpoint to start the change stream
@app.route('/start_stream', methods=['POST'])
def start_stream():
    global change_stream_thread, stop_thread

    if change_stream_thread is None or not change_stream_thread.is_alive():
        stop_thread = False
        change_stream_thread = threading.Thread(target=watch_change_stream)
        change_stream_thread.start()
        return jsonify({"message": "Change stream started successfully!"})
    else:
        return jsonify({"message": "Change stream is already running."})

# Endpoint to stop the change stream
@app.route('/stop_stream', methods=['POST'])
def stop_stream():
    global stop_thread

    stop_thread = True
    if change_stream_thread:
        change_stream_thread.join()
    return jsonify({"message": "Change stream stopped successfully!"})

# Health check endpoint
@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "API is running."})

# Run the Flask app
if __name__ == "__main__":
    initialize_services()
    port = int(os.getenv("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
