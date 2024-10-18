from sentence_transformers import SentenceTransformer
from pinecone import Pinecone
from pymongo.mongo_client import MongoClient
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
    db=client["mytestdb"]
    collection=db["mytestcollection"]
    embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
    cursor = collection.watch(full_document='updateLookup')
    print("Change stream is now open.")
    while True:
        change = next(cursor)
        # If a new document is inserted into the collection, replicate its vector in Pinecone
        if change['operationType'] == 'insert':
            document = change['fullDocument']
            # convert the document's name into an embedding
            vector = embedding_model.encode(document['fullplot'])
            # Ensure the vector is a flat list of floats (and possibly convert to float64)
            vector = vector.tolist()  # Convert from numpy array to list
            vector = [float(x) for x in vector]  # Convert elements to float (usually float64)
            # Prepare the data for Pinecone upsert, which requires a tuple of (id, vector)
            # Assuming 'document['_id']' is the unique ID for the upsert operation
            upsert_data = (str(document['_id']), vector)
            # Insert into Pinecone
            index.upsert([upsert_data])  # Note that upsert_data is enclosed in a list

        elif change['operationType'] == 'update':
            document = change['fullDocument']
            document_id = document['_id']
            updated_fields = change['updateDescription']['updatedFields']

            # if the change is in the name field, generate the embedding and insert
            if updated_fields.get('fullplot'):
                vector = embedding_model.encode(updated_fields['fullplot'])
                upsert_data = (str(document_id), vector)
                # Insert into Pinecone
                index.upsert([upsert_data])  # Note that upsert_data is enclosed in a list

                #pinecone.upsert(index_name="myindex", data=vector, ids=[str(document_id)])

            # If a document is deleted from the collection, remove its vector from Pinecone
        elif change['operationType'] == 'delete':
            index.delete(ids=[str(change['documentKey']['_id'])])
if __name__ == "__main__":
    main() 