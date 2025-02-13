from typing import Dict
from fastapi import FastAPI,HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from tinydb import TinyDB, Query
from uuid import uuid4, UUID
from datetime import datetime
import os

app = FastAPI()

class Document(BaseModel):
    content: Dict

def get_db(collection_name: str):
    file_name = f"{collection_name}.json"
    if not os.path.exists(file_name):
        TinyDB(file_name)
    return TinyDB(file_name)
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"user" : "nandha"}

@app.post("/domo/datastores/v1/collections/{collection_name}")
def create_collection(collection_name: str):
    file_name = f"{collection_name}.json"
    if os.path.exists(file_name):
        raise HTTPException(status_code=400, detail="Collection already exists")
    
    db = TinyDB(file_name)
    return {"message": f"Collection '{collection_name}' created successfully"}

@app.post("/domo/datastores/v1/collections/{collection_name}/documents/")
def create_document(collection_name: str, document: Document):
    db = get_db(collection_name)
    document_data = document.dict()
    
    document_data = {
        "id": str(uuid4()),
        "createdOn": datetime.utcnow().isoformat(),
        "updatedOn": datetime.utcnow().isoformat(),
        "content": document_data["content"],
        "syncRequired": True
    }
    
    db.insert(document_data)
    return {"message": "Document created successfully", "data": document_data}

@app.get("/domo/datastores/v1/collections/{collection_name}/documents/")
def list_documents(collection_name: str):
    db = get_db(collection_name)
    documents = db.all()
    
    formatted_documents = [
        {
            "id": doc.get("id"),
            "createdOn": doc.get("createdOn"),
            "updatedOn": doc.get("updatedOn"),
            "content": doc.get("content"),
            "syncRequired": doc.get("syncRequired")
        }
        for doc in documents
    ]
    
    return formatted_documents

@app.get("/domo/datastores/v1/collections/{collection_name}/documents/{document_id}")
def get_document(collection_name: str, document_id: UUID):
    db = get_db(collection_name)
    DocumentQuery = Query()
    document = db.get(DocumentQuery.id == str(document_id))
    
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    
    formatted_document = {
        "id": document.get("id"),
        "createdOn": document.get("createdOn"),
        "updatedOn": document.get("updatedOn"),
        "content": document.get("content"),
        "syncRequired": document.get("syncRequired")
    }
    
    return {"data": formatted_document}


@app.put("/domo/datastores/v1/collections/{collection_name}/documents/{document_id}")
def update_document(collection_name: str, document_id: UUID, document: Document):
    db = get_db(collection_name)
    DocumentQuery = Query()
    
    existing_document = db.get(DocumentQuery.id == str(document_id))
    
    if not existing_document:
        raise HTTPException(status_code=404, detail="Document not found")
    
    updated_document = {
        "id": existing_document["id"],
        "createdOn": existing_document["createdOn"],
        "updatedOn": datetime.utcnow().isoformat(),  
        "content": document.content,
        "syncRequired": existing_document["syncRequired"]
    }
    
    db.update(updated_document, DocumentQuery.id == str(document_id))
    return {"message": "Document updated successfully", "data": updated_document}


@app.delete("/domo/datastores/v1/collections/{collection_name}/documents/{document_id}")
def delete_document(collection_name: str, document_id: UUID):
    db = get_db(collection_name)
    DocumentQuery = Query()
    
    result = db.remove(DocumentQuery.id == str(document_id))
    
    if result == 0:
        raise HTTPException(status_code=404, detail="Document not found")
    
    return {"message": "Document deleted successfully"}



@app.delete("/domo/datastores/v1/collections/{collection_name}/documents/bulk")
def bulk_delete_documents(collection_name: str, ids: str):
    db = get_db(collection_name)
    id_list = ids.split(',')
    
    DocumentQuery = Query()
    deleted_count = 0
    
    for doc_id in id_list:
        result = db.remove(DocumentQuery.id == doc_id)
        deleted_count += result
        
    return {"Deleted": deleted_count}


@app.delete("/domo/datastores/v1/collections/{collection_name}/documents/{id}")
def delete_documents(collection_name: str, id: str):
    db = get_db(collection_name)
    DocumentQuery = Query()
    deleted_count = 0
    result = db.remove(DocumentQuery.id == id)
    deleted_count += result

    return {"Deleted": deleted_count}

@app.delete("/domo/datastores/v1/collections/{collection_name}")
def delete_collection(collection_name: str):
    file_path = f"{collection_name}.json"
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Collection not found")
    
    os.remove(file_path)
    
    return {"message": "Collection deleted successfully"}


@app.get("/domo/datastores/v1/collections")
def list_collection():
    if os.path:
       files=os.listdir()
       json_files = [file.replace(".json", "") for file in files if file.endswith('.json')]
    print(os.path.exists)
    
    return {"data": json_files}