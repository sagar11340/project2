# migrate_add_student_id_simple.py
from pymongo import MongoClient
from config import MONGO_URI
client = MongoClient(MONGO_URI)
db = client['institute_db']
students = db.students

# get_next_seq is replicated here because easier to run standalone
from pymongo import ReturnDocument
def get_next_seq(db, name="student_id"):
    doc = db.counters.find_one_and_update(
        {"_id": name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    return int(doc["seq"])

def main():
    cursor = students.find({"student_id": {"$exists": False}})
    i = 0
    for doc in cursor:
        new_id = get_next_seq(db, "student_id")
        students.update_one({"_id": doc["_id"]}, {"$set": {"student_id": new_id}})
        print("Assigned", new_id, "to", str(doc["_id"]))
        i += 1
    print("Done. Assigned student_id to", i, "documents.")

if __name__ == "__main__":
    main()
