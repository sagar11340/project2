from bson.objectid import ObjectId
from pymongo import ReturnDocument

def get_next_sequence(db, name):
    """Mongo auto-increment counter pattern"""
    res = db.counters.find_one_and_update(
        {"_id": name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    return res["seq"]

def calc_gst(amount, gst_percent):
    gst = round(amount * gst_percent / 100.0, 2)
    total = round(amount + gst, 2)
    return gst, total
