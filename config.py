import os
from dotenv import load_dotenv
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "static/uploads")
SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret")
GST_PERCENT = float(os.getenv("GST_PERCENT", "18.0"))
