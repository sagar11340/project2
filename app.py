
import os
import sys
import io
import csv
import time
import random
import traceback

from datetime import date, datetime, timedelta
from uuid import uuid4
from functools import wraps
from pprint import pprint
from calendar import calendar

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, send_file, Response, abort, jsonify,
    session, send_from_directory, g
)

from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash

from pymongo import (
    MongoClient, ReturnDocument,
    ASCENDING, DESCENDING
)
from pymongo.errors import DuplicateKeyError
from bson.objectid import ObjectId

from num2words import num2words

from config import MONGO_URI, UPLOAD_FOLDER, SECRET_KEY, GST_PERCENT
from utils import get_next_sequence, calc_gst
from bson.errors import InvalidId
from flask import current_app





# ----------------- APP INIT -----------------
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "change_this_to_a_strong_secret")

# Upload folder
UPLOAD_FOLDER = os.path.join(app.root_path, "static", "uploads")
ALLOWED_EXT = {"png", "jpg", "jpeg", "gif"}
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ----------------- DATABASE CONNECT -----------------
MONGO_URI = os.environ.get("MONGO_URI") or "mongodb://localhost:27017"
client = MongoClient(MONGO_URI)
db = client["institute_db"]

# Collections
students_col   = db.students
batches_col    = db.batches
courses_col    = db.courses
payments_col   = db.payments
faculties_col  = db.faculties
attendance_col = db.attendance
salaries_col   = db.salaries
users_col      = db.users   # <-- IMPORTANT

# Backwards-compatible aliases
students  = students_col
batches   = batches_col
courses   = courses_col
payments  = payments_col
faculties = faculties_col
teachers_col = faculties_col

# ----------------- HELPERS (NOW db EXISTS) -----------------
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXT

def get_users_col():
    return users_col     # db exists now ✔

def ensure_default_admin():
    users = get_users_col()
    if users.count_documents({}) == 0:
        users.insert_one({
            "username": "admin",
            "name": "Administrator",
            "email": "admin@example.com",
            "phone": "",
            "role": "admin",
            "password_hash": generate_password_hash("admin123"),
            "photo": None,
            "created_on": datetime.utcnow()
        })
        print("Default admin created: username='admin' password='admin123'")

# ----------------- RUN ON STARTUP (db already exists ✔) -----------------
ensure_default_admin()

# ----------------- OTHER HELPERS BELOW -----------------

def generate_registration_no():
    return f"RKM{random.randint(10000, 99999)}"

def get_next_sequence(db, name):
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

def get_next_seq(db, name="institute_db"):
    doc = db.counters.find_one_and_update(
        {"_id": name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    return int(doc["seq"])

def get_next_student_id():
    last = students.find_one({"student_id": {"$exists": True}}, sort=[("student_id", -1)])
    if last:
        return int(last["student_id"]) + 1
    return 1

def month_date_range(year: int, month: int):
    start = datetime(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end = datetime(year, month, last_day, 23, 59, 59)
    return start, end




def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            flash("Please login first.", "warning")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper



# _____________ALL ROUTES_________________


@app.route('/dashboard/years')
def years_dashboard():
    # build 'years' from your DB (group by year)
    return render_template('year_dashboard.html', years=years)






@app.template_filter('num2words')
def num2words_filter(num, lang='en_IN'):
    try:
        return num2words(num, lang=lang)
    except:
        return str(num)


# ---------- Home / Statistics ----------
@app.route('/')
@login_required
def index():
    # basic totals
    batch_count = db.batches.count_documents({})
    student_count = db.students.count_documents({})
    male = db.students.count_documents({"gender": "Male"})
    female = db.students.count_documents({"gender": "Female"})

    # batch-wise gender breakdown (your existing pipeline)
    pipeline = [
        {"$lookup": {"from": "batches", "localField": "batch_id", "foreignField": "_id", "as": "batch"}},
        {"$unwind": {"path": "$batch", "preserveNullAndEmptyArrays": True}},
        {"$group": {
            "_id": "$batch.title",
            "boys": {"$sum": {"$cond": [{"$eq": ["$gender", "Male"]}, 1, 0]}},
            "girls": {"$sum": {"$cond": [{"$eq": ["$gender", "Female"]}, 1, 0]}},
            "total": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    batch_stats = list(db.students.aggregate(pipeline))

    # ---------- Students per Faculty ----------
    # Group students by faculty_id (may be ObjectId or string), attach faculty name if present
    faculty_pipeline = [
        {"$group": {"_id": "$faculty_id", "count": {"$sum": 1}}},
        {"$lookup": {"from": "faculties", "localField": "_id", "foreignField": "_id", "as": "faculty"}},
        {"$unwind": {"path": "$faculty", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "faculty_id": "$_id",
            "faculty_name": {"$ifNull": ["$faculty.name", "(Unassigned)"]},
            "count": 1
        }},
        {"$sort": {"count": -1}}
    ]
    by_faculty = list(db.students.aggregate(faculty_pipeline))

    # ---------- Students per Course ----------
    course_pipeline = [
        {"$group": {"_id": "$course_id", "count": {"$sum": 1}}},
        {"$lookup": {"from": "courses", "localField": "_id", "foreignField": "_id", "as": "course"}},
        {"$unwind": {"path": "$course", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "course_id": "$_id",
            "course_name": {"$ifNull": ["$course.name", "(Unassigned)"]},
            "count": 1
        }},
        {"$sort": {"count": -1}}
    ]
    by_course = list(db.students.aggregate(course_pipeline))

    # Render template with all statistics
    return render_template(
        'index.html',
        batch_count=batch_count,
        student_count=student_count,
        male=male,
        female=female,
        batch_stats=batch_stats,
        by_faculty=by_faculty,
        by_course=by_course
    )





# ---------- Batches ----------
@app.route('/batches')
def batches_list():
    batches = list(db.batches.find().sort("start_date", -1))
    return render_template('batches_list.html', batches=batches)

@app.route('/batch/add', methods=['GET','POST'])
def add_batch():
    if request.method == 'POST':
        title = request.form['title']
        start_date = request.form['start_date']  # expect yyyy-mm-dd
        doc = {
            "title": title,
            "start_date": start_date,
            "created_at": datetime.utcnow()
        }
        db.batches.insert_one(doc)
        flash("Batch added.")
        return redirect(url_for('batches_list'))
    return render_template('batch_form.html')

@app.route('/batch/edit/<bid>', methods=['GET','POST'])
def edit_batch(bid):
    batch = db.batches.find_one({"_id": ObjectId(bid)})
    if request.method == 'POST':
        db.batches.update_one({"_id": ObjectId(bid)},
                              {"$set": {"title": request.form['title'], "start_date": request.form['start_date']}})
        flash("Batch updated.")
        return redirect(url_for('batches_list'))
    return render_template('batch_form.html', batch=batch)

@app.route('/batch/delete/<bid>', methods=['POST'])
def delete_batch(bid):
    db.batches.delete_one({"_id": ObjectId(bid)})
    flash("Batch deleted.")
    return redirect(url_for('batches_list'))




# ---------- Courses ----------
@app.route('/courses')
def courses_list():
    courses = list(db.courses.find().sort("name", 1))
    return render_template('courses_list.html', courses=courses)

@app.route('/course/add', methods=['GET','POST'])
def add_course():
    if request.method == 'POST':
        name = request.form['name']
        fee = float(request.form['fee'] or 0)
        db.courses.insert_one({"name": name, "fee": fee})
        flash("Course added.")
        return redirect(url_for('courses_list'))
    return render_template('course_form.html')

@app.route('/course/edit/<cid>', methods=['GET','POST'])
def edit_course(cid):
    course = db.courses.find_one({"_id": ObjectId(cid)})
    if request.method == 'POST':
        db.courses.update_one({"_id": ObjectId(cid)}, {"$set": {"name": request.form['name'], "fee": float(request.form['fee'])}})
        return redirect(url_for('courses_list'))
    return render_template('course_form.html', course=course)

@app.route('/course/delete/<cid>', methods=['POST'])
def delete_course(cid):
    db.courses.delete_one({"_id": ObjectId(cid)})
    flash("Course deleted.")
    return redirect(url_for('courses_list'))


@app.route('/students')
def students_list():
    q = request.args.get('q','').strip()
    query = {}
    if q:
        query = {"$or":[
            {"first_name":{"$regex": q, "$options":"i"}},
            {"last_name":{"$regex": q, "$options":"i"}},
            {"phone":{"$regex": q}},
            {"form_no":{"$regex": q}},
            {"aadhar":{"$regex": q}}
        ]}

    # fetch students (limited)
    students = list(db.students.find(query).sort("created_at",-1).limit(200))

    # Prefetch lookup maps to avoid N queries
    course_ids = {s.get('course_id') for s in students if s.get('course_id')}
    batch_ids = {s.get('batch_id') for s in students if s.get('batch_id')}
    faculty_ids = {s.get('faculty_id') for s in students if s.get('faculty_id')}

    # convert any ObjectId in sets to ObjectId type for queries (if stored as string)
    def norm_ids(idset):
        out = []
        for i in idset:
            if not i:
                continue
            try:
                out.append(ObjectId(i) if not isinstance(i, ObjectId) else i)
            except Exception:
                # skip invalid ids (they might be actual string names)
                pass
        return out

    course_map = {}
    if course_ids:
        rows = db.courses.find({"_id": {"$in": norm_ids(course_ids)}})
        for r in rows:
            course_map[str(r['_id'])] = r.get('name') or r.get('title') or ''

    batch_map = {}
    if batch_ids:
        rows = db.batches.find({"_id": {"$in": norm_ids(batch_ids)}})
        for r in rows:
            batch_map[str(r['_id'])] = r.get('name') or r.get('title') or ''

    faculty_map = {}
    if faculty_ids:
        rows = db.faculties.find({"_id": {"$in": norm_ids(faculty_ids)}})
        for r in rows:
            faculty_map[str(r['_id'])] = r.get('name') or r.get('title') or ''

    # Enrich students for template (and normalise field names)
    enriched = []
    for s in students:
        st = dict(s)  # copy so we don't modify original
        # id as string for URLs
        st['_id'] = str(st.get('_id'))

        # normalize common keys (template uses parent_phone / aadhaar)
        # DB might have 'parents_phone' or 'parent_phone'
        if not st.get('parent_phone') and st.get('parents_phone'):
            st['parent_phone'] = st.get('parents_phone')
        # aadhar vs aadhaar
        if not st.get('aadhaar') and st.get('aadhar'):
            st['aadhaar'] = st.get('aadhar')

        # course name
        cid = st.get('course_id')
        if cid:
            cid_s = str(cid) if not isinstance(cid, str) else cid
            st['course_name'] = course_map.get(cid_s, '')
        else:
            st['course_name'] = st.get('course_name','')  # maybe already present

        # batch title
        bid = st.get('batch_id')
        if bid:
            bid_s = str(bid) if not isinstance(bid, str) else bid
            st['batch'] = batch_map.get(bid_s, '')
        else:
            st['batch'] = st.get('batch','')

        # faculty resolution: prefer explicit faculty text, else lookup faculty_id
        faculty_text = st.get('faculty','') or ''
        if not faculty_text and st.get('faculty_id'):
            fid_s = str(st.get('faculty_id'))
            faculty_text = faculty_map.get(fid_s, '')
        st['faculty'] = faculty_text

        # ensure timing field present (avoid KeyError in template)
        st['timing'] = st.get('timing','')

        # ensure form_no present
        st['form_no'] = st.get('form_no','')

        enriched.append(st)

    # pass lists for filters too (if template uses them)
    courses = list(db.courses.find().sort("name", 1))
    batches = list(db.batches.find().sort("start_date", -1))
    faculties = list(db.faculties.find())

    return render_template('students_list.html',
                           students=enriched,
                           batches=batches,
                           courses=courses,
                           faculties=faculties,
                           q=q)



@app.route('/student/add', methods=['GET','POST'])
def add_student():
    batches = list(db.batches.find())
    courses = list(db.courses.find())
    faculties = list(db.faculties.find()) if 'faculties' in db.list_collection_names() else []

    if request.method == 'POST':
        # --- TAKE form_no FROM USER (manual entry) ---
        form_no = request.form.get('form_no', '').strip()

        # Validate presence
        if not form_no:
            flash("Please enter Form No (manual entry required).", "danger")
            return redirect(url_for('add_student'))

        # Validate uniqueness
        if db.students.find_one({"form_no": form_no}):
            flash("Form No already exists. Please use a different Form No.", "danger")
            return redirect(url_for('add_student'))

        # Basic fields
        data = {
            "first_name": request.form.get('first_name','').strip(),
            "father_name": request.form.get('father_name','').strip(),
            "last_name": request.form.get('last_name','').strip(),
            "dob": request.form.get('dob',''),
            "address": request.form.get('address','').strip(),
            "phone": request.form.get('phone','').strip(),
            "parents_phone": request.form.get('parents_phone','').strip(),
            "aadhar": request.form.get('aadhar','').strip(),
            "email": request.form.get('email','').strip(),
            "gender": request.form.get('gender',''),
            "registration_no": generate_registration_no(),
            "qualification": request.form.get('qualification',''),
            "timing": request.form.get('timing',''),
            "admission_date": request.form.get('admission_date',''),
            "payment_status": request.form.get('payment_status','paying'),
            "reference": request.form.get('reference',''),
            "form_no": form_no,  # <- use manual value only
            "blood_group": request.form.get('blood_group',''),
            "created_at": datetime.utcnow()
        }

        # batch_id (try to convert to ObjectId; else store None)
        if request.form.get('batch_id'):
            try:
                data['batch_id'] = ObjectId(request.form.get('batch_id'))
            except Exception:
                data['batch_id'] = None

        # course_id (try to convert to ObjectId; else store None)
        if request.form.get('course_id'):
            try:
                data['course_id'] = ObjectId(request.form.get('course_id'))
            except Exception:
                data['course_id'] = None

        # Faculty handling
        faculty_name = request.form.get('faculty','').strip()
        faculty_id_raw = request.form.get('faculty_id')
        if faculty_id_raw:
            try:
                fid = ObjectId(faculty_id_raw)
                data['faculty_id'] = fid
                fdoc = db.faculties.find_one({"_id": fid})
                if fdoc and fdoc.get('name'):
                    faculty_name = fdoc['name']
            except Exception:
                pass
        data['faculty'] = faculty_name

        # handle photo BEFORE inserting (so filename saved in document)
        photo = request.files.get('photo')
        if photo and photo.filename:
            fname = secure_filename(photo.filename)
            # make filename unique to avoid collisions
            unique_fname = f"{int(time.time())}_{uuid4().hex}_{fname}"
            path = os.path.join(app.config['UPLOAD_FOLDER'], unique_fname)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            photo.save(path)
            data['photo'] = unique_fname

        # generate student_id (sequence)
        data['student_id'] = get_next_seq(db, "student_id")

        # Defensive: ensure we don't accidentally try to insert a pre-existing _id
        data.pop('_id', None)

        # Try insert once, handle duplicate key errors gracefully
        try:
            res = db.students.insert_one(data)
        except DuplicateKeyError as e:
            # log the error if you have logging
            # app.logger.exception("DuplicateKeyError while inserting student")
            flash("A student with the same unique key already exists. Please check and try again.", "danger")
            return redirect(url_for('add_student'))
        except Exception as e:
            # generic fallback
            # app.logger.exception("Error inserting student")
            flash("An unexpected error occurred while registering the student.", "danger")
            return redirect(url_for('add_student'))

        flash("Student registered.", "success")
        return redirect(url_for('students_list'))

    # GET: render form
    return render_template('student_form.html', batches=batches, courses=courses, faculties=faculties)



@app.route('/student/delete/<sid>', methods=['POST'])
def delete_student(sid):
    db.students.delete_one({"_id": ObjectId(sid)})
    flash("Student removed.")
    return redirect(url_for('students_list'))




@app.route('/receipt/<receipt_no>')
def print_receipt(receipt_no):
    payment = db.payments.find_one({"receipt_no": receipt_no})
    if not payment:
        flash("Receipt not found.")
        return redirect(url_for('payments_list'))
    return render_template('receipt.html', payment=payment)


    # Try to resolve the student document from the payment.student_id (ObjectId)
    student = None
    sid = payment.get("student_id")
    if sid:
        try:
            # if sid is string, convert; if already ObjectId, this is fine
            try:
                sid_obj = ObjectId(sid) if not isinstance(sid, ObjectId) else sid
            except Exception:
                sid_obj = sid if isinstance(sid, ObjectId) else None

            if sid_obj:
                student = db.students.find_one({"_id": sid_obj})
        except Exception:
            student = None

    # If you stored numeric id in payment earlier (recommended), try that too:
    if not student and payment.get("student_numeric_id"):
        student = db.students.find_one({"student_id": int(payment["student_numeric_id"])})

    return render_template('receipt.html', payment=payment, student=student)



@app.route('/reports/payment', methods=['GET', 'POST'])
def payment_report():
    # request.values merges args (GET) and form (POST) — convenient for both methods
    get = request.values.get

    from_date = get("from_date")
    to_date = get("to_date")
    from_receipt = get("from_receipt")
    to_receipt = get("to_receipt")
    course = get("course")
    old_new = get("old_new")     # param name from client: old_new
    faculty = get("faculty")
    submit_date = get("submit_date")

    q = {}

    # Date filter: convert to datetimes if possible
    # Expecting dates in 'YYYY-MM-DD' format (adjust format if different)
    try:
        if from_date:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        else:
            from_dt = None
        if to_date:
            # include end of day
            to_dt = datetime.strptime(to_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        else:
            to_dt = None

        if from_dt and to_dt:
            q["date"] = {"$gte": from_dt, "$lte": to_dt}
        elif from_dt:
            q["date"] = {"$gte": from_dt}
        elif to_dt:
            q["date"] = {"$lte": to_dt}
    except ValueError:
        # If dates are not in expected format, fallback to string-based query (less ideal)
        if from_date and to_date:
            q["date"] = {"$gte": from_date + " 00:00:00", "$lte": to_date + " 23:59:59"}

    # Submit date filter — if stored as string, regex is fine; if stored as datetime you need to convert similarly
    if submit_date:
        q["created_at"] = {"$regex": submit_date}

    # Receipt number range — parse safely
    try:
        if from_receipt and to_receipt:
            q["receipt_no"] = {"$gte": int(from_receipt), "$lte": int(to_receipt)}
        elif from_receipt:
            q["receipt_no"] = {"$gte": int(from_receipt)}
        elif to_receipt:
            q["receipt_no"] = {"$lte": int(to_receipt)}
    except ValueError:
        # ignore invalid ints (or add a flash message if you want)
        pass

    # Course filter
    if course and course != "All":
        q["course"] = course

    # NOTE: check your DB field name for new/old — below I put it into the same key name
    if old_new and old_new != "All":
        q["old_old"] = old_new   # <-- if your DB uses 'new_old', change this key accordingly

    # Faculty filter
    if faculty and faculty != "All":
        q["faculty"] = faculty

    # Debug print — useful while developing
    print("Payment report query:", q, "method:", request.method)

    payments_cursor = payments.find(q).sort("date", -1)
    payment_list = list(payments_cursor)

    # Build safe lists for the template (do not shadow collection names)
    course_list = []
    for c in courses.find():
        # handle dict-like and attribute-like documents safely
        name = c.get("name") if isinstance(c, dict) else getattr(c, "name", None)
        if not name:
            name = c.get("title") if isinstance(c, dict) else getattr(c, "title", None)
        if name:
            course_list.append(str(name).strip())

    faculty_list = []
    for f in faculties.find():
        fname = f.get("name") if isinstance(f, dict) else getattr(f, "name", None)
        if fname:
            faculty_list.append(str(fname).strip())

    # -------------------------
    # COMPUTE TOTAL AMOUNT
    # -------------------------
    total_amount = 0
    for p in payment_list:
        amt = p.get('total') or p.get('amount') or 0
        try:
            total_amount += float(amt)
        except Exception:
            # ignore bad values
            pass

    # -------------------------
    # FIRST & LAST RECEIPT NO (prefer numeric min/max)
    # -------------------------
    if payment_list:
        numeric_receipts = []
        for p in payment_list:
            r = p.get('receipt_no')
            if r is None:
                continue
            try:
                numeric_receipts.append(int(r))
            except Exception:
                # not numeric, skip here
                pass

        if numeric_receipts:
            first_receipt_no = min(numeric_receipts)
            last_receipt_no = max(numeric_receipts)
        else:
            # fallback to first/last in the returned list (string values or unsortable)
            first_receipt_no = payment_list[0].get('receipt_no', '')
            last_receipt_no = payment_list[-1].get('receipt_no', '')
    else:
        first_receipt_no = ''
        last_receipt_no = ''

    # -------------------------
    # RENDER TEMPLATE
    # -------------------------
    return render_template(
        "reports_payment.html",
        payments=payment_list,
        course_list=course_list,
        faculty_list=faculty_list,
        total_amount=total_amount,
        first_receipt_no=first_receipt_no,
        last_receipt_no=last_receipt_no
    )




# @app.route('/reports/students')
# def student_report():
#     students = list(db.students.find().sort("created_at",-1))


#     return render_template('student_report.html', students=students)


@app.route('/reports/students')
def student_report():
    students = list(db.students.find().sort("created_at", -1))

    # Collect unique batch_ids & course_ids
    batch_ids = {s.get('batch_id') for s in students if s.get('batch_id')}
    course_ids = {s.get('course_id') for s in students if s.get('course_id')}

    # Fetch batch docs in one query
    batches = {str(b['_id']): b for b in db.batches.find({
        "_id": {"$in": list(batch_ids)}
    })}

    # Fetch course docs in one query
    courses = {str(c['_id']): c for c in db.courses.find({
        "_id": {"$in": list(course_ids)}
    })}

    # Process each student
    for s in students:

        # ------------------------------
        # 1️⃣ Attach batch details
        # ------------------------------
        if s.get("batch_id"):
            s["batch"] = batches.get(str(s["batch_id"]))

        # ------------------------------
        # 2️⃣ Attach course details
        # ------------------------------
        if s.get("course_id"):
            s["course"] = courses.get(str(s["course_id"]))

        # ------------------------------
        # 3️⃣ Compute Expiry Date
        # ------------------------------
        if not s.get("expiry_date") and s.get("admission_date"):
            try:
                ad = datetime.strptime(s["admission_date"], "%Y-%m-%d")
                # Example: course contains duration_months
                months = int(s["course"].get("duration_months", 0)) if s.get("course") else 0
                expiry = ad + timedelta(days=months * 30)
                s["expiry_date"] = expiry.strftime("%Y-%m-%d")
            except:
                s["expiry_date"] = ""

        # ------------------------------
        # 4️⃣ Compute Balance
        # ------------------------------
        total_paid = 0
        for p in db.payments.find({"student_id": s["_id"]}):
            total_paid += float(p.get("amount", 0))

        course_fee = 0

        # Prefer student fee field, else course fee
        if "fee" in s:
            course_fee = float(s.get("fee", 0))
        elif s.get("course"):
            course_fee = float(s["course"].get("fee", 0))

        s["balance"] = course_fee - total_paid

    return render_template("student_report.html", students=students)



@app.route('/reports/genderwise')
def genderwise_report():
    pipeline = [
        {"$lookup": {"from":"batches","localField":"batch_id","foreignField":"_id","as":"batch"}},
        {"$unwind":{"path":"$batch","preserveNullAndEmptyArrays":True}},
        {"$group":{"_id":"$batch.title","boys":{"$sum":{"$cond":[{"$eq":["$gender","Male"]},1,0]}},
                                   "girls":{"$sum":{"$cond":[{"$eq":["$gender","Female"]},1,0]}},
                                   "total":{"$sum":1}}}
    ]

    results = list(db.students.aggregate(pipeline))
    return render_template('genderwise_report.html', results=results)

# ---------- Payment summary endpoints ----------
@app.route('/summary/today')
def summary_today():
    start = datetime.utcnow().replace(hour=0,minute=0,second=0,microsecond=0)
    end = start + timedelta(days=1)
    total = db.payments.aggregate([{"$match":{"date":{"$gte":start,"$lt":end}}},{"$group":{"_id":None,"sum":{"$sum":"$total"}}}])
    total = list(total)
    total_amount = total[0]['sum'] if total else 0
    return {"date": str(start.date()), "collection": total_amount}







# /payments — shows all students + balance + quick actions.

# /payment/add/<student_id> — payment form with installment history and pay action.

# /payment/details/<student_id> — view installments for a student.

# /receipt/<receipt_no> — print receipt (if you already have this, keep it).

# --- Payments list: show all students with balances & actions ---
@app.route('/payments')
def payments_list():
    q = request.args.get('q','').strip()
    # fetch students (optionally filter by q)
    query = {}
    if q:
        query = {"$or":[
            {"first_name":{"$regex":q,"$options":"i"}},
            {"last_name":{"$regex":q,"$options":"i"}},
            {"phone":{"$regex":q}},
            {"form_no":{"$regex":q}},
            {"aadhar":{"$regex":q}}
        ]}
    students = list(db.students.find(query).sort("created_at",-1))

    # enrich students with course info and balance & last payment date
    enriched = []
    for s in students:
        student = s.copy()
        # get course
        course = None
        if student.get("course_id"):
            try:
                course = db.courses.find_one({"_id": ObjectId(student["course_id"])})
            except:
                course = db.courses.find_one({"_id": student["course_id"]})
        # course fee
        fee = course.get("fee",0) if course else 0

        # sum of payments made (total field in payments)
        paid_agg = db.payments.aggregate([
            {"$match": {"student_id": student.get("_id")}},
            {"$group": {"_id": None, "sumPaid": {"$sum": "$amount"}}}
        ])
        paid_list = list(paid_agg)
        paid = paid_list[0]['sumPaid'] if paid_list else 0.0

        # compute balance = fee - paid (if you store balance separately you can use it instead)
        balance = fee - paid

        # get last payment and number of installments
        last_pay = db.payments.find_one({"student_id": student.get("_id")}, sort=[("date",-1)])
        installments_count = db.payments.count_documents({"student_id": student.get("_id")})

        student['course_name'] = course.get("name") if course else ""
        student['course_fee'] = fee
        student['paid'] = paid
        student['balance'] = balance
        student['last_payment'] = last_pay['date'] if last_pay else None
        student['installments'] = installments_count

        enriched.append(student)

    return render_template('payments_list.html', payments=[], students=enriched, q=q)


#  _______faculty_routes____
@app.route('/faculty')
def faculty_list():
    data = list(faculties.find())
    return render_template('faculty_list.html', faculties=data)

@app.route('/faculty/add', methods=['GET', 'POST'])
def faculty_form():
    if request.method == 'POST':
        doc = {
            "name": request.form.get("name"),
            "phone": request.form.get("phone"),
            "email": request.form.get("email"),
            "subject": request.form.get("subject"),
            "address": request.form.get("address")
        }
        faculties.insert_one(doc)
        flash("Faculty added successfully!")
        return redirect(url_for('faculty_list'))
    return render_template('faculty_form.html', faculty=None)

@app.route('/faculty/edit/<id>', methods=['GET', 'POST'])
def edit_faculty(id):
    faculty = faculties.find_one({"_id": ObjectId(id)})
    if not faculty:
        flash("Faculty not found.")
        return redirect(url_for('faculty_list'))
    if request.method == 'POST':
        faculties.update_one({"_id": ObjectId(id)}, {"$set": {
            "name": request.form.get("name"),
            "phone": request.form.get("phone"),
            "email": request.form.get("email"),
            "subject": request.form.get("subject"),
            "address": request.form.get("address")
        }})
        flash("Faculty updated successfully!")
        return redirect(url_for('faculty_list'))
    return render_template('faculty_form.html', faculty=faculty)

@app.route('/faculty/delete/<id>')
def delete_faculty(id):
    faculties.delete_one({"_id": ObjectId(id)})
    flash("Faculty deleted successfully.")
    return redirect(url_for('faculty_list'))


@app.route('/payment/add/<student_id>', methods=['GET','POST'])
def add_payment(student_id):
    # fetch student
    student = db.students.find_one({"_id": ObjectId(student_id)})
    if not student:
        flash("Student not found.")
        return redirect(url_for('payments_list'))

    # Collect course IDs from student (supports both legacy single field and new list field)
    raw_course_ids = []
    if student.get("course_ids"):            # preferred new field (list)
        raw_course_ids = student["course_ids"]
    elif student.get("course_id"):           # legacy single field
        raw_course_ids = [student["course_id"]]

    # Normalize and convert to ObjectId where possible
    course_obj_ids = []
    for cid in raw_course_ids:
        try:
            course_obj_ids.append(ObjectId(cid))
        except Exception:
            # If it's already an ObjectId, append it; otherwise skip invalid ids
            if isinstance(cid, ObjectId):
                course_obj_ids.append(cid)

    # Fetch course documents (projection - only what you need)
    courses = []
    if course_obj_ids:
        courses = list(db.courses.find({"_id": {"$in": course_obj_ids}}, {"name": 1, "duration": 1}).sort("name", 1))

    # If a student had only one course, set default selected course (None otherwise)
    default_course = None
    if courses:
        default_course = courses[0]

    # fetch installment history (all payments for this student)
    history = list(db.payments.find({"student_id": student["_id"]}).sort("date", -1))

    if request.method == 'POST':
        # Parse form fields
        amount = float(request.form.get('amount', 0) or 0)
        payment_mode = request.form.get('payment_mode','cash')
        installment_label = request.form.get('installment','full')
        faculty = request.form.get('faculty', student.get('faculty', ''))
        remarks = request.form.get('remarks','')

        # Course selected in the form (string)
        selected_course_id = request.form.get('course_id') or (str(default_course['_id']) if default_course else None)

        # Resolve selected course name (prefer from fetched courses, otherwise fetch from DB)
        course_name = ""
        course_id_for_doc = None
        if selected_course_id:
            course_id_for_doc = selected_course_id
            # try to find in previously fetched courses
            found = next((c for c in courses if str(c['_id']) == selected_course_id), None)
            if found:
                course_name = found.get("name", "")
            else:
                # fallback: fetch single course doc (handles case when course wasn't in course_obj_ids)
                try:
                    cdoc = db.courses.find_one({"_id": ObjectId(selected_course_id)}, {"name":1})
                except Exception:
                    cdoc = db.courses.find_one({"_id": selected_course_id}, {"name":1})
                if cdoc:
                    course_name = cdoc.get("name", "")

        # calculate gst & total (use your existing calc_gst function)
        gst, total = calc_gst(amount, GST_PERCENT)

        # receipt number from counters
        receipt_seq = get_next_sequence(db, "receipt_no")
        receipt_no = str(receipt_seq).zfill(6)

        pay_doc = {
            "student_id": student["_id"],
            "student_name": f"{student.get('first_name','')} {student.get('last_name','')}".strip(),
            "course_id": course_id_for_doc,
            "course_name": course_name,
            "date": datetime.utcnow(),
            "amount": amount,        # base amount
            "gst": gst,
            "total": total,          # amount + gst
            "faculty": faculty,
            "payment_mode": payment_mode,
            "installment": installment_label,
            "receipt_no": receipt_no,
            "remarks": remarks,
            "phone": student.get("phone"),
            "gender": student.get("gender"),
        }

        db.payments.insert_one(pay_doc)

        flash(f"Payment recorded. Receipt No: {receipt_no}")
        return redirect(url_for('print_receipt', receipt_no=receipt_no))
    

    

    # GET -> show form
    # pass `courses` (list) to template so user can choose which course payment is for
    return render_template('payment_form.html', student=student, courses=courses, history=history, gst_percent=GST_PERCENT)


# --- View payment/installment history for a student ---
@app.route('/payment/details/<student_id>')
def payment_details(student_id):
    student = db.students.find_one({"_id": ObjectId(student_id)})
    if not student:
        flash("Student not found.")
        return redirect(url_for('payments_list'))
    history = list(db.payments.find({"student_id": student["_id"]}).sort("date", -1))
    return render_template('payment_details.html', student=student, history=history)

# from bson.errors import InvalidId
# from bson.objectid import ObjectId
# from werkzeug.utils import secure_filename

@app.route('/student/edit/<sid>', methods=['GET','POST'])
def edit_student(sid):
    # try treat sid as ObjectId, fallback to form_no (string)
    student = None
    try:
        student = db.students.find_one({"_id": ObjectId(sid)})
    except (InvalidId, TypeError):
        # fallback: maybe user passed a form_no
        student = db.students.find_one({"form_no": sid})

    if not student:
        flash("Student not found.")
        return redirect(url_for('students_list'))

    # convert student ids to strings for template comparison
    student['_id'] = str(student['_id'])
    if student.get('batch_id'):
        try:
            student['batch_id'] = str(student['batch_id'])
        except Exception:
            student['batch_id'] = student.get('batch_id')
    if student.get('course_id'):
        try:
            student['course_id'] = str(student['course_id'])
        except Exception:
            student['course_id'] = student.get('course_id')
    if student.get('faculty_id'):
        try:
            student['faculty_id'] = str(student['faculty_id'])
        except Exception:
            student['faculty_id'] = student.get('faculty_id')

    # load lists and convert their ids to strings for template
    batches = list(db.batches.find())
    courses = list(db.courses.find())
    faculties = list(db.faculties.find())

    for b in batches:
        b['_id'] = str(b['_id'])
    for c in courses:
        c['_id'] = str(c['_id'])
    for f in faculties:
        f['_id'] = str(f['_id'])

    if request.method == 'POST':
        # Collect basic fields
        update = {k: request.form.get(k, '').strip() for k in [
            'first_name','father_name','last_name','dob','address','email','phone',
            'parents_phone','aadhar','gender','qualification','timing',
            'admission_date','payment_status','reference','form_no',
            'blood_group'
        ]}

        # handle batch/course (store as ObjectId if provided, else unset)
        if request.form.get('batch_id'):
            try:
                update['batch_id'] = ObjectId(request.form['batch_id'])
            except InvalidId:
                update['batch_id'] = None
        else:
            update['batch_id'] = None

        if request.form.get('course_id'):
            try:
                update['course_id'] = ObjectId(request.form['course_id'])
            except InvalidId:
                update['course_id'] = None
        else:
            update['course_id'] = None

        # handle faculty: either faculty_id (select) or free-text faculty
        faculty_id = request.form.get('faculty_id')
        faculty_text = request.form.get('faculty', '').strip()
        if faculty_id:
            try:
                oid = ObjectId(faculty_id)
                update['faculty_id'] = oid
                # look up faculty name and store it too for easy display
                fac = db.faculties.find_one({'_id': oid})
                update['faculty'] = fac.get('name') if fac else faculty_text or None
            except (InvalidId, TypeError):
                # invalid id -> fallback to text
                update['faculty_id'] = None
                update['faculty'] = faculty_text or None
        else:
            # no select chosen; use free-text or clear
            update['faculty_id'] = None
            update['faculty'] = faculty_text or None

        # handle photo upload
        photo = request.files.get('photo')
        if photo and getattr(photo, 'filename', None):
            fname = secure_filename(photo.filename)
            upload_folder = app.config.get('UPLOAD_FOLDER') or os.path.join(app.root_path, 'static', 'uploads')
            os.makedirs(upload_folder, exist_ok=True)
            path = os.path.join(upload_folder, fname)
            photo.save(path)
            update['photo'] = fname

        # update DB (use ObjectId for the selector if possible)
        try:
            selector = {"_id": ObjectId(sid)}
        except Exception:
            # sid might be a form_no; find real _id
            doc = db.students.find_one({"form_no": sid})
            selector = {"_id": doc["_id"]} if doc else {"form_no": sid}

        db.students.update_one(selector, {"$set": update})
        flash("Student updated.")
        return redirect(url_for('students_list'))

    # final: render template (pass faculties so select shows)
    return render_template('student_form.html',
                           student=student,
                           batches=batches,
                           courses=courses,
                           faculties=faculties)




# ---------- Helper utilities ----------
def iso_today():
    return date.today().isoformat()

def parse_date(s):
    # expected yyyy-mm-dd
    if not s:
        return iso_today()
    return s

# ---------- Sample seeding route (optional) ----------
@app.route('/seed')
def seed():
    """Seed some example batches and students (run once)."""
    # Only seed if empty to avoid duplicates
    if batches_col.count_documents({}) == 0:
        b1 = batches_col.insert_one({"name": "Batch A"}).inserted_id
        b2 = batches_col.insert_one({"name": "Batch B"}).inserted_id

        students_col.insert_many([
            {"first_name": "Amit", "last_name": "Sharma", "phone": "9876500001", "form_no": "A001", "photo": None, "batch_id": b1},
            {"first_name": "Rina", "last_name": "Kumar", "phone": "9876500002", "form_no": "A002", "photo": None, "batch_id": b1},
            {"first_name": "Sandeep", "last_name": "Das", "phone": "9876500003", "form_no": "B001", "photo": None, "batch_id": b2},
        ])
        return "Seeded sample data"
    return "Already seeded"

# ---------- Attendance page (renders your template) ----------
# ---------- Attendance routes (replace the old block with this) ----------


@app.route('/attendance')
def attendance():
    """
    Renders the attendance register page.
    Query params:
      - date: yyyy-mm-dd (optional)
      - batch: batch id (optional, string)
    """
    q_date = parse_date(request.args.get('date'))
    selected_batch = request.args.get('batch')  # string or None

    # Load batches and convert _id to string for template comparison
    raw_batches = list(batches_col.find({}).sort("start_date", -1))
    batches = []
    for b in raw_batches:
        doc = dict(b)  # copy so we don't mutate DB doc directly
        doc['_id'] = str(b.get('_id'))
        doc['display_name'] = b.get('title') or b.get('name') or doc['_id']
        batches.append(doc)

    # If a batch selected, load its students (students.batch_id stored as ObjectId)
    students = []
    if selected_batch:
        try:
            bid = ObjectId(selected_batch)
        except Exception:
            bid = None
        if bid:
            cursor = students_col.find({"batch_id": bid}).sort([("first_name", 1), ("last_name", 1)])
            for s in cursor:
                s["_id"] = str(s["_id"])         # string id for template form fields
                s["photo"] = s.get("photo")
                students.append(s)

    # Preload existing attendance for the date+batch to pre-select buttons
    # NOTE: we store attendance.batch_id as string in save_attendance, so query with the string
    attendance_map = {}
    if selected_batch:
        docs = attendance_col.find({"date": q_date, "batch_id": selected_batch})
        for d in docs:
            attendance_map[d["student_id"]] = d.get("status", "absent")

    # Attach status to students
    for s in students:
        sid = s["_id"]
        s["status"] = attendance_map.get(sid, "absent")

    return render_template("attendance_register.html",
                           batches=batches,
                           students=students,
                           today=q_date,
                           selected_batch=selected_batch)


@app.route('/attendance/save', methods=['POST'])
def save_attendance():
    """
    Expects form fields:
      - date (hidden_date or date)
      - batch_id (hidden_batch or batch_id)
      - for each student: status_<student_id> (value: present/absent/leave)
    """
    form = request.form
    attend_date = parse_date(form.get("date") or form.get("hidden_date"))
    batch_id = form.get("batch_id") or form.get("hidden_batch")
    if not attend_date or not batch_id:
        return "Missing date or batch", 400

    # Validate batch id for student lookup (students store batch_id as ObjectId)
    try:
        bid_obj = ObjectId(batch_id)
    except Exception:
        return "Invalid batch id", 400

    students = list(students_col.find({"batch_id": bid_obj}))
    now = datetime.utcnow()

    # Save or update each student's attendance record for that date+batch
    # note: attendance documents keep batch_id as the string form for easy URL queries
    for s in students:
        sid = str(s["_id"])
        key = f"status_{sid}"
        status = form.get(key, "absent")
        attendance_col.update_one(
            {"date": attend_date, "batch_id": batch_id, "student_id": sid},
            {"$set": {
                "status": status,
                "updated_at": now,
                "batch_id": batch_id,
                "student_id": sid,
                "date": attend_date
            }},
            upsert=True
        )

    return redirect(url_for('attendance', date=attend_date, batch=batch_id))


@app.route('/attendance/export_csv')
def attendance_export_csv():
    """
    Returns a CSV for given date and optional batch query params:
      - date=yyyy-mm-dd
      - batch=<batch id>
    """
    q_date = parse_date(request.args.get('date'))
    batch_id = request.args.get('batch')

    query = {"date": q_date}
    if batch_id:
        query["batch_id"] = batch_id

    docs = list(attendance_col.find(query))
    status_map = {d["student_id"]: d["status"] for d in docs}

    # If batch specified, fetch students for that batch (students use ObjectId)
    students = []
    if batch_id:
        try:
            students = list(students_col.find({"batch_id": ObjectId(batch_id)}).sort([("first_name", 1)]))
        except Exception:
            return abort(400, "Invalid batch id")
    else:
        # all students referenced in attendance
        student_ids = list(status_map.keys())
        oid_list = []
        for sid in student_ids:
            try:
                oid_list.append(ObjectId(sid))
            except Exception:
                pass
        students = list(students_col.find({"_id": {"$in": oid_list}}))

    # Build CSV
    si = io.StringIO()
    writer = csv.writer(si)
    writer.writerow(["Sr", "Student Name", "Phone", "Admission No", "Status"])

    for i, s in enumerate(students, start=1):
        sid = str(s["_id"])
        name = f"{s.get('first_name','')} {s.get('last_name','')}".strip()
        phone = s.get('phone', '')
        form_no = s.get('form_no', '')
        status = status_map.get(sid, "absent")
        writer.writerow([i, name, phone, form_no, status])

    si.seek(0)
    mem = io.BytesIO()
    mem.write(si.getvalue().encode('utf-8'))
    mem.seek(0)
    filename = f"attendance_{q_date}.csv"
    return send_file(mem, as_attachment=True, download_name=filename, mimetype='text/csv')


@app.route('/api/batch/<batch_id>/students')
def api_students(batch_id):
    try:
        bid = ObjectId(batch_id)
    except Exception:
        return jsonify([])

    students = list(students_col.find({"batch_id": bid}))
    out = []
    for s in students:
        out.append({
            "_id": str(s["_id"]),
            "first_name": s.get("first_name"),
            "last_name": s.get("last_name"),
            "phone": s.get("phone"),
            "form_no": s.get("form_no"),
            "photo": s.get("photo")
        })
    return jsonify(out)


@app.route('/attendance/history')
def attendance_history():
    """
    Show dates & batches that have attendance records.
    Query params: date, batch (both optional)
    """
    q_date = request.args.get('date')
    q_batch = request.args.get('batch')

    q = {}
    if q_date:
        q['date'] = q_date
    if q_batch:
        q['batch_id'] = q_batch

    pipeline = [
        {"$match": q},
        {"$group": {"_id": {"date": "$date", "batch_id": "$batch_id"}, "count": {"$sum": 1}}},
        {"$sort": {"_id.date": -1}}
    ]
    groups = list(attendance_col.aggregate(pipeline))

    # map batch id (string) -> batch title
    batches_map = {}
    for b in batches_col.find({}):
        batches_map[str(b["_id"])] = b.get("title") or b.get("name") or str(b.get("_id"))

    return render_template('attendance_history.html',
                           groups=groups,
                           batches_map=batches_map,
                           filter_date=q_date,
                           filter_batch=q_batch)


@app.route('/attendance/view')
def attendance_view():
    """
    Show attendance rows for specified date and batch. Required query params: date, batch
    """
    q_date = request.args.get('date')
    q_batch = request.args.get('batch')
    if not q_date or not q_batch:
        flash("Provide both date and batch to view attendance.", "warning")
        return redirect(url_for('attendance_history'))

    docs = list(attendance_col.find({"date": q_date, "batch_id": q_batch}))
    student_ids = [d['student_id'] for d in docs]

    student_map = {}
    if student_ids:
        try:
            oid_list = [ObjectId(sid) for sid in student_ids]
            for s in students_col.find({"_id": {"$in": oid_list}}):
                student_map[str(s["_id"])] = s
        except Exception:
            student_map = {}

    rows = []
    for d in docs:
        sid = d['student_id']
        s = student_map.get(sid)
        rows.append({"student_id": sid, "status": d.get("status", "absent"), "student": s})

    rows.sort(key=lambda r: ((r['student'] or {}).get('first_name',''), (r['student'] or {}).get('last_name','')))

    try:
        batch_doc = batches_col.find_one({"_id": ObjectId(q_batch)})
        batch_title = batch_doc.get('name') or batch_doc.get('title') if batch_doc else q_batch
    except Exception:
        batch_title = q_batch

    return render_template('attendance_view.html',
                           date=q_date,
                           batch_id=q_batch,
                           batch_title=batch_title,
                           rows=rows)








def main():
    # find highest existing student_id (if any)
    max_doc = students.find_one(
        {"student_id": {"$exists": True}},
        sort=[("student_id", -1)]
    )
    start = int(max_doc["student_id"]) + 1 if max_doc else 1

    # find docs without student_id
    cursor = students.find({"student_id": {"$exists": False}}).sort("_id", ASCENDING)
    count = 0
    for doc in cursor:
        new_id = start + count
        students.update_one({"_id": doc["_id"]}, {"$set": {"student_id": new_id}})
        print(f"Assigned student_id {new_id} to {doc['_id']}")
        count += 1

    print(f"Done. Assigned student_id to {count} students. Next student_id: {start + count}")



# helper: convert year,month -> start_dt, end_dt (inclusive)
def month_date_range(year: int, month: int):
    start = datetime(year, month, 1)
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    # end is just before next_month
    end = next_month - timedelta(microseconds=1)
    return start, end

# helper: pick collection from globals() or fallback to db collection name
def pick_collection(*names, fallback_name=None):
    for n in names:
        val = globals().get(n)
        if val is not None:
            return val
    if fallback_name and globals().get("db") is not None:
        try:
            return globals()["db"].get_collection(fallback_name)
        except Exception:
            return None
    return None


# --------- Salary routes (paste/replace in app.py) ----------
from flask import (
    render_template, request, jsonify, flash, redirect, url_for, current_app
)
import traceback
from bson.objectid import ObjectId
from datetime import datetime, timedelta

# --- small helper: pick_collection (keeps your previous behavior if already defined) ---
def pick_collection(*possible_names, fallback_name=None):
    """
    Try to return the first defined collection object from globals() by the given names.
    If none found, try to get db[fallback_name] if db exists and fallback_name provided.
    """
    db_obj = globals().get("db")
    # first try explicitly provided names as variables in globals
    for nm in possible_names:
        if nm and nm in globals() and globals()[nm] is not None:
            return globals()[nm]
    # fallback to db.<collection> by fallback_name
    if db_obj is not None and fallback_name:
        try:
            return db_obj[fallback_name]
        except Exception:
            return None
    return None

# --- helper: produce start/end datetimes for a given month (inclusive) ---
def month_date_range(year: int, month: int):
    """
    Return (start_dt, end_dt) datetime objects covering the whole month in UTC naive datetimes.
    start is start of 1st day, end is end of last day (23:59:59.999999).
    """
    start_dt = datetime(year, month, 1)
    # compute next month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    end_dt = next_month - timedelta(microseconds=1)
    return start_dt, end_dt

@app.route('/salary/generate', methods=['GET','POST'])
def salary_generate():
    """
    GET: render salary_form.html with teachers list.
    POST: expects form fields:
      - teacher_id
      - month (YYYY-MM)
      - manual_hours (optional)
      - save (optional)
      - hourly_rate (optional override)
    Returns JSON on POST.
    """
    # ensure db exists
    db_obj = globals().get("db")
    if db_obj is None:
        # current_app should be available if Flask app context exists
        current_app.logger.error("DB object not available")
        return jsonify({"error": "Server configuration error: DB not available"}), 503

    # pick collections safely
    teachers_col   = pick_collection("teachers_col", "faculties_col", fallback_name="faculties")
    attendance_col = pick_collection("attendance_col", fallback_name="attendance")
    salaries_col   = pick_collection("salaries_col", fallback_name="salaries")

    # ---------- GET: render form ----------
    if request.method == 'GET':
        try:
            raw_teachers = []
            if teachers_col is not None:
                try:
                    raw_teachers = list(teachers_col.find({}).sort("name", 1))
                except Exception:
                    current_app.logger.warning("Failed to fetch teachers list", exc_info=True)
                    raw_teachers = []
            normalized = []
            for t in raw_teachers:
                normalized.append({
                    "_id": str(t.get("_id")),
                    "name": t.get("name") or t.get("full_name") or "Unknown",
                    "hourly_rate": float(t.get("hourly_rate") or 0.0)
                })
            return render_template('salary_form.html', teachers=normalized)
        except Exception:
            current_app.logger.exception("Error rendering salary form")
            return render_template('salary_form.html', teachers=[]), 500

    # ---------- POST: compute salary ----------
    try:
        teacher_id = request.form.get('teacher_id') or request.form.get('teacher')
        month_str  = request.form.get('month')
        manual_hours_str = request.form.get('manual_hours')  # optional
        save_flag = (request.form.get('save') == 'on') or (request.form.get('save') == 'true')
        hourly_rate_override = request.form.get('hourly_rate')  # optional override numeric

        # validations
        if not teacher_id:
            return jsonify({"error": "Missing teacher_id"}), 400
        if not month_str:
            return jsonify({"error": "Missing month (YYYY-MM)"}), 400

        # parse month
        try:
            year, month = map(int, month_str.split('-'))
            if month < 1 or month > 12:
                raise ValueError()
        except Exception:
            return jsonify({"error": "Invalid month format. Use YYYY-MM."}), 400

        # resolve teacher id forms
        t_id_str = str(teacher_id).strip()
        t_obj_id = None
        if ObjectId.is_valid(t_id_str):
            try:
                t_obj_id = ObjectId(t_id_str)
            except Exception:
                t_obj_id = None

        # find teacher doc (to obtain name and default hourly_rate)
        if teachers_col is None:
            return jsonify({"error": "Server configuration error: faculties collection not available"}), 500

        teacher = None
        if t_obj_id:
            try:
                teacher = teachers_col.find_one({"_id": t_obj_id})
            except Exception:
                teacher = None
        if not teacher:
            try:
                teacher = teachers_col.find_one({"_id": t_id_str}) or teachers_col.find_one({"name": t_id_str})
            except Exception:
                teacher = None
        if not teacher:
            return jsonify({"error": "Teacher not found in faculties collection"}), 400

        # determine hourly_rate (override takes precedence)
        try:
            if hourly_rate_override and hourly_rate_override.strip() != "":
                hourly_rate = float(hourly_rate_override)
            else:
                hourly_rate = float(teacher.get("hourly_rate") or 0.0)
        except Exception:
            hourly_rate = 0.0

        # If manual_hours provided, use that directly
        total_hours = 0.0
        used_manual = False
        if manual_hours_str and str(manual_hours_str).strip() != "":
            try:
                total_hours = float(manual_hours_str)
                used_manual = True
            except Exception:
                return jsonify({"error": "Invalid manual_hours value"}), 400

        # Otherwise compute from attendance collection (if available)
        start_dt, end_dt = month_date_range(year, month)
        agg_objid = []
        agg_strid = []
        iter_count = 0

        if not used_manual:
            if attendance_col is None:
                current_app.logger.warning("attendance collection not available; total_hours defaults to 0")
                total_hours = 0.0
            else:
                # try aggregation by ObjectId
                if t_obj_id:
                    pipeline_objid = [
                        {"$match": {"teacher_id": t_obj_id, "date": {"$gte": start_dt, "$lte": end_dt}}},
                        {"$group": {"_id": "$teacher_id", "total_hours": {"$sum": "$hours"}}}
                    ]
                    try:
                        agg_objid = list(attendance_col.aggregate(pipeline_objid))
                    except Exception:
                        agg_objid = []

                # if aggregation produced a result, use it
                if agg_objid and len(agg_objid) > 0 and agg_objid[0].get("total_hours") is not None:
                    try:
                        total_hours = float(agg_objid[0]["total_hours"])
                    except Exception:
                        total_hours = 0.0
                else:
                    # try aggregation by string id
                    pipeline_strid = [
                        {"$match": {"teacher_id": t_id_str, "date": {"$gte": start_dt, "$lte": end_dt}}},
                        {"$group": {"_id": "$teacher_id", "total_hours": {"$sum": "$hours"}}}
                    ]
                    try:
                        agg_strid = list(attendance_col.aggregate(pipeline_strid))
                    except Exception:
                        agg_strid = []

                    if agg_strid and len(agg_strid) > 0 and agg_strid[0].get("total_hours") is not None:
                        try:
                            total_hours = float(agg_strid[0]["total_hours"])
                        except Exception:
                            total_hours = 0.0
                    else:
                        # last resort: iterate docs in the month and sum where teacher_id matches
                        try:
                            cursor = attendance_col.find({"date": {"$gte": start_dt, "$lte": end_dt}})
                            s = 0.0
                            c = 0
                            for doc in cursor:
                                doc_tid = doc.get("teacher_id")
                                # match either ObjectId or string
                                if (t_obj_id and doc_tid == t_obj_id) or (str(doc_tid) == t_id_str):
                                    try:
                                        s += float(doc.get("hours") or 0.0)
                                        c += 1
                                    except Exception:
                                        pass
                            total_hours = s
                            iter_count = c
                        except Exception:
                            total_hours = 0.0

        # compute final amount
        amount = round(total_hours * hourly_rate, 2)

        result = {
            "teacher_id": str(teacher.get("_id")),
            "teacher_name": teacher.get("name"),
            "month": f"{year}-{month:02d}",
            "year": year,
            "month_num": month,
            "total_hours": total_hours,
            "hourly_rate": hourly_rate,
            "amount": amount,
            "saved": False,
            "matched": {
                "used_manual": used_manual,
                "agg_objid_count": len(agg_objid) if 'agg_objid' in locals() else 0,
                "agg_strid_count": len(agg_strid) if 'agg_strid' in locals() else 0,
                "iter_count": iter_count
            }
        }

        # Save/upsert if requested (hours-based)
        if save_flag:
            if salaries_col is None:
                current_app.logger.error("salaries collection not available; cannot save salary")
                return jsonify({"error": "Server configuration error: salaries collection not available"}), 500

            # store teacher id as ObjectId if possible
            try:
                stored_teacher_id = ObjectId(str(teacher.get("_id"))) if ObjectId.is_valid(str(teacher.get("_id"))) else str(teacher.get("_id"))
            except Exception:
                stored_teacher_id = str(teacher.get("_id"))

            salary_doc = {
                "teacher_id": stored_teacher_id,
                "teacher_name": teacher.get("name"),
                "year": year,
                "month": month,
                "month_str": f"{year}-{month:02d}",
                "total_hours": total_hours,
                "hourly_rate": hourly_rate,
                "amount": amount,
                "generated_on": datetime.utcnow(),
                "manual_entry": bool(used_manual),
                "mode": "hours"
            }

            query_key = {"teacher_id": stored_teacher_id, "year": year, "month": month, "mode": "hours"}
            try:
                salaries_col.update_one(query_key, {"$set": salary_doc}, upsert=True)
                result["saved"] = True
            except Exception:
                current_app.logger.exception("Failed to upsert salary_doc")
                return jsonify({"error": "Failed to save salary"}), 500

        # Return the computed result as JSON
        return jsonify(result)

    except Exception as e:
        current_app.logger.error("Unexpected error in salary_generate: %s\n%s", str(e), traceback.format_exc())
        return jsonify({"error": f"Server error: {str(e)}"}), 500


# ---------- Days-based save endpoint (new) ----------
@app.route('/salary/generate_days', methods=['POST'])
def salary_generate_days():
    """
    Accept JSON payload from the days-based frontend preview and store the salary.
    Expected JSON keys (the frontend sends these):
      - teacher_id (required)
      - month (YYYY-MM) (required)
      - total_collection (number)
      - fixed_salary (number)
      - days_in_month (number)
      - per_day (number)
      - absent_days, attendance_equiv (number)
      - prorated_salary (number)
      - salary_deduction (number)
      - incentive_pct, incentive_amt
      - pension_add, pension_ded
      - tds_pct, tds_amt
      - food_charges
      - gross
    Returns JSON { saved: True, id: "..."} on success.
    """
    try:
        if not request.is_json:
            return jsonify({"error": "Expected JSON body"}), 400
        payload = request.get_json()

        # minimal validations
        teacher_id = payload.get('teacher_id')
        month_str = payload.get('month')
        if not teacher_id or not month_str:
            return jsonify({"error": "Missing teacher_id or month"}), 400

        # parse month
        try:
            year, month = map(int, str(month_str).split('-'))
            if month < 1 or month > 12:
                raise ValueError()
        except Exception:
            return jsonify({"error": "Invalid month format. Use YYYY-MM."}), 400

        # pick salaries collection
        salaries_col = pick_collection("salaries_col", fallback_name="salaries")
        teachers_col = pick_collection("teachers_col", "faculties_col", fallback_name="faculties")
        if salaries_col is None:
            current_app.logger.error("salaries collection not available; cannot save days salary")
            return jsonify({"error": "Server configuration error: salaries collection not available"}), 500

        # attempt to store teacher id as ObjectId if possible
        try:
            stored_teacher_id = ObjectId(str(teacher_id)) if ObjectId.is_valid(str(teacher_id)) else str(teacher_id)
        except Exception:
            stored_teacher_id = str(teacher_id)

        # resolve teacher name if possible
        teacher_name = payload.get('teacher_name') or ''
        if teachers_col is not None and not teacher_name:
            try:
                tdoc = None
                if isinstance(stored_teacher_id, ObjectId):
                    tdoc = teachers_col.find_one({"_id": stored_teacher_id})
                else:
                    tdoc = teachers_col.find_one({"_id": stored_teacher_id}) or teachers_col.find_one({"name": stored_teacher_id})
                if tdoc:
                    teacher_name = tdoc.get('name') or teacher_name
            except Exception:
                teacher_name = teacher_name or ''

        # build document to store (store many fields so preview matches saved doc)
        def num(k):
            try:
                return float(payload.get(k) or 0)
            except Exception:
                return 0.0

        salary_doc = {
            "teacher_id": stored_teacher_id,
            "teacher_name": teacher_name,
            "year": year,
            "month": month,
            "month_str": f"{year}-{month:02d}",
            "mode": "days",
            "total_collection": num('total_collection'),
            "fixed_salary": num('fixed_salary'),
            "days_in_month": int(payload.get('days_in_month') or 0),
            "per_day": int(payload.get('per_day') or 0),
            "attendance_equiv": float(payload.get('attendance_equiv') or 0.0),
            "absent_days": float(payload.get('absent_days') or 0.0),
            "prorated_salary": float(payload.get('prorated_salary') or 0.0),
            "salary_deduction": float(payload.get('salary_deduction') or 0.0),
            "incentive_pct": float(payload.get('incentive_pct') or 0.0),
            "incentive_amt": float(payload.get('incentive_amt') or 0.0),
            "pension_add": float(payload.get('pension_add') or 0.0),
            "pension_ded": float(payload.get('pension_ded') or 0.0),
            "food_charges": float(payload.get('food_charges') or 0.0),
            "tds_pct": float(payload.get('tds_pct') or 0.0),
            "tds_amt": float(payload.get('tds_amt') or 0.0),
            "gross": float(payload.get('gross') or 0.0),
            "generated_on": datetime.utcnow(),
        }

        # upsert using teacher+year+month+mode as key
        query_key = {"teacher_id": stored_teacher_id, "year": year, "month": month, "mode": "days"}
        try:
            res = salaries_col.update_one(query_key, {"$set": salary_doc}, upsert=True)
            return jsonify({"saved": True, "upserted": bool(res.upserted_id), "matched_count": res.matched_count}), 200
        except Exception:
            current_app.logger.exception("Failed to upsert days salary")
            return jsonify({"error": "Failed to save salary"}), 500

    except Exception as e:
        current_app.logger.error("Unexpected error in salary_generate_days: %s\n%s", str(e), traceback.format_exc())
        return jsonify({"error": f"Server error: {str(e)}"}), 500


# ---------- list / edit / delete routes (kept from your code) ----------
@app.route('/salary/list')
def salary_list():
    db_obj = globals().get("db")
    salaries_col = pick_collection("salaries_col", fallback_name="salaries")
    if salaries_col is None:
        return "salaries collection not available", 503

    try:
        rows = list(salaries_col.find({}).sort([("year", -1), ("month", -1), ("teacher_name", 1)]))
    except Exception:
        current_app.logger.exception("Failed to load salaries")
        rows = []

    # convert ObjectId to string
    for r in rows:
        if isinstance(r.get("teacher_id"), ObjectId):
            r["teacher_id"] = str(r["teacher_id"])
        r["_id"] = str(r.get("_id"))

    return render_template('salary_list.html', salaries=rows)


@app.route('/salary/edit/<id>', methods=['GET', 'POST'])
def salary_edit(id):
    salaries_col = pick_collection("salaries_col", fallback_name="salaries")
    teachers_col = pick_collection("teachers_col", "faculties_col", fallback_name="faculties")
    if salaries_col is None:
        return "salaries collection not available", 503

    # convert id to ObjectId or keep string
    try:
        sal_obj_id = ObjectId(id) if ObjectId.is_valid(id) else id
    except Exception:
        sal_obj_id = id

    # GET -> render edit form
    if request.method == 'GET':
        try:
            sal = salaries_col.find_one({"_id": sal_obj_id}) if isinstance(sal_obj_id, ObjectId) else salaries_col.find_one({"_id": sal_obj_id})
            if not sal:
                flash("Salary record not found.", "warning")
                return redirect(url_for('salary_list'))

            # Normalize teacher list for dropdown (optional)
            teachers = []
            if teachers_col is not None:
                try:
                    teachers = list(teachers_col.find({}).sort("name", 1))
                except Exception:
                    teachers = []

            normalized = [{"_id": str(t.get("_id")), "name": t.get("name") or t.get("full_name") or "Unknown",
                           "hourly_rate": float(t.get("hourly_rate") or 0.0)} for t in teachers]

            # convert some fields for template
            sal["_id"] = str(sal.get("_id"))
            if isinstance(sal.get("teacher_id"), ObjectId):
                sal["teacher_id"] = str(sal["teacher_id"])

            return render_template('salary_edit.html', salary=sal, teachers=normalized)
        except Exception:
            current_app.logger.exception("Error rendering salary edit form")
            flash("Failed to load record.", "danger")
            return redirect(url_for('salary_list'))

    # POST -> save edits
    try:
        teacher_id = request.form.get('teacher_id')
        month_str = request.form.get('month')  # expect YYYY-MM
        total_hours = float(request.form.get('total_hours') or 0.0)
        hourly_rate = float(request.form.get('hourly_rate') or 0.0)
        manual_entry = request.form.get('manual_entry') == 'on'

        # parse month into year/month
        try:
            year, month = map(int, month_str.split('-'))
        except Exception:
            flash("Invalid month format. Use YYYY-MM.", "danger")
            return redirect(url_for('salary_edit', id=id))

        # build stored_teacher_id
        stored_teacher_id = ObjectId(teacher_id) if ObjectId.is_valid(teacher_id) else teacher_id

        # resolve teacher name safely
        teacher_name = ''
        if teachers_col is not None:
            try:
                tdoc = teachers_col.find_one({"_id": stored_teacher_id}) if ObjectId.is_valid(str(stored_teacher_id)) else teachers_col.find_one({"_id": stored_teacher_id})
                teacher_name = (tdoc or {}).get("name") or request.form.get('teacher_name') or ''
            except Exception:
                teacher_name = request.form.get('teacher_name') or ''
        else:
            teacher_name = request.form.get('teacher_name') or ''

        salary_doc = {
            "teacher_id": stored_teacher_id,
            "teacher_name": teacher_name,
            "year": year,
            "month": month,
            "month_str": f"{year}-{month:02d}",
            "total_hours": total_hours,
            "hourly_rate": hourly_rate,
            "amount": round(total_hours * hourly_rate, 2),
            "generated_on": datetime.utcnow(),
            "manual_entry": bool(manual_entry)
        }

        query = {"_id": sal_obj_id} if isinstance(sal_obj_id, ObjectId) else {"_id": sal_obj_id}
        salaries_col.update_one(query, {"$set": salary_doc})
        flash("Salary record updated.", "success")
        return redirect(url_for('salary_list'))

    except Exception as e:
        current_app.logger.error("Error editing salary: %s\n%s", str(e), traceback.format_exc())
        flash("Failed to update salary.", "danger")
        return redirect(url_for('salary_list'))


@app.route('/salary/delete/<id>', methods=['POST'])
def salary_delete(id):
    salaries_col = pick_collection("salaries_col", fallback_name="salaries")
    if salaries_col is None:
        return "salaries collection not available", 503

    try:
        sal_obj_id = ObjectId(id) if ObjectId.is_valid(id) else id
    except Exception:
        sal_obj_id = id

    try:
        res = salaries_col.delete_one({"_id": sal_obj_id} if isinstance(sal_obj_id, ObjectId) else {"_id": sal_obj_id})
        if res.deleted_count:
            flash("Salary record deleted.", "success")
        else:
            flash("Salary record not found.", "warning")
    except Exception as e:
        current_app.logger.exception("Failed to delete salary: %s", e)
        flash("Failed to delete salary.", "danger")

    return redirect(url_for('salary_list'))


@app.route("/debug/faculties_sample")
def debug_faculties_sample():
    db_obj = globals().get("db")
    if db_obj is None:
        return jsonify({"count": 0, "sample": []})
    try:
        docs = list(db_obj.faculties.find({}, {"name":1}).limit(20))
        return jsonify({"count": len(docs), "sample": [{**{"_id": str(d["_id"])}, "name": d.get("name")} for d in docs]})
    except Exception:
        current_app.logger.exception("debug_faculties_sample failed")
        return jsonify({"count": 0, "sample": []})





@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():

    users = users_col
    user = users.find_one({"_id": session["user_id"]})

    if request.method == "POST":
        new_email = request.form.get("email")
        new_pass = request.form.get("password")

        update_doc = {}

        if new_email:
            update_doc["email"] = new_email

        if new_pass:
            update_doc["password_hash"] = generate_password_hash(new_pass)

        if update_doc:
            users.update_one({"_id": user["_id"]}, {"$set": update_doc})
            flash("Settings updated successfully!", "success")

        return redirect(url_for("settings"))

    return render_template("settings.html", user=user)










def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not session.get('user_id'):
            return redirect(url_for('login', next=request.path))
        # optionally load user into g
        u = get_users_col().find_one({"_id": ObjectId(session['user_id'])}) if ObjectId.is_valid(str(session.get('user_id'))) else get_users_col().find_one({"_id": session.get('user_id')})
        g.current_user = u
        return f(*args, **kwargs)
    return wrapped

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username','').strip()
        password = request.form.get('password','')
        users = get_users_col()
        user = users.find_one({"username": username})
        if user and check_password_hash(user.get('password_hash',''), password):
            session['user_id'] = str(user.get('_id'))
            session['user_name'] = user.get('name') or user.get('username')
            flash("Login successful", "success")
            next_url = request.args.get('next') or url_for('index')
            return redirect(next_url)
        else:
            flash("Invalid credentials", "danger")
    return render_template('login.html')

@app.route('/logout')
def logout():
    # we will show a confirmation in the UI; this route actually performs logout
    session.clear()
    flash("You have been logged out.", "info")
    return redirect(url_for('login'))



@app.route('/profile', methods=['GET','POST'])
@login_required
def profile():
    users = get_users_col()
    uid = session.get('user_id')
    user = users.find_one({"_id": ObjectId(uid)}) if ObjectId.is_valid(uid) else users.find_one({"_id": uid})

    if request.method == 'POST':
        name = request.form.get('name') or user.get('name')
        email = request.form.get('email') or user.get('email')
        phone = request.form.get('phone') or user.get('phone')

        update = {"name": name, "email": email, "phone": phone}
        # file upload
        f = request.files.get('photo')
        if f and f.filename and allowed_file(f.filename):
            fname = secure_filename(f.filename)
            # prefix with user id + timestamp to avoid collisions
            basename = f"{uid}_{int(datetime.utcnow().timestamp())}_{fname}"
            dest = os.path.join(UPLOAD_FOLDER, basename)
            f.save(dest)
            update['photo'] = basename

        users.update_one({"_id": user.get('_id')}, {"$set": update})
        flash("Profile updated.", "success")
        return redirect(url_for('profile'))

    return render_template('profile.html', user=user)



@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)


@app.route('/notifications')
@login_required
def notifications():
    # Use the module-level students_col defined at top of file (no globals() truth testing)
    # Fees due: student docs with 'balance' > 0
    fees_due = []
    try:
        for s in students_col.find({"balance": {"$gt": 0}}):
            fees_due.append({
                "type": "fee",
                "student_name": (s.get('first_name', '') + ' ' + (s.get('last_name') or '')).strip(),
                "amount": s.get('balance'),
                "student_id": str(s.get('_id'))
            })
    except Exception:
        fees_due = []

    # Expiry alerts: expiry_date within 14 days (student.expiry_date stored as string YYYY-MM-DD or date)
    expiry_alerts = []
    try:
        today = date.today()
        threshold = today + timedelta(days=14)
        for s in students_col.find({}):
            exp = s.get('expiry_date') or s.get('expiry') or None
            if not exp:
                continue

            # handle dates stored as string 'YYYY-MM-DD' or datetime
            exp_date = None
            if isinstance(exp, str):
                try:
                    exp_date = datetime.strptime(exp.split('T')[0], '%Y-%m-%d').date()
                except Exception:
                    exp_date = None
            elif isinstance(exp, datetime):
                exp_date = exp.date()

            if exp_date and today <= exp_date <= threshold:
                expiry_alerts.append({
                    "type": "expiry",
                    "student_name": (s.get('first_name', '') + ' ' + (s.get('last_name') or '')).strip(),
                    "expiry_date": exp_date.isoformat(),
                    "student_id": str(s.get('_id'))
                })
    except Exception:
        expiry_alerts = []

    # Build notifications list sorted by priority (expiry first, then fees)
    notes = []
    for e in expiry_alerts:
        notes.append({
            "message": f"Course expiring for {e['student_name']} on {e['expiry_date']}",
            "type": "expiry",
            "student_id": e['student_id'],
            "time": "Soon"
        })
    for f in fees_due:
        notes.append({
            "message": f"Fees due: {f['student_name']} (₹{f['amount']})",
            "type": "fee",
            "student_id": f['student_id'],
            "time": "Pending"
        })

    return render_template('notifications.html', notifications=notes)


# simple count endpoint used by navbar badge
@app.route('/notifications/count')
def notifications_count():
    # Use module-level collections defined at top of file (students_col, batches_col, courses_col)
    try:
        fees_count = students_col.count_documents({"balance": {"$gt": 0}})
    except Exception:
        app.logger.exception("fees_count failed")
        fees_count = 0

    expiry_count = 0
    try:
        today = date.today()
        threshold = today + timedelta(days=14)

        # Fast path: try BSON datetime range count (works if expiry_date is stored as datetime)
        try:
            start_dt = datetime.combine(today, datetime.min.time())
            end_dt = datetime.combine(threshold, datetime.max.time())
            expiry_count = students_col.count_documents({
                "expiry_date": {"$gte": start_dt, "$lte": end_dt}
            })
        except Exception:
            app.logger.debug("Fast expiry_date range count failed; falling back to per-doc parsing.")
            expiry_count = 0

        # Fallback: inspect docs for string dates or infer expiry from admission_date + duration
        if expiry_count == 0:
            cursor = students_col.find({}, {
                "expiry_date": 1,
                "admission_date": 1,
                "batch_id": 1,
                "course_id": 1
            })
            for s in cursor:
                exp = s.get("expiry_date")
                exp_date = None

                # parse expiry_date if present
                try:
                    if isinstance(exp, datetime):
                        exp_date = exp.date()
                    elif isinstance(exp, str):
                        try:
                            exp_date = datetime.strptime(exp[:10], "%Y-%m-%d").date()
                        except Exception:
                            try:
                                exp_date = datetime.strptime(exp[:10], "%d-%m-%Y").date()
                            except Exception:
                                exp_date = None
                    elif isinstance(exp, dict) and "$date" in exp:
                        raw = exp["$date"]
                        if isinstance(raw, str):
                            try:
                                exp_date = datetime.strptime(raw[:10], "%Y-%m-%d").date()
                            except Exception:
                                exp_date = None
                except Exception:
                    exp_date = None

                # infer from admission_date + duration if expiry missing
                if exp_date is None:
                    adm = s.get("admission_date")
                    adm_date = None
                    if isinstance(adm, datetime):
                        adm_date = adm.date()
                    elif isinstance(adm, str):
                        try:
                            adm_date = datetime.strptime(adm[:10], "%Y-%m-%d").date()
                        except Exception:
                            adm_date = None

                    if adm_date:
                        dur_days = None
                        bid = s.get("batch_id")
                        cid = s.get("course_id")

                        try:
                            if bid:
                                batch = batches_col.find_one({"_id": bid})
                                if batch:
                                    dur_days = batch.get("duration_days") or batch.get("duration")
                                    if dur_days and isinstance(dur_days, str) and "month" in dur_days:
                                        try:
                                            months = int(''.join(ch for ch in dur_days if ch.isdigit()))
                                            dur_days = months * 30
                                        except Exception:
                                            dur_days = None

                            if dur_days is None and cid:
                                course = courses_col.find_one({"_id": cid})
                                if course:
                                    dur_days = course.get("duration_days") or course.get("duration")
                                    if dur_days and isinstance(dur_days, str) and "month" in dur_days:
                                        try:
                                            months = int(''.join(ch for ch in dur_days if ch.isdigit()))
                                            dur_days = months * 30
                                        except Exception:
                                            dur_days = None
                        except Exception:
                            app.logger.debug("Failed to fetch batch/course for student %r", s.get("_id"))

                        try:
                            if dur_days:
                                dur_days_int = int(dur_days)
                                exp_date = adm_date + timedelta(days=dur_days_int)
                        except Exception:
                            exp_date = None

                if exp_date and today <= exp_date <= threshold:
                    expiry_count += 1

    except Exception:
        app.logger.exception("expiry_count calculation failed")
        expiry_count = 0

    return jsonify({"count": fees_count + expiry_count})


# Certificate generator page + optional students API for autocomplete
@app.route('/certificate-generator')
@login_required   # optional: remove if you want page public
def certificate_generator_page():
    """
    Renders page where user can search/select a student and generate certificate.
    """
    return render_template('certificate_generator.html')


@app.route('/api/all_students')
def api_all_students():
    """
    Returns a small JSON list of students for autocomplete/search.
    Limit and projection keep data light.
    """
    q = request.args.get('q', '').strip()
    query = {}
    if q:
        # search by name or form number or phone
        query = {"$or": [
            {"first_name": {"$regex": q, "$options": "i"}},
            {"last_name": {"$regex": q, "$options": "i"}},
            {"form_no": {"$regex": q, "$options": "i"}},
            {"phone": {"$regex": q}}
        ]}

    # Adjust limit as needed
    docs = list(db.students.find(query, {"first_name":1, "last_name":1, "form_no":1}).sort("created_at",-1).limit(200))
    out = []
    for s in docs:
        out.append({
            "_id": str(s.get("_id")),
            "name": ((s.get("first_name","") + " " + s.get("last_name","")).strip()) or s.get("form_no",""),
            "form_no": s.get("form_no","")
        })
    return jsonify(out)

@app.route("/generate_certificate_manual", methods=["POST"])
def generate_certificate_manual():
    data = {
        "name": request.form.get("name"),
        "father": request.form.get("father"),
        "age": request.form.get("age"),
        "course": request.form.get("course"),
        "courseHours": request.form.get("courseHours"),
        "admission": request.form.get("admission"),
        "completion": request.form.get("completion"),
        "formNo": request.form.get("formNo"),
        "photo": request.form.get("photo") or "default.jpg"
    }

    return render_template("certificate_template.html", **data)

@app.route("/generate_certificate/<id>")
def generate_certificate(id):
    from bson.objectid import ObjectId
    from datetime import date, datetime

    # 1) find student by _id or form_no
    student = None
    try:
        if ObjectId.is_valid(id):
            student = students_col.find_one({"_id": ObjectId(id)})
    except Exception:
        student = None

    if not student:
        student = students_col.find_one({"form_no": id}) or students_col.find_one({"formNo": id})

    if not student:
        app.logger.info("generate_certificate: student not found for id=%s", id)
        return abort(404)

    # 2) Name & father
    name = " ".join(filter(None, [student.get("first_name","").strip(), student.get("last_name","").strip()])).strip() or student.get("name","")
    father = student.get("father_name") or student.get("father") or ""

    # 3) Age from dob (if present) else use age field
    def calc_age(dob):
        try:
            if not dob:
                return ""
            if isinstance(dob, str):
                dob_dt = datetime.fromisoformat(dob)
            elif isinstance(dob, datetime):
                dob_dt = dob
            else:
                return ""
            today = date.today()
            years = today.year - dob_dt.year - ((today.month, today.day) < (dob_dt.month, dob_dt.day))
            return str(years)
        except Exception:
            return student.get("age","")

    age = student.get("age") or calc_age(student.get("dob") or student.get("date_of_birth"))

    # 4) Course lookup (course_id exists in your student doc)
    course = ""
    course_hours = ""
    if student.get("course_id"):
        try:
            cid = student["course_id"]
            # course_id is stored as ObjectId in DB already
            course_doc = courses_col.find_one({"_id": cid})
            if course_doc:
                # prefer common field names
                course = course_doc.get("name") or course_doc.get("course") or course_doc.get("title") or ""
                course_hours = course_doc.get("hours") or course_doc.get("duration") or course_doc.get("courseHours") or ""
        except Exception:
            pass

    # 5) Admission & completion (student.admission_date and batch end date)
    admission = student.get("admission_date") or student.get("admission") or ""
    # format ISO date strings (ensure yyyy-mm-dd)
    def fmt(d):
        try:
            if not d: return ""
            if isinstance(d, datetime):
                return d.strftime("%Y-%m-%d")
            if isinstance(d, str):
                return d.split("T")[0]
            return str(d)
        except:
            return str(d)
    admission = fmt(admission)

    completion = ""
    if student.get("batch_id"):
        try:
            bid = student["batch_id"]
            batch_doc = batches_col.find_one({"_id": bid})
            if batch_doc:
                completion = batch_doc.get("end_date") or batch_doc.get("completion_date") or batch_doc.get("finish_date") or ""
                completion = fmt(completion)
        except Exception:
            pass

    # fallback for course_hours if empty: try to look at student.fee or notes (optional)
    if not course_hours:
        # maybe the students collection stores duration in student's doc
        course_hours = student.get("courseHours") or student.get("course_hours") or ""

    # photo handling (filename stored in student.photo)
    photo = student.get("photo") or ""

    formNo = student.get("form_no") or student.get("formNo") or str(student.get("_id"))

    data = {
        "name": name,
        "father": father,
        "age": age,
        "course": course,
        "courseHours": course_hours,
        "admission": admission,
        "completion": completion,
        "formNo": formNo,
        "photo": photo
    }

    # render (HTML preview). pdfkit fallback handled by template route if you prefer
    return render_template("certificate_template.html", **data)









# --- Daybook: Ledger & Voucher APIs (paste into app.py, no blueprint) ---
from bson.objectid import ObjectId
from bson.errors import InvalidId
from flask import jsonify, request, render_template, abort
from datetime import datetime

@app.route('/daybook')
def daybook():
    return render_template('daybook.html')

# ---------- Helpers ----------
def validate_voucher_payload(payload):
    lines = payload.get("lines") or []
    if not payload.get("date"):
        return "date required"
    if not lines or not any(l.get("account") and float(l.get("amount") or 0) > 0 for l in lines):
        return "at least one ledger line with positive amount required"
    return None

def compute_totals(lines):
    dr = sum(float(l.get("amount") or 0) for l in lines if l.get("type") == "debit")
    cr = sum(float(l.get("amount") or 0) for l in lines if l.get("type") == "credit")
    return dr, cr

def auto_allocate_contra(lines, voucher_type):
    # If contra voucher and only one non-zero line present, add opposite line (Cash/Bank guess).
    if voucher_type != 'contra':
        return lines
    nonzero = [l for l in lines if float(l.get("amount") or 0) > 0]
    if len(nonzero) == 1:
        l = nonzero[0]
        amount = float(l.get("amount") or 0)
        counter = "Bank" if "bank" in (l.get("account") or "").lower() else "Cash"
        opp_type = "credit" if l.get("type") == "debit" else "debit"
        lines.append({"account": counter, "type": opp_type, "amount": amount, "details": ""})
    return lines

# ----------------- Ledger Groups CRUD -----------------
@app.route("/api/ledger_groups", methods=["GET"])
def list_ledger_groups():
    docs = list(db.ledger_groups.find().sort("name", 1))
    out = []
    for d in docs:
        d["_id"] = str(d["_id"])
        out.append(d)
    return jsonify(out)

@app.route("/api/ledger_groups", methods=["POST"])
def create_ledger_group():
    data = request.json or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    doc = {"name": name, "created_at": datetime.utcnow()}
    res = db.ledger_groups.insert_one(doc)
    doc["_id"] = str(res.inserted_id)
    return jsonify(doc), 201

@app.route("/api/ledger_groups/<id>", methods=["DELETE"])
def delete_ledger_group(id):
    try:
        oid = ObjectId(id)
    except Exception:
        return abort(404)
    db.ledger_groups.delete_one({"_id": oid})
    # remove group assignment from ledgers that referenced this group (stored as string id)
    db.ledgers.update_many({"group": id}, {"$unset": {"group": ""}})
    return jsonify({"ok": True})

# ----------------- Ledgers CRUD (supports group) -----------------
@app.route("/api/ledgers", methods=["GET"])
def list_ledgers():
    docs = list(db.ledgers.find().sort("name", 1))
    # build group map for convenience
    group_map = {}
    for g in db.ledger_groups.find():
        group_map[str(g["_id"])] = g.get("name", "")
    out = []
    for d in docs:
        d["_id"] = str(d["_id"])
        g = d.get("group")
        d["group_name"] = group_map.get(g, "") if g else ""
        out.append(d)
    return jsonify(out)

@app.route("/api/ledgers", methods=["POST"])
def create_ledger():
    data = request.json or {}
    name = (data.get("name") or "").strip()
    group = data.get("group") or None  # optional group id (string)
    if not name:
        return jsonify({"error":"name required"}), 400
    doc = {"name": name, "created_at": datetime.utcnow()}
    if group:
        doc["group"] = group
    res = db.ledgers.insert_one(doc)
    doc["_id"] = str(res.inserted_id)
    doc["group_name"] = ""
    if group:
        try:
            g = db.ledger_groups.find_one({"_id": ObjectId(group)})
            doc["group_name"] = g["name"] if g else ""
        except Exception:
            doc["group_name"] = ""
    return jsonify(doc), 201

@app.route("/api/ledgers/<id>", methods=["PUT"])
def update_ledger(id):
    try:
        oid = ObjectId(id)
    except Exception:
        return abort(404)
    data = request.json or {}
    name = (data.get("name") or "").strip()
    group = data.get("group") if "group" in data else None  # explicit allow null to remove
    if not name:
        return jsonify({"error":"name required"}), 400
    update_fields = {"name": name}
    if group is None:
        # if client omitted 'group', leave as-is; if client explicitly set group to null/"" it will clear below
        pass
    else:
        # set or clear group
        if group == "" or group is None:
            update_fields["group"] = None
        else:
            update_fields["group"] = group
    db.ledgers.update_one({"_id": oid}, {"$set": update_fields})
    doc = db.ledgers.find_one({"_id": oid})
    if not doc:
        return abort(404)
    doc["_id"] = str(doc["_id"])
    if doc.get("group"):
        try:
            g = db.ledger_groups.find_one({"_id": ObjectId(doc["group"])})
            doc["group_name"] = g["name"] if g else ""
        except Exception:
            doc["group_name"] = ""
    else:
        doc["group_name"] = ""
    return jsonify(doc)

@app.route("/api/ledgers/<id>", methods=["DELETE"])
def delete_ledger(id):
    try:
        oid = ObjectId(id)
    except Exception:
        return abort(404)
    db.ledgers.delete_one({"_id": oid})
    return jsonify({"ok": True})

# ----------------- Vouchers CRUD -----------------
@app.route("/api/vouchers", methods=["GET"])
def list_vouchers():
    q = {}
    args = request.args
    if args.get("from"):
        q["date"] = q.get("date", {})
        q["date"]["$gte"] = args.get("from")
    if args.get("to"):
        q["date"] = q.get("date", {})
        q["date"]["$lte"] = args.get("to")
    if args.get("search"):
        s = args.get("search")
        q["$or"] = [
            {"no": {"$regex": s, "$options":"i"}},
            {"narration": {"$regex": s, "$options":"i"}},
            {"lines.account": {"$regex": s, "$options":"i"}}
        ]
    docs = list(db.vouchers.find(q).sort("date", 1))
    for d in docs:
        d["_id"] = str(d["_id"])
    return jsonify(docs)

@app.route("/api/vouchers", methods=["POST"])
def create_voucher():
    data = request.json or {}
    err = validate_voucher_payload(data)
    if err:
        return jsonify({"error": err}), 400

    data["lines"] = auto_allocate_contra(data.get("lines", []), data.get("type"))

    dr, cr = compute_totals(data["lines"])
    if abs(dr - cr) > 0.009 and not data.get("allow_unbalanced"):
        return jsonify({"error": "voucher not balanced (debit != credit)", "dr": dr, "cr": cr}), 400

    doc = {
        "date": data.get("date"),
        "type": data.get("type", "journal"),
        "no": data.get("no") or "",
        "narration": data.get("narration") or "",
        "lines": data.get("lines"),
        "created_at": datetime.utcnow()
    }
    res = db.vouchers.insert_one(doc)
    doc["_id"] = str(res.inserted_id)
    return jsonify(doc), 201

@app.route("/api/vouchers/<id>", methods=["PUT"])
def update_voucher(id):
    try:
        oid = ObjectId(id)
    except Exception:
        return abort(404)
    data = request.json or {}
    err = validate_voucher_payload(data)
    if err:
        return jsonify({"error": err}), 400
    data["lines"] = auto_allocate_contra(data.get("lines", []), data.get("type"))
    dr, cr = compute_totals(data["lines"])
    if abs(dr - cr) > 0.009 and not data.get("allow_unbalanced"):
        return jsonify({"error": "voucher not balanced (debit != credit)", "dr": dr, "cr": cr}), 400
    db.vouchers.update_one({"_id": oid}, {"$set": {
        "date": data.get("date"),
        "type": data.get("type"),
        "no": data.get("no"),
        "narration": data.get("narration"),
        "lines": data.get("lines"),
        "updated_at": datetime.utcnow()
    }})
    doc = db.vouchers.find_one({"_id": oid})
    doc["_id"] = str(doc["_id"])
    return jsonify(doc)

@app.route("/api/vouchers/<id>", methods=["DELETE"])
def delete_voucher(id):
    try:
        oid = ObjectId(id)
    except Exception:
        return abort(404)
    db.vouchers.delete_one({"_id": oid})
    return jsonify({"ok": True})

# ----------------- Printable voucher -----------------
@app.route("/voucher/print/<id>")
def print_voucher(id):
    # try ObjectId first, fallback to no or string id
    doc = None
    try:
        oid = ObjectId(id)
        doc = db.vouchers.find_one({"_id": oid})
    except Exception:
        pass
    if not doc:
        doc = db.vouchers.find_one({"no": id}) or db.vouchers.find_one({"_id": id})
    if not doc:
        return abort(404)
    # convert _id for template
    doc["_id"] = str(doc["_id"])
    return render_template("voucher_print.html", v=doc)
# --- End of Daybook APIs ---










if __name__ == "__main__":
    app.run(debug=True)

# .\venv\Scripts\activate; python app.py

#     want your Flask app to be accessible on LAN (192.168.x.x).
# # host='0.0.0.0', port=5000, 
# _______ modules and dependencies _______

import os
import sys
import io
import csv
import time
import random
import traceback

from datetime import date, datetime, timedelta
from uuid import uuid4
from functools import wraps
from pprint import pprint
from calendar import calendar

# 🔑 LOAD ENV FIRST
from dotenv import load_dotenv
load_dotenv()

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, send_file, Response, abort, jsonify,
    session, send_from_directory, g
)

from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash

from pymongo import (
    MongoClient, ReturnDocument,
    ASCENDING, DESCENDING
)
from pymongo.errors import DuplicateKeyError
from datetime import datetime, timezone
datetime.now(timezone.utc)

from bson.errors import InvalidId

from num2words import num2words

# ❌ REMOVED MONGO_URI FROM config
from config import UPLOAD_FOLDER, SECRET_KEY, GST_PERCENT
from utils import get_next_sequence, calc_gst
from flask import current_app


# ----------------- APP INIT -----------------
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "change_this_to_a_strong_secret")

# ----------------- UPLOAD FOLDER -----------------
UPLOAD_FOLDER = os.path.join(app.root_path, "static", "uploads")
ALLOWED_EXT = {"png", "jpg", "jpeg", "gif"}
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ----------------- DATABASE CONNECT (MONGODB ATLAS) -----------------
MONGO_URI = os.environ.get("MONGO_URI")

if not MONGO_URI:
    raise Exception("❌ MONGO_URI not found. Check your .env file")

client = MongoClient(MONGO_URI)
db = client["institute_db"]

# 🔍 Test connection (remove later if you want)
try:
    client.admin.command("ping")
    print("✅ MongoDB Atlas Connected Successfully")
except Exception as e:
    print("❌ MongoDB Atlas Connection Error:", e)

# ----------------- COLLECTIONS -----------------
students_col   = db.students
batches_col    = db.batches
courses_col    = db.courses
payments_col   = db.payments
faculties_col  = db.faculties
attendance_col = db.attendance
salaries_col   = db.salaries
users_col      = db.users   # IMPORTANT

# Backwards-compatible aliases
students  = students_col
batches   = batches_col
courses   = courses_col
payments  = payments_col
faculties = faculties_col
teachers_col = faculties_col






# ----------------- HELPERS (NOW db EXISTS) -----------------
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXT

def get_users_col():
    return users_col     # db exists now ✔

def ensure_default_admin():
    users = get_users_col()
    if users.count_documents({}) == 0:
        users.insert_one({
            "username": "admin",
            "name": "Administrator",
            "email": "admin@example.com",
            "phone": "",
            "role": "admin",
            "password_hash": generate_password_hash("admin123"),
            "photo": None,
            "created_on": datetime.utcnow()
        })
        print("Default admin created: username='admin' password='admin123'")

# ----------------- RUN ON STARTUP (db already exists ✔) -----------------
ensure_default_admin()

# ----------------- OTHER HELPERS BELOW -----------------

def generate_registration_no():
    return f"RKM{random.randint(10000, 99999)}"

def get_next_sequence(db, name):
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

def get_next_seq(db, name="institute_db"):
    doc = db.counters.find_one_and_update(
        {"_id": name},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER
    )
    return int(doc["seq"])

def get_next_student_id():
    last = students.find_one({"student_id": {"$exists": True}}, sort=[("student_id", -1)])
    if last:
        return int(last["student_id"]) + 1
    return 1

def month_date_range(year: int, month: int):
    start = datetime(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end = datetime(year, month, last_day, 23, 59, 59)
    return start, end




def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            flash("Please login first.", "warning")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper



# _____________ALL ROUTES_________________


@app.route('/dashboard/years')
def years_dashboard():
    # build 'years' from your DB (group by year)
    return render_template('year_dashboard.html', years=years)






@app.template_filter('num2words')
def num2words_filter(num, lang='en_IN'):
    try:
        return num2words(num, lang=lang)
    except:
        return str(num)


# ---------- Home / Statistics ----------
@app.route('/')
@login_required
def index():
    # basic totals
    batch_count = db.batches.count_documents({})
    student_count = db.students.count_documents({})
    male = db.students.count_documents({"gender": "Male"})
    female = db.students.count_documents({"gender": "Female"})

    # batch-wise gender breakdown (your existing pipeline)
    pipeline = [
        {"$lookup": {"from": "batches", "localField": "batch_id", "foreignField": "_id", "as": "batch"}},
        {"$unwind": {"path": "$batch", "preserveNullAndEmptyArrays": True}},
        {"$group": {
            "_id": "$batch.title",
            "boys": {"$sum": {"$cond": [{"$eq": ["$gender", "Male"]}, 1, 0]}},
            "girls": {"$sum": {"$cond": [{"$eq": ["$gender", "Female"]}, 1, 0]}},
            "total": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    batch_stats = list(db.students.aggregate(pipeline))

    # ---------- Students per Faculty ----------
    # Group students by faculty_id (may be ObjectId or string), attach faculty name if present
    faculty_pipeline = [
        {"$group": {"_id": "$faculty_id", "count": {"$sum": 1}}},
        {"$lookup": {"from": "faculties", "localField": "_id", "foreignField": "_id", "as": "faculty"}},
        {"$unwind": {"path": "$faculty", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "faculty_id": "$_id",
            "faculty_name": {"$ifNull": ["$faculty.name", "(Unassigned)"]},
            "count": 1
        }},
        {"$sort": {"count": -1}}
    ]
    by_faculty = list(db.students.aggregate(faculty_pipeline))

    # ---------- Students per Course ----------
    course_pipeline = [
        {"$group": {"_id": "$course_id", "count": {"$sum": 1}}},
        {"$lookup": {"from": "courses", "localField": "_id", "foreignField": "_id", "as": "course"}},
        {"$unwind": {"path": "$course", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "course_id": "$_id",
            "course_name": {"$ifNull": ["$course.name", "(Unassigned)"]},
            "count": 1
        }},
        {"$sort": {"count": -1}}
    ]
    by_course = list(db.students.aggregate(course_pipeline))

    # Render template with all statistics
    return render_template(
        'index.html',
        batch_count=batch_count,
        student_count=student_count,
        male=male,
        female=female,
        batch_stats=batch_stats,
        by_faculty=by_faculty,
        by_course=by_course
    )





# ---------- Batches ----------
@app.route('/batches')
def batches_list():
    batches = list(db.batches.find().sort("start_date", -1))
    return render_template('batches_list.html', batches=batches)

@app.route('/batch/add', methods=['GET','POST'])
def add_batch():
    if request.method == 'POST':
        title = request.form['title']
        start_date = request.form['start_date']  # expect yyyy-mm-dd
        doc = {
            "title": title,
            "start_date": start_date,
            "created_at": datetime.utcnow()
        }
        db.batches.insert_one(doc)
        flash("Batch added.")
        return redirect(url_for('batches_list'))
    return render_template('batch_form.html')

@app.route('/batch/edit/<bid>', methods=['GET','POST'])
def edit_batch(bid):
    batch = db.batches.find_one({"_id": ObjectId(bid)})
    if request.method == 'POST':
        db.batches.update_one({"_id": ObjectId(bid)},
                              {"$set": {"title": request.form['title'], "start_date": request.form['start_date']}})
        flash("Batch updated.")
        return redirect(url_for('batches_list'))
    return render_template('batch_form.html', batch=batch)

@app.route('/batch/delete/<bid>', methods=['POST'])
def delete_batch(bid):
    db.batches.delete_one({"_id": ObjectId(bid)})
    flash("Batch deleted.")
    return redirect(url_for('batches_list'))




# ---------- Courses ----------
@app.route('/courses')
def courses_list():
    courses = list(db.courses.find().sort("name", 1))
    return render_template('courses_list.html', courses=courses)

@app.route('/course/add', methods=['GET','POST'])
def add_course():
    if request.method == 'POST':
        name = request.form['name']
        fee = float(request.form['fee'] or 0)
        db.courses.insert_one({"name": name, "fee": fee})
        flash("Course added.")
        return redirect(url_for('courses_list'))
    return render_template('course_form.html')

@app.route('/course/edit/<cid>', methods=['GET','POST'])
def edit_course(cid):
    course = db.courses.find_one({"_id": ObjectId(cid)})
    if request.method == 'POST':
        db.courses.update_one({"_id": ObjectId(cid)}, {"$set": {"name": request.form['name'], "fee": float(request.form['fee'])}})
        return redirect(url_for('courses_list'))
    return render_template('course_form.html', course=course)

@app.route('/course/delete/<cid>', methods=['POST'])
def delete_course(cid):
    db.courses.delete_one({"_id": ObjectId(cid)})
    flash("Course deleted.")
    return redirect(url_for('courses_list'))


@app.route('/students')
def students_list():
    q = request.args.get('q','').strip()
    query = {}
    if q:
        query = {"$or":[
            {"first_name":{"$regex": q, "$options":"i"}},
            {"last_name":{"$regex": q, "$options":"i"}},
            {"phone":{"$regex": q}},
            {"form_no":{"$regex": q}},
            {"aadhar":{"$regex": q}}
        ]}

    # fetch students (limited)
    students = list(db.students.find(query).sort("created_at",-1).limit(200))

    # Prefetch lookup maps to avoid N queries
    course_ids = {s.get('course_id') for s in students if s.get('course_id')}
    batch_ids = {s.get('batch_id') for s in students if s.get('batch_id')}
    faculty_ids = {s.get('faculty_id') for s in students if s.get('faculty_id')}

    # convert any ObjectId in sets to ObjectId type for queries (if stored as string)
    def norm_ids(idset):
        out = []
        for i in idset:
            if not i:
                continue
            try:
                out.append(ObjectId(i) if not isinstance(i, ObjectId) else i)
            except Exception:
                # skip invalid ids (they might be actual string names)
                pass
        return out

    course_map = {}
    if course_ids:
        rows = db.courses.find({"_id": {"$in": norm_ids(course_ids)}})
        for r in rows:
            course_map[str(r['_id'])] = r.get('name') or r.get('title') or ''

    batch_map = {}
    if batch_ids:
        rows = db.batches.find({"_id": {"$in": norm_ids(batch_ids)}})
        for r in rows:
            batch_map[str(r['_id'])] = r.get('name') or r.get('title') or ''

    faculty_map = {}
    if faculty_ids:
        rows = db.faculties.find({"_id": {"$in": norm_ids(faculty_ids)}})
        for r in rows:
            faculty_map[str(r['_id'])] = r.get('name') or r.get('title') or ''

    # Enrich students for template (and normalise field names)
    enriched = []
    for s in students:
        st = dict(s)  # copy so we don't modify original
        # id as string for URLs
        st['_id'] = str(st.get('_id'))

        # normalize common keys (template uses parent_phone / aadhaar)
        # DB might have 'parents_phone' or 'parent_phone'
        if not st.get('parent_phone') and st.get('parents_phone'):
            st['parent_phone'] = st.get('parents_phone')
        # aadhar vs aadhaar
        if not st.get('aadhaar') and st.get('aadhar'):
            st['aadhaar'] = st.get('aadhar')

        # course name
        cid = st.get('course_id')
        if cid:
            cid_s = str(cid) if not isinstance(cid, str) else cid
            st['course_name'] = course_map.get(cid_s, '')
        else:
            st['course_name'] = st.get('course_name','')  # maybe already present

        # batch title
        bid = st.get('batch_id')
        if bid:
            bid_s = str(bid) if not isinstance(bid, str) else bid
            st['batch'] = batch_map.get(bid_s, '')
        else:
            st['batch'] = st.get('batch','')

        # faculty resolution: prefer explicit faculty text, else lookup faculty_id
        faculty_text = st.get('faculty','') or ''
        if not faculty_text and st.get('faculty_id'):
            fid_s = str(st.get('faculty_id'))
            faculty_text = faculty_map.get(fid_s, '')
        st['faculty'] = faculty_text

        # ensure timing field present (avoid KeyError in template)
        st['timing'] = st.get('timing','')

        # ensure form_no present
        st['form_no'] = st.get('form_no','')

        enriched.append(st)

    # pass lists for filters too (if template uses them)
    courses = list(db.courses.find().sort("name", 1))
    batches = list(db.batches.find().sort("start_date", -1))
    faculties = list(db.faculties.find())

    return render_template('students_list.html',
                           students=enriched,
                           batches=batches,
                           courses=courses,
                           faculties=faculties,
                           q=q)



@app.route('/student/add', methods=['GET','POST'])
def add_student():
    batches = list(db.batches.find())
    courses = list(db.courses.find())
    faculties = list(db.faculties.find()) if 'faculties' in db.list_collection_names() else []

    if request.method == 'POST':
        # --- TAKE form_no FROM USER (manual entry) ---
        form_no = request.form.get('form_no', '').strip()

        # Validate presence
        if not form_no:
            flash("Please enter Form No (manual entry required).", "danger")
            return redirect(url_for('add_student'))

        # Validate uniqueness
        if db.students.find_one({"form_no": form_no}):
            flash("Form No already exists. Please use a different Form No.", "danger")
            return redirect(url_for('add_student'))

        # Basic fields
        data = {
            "first_name": request.form.get('first_name','').strip(),
            "father_name": request.form.get('father_name','').strip(),
            "last_name": request.form.get('last_name','').strip(),
            "dob": request.form.get('dob',''),
            "address": request.form.get('address','').strip(),
            "phone": request.form.get('phone','').strip(),
            "parents_phone": request.form.get('parents_phone','').strip(),
            "aadhar": request.form.get('aadhar','').strip(),
            "email": request.form.get('email','').strip(),
            "gender": request.form.get('gender',''),
            "registration_no": generate_registration_no(),
            "qualification": request.form.get('qualification',''),
            "timing": request.form.get('timing',''),
            "admission_date": request.form.get('admission_date',''),
            "payment_status": request.form.get('payment_status','paying'),
            "reference": request.form.get('reference',''),
            "form_no": form_no,  # <- use manual value only
            "blood_group": request.form.get('blood_group',''),
            "created_at": datetime.utcnow()
        }

        # batch_id (try to convert to ObjectId; else store None)
        if request.form.get('batch_id'):
            try:
                data['batch_id'] = ObjectId(request.form.get('batch_id'))
            except Exception:
                data['batch_id'] = None

        # course_id (try to convert to ObjectId; else store None)
        if request.form.get('course_id'):
            try:
                data['course_id'] = ObjectId(request.form.get('course_id'))
            except Exception:
                data['course_id'] = None

        # Faculty handling
        faculty_name = request.form.get('faculty','').strip()
        faculty_id_raw = request.form.get('faculty_id')
        if faculty_id_raw:
            try:
                fid = ObjectId(faculty_id_raw)
                data['faculty_id'] = fid
                fdoc = db.faculties.find_one({"_id": fid})
                if fdoc and fdoc.get('name'):
                    faculty_name = fdoc['name']
            except Exception:
                pass
        data['faculty'] = faculty_name

        # handle photo BEFORE inserting (so filename saved in document)
        photo = request.files.get('photo')
        if photo and photo.filename:
            fname = secure_filename(photo.filename)
            # make filename unique to avoid collisions
            unique_fname = f"{int(time.time())}_{uuid4().hex}_{fname}"
            path = os.path.join(app.config['UPLOAD_FOLDER'], unique_fname)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            photo.save(path)
            data['photo'] = unique_fname

        # generate student_id (sequence)
        data['student_id'] = get_next_seq(db, "student_id")

        # Defensive: ensure we don't accidentally try to insert a pre-existing _id
        data.pop('_id', None)

        # Try insert once, handle duplicate key errors gracefully
        try:
            res = db.students.insert_one(data)
        except DuplicateKeyError as e:
            # log the error if you have logging
            # app.logger.exception("DuplicateKeyError while inserting student")
            flash("A student with the same unique key already exists. Please check and try again.", "danger")
            return redirect(url_for('add_student'))
        except Exception as e:
            # generic fallback
            # app.logger.exception("Error inserting student")
            flash("An unexpected error occurred while registering the student.", "danger")
            return redirect(url_for('add_student'))

        flash("Student registered.", "success")
        return redirect(url_for('students_list'))

    # GET: render form
    return render_template('student_form.html', batches=batches, courses=courses, faculties=faculties)



@app.route('/student/delete/<sid>', methods=['POST'])
def delete_student(sid):
    db.students.delete_one({"_id": ObjectId(sid)})
    flash("Student removed.")
    return redirect(url_for('students_list'))




@app.route('/receipt/<receipt_no>')
def print_receipt(receipt_no):
    payment = db.payments.find_one({"receipt_no": receipt_no})
    if not payment:
        flash("Receipt not found.")
        return redirect(url_for('payments_list'))
    return render_template('receipt.html', payment=payment)


    # Try to resolve the student document from the payment.student_id (ObjectId)
    student = None
    sid = payment.get("student_id")
    if sid:
        try:
            # if sid is string, convert; if already ObjectId, this is fine
            try:
                sid_obj = ObjectId(sid) if not isinstance(sid, ObjectId) else sid
            except Exception:
                sid_obj = sid if isinstance(sid, ObjectId) else None

            if sid_obj:
                student = db.students.find_one({"_id": sid_obj})
        except Exception:
            student = None

    # If you stored numeric id in payment earlier (recommended), try that too:
    if not student and payment.get("student_numeric_id"):
        student = db.students.find_one({"student_id": int(payment["student_numeric_id"])})

    return render_template('receipt.html', payment=payment, student=student)



@app.route('/reports/payment', methods=['GET', 'POST'])
def payment_report():
    # request.values merges args (GET) and form (POST) — convenient for both methods
    get = request.values.get

    from_date = get("from_date")
    to_date = get("to_date")
    from_receipt = get("from_receipt")
    to_receipt = get("to_receipt")
    course = get("course")
    old_new = get("old_new")     # param name from client: old_new
    faculty = get("faculty")
    submit_date = get("submit_date")

    q = {}

    # Date filter: convert to datetimes if possible
    # Expecting dates in 'YYYY-MM-DD' format (adjust format if different)
    try:
        if from_date:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        else:
            from_dt = None
        if to_date:
            # include end of day
            to_dt = datetime.strptime(to_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        else:
            to_dt = None

        if from_dt and to_dt:
            q["date"] = {"$gte": from_dt, "$lte": to_dt}
        elif from_dt:
            q["date"] = {"$gte": from_dt}
        elif to_dt:
            q["date"] = {"$lte": to_dt}
    except ValueError:
        # If dates are not in expected format, fallback to string-based query (less ideal)
        if from_date and to_date:
            q["date"] = {"$gte": from_date + " 00:00:00", "$lte": to_date + " 23:59:59"}

    # Submit date filter — if stored as string, regex is fine; if stored as datetime you need to convert similarly
    if submit_date:
        q["created_at"] = {"$regex": submit_date}

    # Receipt number range — parse safely
    try:
        if from_receipt and to_receipt:
            q["receipt_no"] = {"$gte": int(from_receipt), "$lte": int(to_receipt)}
        elif from_receipt:
            q["receipt_no"] = {"$gte": int(from_receipt)}
        elif to_receipt:
            q["receipt_no"] = {"$lte": int(to_receipt)}
    except ValueError:
        # ignore invalid ints (or add a flash message if you want)
        pass

    # Course filter
    if course and course != "All":
        q["course"] = course

    # NOTE: check your DB field name for new/old — below I put it into the same key name
    if old_new and old_new != "All":
        q["old_old"] = old_new   # <-- if your DB uses 'new_old', change this key accordingly

    # Faculty filter
    if faculty and faculty != "All":
        q["faculty"] = faculty

    # Debug print — useful while developing
    print("Payment report query:", q, "method:", request.method)

    payments_cursor = payments.find(q).sort("date", -1)
    payment_list = list(payments_cursor)

    # Build safe lists for the template (do not shadow collection names)
    course_list = []
    for c in courses.find():
        # handle dict-like and attribute-like documents safely
        name = c.get("name") if isinstance(c, dict) else getattr(c, "name", None)
        if not name:
            name = c.get("title") if isinstance(c, dict) else getattr(c, "title", None)
        if name:
            course_list.append(str(name).strip())

    faculty_list = []
    for f in faculties.find():
        fname = f.get("name") if isinstance(f, dict) else getattr(f, "name", None)
        if fname:
            faculty_list.append(str(fname).strip())

    # -------------------------
    # COMPUTE TOTAL AMOUNT
    # -------------------------
    total_amount = 0
    for p in payment_list:
        amt = p.get('total') or p.get('amount') or 0
        try:
            total_amount += float(amt)
        except Exception:
            # ignore bad values
            pass

    # -------------------------
    # FIRST & LAST RECEIPT NO (prefer numeric min/max)
    # -------------------------
    if payment_list:
        numeric_receipts = []
        for p in payment_list:
            r = p.get('receipt_no')
            if r is None:
                continue
            try:
                numeric_receipts.append(int(r))
            except Exception:
                # not numeric, skip here
                pass

        if numeric_receipts:
            first_receipt_no = min(numeric_receipts)
            last_receipt_no = max(numeric_receipts)
        else:
            # fallback to first/last in the returned list (string values or unsortable)
            first_receipt_no = payment_list[0].get('receipt_no', '')
            last_receipt_no = payment_list[-1].get('receipt_no', '')
    else:
        first_receipt_no = ''
        last_receipt_no = ''

    # -------------------------
    # RENDER TEMPLATE
    # -------------------------
    return render_template(
        "reports_payment.html",
        payments=payment_list,
        course_list=course_list,
        faculty_list=faculty_list,
        total_amount=total_amount,
        first_receipt_no=first_receipt_no,
        last_receipt_no=last_receipt_no
    )




# @app.route('/reports/students')
# def student_report():
#     students = list(db.students.find().sort("created_at",-1))


#     return render_template('student_report.html', students=students)


@app.route('/reports/students')
def student_report():
    students = list(db.students.find().sort("created_at", -1))

    # Collect unique batch_ids & course_ids
    batch_ids = {s.get('batch_id') for s in students if s.get('batch_id')}
    course_ids = {s.get('course_id') for s in students if s.get('course_id')}

    # Fetch batch docs in one query
    batches = {str(b['_id']): b for b in db.batches.find({
        "_id": {"$in": list(batch_ids)}
    })}

    # Fetch course docs in one query
    courses = {str(c['_id']): c for c in db.courses.find({
        "_id": {"$in": list(course_ids)}
    })}

    # Process each student
    for s in students:

        # ------------------------------
        # 1️⃣ Attach batch details
        # ------------------------------
        if s.get("batch_id"):
            s["batch"] = batches.get(str(s["batch_id"]))

        # ------------------------------
        # 2️⃣ Attach course details
        # ------------------------------
        if s.get("course_id"):
            s["course"] = courses.get(str(s["course_id"]))

        # ------------------------------
        # 3️⃣ Compute Expiry Date
        # ------------------------------
        if not s.get("expiry_date") and s.get("admission_date"):
            try:
                ad = datetime.strptime(s["admission_date"], "%Y-%m-%d")
                # Example: course contains duration_months
                months = int(s["course"].get("duration_months", 0)) if s.get("course") else 0
                expiry = ad + timedelta(days=months * 30)
                s["expiry_date"] = expiry.strftime("%Y-%m-%d")
            except:
                s["expiry_date"] = ""

        # ------------------------------
        # 4️⃣ Compute Balance
        # ------------------------------
        total_paid = 0
        for p in db.payments.find({"student_id": s["_id"]}):
            total_paid += float(p.get("amount", 0))

        course_fee = 0

        # Prefer student fee field, else course fee
        if "fee" in s:
            course_fee = float(s.get("fee", 0))
        elif s.get("course"):
            course_fee = float(s["course"].get("fee", 0))

        s["balance"] = course_fee - total_paid

    return render_template("student_report.html", students=students)



@app.route('/reports/genderwise')
def genderwise_report():
    pipeline = [
        {"$lookup": {"from":"batches","localField":"batch_id","foreignField":"_id","as":"batch"}},
        {"$unwind":{"path":"$batch","preserveNullAndEmptyArrays":True}},
        {"$group":{"_id":"$batch.title","boys":{"$sum":{"$cond":[{"$eq":["$gender","Male"]},1,0]}},
                                   "girls":{"$sum":{"$cond":[{"$eq":["$gender","Female"]},1,0]}},
                                   "total":{"$sum":1}}}
    ]

    results = list(db.students.aggregate(pipeline))
    return render_template('genderwise_report.html', results=results)

# ---------- Payment summary endpoints ----------
@app.route('/summary/today')
def summary_today():
    start = datetime.utcnow().replace(hour=0,minute=0,second=0,microsecond=0)
    end = start + timedelta(days=1)
    total = db.payments.aggregate([{"$match":{"date":{"$gte":start,"$lt":end}}},{"$group":{"_id":None,"sum":{"$sum":"$total"}}}])
    total = list(total)
    total_amount = total[0]['sum'] if total else 0
    return {"date": str(start.date()), "collection": total_amount}







# /payments — shows all students + balance + quick actions.

# /payment/add/<student_id> — payment form with installment history and pay action.

# /payment/details/<student_id> — view installments for a student.

# /receipt/<receipt_no> — print receipt (if you already have this, keep it).

# --- Payments list: show all students with balances & actions ---
@app.route('/payments')
def payments_list():
    q = request.args.get('q','').strip()
    # fetch students (optionally filter by q)
    query = {}
    if q:
        query = {"$or":[
            {"first_name":{"$regex":q,"$options":"i"}},
            {"last_name":{"$regex":q,"$options":"i"}},
            {"phone":{"$regex":q}},
            {"form_no":{"$regex":q}},
            {"aadhar":{"$regex":q}}
        ]}
    students = list(db.students.find(query).sort("created_at",-1))

    # enrich students with course info and balance & last payment date
    enriched = []
    for s in students:
        student = s.copy()
        # get course
        course = None
        if student.get("course_id"):
            try:
                course = db.courses.find_one({"_id": ObjectId(student["course_id"])})
            except:
                course = db.courses.find_one({"_id": student["course_id"]})
        # course fee
        fee = course.get("fee",0) if course else 0

        # sum of payments made (total field in payments)
        paid_agg = db.payments.aggregate([
            {"$match": {"student_id": student.get("_id")}},
            {"$group": {"_id": None, "sumPaid": {"$sum": "$amount"}}}
        ])
        paid_list = list(paid_agg)
        paid = paid_list[0]['sumPaid'] if paid_list else 0.0

        # compute balance = fee - paid (if you store balance separately you can use it instead)
        balance = fee - paid

        # get last payment and number of installments
        last_pay = db.payments.find_one({"student_id": student.get("_id")}, sort=[("date",-1)])
        installments_count = db.payments.count_documents({"student_id": student.get("_id")})

        student['course_name'] = course.get("name") if course else ""
        student['course_fee'] = fee
        student['paid'] = paid
        student['balance'] = balance
        student['last_payment'] = last_pay['date'] if last_pay else None
        student['installments'] = installments_count

        enriched.append(student)

    return render_template('payments_list.html', payments=[], students=enriched, q=q)


#  _______faculty_routes____
@app.route('/faculty')
def faculty_list():
    data = list(faculties.find())
    return render_template('faculty_list.html', faculties=data)

@app.route('/faculty/add', methods=['GET', 'POST'])
def faculty_form():
    if request.method == 'POST':
        doc = {
            "name": request.form.get("name"),
            "phone": request.form.get("phone"),
            "email": request.form.get("email"),
            "subject": request.form.get("subject"),
            "address": request.form.get("address")
        }
        faculties.insert_one(doc)
        flash("Faculty added successfully!")
        return redirect(url_for('faculty_list'))
    return render_template('faculty_form.html', faculty=None)

@app.route('/faculty/edit/<id>', methods=['GET', 'POST'])
def edit_faculty(id):
    faculty = faculties.find_one({"_id": ObjectId(id)})
    if not faculty:
        flash("Faculty not found.")
        return redirect(url_for('faculty_list'))
    if request.method == 'POST':
        faculties.update_one({"_id": ObjectId(id)}, {"$set": {
            "name": request.form.get("name"),
            "phone": request.form.get("phone"),
            "email": request.form.get("email"),
            "subject": request.form.get("subject"),
            "address": request.form.get("address")
        }})
        flash("Faculty updated successfully!")
        return redirect(url_for('faculty_list'))
    return render_template('faculty_form.html', faculty=faculty)

@app.route('/faculty/delete/<id>')
def delete_faculty(id):
    faculties.delete_one({"_id": ObjectId(id)})
    flash("Faculty deleted successfully.")
    return redirect(url_for('faculty_list'))


@app.route('/payment/add/<student_id>', methods=['GET','POST'])
def add_payment(student_id):
    # fetch student
    student = db.students.find_one({"_id": ObjectId(student_id)})
    if not student:
        flash("Student not found.")
        return redirect(url_for('payments_list'))

    # Collect course IDs from student (supports both legacy single field and new list field)
    raw_course_ids = []
    if student.get("course_ids"):            # preferred new field (list)
        raw_course_ids = student["course_ids"]
    elif student.get("course_id"):           # legacy single field
        raw_course_ids = [student["course_id"]]

    # Normalize and convert to ObjectId where possible
    course_obj_ids = []
    for cid in raw_course_ids:
        try:
            course_obj_ids.append(ObjectId(cid))
        except Exception:
            # If it's already an ObjectId, append it; otherwise skip invalid ids
            if isinstance(cid, ObjectId):
                course_obj_ids.append(cid)

    # Fetch course documents (projection - only what you need)
    courses = []
    if course_obj_ids:
        courses = list(db.courses.find({"_id": {"$in": course_obj_ids}}, {"name": 1, "duration": 1}).sort("name", 1))

    # If a student had only one course, set default selected course (None otherwise)
    default_course = None
    if courses:
        default_course = courses[0]

    # fetch installment history (all payments for this student)
    history = list(db.payments.find({"student_id": student["_id"]}).sort("date", -1))

    if request.method == 'POST':
        # Parse form fields
        amount = float(request.form.get('amount', 0) or 0)
        payment_mode = request.form.get('payment_mode','cash')
        installment_label = request.form.get('installment','full')
        faculty = request.form.get('faculty', student.get('faculty', ''))
        remarks = request.form.get('remarks','')

        # Course selected in the form (string)
        selected_course_id = request.form.get('course_id') or (str(default_course['_id']) if default_course else None)

        # Resolve selected course name (prefer from fetched courses, otherwise fetch from DB)
        course_name = ""
        course_id_for_doc = None
        if selected_course_id:
            course_id_for_doc = selected_course_id
            # try to find in previously fetched courses
            found = next((c for c in courses if str(c['_id']) == selected_course_id), None)
            if found:
                course_name = found.get("name", "")
            else:
                # fallback: fetch single course doc (handles case when course wasn't in course_obj_ids)
                try:
                    cdoc = db.courses.find_one({"_id": ObjectId(selected_course_id)}, {"name":1})
                except Exception:
                    cdoc = db.courses.find_one({"_id": selected_course_id}, {"name":1})
                if cdoc:
                    course_name = cdoc.get("name", "")

        # calculate gst & total (use your existing calc_gst function)
        gst, total = calc_gst(amount, GST_PERCENT)

        # receipt number from counters
        receipt_seq = get_next_sequence(db, "receipt_no")
        receipt_no = str(receipt_seq).zfill(6)

        pay_doc = {
            "student_id": student["_id"],
            "student_name": f"{student.get('first_name','')} {student.get('last_name','')}".strip(),
            "course_id": course_id_for_doc,
            "course_name": course_name,
            "date": datetime.utcnow(),
            "amount": amount,        # base amount
            "gst": gst,
            "total": total,          # amount + gst
            "faculty": faculty,
            "payment_mode": payment_mode,
            "installment": installment_label,
            "receipt_no": receipt_no,
            "remarks": remarks,
            "phone": student.get("phone"),
            "gender": student.get("gender"),
        }

        db.payments.insert_one(pay_doc)

        flash(f"Payment recorded. Receipt No: {receipt_no}")
        return redirect(url_for('print_receipt', receipt_no=receipt_no))
    

    

    # GET -> show form
    # pass `courses` (list) to template so user can choose which course payment is for
    return render_template('payment_form.html', student=student, courses=courses, history=history, gst_percent=GST_PERCENT)


# --- View payment/installment history for a student ---
@app.route('/payment/details/<student_id>')
def payment_details(student_id):
    student = db.students.find_one({"_id": ObjectId(student_id)})
    if not student:
        flash("Student not found.")
        return redirect(url_for('payments_list'))
    history = list(db.payments.find({"student_id": student["_id"]}).sort("date", -1))
    return render_template('payment_details.html', student=student, history=history)



@app.route('/student/edit/<sid>', methods=['GET','POST'])
def edit_student(sid):
    # try treat sid as ObjectId, fallback to form_no (string)
    student = None
    try:
        student = db.students.find_one({"_id": ObjectId(sid)})
    except (InvalidId, TypeError):
        # fallback: maybe user passed a form_no
        student = db.students.find_one({"form_no": sid})

    if not student:
        flash("Student not found.")
        return redirect(url_for('students_list'))

    # convert student ids to strings for template comparison
    student['_id'] = str(student['_id'])
    if student.get('batch_id'):
        try:
            student['batch_id'] = str(student['batch_id'])
        except Exception:
            student['batch_id'] = student.get('batch_id')
    if student.get('course_id'):
        try:
            student['course_id'] = str(student['course_id'])
        except Exception:
            student['course_id'] = student.get('course_id')
    if student.get('faculty_id'):
        try:
            student['faculty_id'] = str(student['faculty_id'])
        except Exception:
            student['faculty_id'] = student.get('faculty_id')

    # load lists and convert their ids to strings for template
    batches = list(db.batches.find())
    courses = list(db.courses.find())
    faculties = list(db.faculties.find())

    for b in batches:
        b['_id'] = str(b['_id'])
    for c in courses:
        c['_id'] = str(c['_id'])
    for f in faculties:
        f['_id'] = str(f['_id'])

    if request.method == 'POST':
        # Collect basic fields
        update = {k: request.form.get(k, '').strip() for k in [
            'first_name','father_name','last_name','dob','address','email','phone',
            'parents_phone','aadhar','gender','qualification','timing',
            'admission_date','payment_status','reference','form_no',
            'blood_group'
        ]}

        # handle batch/course (store as ObjectId if provided, else unset)
        if request.form.get('batch_id'):
            try:
                update['batch_id'] = ObjectId(request.form['batch_id'])
            except InvalidId:
                update['batch_id'] = None
        else:
            update['batch_id'] = None

        if request.form.get('course_id'):
            try:
                update['course_id'] = ObjectId(request.form['course_id'])
            except InvalidId:
                update['course_id'] = None
        else:
            update['course_id'] = None

        # handle faculty: either faculty_id (select) or free-text faculty
        faculty_id = request.form.get('faculty_id')
        faculty_text = request.form.get('faculty', '').strip()
        if faculty_id:
            try:
                oid = ObjectId(faculty_id)
                update['faculty_id'] = oid
                # look up faculty name and store it too for easy display
                fac = db.faculties.find_one({'_id': oid})
                update['faculty'] = fac.get('name') if fac else faculty_text or None
            except (InvalidId, TypeError):
                # invalid id -> fallback to text
                update['faculty_id'] = None
                update['faculty'] = faculty_text or None
        else:
            # no select chosen; use free-text or clear
            update['faculty_id'] = None
            update['faculty'] = faculty_text or None

        # handle photo upload
        photo = request.files.get('photo')
        if photo and getattr(photo, 'filename', None):
            fname = secure_filename(photo.filename)
            upload_folder = app.config.get('UPLOAD_FOLDER') or os.path.join(app.root_path, 'static', 'uploads')
            os.makedirs(upload_folder, exist_ok=True)
            path = os.path.join(upload_folder, fname)
            photo.save(path)
            update['photo'] = fname

        # update DB (use ObjectId for the selector if possible)
        try:
            selector = {"_id": ObjectId(sid)}
        except Exception:
            # sid might be a form_no; find real _id
            doc = db.students.find_one({"form_no": sid})
            selector = {"_id": doc["_id"]} if doc else {"form_no": sid}

        db.students.update_one(selector, {"$set": update})
        flash("Student updated.")
        return redirect(url_for('students_list'))

    # final: render template (pass faculties so select shows)
    return render_template('student_form.html',
                           student=student,
                           batches=batches,
                           courses=courses,
                           faculties=faculties)




# ---------- Helper utilities ----------
def iso_today():
    return date.today().isoformat()

def parse_date(s):
    # expected yyyy-mm-dd
    if not s:
        return iso_today()
    return s

# ---------- Sample seeding route (optional) ----------
@app.route('/seed')
def seed():
    """Seed some example batches and students (run once)."""
    # Only seed if empty to avoid duplicates
    if batches_col.count_documents({}) == 0:
        b1 = batches_col.insert_one({"name": "Batch A"}).inserted_id
        b2 = batches_col.insert_one({"name": "Batch B"}).inserted_id

        students_col.insert_many([
            {"first_name": "Amit", "last_name": "Sharma", "phone": "9876500001", "form_no": "A001", "photo": None, "batch_id": b1},
            {"first_name": "Rina", "last_name": "Kumar", "phone": "9876500002", "form_no": "A002", "photo": None, "batch_id": b1},
            {"first_name": "Sandeep", "last_name": "Das", "phone": "9876500003", "form_no": "B001", "photo": None, "batch_id": b2},
        ])
        return "Seeded sample data"
    return "Already seeded"

# ---------- Attendance page (renders your template) ----------
# ---------- Attendance routes (replace the old block with this) ----------


@app.route('/attendance')
def attendance():
    """
    Renders the attendance register page.
    Query params:
      - date: yyyy-mm-dd (optional)
      - batch: batch id (optional, string)
    """
    q_date = parse_date(request.args.get('date'))
    selected_batch = request.args.get('batch')  # string or None

    # Load batches and convert _id to string for template comparison
    raw_batches = list(batches_col.find({}).sort("start_date", -1))
    batches = []
    for b in raw_batches:
        doc = dict(b)  # copy so we don't mutate DB doc directly
        doc['_id'] = str(b.get('_id'))
        doc['display_name'] = b.get('title') or b.get('name') or doc['_id']
        batches.append(doc)

    # If a batch selected, load its students (students.batch_id stored as ObjectId)
    students = []
    if selected_batch:
        try:
            bid = ObjectId(selected_batch)
        except Exception:
            bid = None
        if bid:
            cursor = students_col.find({"batch_id": bid}).sort([("first_name", 1), ("last_name", 1)])
            for s in cursor:
                s["_id"] = str(s["_id"])         # string id for template form fields
                s["photo"] = s.get("photo")
                students.append(s)

    # Preload existing attendance for the date+batch to pre-select buttons
    # NOTE: we store attendance.batch_id as string in save_attendance, so query with the string
    attendance_map = {}
    if selected_batch:
        docs = attendance_col.find({"date": q_date, "batch_id": selected_batch})
        for d in docs:
            attendance_map[d["student_id"]] = d.get("status", "absent")

    # Attach status to students
    for s in students:
        sid = s["_id"]
        s["status"] = attendance_map.get(sid, "absent")

    return render_template("attendance_register.html",
                           batches=batches,
                           students=students,
                           today=q_date,
                           selected_batch=selected_batch)


@app.route('/attendance/save', methods=['POST'])
def save_attendance():
    """
    Expects form fields:
      - date (hidden_date or date)
      - batch_id (hidden_batch or batch_id)
      - for each student: status_<student_id> (value: present/absent/leave)
    """
    form = request.form
    attend_date = parse_date(form.get("date") or form.get("hidden_date"))
    batch_id = form.get("batch_id") or form.get("hidden_batch")
    if not attend_date or not batch_id:
        return "Missing date or batch", 400

    # Validate batch id for student lookup (students store batch_id as ObjectId)
    try:
        bid_obj = ObjectId(batch_id)
    except Exception:
        return "Invalid batch id", 400

    students = list(students_col.find({"batch_id": bid_obj}))
    now = datetime.utcnow()

    # Save or update each student's attendance record for that date+batch
    # note: attendance documents keep batch_id as the string form for easy URL queries
    for s in students:
        sid = str(s["_id"])
        key = f"status_{sid}"
        status = form.get(key, "absent")
        attendance_col.update_one(
            {"date": attend_date, "batch_id": batch_id, "student_id": sid},
            {"$set": {
                "status": status,
                "updated_at": now,
                "batch_id": batch_id,
                "student_id": sid,
                "date": attend_date
            }},
            upsert=True
        )

    return redirect(url_for('attendance', date=attend_date, batch=batch_id))


@app.route('/attendance/export_csv')
def attendance_export_csv():
    """
    Returns a CSV for given date and optional batch query params:
      - date=yyyy-mm-dd
      - batch=<batch id>
    """
    q_date = parse_date(request.args.get('date'))
    batch_id = request.args.get('batch')

    query = {"date": q_date}
    if batch_id:
        query["batch_id"] = batch_id

    docs = list(attendance_col.find(query))
    status_map = {d["student_id"]: d["status"] for d in docs}

    # If batch specified, fetch students for that batch (students use ObjectId)
    students = []
    if batch_id:
        try:
            students = list(students_col.find({"batch_id": ObjectId(batch_id)}).sort([("first_name", 1)]))
        except Exception:
            return abort(400, "Invalid batch id")
    else:
        # all students referenced in attendance
        student_ids = list(status_map.keys())
        oid_list = []
        for sid in student_ids:
            try:
                oid_list.append(ObjectId(sid))
            except Exception:
                pass
        students = list(students_col.find({"_id": {"$in": oid_list}}))

    # Build CSV
    si = io.StringIO()
    writer = csv.writer(si)
    writer.writerow(["Sr", "Student Name", "Phone", "Admission No", "Status"])

    for i, s in enumerate(students, start=1):
        sid = str(s["_id"])
        name = f"{s.get('first_name','')} {s.get('last_name','')}".strip()
        phone = s.get('phone', '')
        form_no = s.get('form_no', '')
        status = status_map.get(sid, "absent")
        writer.writerow([i, name, phone, form_no, status])

    si.seek(0)
    mem = io.BytesIO()
    mem.write(si.getvalue().encode('utf-8'))
    mem.seek(0)
    filename = f"attendance_{q_date}.csv"
    return send_file(mem, as_attachment=True, download_name=filename, mimetype='text/csv')


@app.route('/api/batch/<batch_id>/students')
def api_students(batch_id):
    try:
        bid = ObjectId(batch_id)
    except Exception:
        return jsonify([])

    students = list(students_col.find({"batch_id": bid}))
    out = []
    for s in students:
        out.append({
            "_id": str(s["_id"]),
            "first_name": s.get("first_name"),
            "last_name": s.get("last_name"),
            "phone": s.get("phone"),
            "form_no": s.get("form_no"),
            "photo": s.get("photo")
        })
    return jsonify(out)


@app.route('/attendance/history')
def attendance_history():
    """
    Show dates & batches that have attendance records.
    Query params: date, batch (both optional)
    """
    q_date = request.args.get('date')
    q_batch = request.args.get('batch')

    q = {}
    if q_date:
        q['date'] = q_date
    if q_batch:
        q['batch_id'] = q_batch

    pipeline = [
        {"$match": q},
        {"$group": {"_id": {"date": "$date", "batch_id": "$batch_id"}, "count": {"$sum": 1}}},
        {"$sort": {"_id.date": -1}}
    ]
    groups = list(attendance_col.aggregate(pipeline))

    # map batch id (string) -> batch title
    batches_map = {}
    for b in batches_col.find({}):
        batches_map[str(b["_id"])] = b.get("title") or b.get("name") or str(b.get("_id"))

    return render_template('attendance_history.html',
                           groups=groups,
                           batches_map=batches_map,
                           filter_date=q_date,
                           filter_batch=q_batch)


@app.route('/attendance/view')
def attendance_view():
    """
    Show attendance rows for specified date and batch. Required query params: date, batch
    """
    q_date = request.args.get('date')
    q_batch = request.args.get('batch')
    if not q_date or not q_batch:
        flash("Provide both date and batch to view attendance.", "warning")
        return redirect(url_for('attendance_history'))

    docs = list(attendance_col.find({"date": q_date, "batch_id": q_batch}))
    student_ids = [d['student_id'] for d in docs]

    student_map = {}
    if student_ids:
        try:
            oid_list = [ObjectId(sid) for sid in student_ids]
            for s in students_col.find({"_id": {"$in": oid_list}}):
                student_map[str(s["_id"])] = s
        except Exception:
            student_map = {}

    rows = []
    for d in docs:
        sid = d['student_id']
        s = student_map.get(sid)
        rows.append({"student_id": sid, "status": d.get("status", "absent"), "student": s})

    rows.sort(key=lambda r: ((r['student'] or {}).get('first_name',''), (r['student'] or {}).get('last_name','')))

    try:
        batch_doc = batches_col.find_one({"_id": ObjectId(q_batch)})
        batch_title = batch_doc.get('name') or batch_doc.get('title') if batch_doc else q_batch
    except Exception:
        batch_title = q_batch

    return render_template('attendance_view.html',
                           date=q_date,
                           batch_id=q_batch,
                           batch_title=batch_title,
                           rows=rows)








def main():
    # find highest existing student_id (if any)
    max_doc = students.find_one(
        {"student_id": {"$exists": True}},
        sort=[("student_id", -1)]
    )
    start = int(max_doc["student_id"]) + 1 if max_doc else 1

    # find docs without student_id
    cursor = students.find({"student_id": {"$exists": False}}).sort("_id", ASCENDING)
    count = 0
    for doc in cursor:
        new_id = start + count
        students.update_one({"_id": doc["_id"]}, {"$set": {"student_id": new_id}})
        print(f"Assigned student_id {new_id} to {doc['_id']}")
        count += 1

    print(f"Done. Assigned student_id to {count} students. Next student_id: {start + count}")



# helper: convert year,month -> start_dt, end_dt (inclusive)
def month_date_range(year: int, month: int):
    start = datetime(year, month, 1)
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    # end is just before next_month
    end = next_month - timedelta(microseconds=1)
    return start, end

# helper: pick collection from globals() or fallback to db collection name
def pick_collection(*names, fallback_name=None):
    for n in names:
        val = globals().get(n)
        if val is not None:
            return val
    if fallback_name and globals().get("db") is not None:
        try:
            return globals()["db"].get_collection(fallback_name)
        except Exception:
            return None
    return None


# --------- Salary routes (paste/replace in app.py) ----------
from flask import (
    render_template, request, jsonify, flash, redirect, url_for, current_app
)
import traceback

# --- small helper: pick_collection (keeps your previous behavior if already defined) ---
def pick_collection(*possible_names, fallback_name=None):
    """
    Try to return the first defined collection object from globals() by the given names.
    If none found, try to get db[fallback_name] if db exists and fallback_name provided.
    """
    db_obj = globals().get("db")
    # first try explicitly provided names as variables in globals
    for nm in possible_names:
        if nm and nm in globals() and globals()[nm] is not None:
            return globals()[nm]
    # fallback to db.<collection> by fallback_name
    if db_obj is not None and fallback_name:
        try:
            return db_obj[fallback_name]
        except Exception:
            return None
    return None

# --- helper: produce start/end datetimes for a given month (inclusive) ---
def month_date_range(year: int, month: int):
    """
    Return (start_dt, end_dt) datetime objects covering the whole month in UTC naive datetimes.
    start is start of 1st day, end is end of last day (23:59:59.999999).
    """
    start_dt = datetime(year, month, 1)
    # compute next month
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    end_dt = next_month - timedelta(microseconds=1)
    return start_dt, end_dt

@app.route('/salary/generate', methods=['GET','POST'])
def salary_generate():
    """
    GET: render salary_form.html with teachers list.
    POST: expects form fields:
      - teacher_id
      - month (YYYY-MM)
      - manual_hours (optional)
      - save (optional)
      - hourly_rate (optional override)
    Returns JSON on POST.
    """
    # ensure db exists
    db_obj = globals().get("db")
    if db_obj is None:
        # current_app should be available if Flask app context exists
        current_app.logger.error("DB object not available")
        return jsonify({"error": "Server configuration error: DB not available"}), 503

    # pick collections safely
    teachers_col   = pick_collection("teachers_col", "faculties_col", fallback_name="faculties")
    attendance_col = pick_collection("attendance_col", fallback_name="attendance")
    salaries_col   = pick_collection("salaries_col", fallback_name="salaries")

    # ---------- GET: render form ----------
    if request.method == 'GET':
        try:
            raw_teachers = []
            if teachers_col is not None:
                try:
                    raw_teachers = list(teachers_col.find({}).sort("name", 1))
                except Exception:
                    current_app.logger.warning("Failed to fetch teachers list", exc_info=True)
                    raw_teachers = []
            normalized = []
            for t in raw_teachers:
                normalized.append({
                    "_id": str(t.get("_id")),
                    "name": t.get("name") or t.get("full_name") or "Unknown",
                    "hourly_rate": float(t.get("hourly_rate") or 0.0)
                })
            return render_template('salary_form.html', teachers=normalized)
        except Exception:
            current_app.logger.exception("Error rendering salary form")
            return render_template('salary_form.html', teachers=[]), 500

    # ---------- POST: compute salary ----------
    try:
        teacher_id = request.form.get('teacher_id') or request.form.get('teacher')
        month_str  = request.form.get('month')
        manual_hours_str = request.form.get('manual_hours')  # optional
        save_flag = (request.form.get('save') == 'on') or (request.form.get('save') == 'true')
        hourly_rate_override = request.form.get('hourly_rate')  # optional override numeric

        # validations
        if not teacher_id:
            return jsonify({"error": "Missing teacher_id"}), 400
        if not month_str:
            return jsonify({"error": "Missing month (YYYY-MM)"}), 400

        # parse month
        try:
            year, month = map(int, month_str.split('-'))
            if month < 1 or month > 12:
                raise ValueError()
        except Exception:
            return jsonify({"error": "Invalid month format. Use YYYY-MM."}), 400

        # resolve teacher id forms
        t_id_str = str(teacher_id).strip()
        t_obj_id = None
        if ObjectId.is_valid(t_id_str):
            try:
                t_obj_id = ObjectId(t_id_str)
            except Exception:
                t_obj_id = None

        # find teacher doc (to obtain name and default hourly_rate)
        if teachers_col is None:
            return jsonify({"error": "Server configuration error: faculties collection not available"}), 500

        teacher = None
        if t_obj_id:
            try:
                teacher = teachers_col.find_one({"_id": t_obj_id})
            except Exception:
                teacher = None
        if not teacher:
            try:
                teacher = teachers_col.find_one({"_id": t_id_str}) or teachers_col.find_one({"name": t_id_str})
            except Exception:
                teacher = None
        if not teacher:
            return jsonify({"error": "Teacher not found in faculties collection"}), 400

        # determine hourly_rate (override takes precedence)
        try:
            if hourly_rate_override and hourly_rate_override.strip() != "":
                hourly_rate = float(hourly_rate_override)
            else:
                hourly_rate = float(teacher.get("hourly_rate") or 0.0)
        except Exception:
            hourly_rate = 0.0

        # If manual_hours provided, use that directly
        total_hours = 0.0
        used_manual = False
        if manual_hours_str and str(manual_hours_str).strip() != "":
            try:
                total_hours = float(manual_hours_str)
                used_manual = True
            except Exception:
                return jsonify({"error": "Invalid manual_hours value"}), 400

        # Otherwise compute from attendance collection (if available)
        start_dt, end_dt = month_date_range(year, month)
        agg_objid = []
        agg_strid = []
        iter_count = 0

        if not used_manual:
            if attendance_col is None:
                current_app.logger.warning("attendance collection not available; total_hours defaults to 0")
                total_hours = 0.0
            else:
                # try aggregation by ObjectId
                if t_obj_id:
                    pipeline_objid = [
                        {"$match": {"teacher_id": t_obj_id, "date": {"$gte": start_dt, "$lte": end_dt}}},
                        {"$group": {"_id": "$teacher_id", "total_hours": {"$sum": "$hours"}}}
                    ]
                    try:
                        agg_objid = list(attendance_col.aggregate(pipeline_objid))
                    except Exception:
                        agg_objid = []

                # if aggregation produced a result, use it
                if agg_objid and len(agg_objid) > 0 and agg_objid[0].get("total_hours") is not None:
                    try:
                        total_hours = float(agg_objid[0]["total_hours"])
                    except Exception:
                        total_hours = 0.0
                else:
                    # try aggregation by string id
                    pipeline_strid = [
                        {"$match": {"teacher_id": t_id_str, "date": {"$gte": start_dt, "$lte": end_dt}}},
                        {"$group": {"_id": "$teacher_id", "total_hours": {"$sum": "$hours"}}}
                    ]
                    try:
                        agg_strid = list(attendance_col.aggregate(pipeline_strid))
                    except Exception:
                        agg_strid = []

                    if agg_strid and len(agg_strid) > 0 and agg_strid[0].get("total_hours") is not None:
                        try:
                            total_hours = float(agg_strid[0]["total_hours"])
                        except Exception:
                            total_hours = 0.0
                    else:
                        # last resort: iterate docs in the month and sum where teacher_id matches
                        try:
                            cursor = attendance_col.find({"date": {"$gte": start_dt, "$lte": end_dt}})
                            s = 0.0
                            c = 0
                            for doc in cursor:
                                doc_tid = doc.get("teacher_id")
                                # match either ObjectId or string
                                if (t_obj_id and doc_tid == t_obj_id) or (str(doc_tid) == t_id_str):
                                    try:
                                        s += float(doc.get("hours") or 0.0)
                                        c += 1
                                    except Exception:
                                        pass
                            total_hours = s
                            iter_count = c
                        except Exception:
                            total_hours = 0.0

        # compute final amount
        amount = round(total_hours * hourly_rate, 2)

        result = {
            "teacher_id": str(teacher.get("_id")),
            "teacher_name": teacher.get("name"),
            "month": f"{year}-{month:02d}",
            "year": year,
            "month_num": month,
            "total_hours": total_hours,
            "hourly_rate": hourly_rate,
            "amount": amount,
            "saved": False,
            "matched": {
                "used_manual": used_manual,
                "agg_objid_count": len(agg_objid) if 'agg_objid' in locals() else 0,
                "agg_strid_count": len(agg_strid) if 'agg_strid' in locals() else 0,
                "iter_count": iter_count
            }
        }

        # Save/upsert if requested (hours-based)
        if save_flag:
            if salaries_col is None:
                current_app.logger.error("salaries collection not available; cannot save salary")
                return jsonify({"error": "Server configuration error: salaries collection not available"}), 500

            # store teacher id as ObjectId if possible
            try:
                stored_teacher_id = ObjectId(str(teacher.get("_id"))) if ObjectId.is_valid(str(teacher.get("_id"))) else str(teacher.get("_id"))
            except Exception:
                stored_teacher_id = str(teacher.get("_id"))

            salary_doc = {
                "teacher_id": stored_teacher_id,
                "teacher_name": teacher.get("name"),
                "year": year,
                "month": month,
                "month_str": f"{year}-{month:02d}",
                "total_hours": total_hours,
                "hourly_rate": hourly_rate,
                "amount": amount,
                "generated_on": datetime.utcnow(),
                "manual_entry": bool(used_manual),
                "mode": "hours"
            }

            query_key = {"teacher_id": stored_teacher_id, "year": year, "month": month, "mode": "hours"}
            try:
                salaries_col.update_one(query_key, {"$set": salary_doc}, upsert=True)
                result["saved"] = True
            except Exception:
                current_app.logger.exception("Failed to upsert salary_doc")
                return jsonify({"error": "Failed to save salary"}), 500

        # Return the computed result as JSON
        return jsonify(result)

    except Exception as e:
        current_app.logger.error("Unexpected error in salary_generate: %s\n%s", str(e), traceback.format_exc())
        return jsonify({"error": f"Server error: {str(e)}"}), 500


# ---------- Days-based save endpoint (new) ----------
@app.route('/salary/generate_days', methods=['POST'])
def salary_generate_days():
    """
    Accept JSON payload from the days-based frontend preview and store the salary.
    Expected JSON keys (the frontend sends these):
      - teacher_id (required)
      - month (YYYY-MM) (required)
      - total_collection (number)
      - fixed_salary (number)
      - days_in_month (number)
      - per_day (number)
      - absent_days, attendance_equiv (number)
      - prorated_salary (number)
      - salary_deduction (number)
      - incentive_pct, incentive_amt
      - pension_add, pension_ded
      - tds_pct, tds_amt
      - food_charges
      - gross
    Returns JSON { saved: True, id: "..."} on success.
    """
    try:
        if not request.is_json:
            return jsonify({"error": "Expected JSON body"}), 400
        payload = request.get_json()

        # minimal validations
        teacher_id = payload.get('teacher_id')
        month_str = payload.get('month')
        if not teacher_id or not month_str:
            return jsonify({"error": "Missing teacher_id or month"}), 400

        # parse month
        try:
            year, month = map(int, str(month_str).split('-'))
            if month < 1 or month > 12:
                raise ValueError()
        except Exception:
            return jsonify({"error": "Invalid month format. Use YYYY-MM."}), 400

        # pick salaries collection
        salaries_col = pick_collection("salaries_col", fallback_name="salaries")
        teachers_col = pick_collection("teachers_col", "faculties_col", fallback_name="faculties")
        if salaries_col is None:
            current_app.logger.error("salaries collection not available; cannot save days salary")
            return jsonify({"error": "Server configuration error: salaries collection not available"}), 500

        # attempt to store teacher id as ObjectId if possible
        try:
            stored_teacher_id = ObjectId(str(teacher_id)) if ObjectId.is_valid(str(teacher_id)) else str(teacher_id)
        except Exception:
            stored_teacher_id = str(teacher_id)

        # resolve teacher name if possible
        teacher_name = payload.get('teacher_name') or ''
        if teachers_col is not None and not teacher_name:
            try:
                tdoc = None
                if isinstance(stored_teacher_id, ObjectId):
                    tdoc = teachers_col.find_one({"_id": stored_teacher_id})
                else:
                    tdoc = teachers_col.find_one({"_id": stored_teacher_id}) or teachers_col.find_one({"name": stored_teacher_id})
                if tdoc:
                    teacher_name = tdoc.get('name') or teacher_name
            except Exception:
                teacher_name = teacher_name or ''

        # build document to store (store many fields so preview matches saved doc)
        def num(k):
            try:
                return float(payload.get(k) or 0)
            except Exception:
                return 0.0

        salary_doc = {
            "teacher_id": stored_teacher_id,
            "teacher_name": teacher_name,
            "year": year,
            "month": month,
            "month_str": f"{year}-{month:02d}",
            "mode": "days",
            "total_collection": num('total_collection'),
            "fixed_salary": num('fixed_salary'),
            "days_in_month": int(payload.get('days_in_month') or 0),
            "per_day": int(payload.get('per_day') or 0),
            "attendance_equiv": float(payload.get('attendance_equiv') or 0.0),
            "absent_days": float(payload.get('absent_days') or 0.0),
            "prorated_salary": float(payload.get('prorated_salary') or 0.0),
            "salary_deduction": float(payload.get('salary_deduction') or 0.0),
            "incentive_pct": float(payload.get('incentive_pct') or 0.0),
            "incentive_amt": float(payload.get('incentive_amt') or 0.0),
            "pension_add": float(payload.get('pension_add') or 0.0),
            "pension_ded": float(payload.get('pension_ded') or 0.0),
            "food_charges": float(payload.get('food_charges') or 0.0),
            "tds_pct": float(payload.get('tds_pct') or 0.0),
            "tds_amt": float(payload.get('tds_amt') or 0.0),
            "gross": float(payload.get('gross') or 0.0),
            "generated_on": datetime.utcnow(),
        }

        # upsert using teacher+year+month+mode as key
        query_key = {"teacher_id": stored_teacher_id, "year": year, "month": month, "mode": "days"}
        try:
            res = salaries_col.update_one(query_key, {"$set": salary_doc}, upsert=True)
            return jsonify({"saved": True, "upserted": bool(res.upserted_id), "matched_count": res.matched_count}), 200
        except Exception:
            current_app.logger.exception("Failed to upsert days salary")
            return jsonify({"error": "Failed to save salary"}), 500

    except Exception as e:
        current_app.logger.error("Unexpected error in salary_generate_days: %s\n%s", str(e), traceback.format_exc())
        return jsonify({"error": f"Server error: {str(e)}"}), 500


# ---------- list / edit / delete routes (kept from your code) ----------
@app.route('/salary/list')
def salary_list():
    db_obj = globals().get("db")
    salaries_col = pick_collection("salaries_col", fallback_name="salaries")
    if salaries_col is None:
        return "salaries collection not available", 503

    try:
        rows = list(salaries_col.find({}).sort([("year", -1), ("month", -1), ("teacher_name", 1)]))
    except Exception:
        current_app.logger.exception("Failed to load salaries")
        rows = []

    # convert ObjectId to string
    for r in rows:
        if isinstance(r.get("teacher_id"), ObjectId):
            r["teacher_id"] = str(r["teacher_id"])
        r["_id"] = str(r.get("_id"))

    return render_template('salary_list.html', salaries=rows)


@app.route('/salary/edit/<id>', methods=['GET', 'POST'])
def salary_edit(id):
    salaries_col = pick_collection("salaries_col", fallback_name="salaries")
    teachers_col = pick_collection("teachers_col", "faculties_col", fallback_name="faculties")
    if salaries_col is None:
        return "salaries collection not available", 503

    # convert id to ObjectId or keep string
    try:
        sal_obj_id = ObjectId(id) if ObjectId.is_valid(id) else id
    except Exception:
        sal_obj_id = id

    # GET -> render edit form
    if request.method == 'GET':
        try:
            sal = salaries_col.find_one({"_id": sal_obj_id}) if isinstance(sal_obj_id, ObjectId) else salaries_col.find_one({"_id": sal_obj_id})
            if not sal:
                flash("Salary record not found.", "warning")
                return redirect(url_for('salary_list'))

            # Normalize teacher list for dropdown (optional)
            teachers = []
            if teachers_col is not None:
                try:
                    teachers = list(teachers_col.find({}).sort("name", 1))
                except Exception:
                    teachers = []

            normalized = [{"_id": str(t.get("_id")), "name": t.get("name") or t.get("full_name") or "Unknown",
                           "hourly_rate": float(t.get("hourly_rate") or 0.0)} for t in teachers]

            # convert some fields for template
            sal["_id"] = str(sal.get("_id"))
            if isinstance(sal.get("teacher_id"), ObjectId):
                sal["teacher_id"] = str(sal["teacher_id"])

            return render_template('salary_edit.html', salary=sal, teachers=normalized)
        except Exception:
            current_app.logger.exception("Error rendering salary edit form")
            flash("Failed to load record.", "danger")
            return redirect(url_for('salary_list'))

    # POST -> save edits
    try:
        teacher_id = request.form.get('teacher_id')
        month_str = request.form.get('month')  # expect YYYY-MM
        total_hours = float(request.form.get('total_hours') or 0.0)
        hourly_rate = float(request.form.get('hourly_rate') or 0.0)
        manual_entry = request.form.get('manual_entry') == 'on'

        # parse month into year/month
        try:
            year, month = map(int, month_str.split('-'))
        except Exception:
            flash("Invalid month format. Use YYYY-MM.", "danger")
            return redirect(url_for('salary_edit', id=id))

        # build stored_teacher_id
        stored_teacher_id = ObjectId(teacher_id) if ObjectId.is_valid(teacher_id) else teacher_id

        # resolve teacher name safely
        teacher_name = ''
        if teachers_col is not None:
            try:
                tdoc = teachers_col.find_one({"_id": stored_teacher_id}) if ObjectId.is_valid(str(stored_teacher_id)) else teachers_col.find_one({"_id": stored_teacher_id})
                teacher_name = (tdoc or {}).get("name") or request.form.get('teacher_name') or ''
            except Exception:
                teacher_name = request.form.get('teacher_name') or ''
        else:
            teacher_name = request.form.get('teacher_name') or ''

        salary_doc = {
            "teacher_id": stored_teacher_id,
            "teacher_name": teacher_name,
            "year": year,
            "month": month,
            "month_str": f"{year}-{month:02d}",
            "total_hours": total_hours,
            "hourly_rate": hourly_rate,
            "amount": round(total_hours * hourly_rate, 2),
            "generated_on": datetime.utcnow(),
            "manual_entry": bool(manual_entry)
        }

        query = {"_id": sal_obj_id} if isinstance(sal_obj_id, ObjectId) else {"_id": sal_obj_id}
        salaries_col.update_one(query, {"$set": salary_doc})
        flash("Salary record updated.", "success")
        return redirect(url_for('salary_list'))

    except Exception as e:
        current_app.logger.error("Error editing salary: %s\n%s", str(e), traceback.format_exc())
        flash("Failed to update salary.", "danger")
        return redirect(url_for('salary_list'))


@app.route('/salary/delete/<id>', methods=['POST'])
def salary_delete(id):
    salaries_col = pick_collection("salaries_col", fallback_name="salaries")
    if salaries_col is None:
        return "salaries collection not available", 503

    try:
        sal_obj_id = ObjectId(id) if ObjectId.is_valid(id) else id
    except Exception:
        sal_obj_id = id

    try:
        res = salaries_col.delete_one({"_id": sal_obj_id} if isinstance(sal_obj_id, ObjectId) else {"_id": sal_obj_id})
        if res.deleted_count:
            flash("Salary record deleted.", "success")
        else:
            flash("Salary record not found.", "warning")
    except Exception as e:
        current_app.logger.exception("Failed to delete salary: %s", e)
        flash("Failed to delete salary.", "danger")

    return redirect(url_for('salary_list'))


@app.route("/debug/faculties_sample")
def debug_faculties_sample():
    db_obj = globals().get("db")
    if db_obj is None:
        return jsonify({"count": 0, "sample": []})
    try:
        docs = list(db_obj.faculties.find({}, {"name":1}).limit(20))
        return jsonify({"count": len(docs), "sample": [{**{"_id": str(d["_id"])}, "name": d.get("name")} for d in docs]})
    except Exception:
        current_app.logger.exception("debug_faculties_sample failed")
        return jsonify({"count": 0, "sample": []})





@app.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():

    users = users_col
    user = users.find_one({"_id": session["user_id"]})

    if request.method == "POST":
        new_email = request.form.get("email")
        new_pass = request.form.get("password")

        update_doc = {}

        if new_email:
            update_doc["email"] = new_email

        if new_pass:
            update_doc["password_hash"] = generate_password_hash(new_pass)

        if update_doc:
            users.update_one({"_id": user["_id"]}, {"$set": update_doc})
            flash("Settings updated successfully!", "success")

        return redirect(url_for("settings"))

    return render_template("settings.html", user=user)










def login_required(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not session.get('user_id'):
            return redirect(url_for('login', next=request.path))
        # optionally load user into g
        u = get_users_col().find_one({"_id": ObjectId(session['user_id'])}) if ObjectId.is_valid(str(session.get('user_id'))) else get_users_col().find_one({"_id": session.get('user_id')})
        g.current_user = u
        return f(*args, **kwargs)
    return wrapped

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username','').strip()
        password = request.form.get('password','')
        users = get_users_col()
        user = users.find_one({"username": username})
        if user and check_password_hash(user.get('password_hash',''), password):
            session['user_id'] = str(user.get('_id'))
            session['user_name'] = user.get('name') or user.get('username')
            flash("Login successful", "success")
            next_url = request.args.get('next') or url_for('index')
            return redirect(next_url)
        else:
            flash("Invalid credentials", "danger")
    return render_template('login.html')

@app.route('/logout')
def logout():
    # we will show a confirmation in the UI; this route actually performs logout
    session.clear()
    flash("You have been logged out.", "info")
    return redirect(url_for('login'))



@app.route('/profile', methods=['GET','POST'])
@login_required
def profile():
    users = get_users_col()
    uid = session.get('user_id')
    user = users.find_one({"_id": ObjectId(uid)}) if ObjectId.is_valid(uid) else users.find_one({"_id": uid})

    if request.method == 'POST':
        name = request.form.get('name') or user.get('name')
        email = request.form.get('email') or user.get('email')
        phone = request.form.get('phone') or user.get('phone')

        update = {"name": name, "email": email, "phone": phone}
        # file upload
        f = request.files.get('photo')
        if f and f.filename and allowed_file(f.filename):
            fname = secure_filename(f.filename)
            # prefix with user id + timestamp to avoid collisions
            basename = f"{uid}_{int(datetime.utcnow().timestamp())}_{fname}"
            dest = os.path.join(UPLOAD_FOLDER, basename)
            f.save(dest)
            update['photo'] = basename

        users.update_one({"_id": user.get('_id')}, {"$set": update})
        flash("Profile updated.", "success")
        return redirect(url_for('profile'))

    return render_template('profile.html', user=user)



@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)


@app.route('/notifications')
@login_required
def notifications():
    # Use the module-level students_col defined at top of file (no globals() truth testing)
    # Fees due: student docs with 'balance' > 0
    fees_due = []
    try:
        for s in students_col.find({"balance": {"$gt": 0}}):
            fees_due.append({
                "type": "fee",
                "student_name": (s.get('first_name', '') + ' ' + (s.get('last_name') or '')).strip(),
                "amount": s.get('balance'),
                "student_id": str(s.get('_id'))
            })
    except Exception:
        fees_due = []

    # Expiry alerts: expiry_date within 14 days (student.expiry_date stored as string YYYY-MM-DD or date)
    expiry_alerts = []
    try:
        today = date.today()
        threshold = today + timedelta(days=14)
        for s in students_col.find({}):
            exp = s.get('expiry_date') or s.get('expiry') or None
            if not exp:
                continue

            # handle dates stored as string 'YYYY-MM-DD' or datetime
            exp_date = None
            if isinstance(exp, str):
                try:
                    exp_date = datetime.strptime(exp.split('T')[0], '%Y-%m-%d').date()
                except Exception:
                    exp_date = None
            elif isinstance(exp, datetime):
                exp_date = exp.date()

            if exp_date and today <= exp_date <= threshold:
                expiry_alerts.append({
                    "type": "expiry",
                    "student_name": (s.get('first_name', '') + ' ' + (s.get('last_name') or '')).strip(),
                    "expiry_date": exp_date.isoformat(),
                    "student_id": str(s.get('_id'))
                })
    except Exception:
        expiry_alerts = []

    # Build notifications list sorted by priority (expiry first, then fees)
    notes = []
    for e in expiry_alerts:
        notes.append({
            "message": f"Course expiring for {e['student_name']} on {e['expiry_date']}",
            "type": "expiry",
            "student_id": e['student_id'],
            "time": "Soon"
        })
    for f in fees_due:
        notes.append({
            "message": f"Fees due: {f['student_name']} (₹{f['amount']})",
            "type": "fee",
            "student_id": f['student_id'],
            "time": "Pending"
        })

    return render_template('notifications.html', notifications=notes)


# simple count endpoint used by navbar badge
@app.route('/notifications/count')
def notifications_count():
    # Use module-level collections defined at top of file (students_col, batches_col, courses_col)
    try:
        fees_count = students_col.count_documents({"balance": {"$gt": 0}})
    except Exception:
        app.logger.exception("fees_count failed")
        fees_count = 0

    expiry_count = 0
    try:
        today = date.today()
        threshold = today + timedelta(days=14)

        # Fast path: try BSON datetime range count (works if expiry_date is stored as datetime)
        try:
            start_dt = datetime.combine(today, datetime.min.time())
            end_dt = datetime.combine(threshold, datetime.max.time())
            expiry_count = students_col.count_documents({
                "expiry_date": {"$gte": start_dt, "$lte": end_dt}
            })
        except Exception:
            app.logger.debug("Fast expiry_date range count failed; falling back to per-doc parsing.")
            expiry_count = 0

        # Fallback: inspect docs for string dates or infer expiry from admission_date + duration
        if expiry_count == 0:
            cursor = students_col.find({}, {
                "expiry_date": 1,
                "admission_date": 1,
                "batch_id": 1,
                "course_id": 1
            })
            for s in cursor:
                exp = s.get("expiry_date")
                exp_date = None

                # parse expiry_date if present
                try:
                    if isinstance(exp, datetime):
                        exp_date = exp.date()
                    elif isinstance(exp, str):
                        try:
                            exp_date = datetime.strptime(exp[:10], "%Y-%m-%d").date()
                        except Exception:
                            try:
                                exp_date = datetime.strptime(exp[:10], "%d-%m-%Y").date()
                            except Exception:
                                exp_date = None
                    elif isinstance(exp, dict) and "$date" in exp:
                        raw = exp["$date"]
                        if isinstance(raw, str):
                            try:
                                exp_date = datetime.strptime(raw[:10], "%Y-%m-%d").date()
                            except Exception:
                                exp_date = None
                except Exception:
                    exp_date = None

                # infer from admission_date + duration if expiry missing
                if exp_date is None:
                    adm = s.get("admission_date")
                    adm_date = None
                    if isinstance(adm, datetime):
                        adm_date = adm.date()
                    elif isinstance(adm, str):
                        try:
                            adm_date = datetime.strptime(adm[:10], "%Y-%m-%d").date()
                        except Exception:
                            adm_date = None

                    if adm_date:
                        dur_days = None
                        bid = s.get("batch_id")
                        cid = s.get("course_id")

                        try:
                            if bid:
                                batch = batches_col.find_one({"_id": bid})
                                if batch:
                                    dur_days = batch.get("duration_days") or batch.get("duration")
                                    if dur_days and isinstance(dur_days, str) and "month" in dur_days:
                                        try:
                                            months = int(''.join(ch for ch in dur_days if ch.isdigit()))
                                            dur_days = months * 30
                                        except Exception:
                                            dur_days = None

                            if dur_days is None and cid:
                                course = courses_col.find_one({"_id": cid})
                                if course:
                                    dur_days = course.get("duration_days") or course.get("duration")
                                    if dur_days and isinstance(dur_days, str) and "month" in dur_days:
                                        try:
                                            months = int(''.join(ch for ch in dur_days if ch.isdigit()))
                                            dur_days = months * 30
                                        except Exception:
                                            dur_days = None
                        except Exception:
                            app.logger.debug("Failed to fetch batch/course for student %r", s.get("_id"))

                        try:
                            if dur_days:
                                dur_days_int = int(dur_days)
                                exp_date = adm_date + timedelta(days=dur_days_int)
                        except Exception:
                            exp_date = None

                if exp_date and today <= exp_date <= threshold:
                    expiry_count += 1

    except Exception:
        app.logger.exception("expiry_count calculation failed")
        expiry_count = 0

    return jsonify({"count": fees_count + expiry_count})


# Certificate generator page + optional students API for autocomplete
@app.route('/certificate-generator')
@login_required   # optional: remove if you want page public
def certificate_generator_page():
    """
    Renders page where user can search/select a student and generate certificate.
    """
    return render_template('certificate_generator.html')


@app.route('/api/all_students')
def api_all_students():
    """
    Returns a small JSON list of students for autocomplete/search.
    Limit and projection keep data light.
    """
    q = request.args.get('q', '').strip()
    query = {}
    if q:
        # search by name or form number or phone
        query = {"$or": [
            {"first_name": {"$regex": q, "$options": "i"}},
            {"last_name": {"$regex": q, "$options": "i"}},
            {"form_no": {"$regex": q, "$options": "i"}},
            {"phone": {"$regex": q}}
        ]}

    # Adjust limit as needed
    docs = list(db.students.find(query, {"first_name":1, "last_name":1, "form_no":1}).sort("created_at",-1).limit(200))
    out = []
    for s in docs:
        out.append({
            "_id": str(s.get("_id")),
            "name": ((s.get("first_name","") + " " + s.get("last_name","")).strip()) or s.get("form_no",""),
            "form_no": s.get("form_no","")
        })
    return jsonify(out)

@app.route("/generate_certificate_manual", methods=["POST"])
def generate_certificate_manual():
    data = {
        "name": request.form.get("name"),
        "father": request.form.get("father"),
        "age": request.form.get("age"),
        "course": request.form.get("course"),
        "courseHours": request.form.get("courseHours"),
        "admission": request.form.get("admission"),
        "completion": request.form.get("completion"),
        "formNo": request.form.get("formNo"),
        "photo": request.form.get("photo") or "default.jpg"
    }

    return render_template("certificate_template.html", **data)

@app.route("/generate_certificate/<id>")
def generate_certificate(id):
    

    # 1) find student by _id or form_no
    student = None
    try:
        if ObjectId.is_valid(id):
            student = students_col.find_one({"_id": ObjectId(id)})
    except Exception:
        student = None

    if not student:
        student = students_col.find_one({"form_no": id}) or students_col.find_one({"formNo": id})

    if not student:
        app.logger.info("generate_certificate: student not found for id=%s", id)
        return abort(404)

    # 2) Name & father
    name = " ".join(filter(None, [student.get("first_name","").strip(), student.get("last_name","").strip()])).strip() or student.get("name","")
    father = student.get("father_name") or student.get("father") or ""

    # 3) Age from dob (if present) else use age field
    def calc_age(dob):
        try:
            if not dob:
                return ""
            if isinstance(dob, str):
                dob_dt = datetime.fromisoformat(dob)
            elif isinstance(dob, datetime):
                dob_dt = dob
            else:
                return ""
            today = date.today()
            years = today.year - dob_dt.year - ((today.month, today.day) < (dob_dt.month, dob_dt.day))
            return str(years)
        except Exception:
            return student.get("age","")

    age = student.get("age") or calc_age(student.get("dob") or student.get("date_of_birth"))

    # 4) Course lookup (course_id exists in your student doc)
    course = ""
    course_hours = ""
    if student.get("course_id"):
        try:
            cid = student["course_id"]
            # course_id is stored as ObjectId in DB already
            course_doc = courses_col.find_one({"_id": cid})
            if course_doc:
                # prefer common field names
                course = course_doc.get("name") or course_doc.get("course") or course_doc.get("title") or ""
                course_hours = course_doc.get("hours") or course_doc.get("duration") or course_doc.get("courseHours") or ""
        except Exception:
            pass

    # 5) Admission & completion (student.admission_date and batch end date)
    admission = student.get("admission_date") or student.get("admission") or ""
    # format ISO date strings (ensure yyyy-mm-dd)
    def fmt(d):
        try:
            if not d: return ""
            if isinstance(d, datetime):
                return d.strftime("%Y-%m-%d")
            if isinstance(d, str):
                return d.split("T")[0]
            return str(d)
        except:
            return str(d)
    admission = fmt(admission)

    completion = ""
    if student.get("batch_id"):
        try:
            bid = student["batch_id"]
            batch_doc = batches_col.find_one({"_id": bid})
            if batch_doc:
                completion = batch_doc.get("end_date") or batch_doc.get("completion_date") or batch_doc.get("finish_date") or ""
                completion = fmt(completion)
        except Exception:
            pass

    # fallback for course_hours if empty: try to look at student.fee or notes (optional)
    if not course_hours:
        # maybe the students collection stores duration in student's doc
        course_hours = student.get("courseHours") or student.get("course_hours") or ""

    # photo handling (filename stored in student.photo)
    photo = student.get("photo") or ""

    formNo = student.get("form_no") or student.get("formNo") or str(student.get("_id"))

    data = {
        "name": name,
        "father": father,
        "age": age,
        "course": course,
        "courseHours": course_hours,
        "admission": admission,
        "completion": completion,
        "formNo": formNo,
        "photo": photo
    }

    # render (HTML preview). pdfkit fallback handled by template route if you prefer
    return render_template("certificate_template.html", **data)









# --- Daybook: Ledger & Voucher APIs (paste into app.py, no blueprint) ---

from bson.errors import InvalidId
from flask import jsonify, request, render_template, abort


@app.route('/daybook')
def daybook():
    return render_template('daybook.html')

# ---------- Helpers ----------
def validate_voucher_payload(payload):
    lines = payload.get("lines") or []
    if not payload.get("date"):
        return "date required"
    if not lines or not any(l.get("account") and float(l.get("amount") or 0) > 0 for l in lines):
        return "at least one ledger line with positive amount required"
    return None

def compute_totals(lines):
    dr = sum(float(l.get("amount") or 0) for l in lines if l.get("type") == "debit")
    cr = sum(float(l.get("amount") or 0) for l in lines if l.get("type") == "credit")
    return dr, cr

def auto_allocate_contra(lines, voucher_type):
    # If contra voucher and only one non-zero line present, add opposite line (Cash/Bank guess).
    if voucher_type != 'contra':
        return lines
    nonzero = [l for l in lines if float(l.get("amount") or 0) > 0]
    if len(nonzero) == 1:
        l = nonzero[0]
        amount = float(l.get("amount") or 0)
        counter = "Bank" if "bank" in (l.get("account") or "").lower() else "Cash"
        opp_type = "credit" if l.get("type") == "debit" else "debit"
        lines.append({"account": counter, "type": opp_type, "amount": amount, "details": ""})
    return lines

# ----------------- Ledger Groups CRUD -----------------
@app.route("/api/ledger_groups", methods=["GET"])
def list_ledger_groups():
    docs = list(db.ledger_groups.find().sort("name", 1))
    out = []
    for d in docs:
        d["_id"] = str(d["_id"])
        out.append(d)
    return jsonify(out)

@app.route("/api/ledger_groups", methods=["POST"])
def create_ledger_group():
    data = request.json or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    doc = {"name": name, "created_at": datetime.utcnow()}
    res = db.ledger_groups.insert_one(doc)
    doc["_id"] = str(res.inserted_id)
    return jsonify(doc), 201

@app.route("/api/ledger_groups/<id>", methods=["DELETE"])
def delete_ledger_group(id):
    try:
        oid = ObjectId(id)
    except Exception:
        return abort(404)
    db.ledger_groups.delete_one({"_id": oid})
    # remove group assignment from ledgers that referenced this group (stored as string id)
    db.ledgers.update_many({"group": id}, {"$unset": {"group": ""}})
    return jsonify({"ok": True})

# ----------------- Ledgers CRUD (supports group) -----------------
@app.route("/api/ledgers", methods=["GET"])
def list_ledgers():
    docs = list(db.ledgers.find().sort("name", 1))
    # build group map for convenience
    group_map = {}
    for g in db.ledger_groups.find():
        group_map[str(g["_id"])] = g.get("name", "")
    out = []
    for d in docs:
        d["_id"] = str(d["_id"])
        g = d.get("group")
        d["group_name"] = group_map.get(g, "") if g else ""
        out.append(d)
    return jsonify(out)

@app.route("/api/ledgers", methods=["POST"])
def create_ledger():
    data = request.json or {}
    name = (data.get("name") or "").strip()
    group = data.get("group") or None  # optional group id (string)
    if not name:
        return jsonify({"error":"name required"}), 400
    doc = {"name": name, "created_at": datetime.utcnow()}
    if group:
        doc["group"] = group
    res = db.ledgers.insert_one(doc)
    doc["_id"] = str(res.inserted_id)
    doc["group_name"] = ""
    if group:
        try:
            g = db.ledger_groups.find_one({"_id": ObjectId(group)})
            doc["group_name"] = g["name"] if g else ""
        except Exception:
            doc["group_name"] = ""
    return jsonify(doc), 201

@app.route("/api/ledgers/<id>", methods=["PUT"])
def update_ledger(id):
    try:
        oid = ObjectId(id)
    except Exception:
        return abort(404)
    data = request.json or {}
    name = (data.get("name") or "").strip()
    group = data.get("group") if "group" in data else None  # explicit allow null to remove
    if not name:
        return jsonify({"error":"name required"}), 400
    update_fields = {"name": name}
    if group is None:
        # if client omitted 'group', leave as-is; if client explicitly set group to null/"" it will clear below
        pass
    else:
        # set or clear group
        if group == "" or group is None:
            update_fields["group"] = None
        else:
            update_fields["group"] = group
    db.ledgers.update_one({"_id": oid}, {"$set": update_fields})
    doc = db.ledgers.find_one({"_id": oid})
    if not doc:
        return abort(404)
    doc["_id"] = str(doc["_id"])
    if doc.get("group"):
        try:
            g = db.ledger_groups.find_one({"_id": ObjectId(doc["group"])})
            doc["group_name"] = g["name"] if g else ""
        except Exception:
            doc["group_name"] = ""
    else:
        doc["group_name"] = ""
    return jsonify(doc)

@app.route("/api/ledgers/<id>", methods=["DELETE"])
def delete_ledger(id):
    try:
        oid = ObjectId(id)
    except Exception:
        return abort(404)
    db.ledgers.delete_one({"_id": oid})
    return jsonify({"ok": True})

# ----------------- Vouchers CRUD -----------------
@app.route("/api/vouchers", methods=["GET"])
def list_vouchers():
    q = {}
    args = request.args
    if args.get("from"):
        q["date"] = q.get("date", {})
        q["date"]["$gte"] = args.get("from")
    if args.get("to"):
        q["date"] = q.get("date", {})
        q["date"]["$lte"] = args.get("to")
    if args.get("search"):
        s = args.get("search")
        q["$or"] = [
            {"no": {"$regex": s, "$options":"i"}},
            {"narration": {"$regex": s, "$options":"i"}},
            {"lines.account": {"$regex": s, "$options":"i"}}
        ]
    docs = list(db.vouchers.find(q).sort("date", 1))
    for d in docs:
        d["_id"] = str(d["_id"])
    return jsonify(docs)

@app.route("/api/vouchers", methods=["POST"])
def create_voucher():
    data = request.json or {}
    err = validate_voucher_payload(data)
    if err:
        return jsonify({"error": err}), 400

    data["lines"] = auto_allocate_contra(data.get("lines", []), data.get("type"))

    dr, cr = compute_totals(data["lines"])
    if abs(dr - cr) > 0.009 and not data.get("allow_unbalanced"):
        return jsonify({"error": "voucher not balanced (debit != credit)", "dr": dr, "cr": cr}), 400

    doc = {
        "date": data.get("date"),
        "type": data.get("type", "journal"),
        "no": data.get("no") or "",
        "narration": data.get("narration") or "",
        "lines": data.get("lines"),
        "created_at": datetime.utcnow()
    }
    res = db.vouchers.insert_one(doc)
    doc["_id"] = str(res.inserted_id)
    return jsonify(doc), 201

@app.route("/api/vouchers/<id>", methods=["PUT"])
def update_voucher(id):
    try:
        oid = ObjectId(id)
    except Exception:
        return abort(404)
    data = request.json or {}
    err = validate_voucher_payload(data)
    if err:
        return jsonify({"error": err}), 400
    data["lines"] = auto_allocate_contra(data.get("lines", []), data.get("type"))
    dr, cr = compute_totals(data["lines"])
    if abs(dr - cr) > 0.009 and not data.get("allow_unbalanced"):
        return jsonify({"error": "voucher not balanced (debit != credit)", "dr": dr, "cr": cr}), 400
    db.vouchers.update_one({"_id": oid}, {"$set": {
        "date": data.get("date"),
        "type": data.get("type"),
        "no": data.get("no"),
        "narration": data.get("narration"),
        "lines": data.get("lines"),
        "updated_at": datetime.utcnow()
    }})
    doc = db.vouchers.find_one({"_id": oid})
    doc["_id"] = str(doc["_id"])
    return jsonify(doc)

@app.route("/api/vouchers/<id>", methods=["DELETE"])
def delete_voucher(id):
    try:
        oid = ObjectId(id)
    except Exception:
        return abort(404)
    db.vouchers.delete_one({"_id": oid})
    return jsonify({"ok": True})

# ----------------- Printable voucher -----------------
@app.route("/voucher/print/<id>")
def print_voucher(id):
    # try ObjectId first, fallback to no or string id
    doc = None
    try:
        oid = ObjectId(id)
        doc = db.vouchers.find_one({"_id": oid})
    except Exception:
        pass
    if not doc:
        doc = db.vouchers.find_one({"no": id}) or db.vouchers.find_one({"_id": id})
    if not doc:
        return abort(404)
    # convert _id for template
    doc["_id"] = str(doc["_id"])
    return render_template("voucher_print.html", v=doc)
# --- End of Daybook APIs ---








if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)

