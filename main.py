# @title 🚀 Run Generator (Render Compatible - Logic Unchanged)
import requests
import random
import threading
import time
import warnings
import logging
import os # Added for Render PORT
from flask import Flask # Added for Render Web Service
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from appwrite.client import Client
from appwrite.services.databases import Databases

# ------------------------------------------------------------------
# 🔇 SUPPRESS WARNINGS
# ------------------------------------------------------------------
warnings.filterwarnings("ignore", category=DeprecationWarning)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("werkzeug").setLevel(logging.ERROR) # Hide Flask logs

# ------------------------------------------------------------------
# ⚙️ CONFIGURATION
# ------------------------------------------------------------------
# Render provides the PORT env var. Default to 10000 if local.
PORT = int(os.environ.get("PORT", 10000))

PREFIXES = [94718, 78359, 77668, 93135, 97161, 62092, 90157, 78277, 88513, 99104, 98916,74799, 70114, 92679, 99104, 87894, 87578]
API_URL = "https://api.x10.network/numapi.php"
API_KEY = "num_devil"

# Appwrite Config
APPWRITE_ENDPOINT = "https://fra.cloud.appwrite.io/v1"
APPWRITE_PROJECT_ID = "692c7cce0036aa32cb12"
APPWRITE_API_KEY = "standard_e1eb40bc704c26bff01939550bfa18f741d15a704b92ef416769d1f92f5c3358cc37d716261dcc9d775ab20375e1a51288a3330ba0156f385e60748932446e7ff2e64678b0454d9a8883fe2f38f1311278969e045c1328b829e58e55fa090e1e3d2b12d6df904438709c9b6b97cbeafc14e5ff4f533b9f565f33fe3824369814"
APPWRITE_DB_ID = "697299e8002cc13b21b4"
APPWRITE_COLLECTION_ID = "data"

CONCURRENT_THREADS = 3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"
]

# ------------------------------------------------------------------
# 🔌 ROBUST SETUP
# ------------------------------------------------------------------

# 1. Setup Session with Retry & Increased Pool Size
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(
    max_retries=retry_strategy,
    pool_connections=100,
    pool_maxsize=100
)
session.mount("https://", adapter)
session.mount("http://", adapter)

# 2. Appwrite Setup
client = Client()
client.set_endpoint(APPWRITE_ENDPOINT)
client.set_project(APPWRITE_PROJECT_ID)
client.set_key(APPWRITE_API_KEY)
databases = Databases(client)

stats = {"total": 0, "success": 0, "duplicates": 0, "errors": 0}
stats_lock = threading.Lock()

# ------------------------------------------------------------------
# 🛠 LOGIC
# ------------------------------------------------------------------

def save_to_appwrite(data_dict):
    try:
        databases.create_document(
            APPWRITE_DB_ID,
            APPWRITE_COLLECTION_ID,
            data_dict['mobile'],
            data_dict
        )
        return "success"
    except Exception as e:
        if "409" in str(e): return "duplicate"
        print(f"⚠️ DB Error: {str(e)[:50]}")
        return "error"

def generate_random_number():
    prefix = random.choice(PREFIXES)
    suffix = random.randint(10000, 99999)
    return f"{prefix}{suffix}"

def process_pipeline(thread_id):
    print(f"🚀 Pipeline {thread_id} active...")

    while True:
        mobile_number = generate_random_number()
        headers = {"User-Agent": random.choice(USER_AGENTS)}

        try:
            params = {"action": "api", "key": API_KEY, "number": mobile_number}

            response = session.get(
                API_URL,
                params=params,
                headers=headers,
                timeout=20
            )

            try:
                data = response.json()
            except:
                continue

            # --- Type Checking ---
            results = []

            if isinstance(data, list):
                results = data
            elif isinstance(data, dict):
                # If it's a dict, check if it's an error or actual data
                if data.get('error') or data.get('response') == 'error':
                    results = []
                else:
                    results = [data]
            else:
                results = []

            # Update Stats Total
            with stats_lock:
                stats["total"] += 1
                curr_total = stats["total"]

            if results:
                found_valid_data = False

                for p in results:
                    # --- CRITICAL FIX: SKIP EMPTY/N/A DATA ---
                    raw_name = p.get("name")

                    # If name is None, Empty String, or explicitly "N/A" -> SKIP
                    if not raw_name or str(raw_name).strip() == "" or str(raw_name).strip() == "N/A":
                        continue # Skip this iteration, do not save

                    # If we passed the check, mark that we found something
                    found_valid_data = True

                    # Prepare Data
                    raw_address = str(p.get("address", "N/A"))
                    clean_address = raw_address.replace("!", ", ").replace(" ,", ",").strip()
                    if len(clean_address) > 250: clean_address = clean_address[:250]

                    record = {
                        'name': str(raw_name), # We know this is valid now
                        'fname': str(p.get("father_name", "N/A")),
                        'mobile': str(p.get("mobile", mobile_number)),
                        'address': clean_address
                    }

                    status = save_to_appwrite(record)

                    if status == "success":
                        with stats_lock: stats["success"] += 1
                        print(f"✅ [{curr_total}] SAVED | {record['mobile']}")
                    elif status == "duplicate":
                        with stats_lock: stats["duplicates"] += 1
                        print(f"🔁 [{curr_total}] EXISTS | {record['mobile']}")

                # If we had results (list was not empty), but all were N/A/Empty
                if not found_valid_data:
                     print(f"⚠️ [{curr_total}] Skipped (Data was empty/N/A) | {mobile_number}")

            else:
                print(f"❌ [{curr_total}] Not Found | {mobile_number}")

        except requests.exceptions.Timeout:
            print(f"⏳ [{thread_id}] Slow Network... Waiting 5s")
            time.sleep(5)

        except requests.exceptions.ConnectionError:
            print(f"🔌 [{thread_id}] Connection Lost... Waiting 10s")
            time.sleep(10)

        except Exception as e:
            with stats_lock: stats["errors"] += 1
            print(f"⚠️ Error: {str(e)[:50]}")

        # Throttle
        time.sleep(1.5)

# ------------------------------------------------------------------
# 🌐 FLASK SERVER (Render Requirement)
# ------------------------------------------------------------------
app = Flask(__name__)

@app.route('/')
def home():
    with stats_lock:
        return f"Worker Active. Total Scanned: {stats['total']} | Success: {stats['success']}"

def run_scraper_background():
    print(f"🔥 Starting {CONCURRENT_THREADS} Pipelines (Fixed N/A Handling)...")
    with ThreadPoolExecutor(max_workers=CONCURRENT_THREADS) as executor:
        for i in range(CONCURRENT_THREADS):
            executor.submit(process_pipeline, i+1)

if __name__ == "__main__":
    # 1. Start Scraper in a background thread
    threading.Thread(target=run_scraper_background, daemon=True).start()
    
    # 2. Start Web Server (Blocks main thread to keep Render happy)
    app.run(host="0.0.0.0", port=PORT)
