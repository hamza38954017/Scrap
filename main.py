import requests
import random
import threading
import time
import warnings
import logging
import os
from flask import Flask
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
logging.getLogger("werkzeug").setLevel(logging.ERROR)

# ------------------------------------------------------------------
# ⚙️ CONFIGURATION
# ------------------------------------------------------------------
PORT = int(os.environ.get("PORT", 10000))

PREFIXES = [99109, 89290, 95082, 98181, 98114]
# Updated API Endpoint and Key based on your request
API_URL = "https://api.paanel.shop/numapi.php" 
API_KEY = "ahsjyash"

# Appwrite Config
APPWRITE_ENDPOINT = "https://fra.cloud.appwrite.io/v1"
APPWRITE_PROJECT_ID = "6986cc890027e3b7def9"
APPWRITE_API_KEY = "standard_72cdac3120d700c22823ff92201c9ba3ebf6a35cfab43d5dce97a4bbe8867ead1d95740a84c15dd052ff4b733a2bcd7c72b3cf36c39cbe69fd58504ea77b881b95d41556bdb25c1ce890477039d83436075c65c52c54680c6c511259e81958bb793afb19da6ac4bbccbce40f8ce687e7acb42aa77248187535bca7b2465bf570"
APPWRITE_DB_ID = "6986cd6f00356b66de5d"
APPWRITE_COLLECTION_ID = "hell"

CONCURRENT_THREADS = 2

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36"
]

# ------------------------------------------------------------------
# 🔌 ROBUST SETUP
# ------------------------------------------------------------------
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
        # Log unexpected errors but keep running
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
            # Updated to use the new parameters structure
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

            # --- Type Checking (UPDATED FOR SPECIFIC LIST RESPONSE) ---
            results = []

            # 1. Direct List (Matches your provided example)
            if isinstance(data, list):
                results = data
            # 2. Dict with "results" key (Fallback)
            elif isinstance(data, dict) and "results" in data and isinstance(data["results"], list):
                results = data["results"]
            # 3. Single Dict (Fallback)
            elif isinstance(data, dict):
                if data.get('error') or data.get('response') == 'error':
                    results = []
                else:
                    results = [data]
            else:
                results = []

            with stats_lock:
                stats["total"] += 1
                curr_total = stats["total"]

            if results:
                found_valid_data = False

                for p in results:
                    # -------------------------------------------------------
                    # 🛡️ UPDATED: HANDLING YOUR SPECIFIC API FIELDS
                    # -------------------------------------------------------
                    
                    # Helper to safely get string from None/Null/missing keys
                    def safe_str(val):
                        if val is None: return ""
                        return str(val).strip()

                    # 1. Extract Values using keys from your provided JSON
                    raw_mobile = safe_str(p.get("mobile", mobile_number))
                    raw_name = safe_str(p.get("name"))
                    raw_fname = safe_str(p.get("fname")) # Your API explicitly uses "fname"
                    raw_address = safe_str(p.get("address"))
                    
                    # (Optional) Extract new fields if you decide to add them to Appwrite schema later
                    # raw_alt = safe_str(p.get("alt"))
                    # raw_email = safe_str(p.get("email"))

                    # 2. Define what counts as "Empty" or "Bad Data"
                    # Note: "NA" appears in your example (e.g., id: "NA")
                    bad_values = ["", "N/A", "n/a", "None", "null", "NULL"]

                    # 3. Check Name, Father Name, AND Address
                    if (raw_name in bad_values) or (raw_fname in bad_values) or (raw_address in bad_values):
                        continue
                    
                    # -------------------------------------------------------
                    # ✅ DATA IS CLEAN
                    # -------------------------------------------------------
                    found_valid_data = True

                    # Clean Address formatting (Handles "!!" from your new API)
                    clean_address = raw_address.replace("!!", ", ").replace("!", ", ").replace(" ,", ",").strip()
                    # Remove leading comma if it exists after replacement
                    if clean_address.startswith(", "): clean_address = clean_address[2:]
                    
                    if len(clean_address) > 250: clean_address = clean_address[:250]

                    record = {
                        'name': raw_name,
                        'fname': raw_fname,
                        'mobile': raw_mobile,
                        'address': clean_address
                    }

                    status = save_to_appwrite(record)

                    if status == "success":
                        with stats_lock: stats["success"] += 1
                        print(f"✅ [{curr_total}] SAVED | {record['mobile']}")
                    elif status == "duplicate":
                        with stats_lock: stats["duplicates"] += 1
                        print(f"🔁 [{curr_total}] EXISTS | {record['mobile']}")

                if not found_valid_data:
                     print(f"⚠️ [{curr_total}] Skipped (Data contained N/A/Null) | {mobile_number}")

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

        time.sleep(1.5)

# ------------------------------------------------------------------
# 🌐 FLASK SERVER
# ------------------------------------------------------------------
app = Flask(__name__)

@app.route('/')
def home():
    with stats_lock:
        return f"Worker Active. Total Scanned: {stats['total']} | Success: {stats['success']}"

def run_scraper_background():
    print(f"🔥 Starting {CONCURRENT_THREADS} Pipelines (Updated API Key & Logic)...")
    with ThreadPoolExecutor(max_workers=CONCURRENT_THREADS) as executor:
        for i in range(CONCURRENT_THREADS):
            executor.submit(process_pipeline, i+1)

if __name__ == "__main__":
    threading.Thread(target=run_scraper_background, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT)
