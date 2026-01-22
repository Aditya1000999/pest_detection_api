from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
from datetime import datetime, timezone, timedelta
import base64
import json
import cv2
import numpy as np
from roboflow import Roboflow
import paho.mqtt.client as mqtt
import threading
import os
import uuid
import ssl
import time


app = Flask(__name__)
CORS(app)

# ===== SET TIMEZONE =====
# Set environment timezone to WIB
import os
os.environ['TZ'] = 'Asia/Jakarta'
try:
    time.tzset()  # Apply timezone (Unix only)
except AttributeError:
    pass  # Windows doesn't have tzset

# ===== TIMEZONE CONFIGURATION =====
# Indonesia WIB (GMT+7)
WIB_OFFSET = timedelta(hours=7)
WIB_TZ = timezone(WIB_OFFSET)


DB_CONFIG = {
    "host": os.environ.get("MYSQLHOST", "mysql.railway.internal"),
    "port": int(os.environ.get("MYSQLPORT", 3306)),
    "user": os.environ.get("MYSQLUSER", "root"),
    "password": os.environ.get("MYSQLPASSWORD", "pkcjdgpmRJGsfHeioDCWgFgFsVIOJDbb"),
    "database": os.environ.get("MYSQLDATABASE", "railway"),
    "time_zone": "+07:00"  # ✅ SET TIMEZONE WIB
}

# ===== KONFIGURASI MQTT =====
MQTT_BROKER = os.environ.get('MQTT_BROKER', 'a96a40f3763c4eb99b42e2ed2bc5efdd.s1.eu.hivemq.cloud')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 8883))
MQTT_USERNAME = os.environ.get('MQTT_USERNAME', 'wormteam')
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD', 'Worm.1212')
MQTT_CLIENT_ID = f"pest_api_{uuid.uuid4().hex[:8]}"

# MQTT Topics
TOPIC_IMAGE = "pest/image"
TOPIC_STATUS = "pest/status"
TOPIC_COMMAND = "pest/command"
TOPIC_DETECTION = "pest/detection"

# ===== KONFIGURASI ROBOFLOW =====
ROBOFLOW_API_KEY = os.environ.get('ROBOFLOW_API_KEY', 'Frwruit34mrF3dLM4AtX')
ROBOFLOW_PROJECT = os.environ.get('ROBOFLOW_PROJECT', 'rice-pest-detection-new/1')
CONFIDENCE_THRESHOLD = int(os.environ.get('CONFIDENCE_THRESHOLD', 40))

# ===== PEST NAMES MAPPING =====
PEST_NAMES = {
    'Whitebacked_Planthopper': 'Wereng Punggung Putih',
    'bird': 'Burung (Bird)',
    'caterpillar': 'Ulat (Caterpillar)',
    'leafhoppers': 'Wereng Daun (Leafhoppers)',
    'rat': 'Tikus (Rat)',
    'snail': 'Keong Mas (Snail)',
    'weevil': 'Kumbang Moncong (Weevil)'
}

ESP32_TIMEOUT_SECONDS = 5 # ESP32 dianggap offline setelah 5 detik

# ===== GLOBAL VARIABLES =====
sent_image_ids = set()
sent_image_ids_lock = threading.Lock()
roboflow_model = None
mqtt_client = None
mqtt_connected = False
mqtt_reconnect_timer = None
esp32_status = {
    'online': False,
    'last_seen': None,
    'ldr_value': 0,
    'total_captures': 0,
    'system_enabled': True,  # ✅ NEW
    'camera_sleep_mode': False  # ✅ NEW
}

processed_messages = set()
processed_messages_lock = threading.Lock()

# ===== CHUNKED MESSAGE STORAGE =====
chunk_storage = {}
chunk_storage_lock = threading.Lock()

# ===== TIMESTAMP HELPER =====
def get_current_wib_time():
    """Get current time in WIB timezone"""
    utc_now = datetime.utcnow()
    wib_now = utc_now + WIB_OFFSET
    return wib_now

def format_detection_time(dt):
    """Format detection_time dari database ke string"""
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    return dt.strftime('%Y-%m-%d %H:%M:%S')

# ===== CHUNKED MESSAGE HANDLER =====
def cleanup_old_sessions():
    """Cleanup sessions older than 5 minutes"""
    while True:
        time.sleep(300)  # Every 5 minutes
        current_time = time.time()
        
        with chunk_storage_lock:
            sessions_to_delete = []
            for session_id, data in chunk_storage.items():
                if current_time - data['last_update'] > 300:  # 5 minutes
                    sessions_to_delete.append(session_id)
            
            for session_id in sessions_to_delete:
                print(f"🧹 Cleaning up old session: {session_id}")
                del chunk_storage[session_id]

def check_esp32_timeout():
    """
    ✅ Background thread untuk mengecek ESP32 timeout
    Mengupdate status esp32_status['online'] secara otomatis
    """
    global esp32_status
    
    print("🔍 ESP32 timeout monitor started")
    
    while True:
        time.sleep(5)  # Check every 5 seconds
        
        if esp32_status['last_seen']:
            time_diff = (get_current_wib_time() - esp32_status['last_seen']).total_seconds()
            
            # Jika lebih dari timeout
            if time_diff > ESP32_TIMEOUT_SECONDS:
                if esp32_status['online']:
                    print(f"⚠️ ESP32 TIMEOUT! Last seen {int(time_diff)}s ago")
                    esp32_status['online'] = False
            else:
                # Jika dalam batas waktu dan statusnya false, set ke true
                if not esp32_status['online']:
                    print(f"✅ ESP32 BACK ONLINE!")
                    esp32_status['online'] = True


def get_esp32_realtime_status():
    """
    ✅ Helper function untuk mendapatkan status ESP32 secara realtime
    Digunakan di berbagai endpoints
    """
    if not esp32_status['last_seen']:
        return False, None
    
    time_diff = (get_current_wib_time() - esp32_status['last_seen']).total_seconds()
    is_online = (time_diff < ESP32_TIMEOUT_SECONDS)
    
    return is_online, int(time_diff)


def handle_chunked_image(data):
    """
    ✅ Handle chunked image transmission from ESP32
    """
    session_id = data.get('session_id')
    chunk_index = data.get('chunk_index')
    total_chunks = data.get('total_chunks')
    chunk_data = data.get('data')
    is_last = data.get('is_last', False)
    
    if not all([session_id, chunk_index is not None, total_chunks, chunk_data]):
        print("⚠️ Invalid chunk data - missing required fields")
        return False
    
    with chunk_storage_lock:
        # Initialize session if new
        if session_id not in chunk_storage:
            print(f"\n📦 NEW CHUNKED SESSION: {session_id}")
            print(f"   Total chunks expected: {total_chunks}")
            chunk_storage[session_id] = {
                'chunks': {},
                'total_chunks': total_chunks,
                'received_chunks': 0,
                'start_time': time.time(),
                'last_update': time.time()
            }
        
        session = chunk_storage[session_id]
        
        # Store chunk
        if chunk_index not in session['chunks']:
            session['chunks'][chunk_index] = chunk_data
            session['received_chunks'] += 1
            session['last_update'] = time.time()
            
            print(f"   ✓ Chunk {chunk_index + 1}/{total_chunks} received " +
                  f"({session['received_chunks']}/{total_chunks})")
        else:
            print(f"   ⚠️ Duplicate chunk {chunk_index} - skipping")
        
        # Check if all chunks received
        if session['received_chunks'] == total_chunks:
            print(f"\n✅ All chunks received for session {session_id}")
            print(f"   Assembling image...")
            
            # Assemble chunks in order
            assembled_b64 = ''
            for i in range(total_chunks):
                if i not in session['chunks']:
                    print(f"   ❌ Missing chunk {i} - cannot assemble")
                    del chunk_storage[session_id]
                    return False
                assembled_b64 += session['chunks'][i]
            
            print(f"   Assembled base64 length: {len(assembled_b64)} chars")
            
            # Get metadata from last chunk
            original_size = data.get('original_size', 0)
            capture_number = data.get('capture_number', 0)
            
            # ✅ Get timestamp from ESP32
            timestamp_unix = data.get('timestamp', None)
            timestamp_str = data.get('timestamp_str', None)
            
            print(f"   Original size: {original_size} bytes")
            print(f"   Capture number: {capture_number}")
            
            if timestamp_unix:
                print(f"   📅 Unix Timestamp: {timestamp_unix}")
            if timestamp_str:
                print(f"   📅 WIB Time: {timestamp_str}")
            
            print(f"   Processing with Roboflow...")
            
            # Process the complete image
            predictions = detect_pests_roboflow(assembled_b64)
            
            # Clean up session
            del chunk_storage[session_id]
            
            if predictions is None:
                print("   ❌ Detection error")
                return False
            
            if len(predictions) == 0:
                print("   ✅ No pests detected - not saving")
                return True
            
            print(f"   🐛 {len(predictions)} pests detected!")
            
            # ✅ Save with timestamp from ESP32
            save_detection_to_db_with_timestamp(assembled_b64, predictions, timestamp_unix or timestamp_str)
            return True
        
        return False  # Not complete yet

# ===== DATABASE FUNCTIONS =====
def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as err:
        print(f"❌ Database error: {err}")
        return None

def init_database():
    print("\n🗄️  Checking database tables...")
    conn = get_db_connection()
    if not conn:
        print("❌ Cannot connect to database")
        return False
    
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS detection_summary (
                id INT AUTO_INCREMENT PRIMARY KEY,
                detection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                image_base64 LONGTEXT NOT NULL,
                total_pests_found INT DEFAULT 0,
                pest_details JSON,
                max_confidence FLOAT DEFAULT 0,
                INDEX idx_detection_time (detection_time)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS detections (
                id INT AUTO_INCREMENT PRIMARY KEY,
                summary_id INT NOT NULL,
                pest_type VARCHAR(100),
                pest_name_id VARCHAR(100),
                confidence FLOAT,
                location_x INT,
                location_y INT,
                width INT,
                height INT,
                total_pests INT DEFAULT 0,
                detection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (summary_id) REFERENCES detection_summary(id) ON DELETE CASCADE,
                INDEX idx_summary_id (summary_id),
                INDEX idx_pest_name (pest_name_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_status (
                id INT PRIMARY KEY,
                system_active BOOLEAN DEFAULT TRUE,
                total_detections INT DEFAULT 0,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS esp32_commands (
                id INT AUTO_INCREMENT PRIMARY KEY,
                command VARCHAR(50) NOT NULL,
                status ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED') DEFAULT 'PENDING',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP NULL,
                completed_at TIMESTAMP NULL,
                INDEX idx_status (status),
                INDEX idx_created (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        cursor.execute("""
            INSERT IGNORE INTO system_status (id, system_active, total_detections)
            VALUES (1, TRUE, 0)
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Database tables ready!")
        return True
        
    except Exception as e:
        print(f"❌ Database init error: {e}")
        return False

# ===== MQTT CALLBACKS =====
def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    
    if rc == 0:
        print(f"✅ Connected to MQTT Broker! (Client ID: {MQTT_CLIENT_ID})")
        mqtt_connected = True
        
        client.subscribe(TOPIC_IMAGE, qos=1)
        client.subscribe(TOPIC_STATUS, qos=1)
        client.subscribe(TOPIC_DETECTION, qos=1)
        
        print(f"📡 Subscribed to:")
        print(f"   • {TOPIC_IMAGE}")
        print(f"   • {TOPIC_STATUS}")
        print(f"   • {TOPIC_DETECTION}")
    else:
        print(f"❌ MQTT Connection failed with code {rc}")
        mqtt_connected = False

def on_disconnect(client, userdata, rc):
    global mqtt_connected, mqtt_reconnect_timer
    mqtt_connected = False
    print(f"⚠️  Disconnected from MQTT Broker (rc={rc})")
    
    if rc != 0:
        print("   Attempting to reconnect in 5 seconds...")
        mqtt_reconnect_timer = threading.Timer(5.0, attempt_mqtt_reconnect)
        mqtt_reconnect_timer.start()

def attempt_mqtt_reconnect():
    global mqtt_client, mqtt_connected
    
    if not mqtt_connected and mqtt_client:
        try:
            print("🔄 Attempting MQTT reconnect...")
            mqtt_client.reconnect()
        except Exception as e:
            print(f"❌ Reconnect failed: {e}")
            timer = threading.Timer(10.0, attempt_mqtt_reconnect)
            timer.start()

def on_message(client, userdata, msg):
    topic = msg.topic
    
    try:
        try:
            payload = msg.payload.decode('utf-8')
        except UnicodeDecodeError:
            print(f"❌ Cannot decode payload as UTF-8")
            return
        
        data = json.loads(payload)
        
        message_id = f"{topic}_{data.get('timestamp', '')}_{data.get('session_id', '')}_{data.get('chunk_index', '')}"
        
        with processed_messages_lock:
            if message_id in processed_messages:
                return  # Skip duplicate
            
            processed_messages.add(message_id)
            
            if len(processed_messages) > 2000:
                old_messages = list(processed_messages)[:1000]
                processed_messages.difference_update(old_messages)
        
        print(f"\n📨 MQTT Message: {topic}")
        
        if topic == TOPIC_IMAGE:
            handle_image_message(data)
        elif topic == TOPIC_STATUS:
            handle_status_message(data)
        elif topic == TOPIC_DETECTION:
            handle_detection_message(data)
            
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
    except Exception as e:
        print(f"❌ Error handling message: {e}")
        import traceback
        traceback.print_exc()

def handle_image_message(data):
    """Handle both chunked and non-chunked image messages"""
    
    # ✅ Check if it's a chunked message
    if 'chunk_index' in data and 'total_chunks' in data:
        print(f"   Type: CHUNKED IMAGE")
        handle_chunked_image(data)
        return
    
    # ✅ Non-chunked (from Python GUI)
    print(f"   Type: IMAGE (Non-chunked)")
    
    image_base64 = data.get('image', '')
    timestamp = data.get('timestamp', '')
    
    if not image_base64:
        print("   ⚠️ No image data")
        return
    
    print(f"   Timestamp received: {timestamp}")
    
    # ✅ Convert timestamp if Unix timestamp
    detection_time = None
    if timestamp:
        try:
            if isinstance(timestamp, (int, float)):
                # Unix timestamp - convert ke WIB
                utc_time = datetime.utcfromtimestamp(timestamp)
                detection_time = utc_time + WIB_OFFSET
                print(f"   📅 Converted: UTC {utc_time} -> WIB {detection_time}")
            elif isinstance(timestamp, str) and timestamp.isdigit():
                # Unix timestamp as string
                utc_time = datetime.utcfromtimestamp(int(timestamp))
                detection_time = utc_time + WIB_OFFSET
                print(f"   📅 Converted: UTC {utc_time} -> WIB {detection_time}")
        except:
            pass
    
    print(f"   Processing with Roboflow...")
    
    predictions = detect_pests_roboflow(image_base64)
    
    if predictions is None:
        print("   ❌ Detection error")
        return
    
    if len(predictions) == 0:
        print("   ✅ No pests detected - not saving")
        return
    
    print(f"   🐛 {len(predictions)} pests detected!")
    
    # ✅ Save dengan timestamp yang sudah dikonversi
    if detection_time:
        save_detection_to_db_with_timestamp(image_base64, predictions, detection_time)
    else:
        save_detection_to_db(image_base64, predictions)

def handle_status_message(data):
    global esp32_status
    
    print(f"   Type: STATUS")
    
    esp32_status = {
        'online': data.get('online', False),
        'last_seen': get_current_wib_time(),  # ✅ WIB time
        'ldr_value': data.get('ldr_value', 0),
        'light_ok': data.get('light_ok', False),
        'total_captures': data.get('total_captures', 0),
        'wifi_rssi': data.get('wifi_rssi', 0),
        'free_heap': data.get('free_heap', 0),
        'system_enabled': data.get('system_enabled', True),  # ✅ NEW
        'camera_sleep_mode': data.get('camera_sleep_mode', False)  # ✅ NEW
    }
    
    print(f"   ESP32 Online: {esp32_status['online']}")
    print(f"   System Enabled: {esp32_status['system_enabled']}")
    print(f"   Camera Sleep: {esp32_status['camera_sleep_mode']}")

def handle_detection_message(data):
    print(f"   Type: DETECTION (Direct Save)")
    
    image_base64 = data.get('image_base64', '')
    detection_time = data.get('detection_time', None)
    total_pests = data.get('total_pests_found', 0)
    pest_details = data.get('pest_details', [])
    max_confidence = data.get('max_confidence', 0)
    
    if not image_base64:
        print("   ⚠️ No image data")
        return
    
    if not pest_details or len(pest_details) == 0:
        print("   ⚠️ No pest details")
        return
    
    print(f"   Total Pests: {total_pests}")
    print(f"   🐛 Saving to database...")
    
    save_detection_to_db_direct(image_base64, pest_details, detection_time, max_confidence)

# ===== MQTT CLIENT SETUP =====
def init_mqtt():
    global mqtt_client

    print("🔌 Initializing MQTT...")
    print(f"🆔 CLIENT ID: {MQTT_CLIENT_ID}")

    mqtt_client = mqtt.Client(
        client_id=MQTT_CLIENT_ID,
        protocol=mqtt.MQTTv311
    )

    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqtt_client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
    mqtt_client.tls_insecure_set(False)

    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message

    mqtt_client.reconnect_delay_set(min_delay=2, max_delay=10)

    print(f"🌐 Connecting to {MQTT_BROKER}:{MQTT_PORT}")
    
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()
        print("✅ MQTT client started")
        return True
    except Exception as e:
        print(f"❌ MQTT error: {e}")
        return False

def publish_command(command):
    global mqtt_client, mqtt_connected
    
    if not mqtt_connected:
        print("❌ MQTT not connected")
        return False
    
    try:
        payload = json.dumps(command)
        result = mqtt_client.publish(TOPIC_COMMAND, payload, qos=1)
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"✅ Command published: {command}")
            return True
        else:
            print(f"❌ Publish failed: {result.rc}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

# ===== ROBOFLOW =====
def init_roboflow():
    global roboflow_model
    
    print("\n🤖 Initializing Roboflow AI...")
    
    try:
        rf = Roboflow(api_key=ROBOFLOW_API_KEY)
        project_name, version = ROBOFLOW_PROJECT.rsplit('/', 1)
        project = rf.workspace().project(project_name)
        roboflow_model = project.version(int(version)).model
        
        print("✅ Roboflow ready!")
        print(f"   Model: {ROBOFLOW_PROJECT}")
        print(f"   Confidence: {CONFIDENCE_THRESHOLD}%")
        return True
        
    except Exception as e:
        print(f"❌ Roboflow error: {e}")
        roboflow_model = None
        return False

def detect_pests_roboflow(image_base64):
    global roboflow_model
    
    if roboflow_model is None:
        print("❌ Roboflow not initialized")
        return None
    
    try:
        image_data = base64.b64decode(image_base64)
        nparr = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if img is None:
            print("❌ Failed to decode image")
            return None
        
        temp_path = "/tmp/temp_detection.jpg"
        cv2.imwrite(temp_path, img)
        
        prediction = roboflow_model.predict(
            temp_path,
            confidence=CONFIDENCE_THRESHOLD
        ).json()
        
        predictions = prediction.get('predictions', [])
        return predictions
        
    except Exception as e:
        print(f"❌ Detection error: {e}")
        return None

def save_detection_to_db(image_base64, predictions):
    """Save detection without explicit timestamp (uses database default)"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        detections = []
        max_confidence = 0
        
        for pred in predictions:
            pest_id = pred.get('class')
            confidence = pred.get('confidence', 0)
            
            pest_name = PEST_NAMES.get(pest_id, pest_id.replace('_', ' ').title())
            
            detections.append({
                'pest_type': str(pest_id),
                'pest_name_id': str(pest_name),
                'confidence': float(confidence),
                'x': int(pred.get('x', 0)),
                'y': int(pred.get('y', 0)),
                'width': int(pred.get('width', 0)),
                'height': int(pred.get('height', 0))
            })
            
            if confidence > max_confidence:
                max_confidence = confidence
        
        cursor.execute("""
            INSERT INTO detection_summary 
            (image_base64, total_pests_found, pest_details, max_confidence)
            VALUES (%s, %s, %s, %s)
        """, (
            image_base64,
            len(detections),
            json.dumps(detections),
            float(max_confidence)
        ))
        
        summary_id = cursor.lastrowid
        
        for det in detections:
            cursor.execute("""
                INSERT INTO detections 
                (summary_id, pest_type, pest_name_id, confidence, 
                 location_x, location_y, width, height, total_pests)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                summary_id,
                det['pest_type'],
                det['pest_name_id'],
                det['confidence'],
                det['x'],
                det['y'],
                det['width'],
                det['height'],
                len(detections)
            ))
        
        cursor.execute("""
            UPDATE system_status 
            SET total_detections = total_detections + 1,
                last_update = NOW()
            WHERE id = 1
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Saved: ID={summary_id}, Pests={len(detections)}")
        return True
        
    except Exception as e:
        print(f"❌ DB error: {e}")
        return False


def save_detection_to_db_with_timestamp(image_base64, predictions, timestamp_value=None):
    """Save detection WITH timestamp from ESP32"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        detections = []
        max_confidence = 0
        
        for pred in predictions:
            pest_id = pred.get('class')
            confidence = pred.get('confidence', 0)
            
            pest_name = PEST_NAMES.get(pest_id, pest_id.replace('_', ' ').title())
            
            detections.append({
                'pest_type': str(pest_id),
                'pest_name_id': str(pest_name),
                'confidence': float(confidence),
                'x': int(pred.get('x', 0)),
                'y': int(pred.get('y', 0)),
                'width': int(pred.get('width', 0)),
                'height': int(pred.get('height', 0))
            })
            
            if confidence > max_confidence:
                max_confidence = confidence
        
        # ✅ Convert timestamp if provided (DENGAN TIMEZONE WIB)
        detection_time = None
        if timestamp_value:
            try:
                if isinstance(timestamp_value, (int, float)):
                    # Unix timestamp dari ESP32
                    utc_time = datetime.utcfromtimestamp(timestamp_value)
                    detection_time = utc_time + WIB_OFFSET
                    print(f"   📅 UTC: {utc_time} -> WIB: {detection_time}")
                    
                elif isinstance(timestamp_value, str) and timestamp_value.isdigit():
                    # Unix timestamp as string
                    utc_time = datetime.utcfromtimestamp(int(timestamp_value))
                    detection_time = utc_time + WIB_OFFSET
                    print(f"   📅 UTC (str): {utc_time} -> WIB: {detection_time}")
                    
                elif isinstance(timestamp_value, str):
                    # Already formatted string (anggap sudah WIB)
                    try:
                        detection_time = datetime.strptime(timestamp_value, '%Y-%m-%d %H:%M:%S')
                        print(f"   📅 Parsed datetime string: {timestamp_value}")
                    except ValueError:
                        detection_time = None
                        print(f"   ⚠️ Failed to parse timestamp: {timestamp_value}")
                        
            except (ValueError, OSError) as e:
                print(f"   ⚠️ Timestamp conversion error: {e}")
                detection_time = None
        
        # If no valid timestamp, use current time
        if not detection_time:
            detection_time = get_current_wib_time()  # ✅ WIB time
            print(f"   📅 Using current WIB time: {detection_time}")
        
        # Insert with timestamp
        cursor.execute("""
            INSERT INTO detection_summary 
            (detection_time, image_base64, total_pests_found, pest_details, max_confidence)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            detection_time,
            image_base64,
            len(detections),
            json.dumps(detections),
            float(max_confidence)
        ))
        
        summary_id = cursor.lastrowid
        
        for det in detections:
            cursor.execute("""
                INSERT INTO detections 
                (summary_id, pest_type, pest_name_id, confidence, 
                 location_x, location_y, width, height, total_pests)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                summary_id,
                det['pest_type'],
                det['pest_name_id'],
                det['confidence'],
                det['x'],
                det['y'],
                det['width'],
                det['height'],
                len(detections)
            ))
        
        cursor.execute("""
            UPDATE system_status 
            SET total_detections = total_detections + 1,
                last_update = NOW()
            WHERE id = 1
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Saved: ID={summary_id}, Pests={len(detections)}, Time={detection_time}")
        return True
        
    except Exception as e:
        print(f"❌ DB error: {e}")
        import traceback
        traceback.print_exc()
        return False

def save_detection_to_db_direct(image_base64, pest_details, detection_time=None, max_confidence=None):
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        if max_confidence is None:
            max_confidence = max([float(p.get('confidence', 0)) for p in pest_details], default=0)
        
        # ✅ Convert Unix timestamp to datetime if needed (DENGAN TIMEZONE WIB)
        converted_time = None
        if detection_time:
            try:
                if isinstance(detection_time, (int, float)):
                    utc_time = datetime.utcfromtimestamp(detection_time)
                    converted_time = utc_time + WIB_OFFSET
                    print(f"   📅 UTC: {utc_time} -> WIB: {converted_time}")
                    
                elif isinstance(detection_time, str) and detection_time.isdigit():
                    utc_time = datetime.utcfromtimestamp(int(detection_time))
                    converted_time = utc_time + WIB_OFFSET
                    print(f"   📅 UTC (str): {utc_time} -> WIB: {converted_time}")
                    
                elif isinstance(detection_time, str):
                    try:
                        converted_time = datetime.strptime(detection_time, '%Y-%m-%d %H:%M:%S')
                        print(f"   📅 Parsed datetime string: {detection_time}")
                    except ValueError:
                        converted_time = get_current_wib_time()
                        print(f"   ⚠️ Parse failed, using current WIB time")
                        
            except (ValueError, OSError) as e:
                converted_time = get_current_wib_time()
                print(f"   ⚠️ Timestamp error: {e}, using current WIB time")
        
        if converted_time:
            cursor.execute("""
                INSERT INTO detection_summary 
                (detection_time, image_base64, total_pests_found, pest_details, max_confidence)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                converted_time,
                image_base64,
                len(pest_details),
                json.dumps(pest_details),
                float(max_confidence)
            ))
        else:
            cursor.execute("""
                INSERT INTO detection_summary 
                (image_base64, total_pests_found, pest_details, max_confidence)
                VALUES (%s, %s, %s, %s)
            """, (
                image_base64,
                len(pest_details),
                json.dumps(pest_details),
                float(max_confidence)
            ))
        
        summary_id = cursor.lastrowid
        
        for det in pest_details:
            cursor.execute("""
                INSERT INTO detections 
                (summary_id, pest_type, pest_name_id, confidence, 
                 location_x, location_y, width, height, total_pests)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                summary_id,
                det.get('pest_type', 'unknown'),
                det.get('pest_name_id', 'Unknown'),
                float(det.get('confidence', 0)),
                int(det.get('x', 0)),
                int(det.get('y', 0)),
                int(det.get('width', 0)),
                int(det.get('height', 0)),
                len(pest_details)
            ))
        
        cursor.execute("""
            UPDATE system_status 
            SET total_detections = total_detections + 1,
                last_update = NOW()
            WHERE id = 1
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Saved: ID={summary_id}")
        return True
        
    except Exception as e:
        print(f"❌ DB error: {e}")
        return False

def cleanup_sent_ids():
    while True:
        time.sleep(3600)
        with sent_image_ids_lock:
            if len(sent_image_ids) > 100:
                print(f"🧹 Cleanup sent_image_ids")
                sent_image_ids.clear()

# ===== REST API ENDPOINTS =====

@app.route('/api/trigger-capture', methods=['POST'])
def trigger_capture():
    try:
        command = {"action": "CAPTURE"}
        success = publish_command(command)
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Capture command sent'
            }), 200
        else:
            return jsonify({
                'success': False,
                'error': 'MQTT not connected'
            }), 503
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/data', methods=['GET'])
def get_data():
    global sent_image_ids, esp32_status
    
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database error'}), 500
        
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT * FROM system_status WHERE id = 1")
        status = cursor.fetchone()
        
        if not status:
            status = {'system_active': True, 'total_detections': 0}
        
        with sent_image_ids_lock:
            if sent_image_ids:
                placeholders = ','.join(['%s'] * len(sent_image_ids))
                query = f"""
                    SELECT id, detection_time, image_base64, max_confidence,
                           total_pests_found, pest_details
                    FROM detection_summary
                    WHERE id NOT IN ({placeholders})
                    ORDER BY detection_time DESC LIMIT 1
                """
                cursor.execute(query, tuple(sent_image_ids))
            else:
                cursor.execute("""
                    SELECT id, detection_time, image_base64, max_confidence,
                           total_pests_found, pest_details
                    FROM detection_summary
                    ORDER BY detection_time DESC LIMIT 1
                """)
        
        latest = cursor.fetchone()
        
        pest_names = []
        if latest:
            cursor.execute("""
                SELECT pest_name_id, MAX(confidence) as max_conf
                FROM detections 
                WHERE summary_id = %s
                GROUP BY pest_name_id
                ORDER BY max_conf DESC
            """, (latest['id'],))
            
            pest_results = cursor.fetchall()
            pest_names = [p['pest_name_id'] for p in pest_results if p['pest_name_id']]
            
            if not pest_names:
                try:
                    pest_details = json.loads(latest.get('pest_details', '[]'))
                    pest_names = list(set([d.get('pest_name_id') for d in pest_details if d.get('pest_name_id')]))
                except:
                    pest_names = ['Unknown']
        
        cursor.execute("""
            SELECT detection_time FROM detection_summary 
            ORDER BY detection_time DESC LIMIT 1
        """)
        last_record = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        # ✅ Get realtime ESP32 status
        esp32_online, last_seen_ago = get_esp32_realtime_status()
        
        response = {
            'motion': False,
            'totalDetections': status['total_detections'],
            'lastDetection': format_detection_time(last_record['detection_time']) if last_record else '-',
            'systemActive': bool(status['system_active']),
            'newDetection': False,
            'timestamp': get_current_wib_time().strftime('%Y-%m-%d %H:%M:%S'),
            'confidence': 85,
            'pestName': 'Unknown',
            'pestNames': [],
            'esp32Status': {
                'online': esp32_online,
                'lastSeen': esp32_status['last_seen'].strftime('%Y-%m-%d %H:%M:%S') if esp32_status['last_seen'] else None,
                'lastSeenSecondsAgo': last_seen_ago,
                'ldrValue': esp32_status['ldr_value'],
                'totalCaptures': esp32_status['total_captures'],
                'systemEnabled': esp32_status['system_enabled'],  # ✅ NEW
                'cameraSleepMode': esp32_status['camera_sleep_mode']  # ✅ NEW
            }
        }
        
        if latest:
            with sent_image_ids_lock:
                if latest['id'] not in sent_image_ids:
                    response['newDetection'] = True
                    response['motion'] = True
                    response['image'] = latest['image_base64']
                    response['id'] = latest['id']
                    response['detectionTime'] = format_detection_time(latest['detection_time'])
                    response['confidence'] = int(float(latest['max_confidence']) * 100) if latest['max_confidence'] else 85
                    response['pestNames'] = pest_names
                    response['pestName'] = ', '.join(pest_names) if pest_names else 'Unknown'
                    
                    sent_image_ids.add(latest['id'])
                    
                    if len(sent_image_ids) > 100:
                        sorted_ids = sorted(sent_image_ids)
                        sent_image_ids -= set(sorted_ids[:50])
        
        return jsonify(response), 200
        
    except Exception as e:
        print(f"❌ Error /data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history', methods=['GET'])
def get_history():
    try:
        limit = request.args.get('limit', 50, type=int)
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database error'}), 500
        
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT id, detection_time as timestamp, image_base64 as image,
                   max_confidence as confidence, total_pests_found, pest_details
            FROM detection_summary
            ORDER BY detection_time DESC
            LIMIT %s
        """, (limit,))
        
        history = cursor.fetchall()
        
        for item in history:
            cursor.execute("""
                SELECT pest_name_id, MAX(confidence) as max_conf
                FROM detections 
                WHERE summary_id = %s
                GROUP BY pest_name_id
                ORDER BY max_conf DESC
            """, (item['id'],))
            
            pest_results = cursor.fetchall()
            pest_names = [p['pest_name_id'] for p in pest_results if p['pest_name_id']]
            
            if not pest_names:
                try:
                    pest_details = json.loads(item.get('pest_details', '[]'))
                    pest_names = list(set([d.get('pest_name_id') for d in pest_details if d.get('pest_name_id')]))
                except:
                    pest_names = ['Unknown']
            
            item['timestamp'] = format_detection_time(item['timestamp'])
            item['confidence'] = int(float(item['confidence']) * 100) if item['confidence'] else 85
            item['motionDetected'] = True
            item['pestNames'] = pest_names
            item['pestName'] = ', '.join(pest_names) if pest_names else 'Unknown'
        
        cursor.close()
        conn.close()
        
        return jsonify(history), 200
        
    except Exception as e:
        print(f"❌ Error /api/history: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/control', methods=['POST'])
def control():
    try:
        data = request.json
        
        if 'systemActive' in data:
            conn = get_db_connection()
            if not conn:
                return jsonify({'success': False}), 500
            
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE system_status 
                SET system_active = %s, last_update = NOW()
                WHERE id = 1
            """, (bool(data['systemActive']),))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return jsonify({
                'success': True,
                'systemActive': bool(data['systemActive'])
            }), 200
        
        return jsonify({'success': False}), 400
        
    except Exception as e:
        print(f"❌ Error /control: {e}")
        return jsonify({'success': False}), 500


@app.route('/api/delete/<int:summary_id>', methods=['DELETE'])
def delete_detection(summary_id):
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'success': False}), 500
        
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM detection_summary WHERE id = %s", (summary_id,))
        if not cursor.fetchone():
            cursor.close()
            conn.close()
            return jsonify({'success': False}), 404
        
        cursor.execute("DELETE FROM detections WHERE summary_id = %s", (summary_id,))
        cursor.execute("DELETE FROM detection_summary WHERE id = %s", (summary_id,))
        cursor.execute("""
            UPDATE system_status 
            SET total_detections = GREATEST(total_detections - 1, 0)
            WHERE id = 1
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        with sent_image_ids_lock:
            sent_image_ids.discard(summary_id)
        
        return jsonify({'success': True}), 200
        
    except Exception as e:
        print(f"❌ Error /api/delete: {e}")
        return jsonify({'success': False}), 500


# ===== ✅ NEW ENDPOINTS: SYSTEM CONTROL =====

@app.route('/api/system/control', methods=['POST'])
def system_control():
    """
    ✅ NEW: Control sistem ESP32 (CAMERA SLEEP/WAKE + Database status)
    Body: {"active": true/false}
    - true: Resume camera + set system_active = true
    - false: Sleep camera + set system_active = false
    """
    try:
        data = request.json
        
        if 'active' not in data:
            return jsonify({
                'success': False,
                'error': 'Missing "active" parameter'
            }), 400
        
        system_active = bool(data['active'])
        
        # 1. Update database
        conn = get_db_connection()
        if not conn:
            return jsonify({
                'success': False,
                'error': 'Database error'
            }), 500
        
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE system_status 
            SET system_active = %s, last_update = NOW()
            WHERE id = 1
        """, (system_active,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # 2. Send MQTT command to ESP32
        mqtt_command = {
            "action": "SYSTEM_CONTROL",
            "active": system_active,
            "timestamp": get_current_wib_time().isoformat()
        }
        
        mqtt_success = False
        if mqtt_connected:
            mqtt_success = publish_command(mqtt_command)
        
        return jsonify({
            'success': True,
            'system_active': system_active,
            'mqtt_sent': mqtt_success,
            'message': f"System {'activated - camera resumed' if system_active else 'deactivated - camera sleeping'}"
        }), 200
        
    except Exception as e:
        print(f"❌ Error /api/system/control: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/system/status', methods=['GET'])
def get_system_status():
    """
    ✅ NEW: Get current system status (from database + ESP32)
    """
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database error'}), 500
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM system_status WHERE id = 1")
        status = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not status:
            status = {'system_active': True, 'total_detections': 0}
        
        # Get realtime ESP32 status
        esp32_online, last_seen_ago = get_esp32_realtime_status()
        
        return jsonify({
            'success': True,
            'system_active': bool(status['system_active']),
            'total_detections': status['total_detections'],
            'last_update': format_detection_time(status.get('last_update')),
            'esp32_online': esp32_online,
            'esp32_last_seen_seconds_ago': last_seen_ago,
            'esp32_system_enabled': esp32_status['system_enabled'],
            'esp32_camera_sleep_mode': esp32_status['camera_sleep_mode'],
            'mqtt_connected': mqtt_connected
        }), 200
        
    except Exception as e:
        print(f"❌ Error /api/system/status: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/ping', methods=['GET'])
def ping():
    db_ok = False
    try:
        conn = get_db_connection()
        if conn:
            db_ok = True
            conn.close()
    except:
        db_ok = False

    # ✅ Get realtime status
    esp32_online, last_seen_ago = get_esp32_realtime_status()

    return jsonify({
        'status': 'online',
        'database': 'connected' if db_ok else 'error',
        'timestamp': get_current_wib_time().isoformat(),
        'timezone': 'Asia/Jakarta (WIB/GMT+7)',
        'mqtt_connected': mqtt_connected,
        'mqtt_client_id': MQTT_CLIENT_ID,
        'roboflow_ready': roboflow_model is not None,
        'esp32_online': esp32_online,
        'esp32_last_seen': esp32_status['last_seen'].isoformat() if esp32_status['last_seen'] else None,
        'esp32_last_seen_seconds_ago': last_seen_ago,
        'esp32_timeout_threshold': ESP32_TIMEOUT_SECONDS,
        'esp32_system_enabled': esp32_status['system_enabled'],  # ✅ NEW
        'esp32_camera_sleep_mode': esp32_status['camera_sleep_mode'],  # ✅ NEW
        'chunked_sessions': len(chunk_storage)
    }), 200

@app.route('/api/mqtt-status', methods=['GET'])
def mqtt_status():
    # ✅ Get realtime status
    esp32_online, last_seen_ago = get_esp32_realtime_status()
    
    return jsonify({
        'connected': mqtt_connected,
        'client_id': MQTT_CLIENT_ID,
        'broker': MQTT_BROKER,
        'port': MQTT_PORT,
        'chunked_sessions_active': len(chunk_storage),
        'esp32_status': {
            'online': esp32_online,
            'ldr_value': esp32_status['ldr_value'],
            'light_ok': esp32_status.get('light_ok', False),
            'total_captures': esp32_status['total_captures'],
            'last_seen': esp32_status['last_seen'].isoformat() if esp32_status['last_seen'] else None,
            'last_seen_seconds_ago': last_seen_ago,
            'timeout_threshold': ESP32_TIMEOUT_SECONDS,
            'system_enabled': esp32_status['system_enabled'],  # ✅ NEW
            'camera_sleep_mode': esp32_status['camera_sleep_mode']  # ✅ NEW
        }
    }), 200


@app.route('/api/stats', methods=['GET'])
def get_stats():
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database error'}), 500
        
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT COUNT(*) as total FROM detection_summary")
        total = cursor.fetchone()['total']
        
        cursor.execute("""
            SELECT COUNT(*) as today 
            FROM detection_summary 
            WHERE DATE(detection_time) = CURDATE()
        """)
        today = cursor.fetchone()['today']
        
        cursor.execute("""
            SELECT pest_name_id, COUNT(*) as count
            FROM detections
            GROUP BY pest_name_id
            ORDER BY count DESC
            LIMIT 1
        """)
        top_pest = cursor.fetchone()
        
        cursor.execute("""
            SELECT pest_name_id, COUNT(*) as count
            FROM detections
            GROUP BY pest_name_id
            ORDER BY count DESC
        """)
        pest_distribution = cursor.fetchall()
        
        cursor.execute("""
            SELECT detection_time 
            FROM detection_summary 
            ORDER BY detection_time DESC 
            LIMIT 1
        """)
        last_detection = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        # ✅ Get realtime ESP32 status
        esp32_online, last_seen_ago = get_esp32_realtime_status()
        
        return jsonify({
            'total_detections': total,
            'today_detections': today,
            'most_detected_pest': top_pest['pest_name_id'] if top_pest else 'None',
            'most_detected_count': top_pest['count'] if top_pest else 0,
            'pest_distribution': pest_distribution,
            'mqtt_connected': mqtt_connected,
            'esp32_online': esp32_online,
            'esp32_last_seen_seconds_ago': last_seen_ago,
            'esp32_system_enabled': esp32_status['system_enabled'],  # ✅ NEW
            'esp32_camera_sleep_mode': esp32_status['camera_sleep_mode'],  # ✅ NEW
            'chunked_sessions': len(chunk_storage),
            'lastDetectionTime': format_detection_time(last_detection['detection_time']) if last_detection else None
        }), 200
        
    except Exception as e:
        print(f"❌ Error /api/stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/detection/<int:summary_id>', methods=['GET'])
def get_detection_by_id(summary_id):
    """Get detection detail dengan timestamp pengambilan gambar"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database error'}), 500
        
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT id, detection_time, image_base64, max_confidence,
                   total_pests_found, pest_details
            FROM detection_summary
            WHERE id = %s
        """, (summary_id,))
        
        detection = cursor.fetchone()
        
        if not detection:
            cursor.close()
            conn.close()
            return jsonify({'error': 'Detection not found'}), 404
        
        cursor.execute("""
            SELECT pest_name_id, confidence, location_x, location_y, 
                   width, height
            FROM detections 
            WHERE summary_id = %s
            ORDER BY confidence DESC
        """, (summary_id,))
        
        detections = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        response = {
            'id': detection['id'],
            'detectionTime': format_detection_time(detection['detection_time']),
            'image': detection['image_base64'],
            'totalPests': detection['total_pests_found'],
            'maxConfidence': float(detection['max_confidence']) if detection['max_confidence'] else 0,
            'pestDetails': json.loads(detection['pest_details']) if detection['pest_details'] else [],
            'detections': []
        }
        
        for det in detections:
            response['detections'].append({
                'pestName': det['pest_name_id'],
                'confidence': float(det['confidence']),
                'location': {
                    'x': det['location_x'],
                    'y': det['location_y'],
                    'width': det['width'],
                    'height': det['height']
                }
            })
        
        return jsonify(response), 200
        
    except Exception as e:
        print(f"❌ Error /api/detection/{summary_id}: {e}")
        return jsonify({'error': str(e)}), 500


# ===== INITIALIZE ON STARTUP =====
print("\n" + "="*60)
print("  🐛 PEST DETECTION API - CAMERA SLEEP MODE")
print("  ESP32 stays connected, camera can sleep")
print("="*60)

print("\n📦 Initializing...")

db_ok = init_database()
roboflow_ok = init_roboflow()
mqtt_ok = init_mqtt()

# Start cleanup threads
cleanup_thread = threading.Thread(target=cleanup_sent_ids, daemon=True)
cleanup_thread.start()

session_cleanup_thread = threading.Thread(target=cleanup_old_sessions, daemon=True)
session_cleanup_thread.start()

esp32_timeout_thread = threading.Thread(target=check_esp32_timeout, daemon=True)
esp32_timeout_thread.start()

print("✅ ESP32 timeout monitor started")

print("\n" + "="*60)
if db_ok and roboflow_ok and mqtt_ok:
    print("  ✅ ALL SYSTEMS READY!")
else:
    print("  ⚠️ SOME SYSTEMS FAILED")
    if not db_ok:
        print("     ❌ Database")
    if not roboflow_ok:
        print("     ❌ Roboflow")
    if not mqtt_ok:
        print("     ❌ MQTT")
print("="*60)

print(f"\n🐛 Pest Types ({len(PEST_NAMES)}):")
for i, (key, name) in enumerate(PEST_NAMES.items(), 1):
    print(f"   {i}. {name}")

print(f"\n📡 MQTT Config:")
print(f"   Broker: {MQTT_BROKER}:{MQTT_PORT}")
print(f"   Client: {MQTT_CLIENT_ID}")
print(f"   Mode: Camera Sleep Control")
print(f"   Topics: {TOPIC_IMAGE}, {TOPIC_STATUS}, {TOPIC_DETECTION}, {TOPIC_COMMAND}")

print(f"\n🌐 API Endpoints:")
print(f"   POST   /api/trigger-capture")
print(f"   POST   /api/system/control        ✅ NEW")
print(f"   GET    /api/system/status         ✅ NEW")
print(f"   GET    /data")
print(f"   GET    /api/history")
print(f"   POST   /control")
print(f"   DELETE /api/delete/<id>")
print(f"   GET    /api/detection/<id>")
print(f"   GET    /ping")
print(f"   GET    /api/mqtt-status")
print(f"   GET    /api/stats")
print("="*60 + "\n")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"🚀 Starting on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)