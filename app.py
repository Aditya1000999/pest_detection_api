# hahahahhahahahahaahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahahaha

from flask import Flask, request, jsonify
from flask_cors import CORS
import mysql.connector
from datetime import datetime
import base64
import json
import cv2
import numpy as np
from roboflow import Roboflow
import paho.mqtt.client as mqtt
import threading
import os

app = Flask(__name__)
CORS(app)

# ===== KONFIGURASI DATABASE RAILWAY =====
DB_CONFIG = {
    'host': os.environ.get('MYSQLHOST', 'mysql.railway.internal'),
    'port': int(os.environ.get('MYSQLPORT', 3306)),
    'user': os.environ.get('MYSQLUSER', 'root'),
    'password': os.environ.get('MYSQL_ROOT_PASSWORD', ''),
    'database': os.environ.get('MYSQL_DATABASE', 'railway')
}

# ===== KONFIGURASI MQTT =====
MQTT_BROKER = os.environ.get('MQTT_BROKER', 'broker.hivemq.com')
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_USERNAME = os.environ.get('MQTT_USERNAME', '')
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD', '')
MQTT_CLIENT_ID = "pest_detection_api"

# MQTT Topics
TOPIC_IMAGE = "pest/image"
TOPIC_STATUS = "pest/status"
TOPIC_COMMAND = "pest/command"

# ===== KONFIGURASI ROBOFLOW =====
ROBOFLOW_API_KEY = os.environ.get('ROBOFLOW_API_KEY', 'Frwruit34mrF3dLM4AtX')
ROBOFLOW_PROJECT = os.environ.get('ROBOFLOW_PROJECT', 'rice-pest-detection-new/1')
CONFIDENCE_THRESHOLD = int(os.environ.get('CONFIDENCE_THRESHOLD', 40))

# ===== PEST NAMES MAPPING (8 HAMA SAJA) =====
PEST_NAMES = {
    'bacterial_blight': 'Hawar Bakteri (Bacterial Blight)',
    'blast': 'Blas (Blast)',
    'brown_spot': 'Bercak Coklat (Brown Spot)',
    'tungro': 'Tungro',
    'Wereng_Daun': 'Wereng Daun',
    'snail': 'Keong Mas (Snail)',
    'kumbang_Penggerek': 'Kumbang Penggerek',
    'rat': 'Tikus (Rat)'
}

# ===== GLOBAL VARIABLES =====
sent_image_ids = set()
roboflow_model = None
mqtt_client = None
mqtt_connected = False
esp32_status = {
    'online': False,
    'last_seen': None,
    'ldr_value': 0,
    'total_captures': 0
}

# ===== DATABASE FUNCTIONS =====
def get_db_connection():
    """Koneksi ke database Railway"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as err:
        print(f"❌ Database error: {err}")
        print(f"   Config: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        return None

def init_database():
    """Initialize database tables jika belum ada"""
    print("\n🗄️  Checking database tables...")
    
    conn = get_db_connection()
    if not conn:
        print("❌ Cannot connect to database")
        return False
    
    cursor = conn.cursor()
    
    try:
        # Create detection_summary table
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
        
        # Create detections table
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
        
        # Create system_status table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_status (
                id INT PRIMARY KEY,
                system_active BOOLEAN DEFAULT TRUE,
                total_detections INT DEFAULT 0,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Create esp32_commands table
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
        
        # Insert default system status
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
    """Callback saat connect ke MQTT broker"""
    global mqtt_connected
    
    if rc == 0:
        print("✅ Connected to MQTT Broker!")
        mqtt_connected = True
        
        # Subscribe ke topics
        client.subscribe(TOPIC_IMAGE, qos=1)
        client.subscribe(TOPIC_STATUS, qos=1)
        
        print(f"📡 Subscribed to:")
        print(f"   • {TOPIC_IMAGE}")
        print(f"   • {TOPIC_STATUS}")
    else:
        print(f"❌ MQTT Connection failed with code {rc}")
        mqtt_connected = False

def on_disconnect(client, userdata, rc):
    """Callback saat disconnect dari MQTT"""
    global mqtt_connected
    mqtt_connected = False
    print(f"⚠️  Disconnected from MQTT Broker (rc={rc})")
    
    if rc != 0:
        print("   Attempting to reconnect...")

def on_message(client, userdata, msg):
    """Callback saat menerima message dari MQTT"""
    topic = msg.topic
    payload = msg.payload.decode('utf-8')
    
    print(f"\n📨 MQTT Message Received:")
    print(f"   Topic: {topic}")
    print(f"   Size: {len(payload)} bytes")
    
    try:
        data = json.loads(payload)
        
        if topic == TOPIC_IMAGE:
            handle_image_message(data)
        elif topic == TOPIC_STATUS:
            handle_status_message(data)
            
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
    except Exception as e:
        print(f"❌ Error handling message: {e}")

def handle_image_message(data):
    """Handle image message dari ESP32"""
    print(f"   Type: IMAGE")
    
    image_base64 = data.get('image', '')
    timestamp = data.get('timestamp', '')
    
    if not image_base64:
        print("   ⚠️ No image data")
        return
    
    print(f"   Timestamp: {timestamp}")
    print(f"   Processing with Roboflow...")
    
    # Deteksi hama
    predictions = detect_pests_roboflow(image_base64)
    
    if not predictions or len(predictions) == 0:
        print("   ✅ No pests detected - not saving")
        return
    
    print(f"   🐛 {len(predictions)} pests detected!")
    
    # Simpan ke database
    save_detection_to_db(image_base64, predictions)

def handle_status_message(data):
    """Handle status message dari ESP32"""
    global esp32_status
    
    print(f"   Type: STATUS")
    
    esp32_status = {
        'online': data.get('online', False),
        'last_seen': datetime.now(),
        'ldr_value': data.get('ldr_value', 0),
        'light_ok': data.get('light_ok', False),
        'total_captures': data.get('total_captures', 0),
        'wifi_rssi': data.get('wifi_rssi', 0),
        'free_heap': data.get('free_heap', 0)
    }
    
    print(f"   ESP32 Status:")
    print(f"     • Online: {esp32_status['online']}")
    print(f"     • LDR: {esp32_status['ldr_value']}")
    print(f"     • Light OK: {esp32_status['light_ok']}")
    print(f"     • Captures: {esp32_status['total_captures']}")

# ===== MQTT CLIENT SETUP =====
def init_mqtt():
    """Initialize MQTT client"""
    global mqtt_client
    
    print("\n🔌 Initializing MQTT Client...")
    print(f"   Broker: {MQTT_BROKER}:{MQTT_PORT}")
    
    mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message
    
    # Set username/password jika ada
    if MQTT_USERNAME and MQTT_PASSWORD:
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        print(f"   Auth: Enabled (user: {MQTT_USERNAME})")
    
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        # Start loop in background thread
        mqtt_thread = threading.Thread(target=mqtt_client.loop_forever, daemon=True)
        mqtt_thread.start()
        
        print("✅ MQTT Client started in background")
        return True
        
    except Exception as e:
        print(f"❌ MQTT Connection error: {e}")
        return False

def publish_command(command):
    """Publish command ke ESP32 via MQTT"""
    global mqtt_client, mqtt_connected
    
    if not mqtt_connected:
        print("❌ MQTT not connected, cannot send command")
        return False
    
    try:
        payload = json.dumps(command)
        result = mqtt_client.publish(TOPIC_COMMAND, payload, qos=1)
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"✅ Command published to {TOPIC_COMMAND}")
            print(f"   Payload: {payload}")
            return True
        else:
            print(f"❌ Publish failed with rc={result.rc}")
            return False
            
    except Exception as e:
        print(f"❌ Error publishing command: {e}")
        return False

# ===== ROBOFLOW =====
def init_roboflow():
    """Initialize Roboflow model"""
    global roboflow_model
    
    print("\n🤖 Initializing Roboflow AI...")
    
    try:
        rf = Roboflow(api_key=ROBOFLOW_API_KEY)
        project_name, version = ROBOFLOW_PROJECT.rsplit('/', 1)
        project = rf.workspace().project(project_name)
        roboflow_model = project.version(int(version)).model
        
        print("✅ Roboflow model ready!")
        print(f"   Model: {ROBOFLOW_PROJECT}")
        print(f"   Confidence: {CONFIDENCE_THRESHOLD}%")
        print(f"   Pest Types: {len(PEST_NAMES)}")
        print(f"   Pests: {', '.join(PEST_NAMES.values())}")
        return True
        
    except Exception as e:
        print(f"❌ Roboflow init error: {e}")
        return False

def detect_pests_roboflow(image_base64):
    """Deteksi hama menggunakan Roboflow AI"""
    global roboflow_model
    
    if roboflow_model is None:
        return []
    
    try:
        # Decode base64 to image
        image_data = base64.b64decode(image_base64)
        nparr = np.frombuffer(image_data, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if img is None:
            return []
        
        # Save temporary file
        temp_path = "/tmp/temp_detection.jpg"
        cv2.imwrite(temp_path, img)
        
        # Run detection
        prediction = roboflow_model.predict(
            temp_path,
            confidence=CONFIDENCE_THRESHOLD
        ).json()
        
        predictions = prediction.get('predictions', [])
        return predictions
        
    except Exception as e:
        print(f"❌ Detection error: {e}")
        return []

def save_detection_to_db(image_base64, predictions):
    """Simpan hasil deteksi ke database"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Build detections
        detections = []
        max_confidence = 0
        
        for pred in predictions:
            pest_id = pred.get('class')
            confidence = pred.get('confidence', 0)
            
            # Get pest name dari mapping
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
        
        # Insert ke detection_summary
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
        
        # Insert detail deteksi
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
        
        # Update system status
        cursor.execute("""
            UPDATE system_status 
            SET total_detections = total_detections + 1,
                last_update = NOW()
            WHERE id = 1
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Saved to database: ID={summary_id}, Pests={len(detections)}")
        print(f"   Detected: {', '.join([d['pest_name_id'] for d in detections])}")
        return True
        
    except Exception as e:
        print(f"❌ Database save error: {e}")
        return False

# ===== REST API ENDPOINTS =====

@app.route('/api/trigger-capture', methods=['POST'])
def trigger_capture():
    """Flutter trigger manual capture via MQTT"""
    try:
        command = {"action": "CAPTURE"}
        success = publish_command(command)
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Capture command sent to ESP32 via MQTT'
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
    """Flutter ambil data deteksi terbaru"""
    global sent_image_ids, esp32_status
    
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database error'}), 500
        
        cursor = conn.cursor(dictionary=True)
        
        # Get system status
        cursor.execute("SELECT * FROM system_status WHERE id = 1")
        status = cursor.fetchone()
        
        if not status:
            status = {'system_active': True, 'total_detections': 0}
        
        # Get latest detection
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
        
        # Get pest names
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
                    pest_names = ['Unknown Pest']
        
        cursor.execute("""
            SELECT detection_time FROM detection_summary 
            ORDER BY detection_time DESC LIMIT 1
        """)
        last_record = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        # Build response
        response = {
            'motion': False,
            'totalDetections': status['total_detections'],
            'lastDetection': last_record['detection_time'].strftime('%Y-%m-%d %H:%M:%S') if last_record else '-',
            'systemActive': bool(status['system_active']),
            'newDetection': False,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'confidence': 85,
            'pestName': 'Unknown Pest',
            'pestNames': [],
            'esp32Status': {
                'online': esp32_status['online'],
                'lastSeen': esp32_status['last_seen'].strftime('%Y-%m-%d %H:%M:%S') if esp32_status['last_seen'] else None,
                'ldrValue': esp32_status['ldr_value'],
                'totalCaptures': esp32_status['total_captures']
            }
        }
        
        # Send new detection if available
        if latest and latest['id'] not in sent_image_ids:
            response['newDetection'] = True
            response['motion'] = True
            response['image'] = latest['image_base64']
            response['id'] = latest['id']
            response['confidence'] = int(float(latest['max_confidence']) * 100) if latest['max_confidence'] else 85
            response['pestNames'] = pest_names
            response['pestName'] = ', '.join(pest_names) if pest_names else 'Unknown Pest'
            
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
    """Flutter ambil riwayat deteksi"""
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
                    pest_names = ['Unknown Pest']
            
            item['timestamp'] = item['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            item['confidence'] = int(float(item['confidence']) * 100) if item['confidence'] else 85
            item['motionDetected'] = True
            item['pestNames'] = pest_names
            item['pestName'] = ', '.join(pest_names) if pest_names else 'Unknown Pest'
        
        cursor.close()
        conn.close()
        
        return jsonify(history), 200
        
    except Exception as e:
        print(f"❌ Error /api/history: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/control', methods=['POST'])
def control():
    """Flutter control system on/off"""
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
    """Flutter hapus deteksi"""
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
        
        global sent_image_ids
        sent_image_ids.discard(summary_id)
        
        return jsonify({'success': True}), 200
        
    except Exception as e:
        print(f"❌ Error /api/delete: {e}")
        return jsonify({'success': False}), 500

@app.route('/ping', methods=['GET'])
def ping():
    """Health check"""
    return jsonify({
        'status': 'online',
        'timestamp': datetime.now().isoformat(),
        'mqtt_connected': mqtt_connected,
        'roboflow_ready': roboflow_model is not None,
        'esp32_online': esp32_status['online'],
        'database': 'railway',
        'pest_types': len(PEST_NAMES)
    }), 200

@app.route('/api/mqtt-status', methods=['GET'])
def mqtt_status():
    """Get MQTT connection status"""
    return jsonify({
        'connected': mqtt_connected,
        'broker': MQTT_BROKER,
        'port': MQTT_PORT,
        'esp32_status': {
            'online': esp32_status['online'],
            'ldr_value': esp32_status['ldr_value'],
            'light_ok': esp32_status.get('light_ok', False),
            'total_captures': esp32_status['total_captures'],
            'last_seen': esp32_status['last_seen'].isoformat() if esp32_status['last_seen'] else None
        }
    }), 200

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get detection statistics"""
    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database error'}), 500
        
        cursor = conn.cursor(dictionary=True)
        
        # Total detections
        cursor.execute("SELECT COUNT(*) as total FROM detection_summary")
        total = cursor.fetchone()['total']
        
        # Today's detections
        cursor.execute("""
            SELECT COUNT(*) as today 
            FROM detection_summary 
            WHERE DATE(detection_time) = CURDATE()
        """)
        today = cursor.fetchone()['today']
        
        # Top pest
        cursor.execute("""
            SELECT pest_name_id, COUNT(*) as count
            FROM detections
            GROUP BY pest_name_id
            ORDER BY count DESC
            LIMIT 1
        """)
        top_pest = cursor.fetchone()
        
        # Pest distribution
        cursor.execute("""
            SELECT pest_name_id, COUNT(*) as count
            FROM detections
            GROUP BY pest_name_id
            ORDER BY count DESC
        """)
        pest_distribution = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'total_detections': total,
            'today_detections': today,
            'most_detected_pest': top_pest['pest_name_id'] if top_pest else 'None',
            'most_detected_count': top_pest['count'] if top_pest else 0,
            'pest_distribution': pest_distribution,
            'mqtt_connected': mqtt_connected,
            'esp32_online': esp32_status['online']
        }), 200
        
    except Exception as e:
        print(f"❌ Error /api/stats: {e}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    print("\n" + "="*60)
    print("  🐛 PEST DETECTION API WITH MQTT")
    print("  8 Rice Pest Types Detection System")
    print("="*60)
    
    # Initialize components
    print("\n📦 Initializing components...")
    init_database()
    init_roboflow()
    init_mqtt()
    
    print("\n" + "="*60)
    print("  ✅ API READY!")
    print("="*60)
    
    print(f"\n🐛 Pest Types ({len(PEST_NAMES)}):")
    for i, (key, name) in enumerate(PEST_NAMES.items(), 1):
        print(f"   {i}. {name}")
    
    print(f"\n📡 MQTT Topics:")
    print(f"   Subscribe:")
    print(f"     • {TOPIC_IMAGE} (ESP32 → API)")
    print(f"     • {TOPIC_STATUS} (ESP32 → API)")
    print(f"   Publish:")
    print(f"     • {TOPIC_COMMAND} (API → ESP32)")
    
    print(f"\n🌐 REST API Endpoints:")
    print(f"   • POST   /api/trigger-capture")
    print(f"   • GET    /data")
    print(f"   • GET    /api/history")
    print(f"   • POST   /control")
    print(f"   • DELETE /api/delete/<id>")
    print(f"   • GET    /ping")
    print(f"   • GET    /api/mqtt-status")
    print(f"   • GET    /api/stats")
    print("="*60 + "\n")
    
    app.run(host='0.0.0.0', port=5000, debug=True)
    print(f"\n🚪 Shutting down API...")