import paho.mqtt.client as mqtt
import json
import datetime
import logging
import sqlite3
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('noise_server.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class NoiseMapServer:
    def __init__(self):
        self.setup_database()
        
        # MQTT клиент
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
    def setup_database(self):
        """Настройка SQLite базы данных"""
        self.conn = sqlite3.connect('noise_data.db', check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        # Создаем таблицу если её нет
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS noise_measurements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                noise_level REAL NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                timestamp INTEGER,
                server_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()
        logging.info("✅ База данных инициализирована")
    
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("✅ Подключение к MQTT брокеру установлено")
            client.subscribe("noise/map/data")
            logging.info("📡 Подписан на тему: noise/map/data")
        else:
            logging.error(f"❌ Ошибка подключения MQTT: {rc}")
    
    def on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            logging.info(f"📨 Получены данные от {data.get('device_id', 'unknown')}")
            
            # Сохраняем в базу данных
            self.save_to_database(data)
            
        except json.JSONDecodeError as e:
            logging.error(f"❌ Ошибка парсинга JSON: {e}")
        except Exception as e:
            logging.error(f"❌ Ошибка обработки сообщения: {e}")
    
    def save_to_database(self, data):
        """Сохранение данных в базу данных"""
        try:
            self.cursor.execute('''
                INSERT INTO noise_measurements 
                (device_id, noise_level, latitude, longitude, timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                data.get('device_id'),
                data.get('noise_level'),
                data.get('latitude'),
                data.get('longitude'),
                data.get('timestamp')
            ))
            self.conn.commit()
            
            logging.info(f"💾 Данные сохранены: {data['device_id']} - {data['noise_level']} дБ")
            
        except Exception as e:
            logging.error(f"❌ Ошибка сохранения в БД: {e}")
    
    def get_statistics(self):
        """Получение статистики"""
        self.cursor.execute('''
            SELECT 
                COUNT(*) as total,
                AVG(noise_level) as avg_noise,
                MAX(noise_level) as max_noise,
                MIN(noise_level) as min_noise
            FROM noise_measurements
        ''')
        result = self.cursor.fetchone()
        
        if result and result[0] > 0:
            stats = {
                'total_measurements': result[0],
                'avg_noise_level': round(result[1], 2),
                'max_noise_level': result[2],
                'min_noise_level': result[3]
            }
            return stats
        return {}
    
    def start(self):
        """Запуск сервера"""
        logging.info("🚀 Запуск сервера шумовых карт...")
        logging.info("Для остановки нажмите Ctrl+C")
        
        try:
            self.client.connect("localhost", 1883, 60)
            self.client.loop_forever()
            
        except KeyboardInterrupt:
            logging.info("🛑 Остановка сервера...")
            self.client.disconnect()
            self.conn.close()
        except Exception as e:
            logging.error(f"❌ Ошибка сервера: {e}")

# Запуск сервера
if __name__ == "__main__":
    server = NoiseMapServer()
    server.start()