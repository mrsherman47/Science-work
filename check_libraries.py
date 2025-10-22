print("=== ПРОВЕРКА БИБЛИОТЕК ===")

# Проверка встроенных библиотек
try:
    import sqlite3
    print("✅ SQLite3 - ВСТРОЕННАЯ (уже установлена)")
except ImportError:
    print("❌ SQLite3 - ошибка")

try:
    import datetime
    print("✅ datetime - ВСТРОЕННАЯ (уже установлена)")
except ImportError:
    print("❌ datetime - ошибка")

try:
    import logging
    print("✅ logging - ВСТРОЕННАЯ (уже установлена)")
except ImportError:
    print("❌ logging - ошибка")

# Проверка устанавливаемых библиотек
try:
    import paho.mqtt.client as mqtt
    print("✅ paho-mqtt - УСТАНОВЛЕНА")
except ImportError:
    print("❌ paho-mqtt - НЕ установлена")

print("\n=== ВСЁ ГОТОВО К РАБОТЕ ===")
input("Нажмите Enter для выхода...")