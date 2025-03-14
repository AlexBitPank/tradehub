import os
import time
from flask import Flask, render_template, jsonify
from flask_httpauth import HTTPBasicAuth
import mysql.connector
from mysql.connector import Error

app = Flask(__name__)
auth = HTTPBasicAuth()

# Чтение данных о пользователях из переменных окружения
users = {
    os.getenv("ADMIN_USER"): os.getenv("ADMIN_PASSWORD")
}

# Функция для аутентификации
@auth.verify_password
def verify_password(username, password):
    if username in users and users[username] == password:
        return username

# Подключение к базе данных с повторными попытками
def get_db_connection():
    max_attempts = 5
    attempt = 0
    while attempt < max_attempts:
        try:
            connection = mysql.connector.connect(
                host=os.getenv("DB_HOST"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                database=os.getenv("DB_NAME")
            )
            if connection.is_connected():
                return connection
        except Error as err:
            print(f"Attempt {attempt + 1}: Error while connecting to MySQL: {err}")
            time.sleep(5)  # Ждем 5 секунд перед повторной попыткой
            attempt += 1

    print("Failed to connect to the database after multiple attempts.")
    return None

# Главная страница
@app.route('/')
@auth.login_required  # Ограничиваем доступ к главной странице
def index():
    return render_template('index.html')

# Получение логов
@app.route('/logs')
@auth.login_required  # Ограничиваем доступ к логам
def logs():
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    
    # Получаем значение log_limit из таблицы settings
    cursor.execute("SELECT setting_value FROM settings WHERE setting_key = 'log_limit';")
    result = cursor.fetchone()
    
    # Устанавливаем лимит, если настройка найдена и значение корректное, иначе используем 300 по умолчанию
    log_limit = int(result['setting_value']) if result and result['setting_value'].isdigit() else 300

    # Выполняем запрос с использованием лимита
    query = (
        "SELECT * FROM "
        "(SELECT * FROM open_interest_log ORDER BY log_time DESC LIMIT %s) AS recent_logs "
        "ORDER BY log_time;"
    )
    cursor.execute(query, (log_limit,))
    logs = cursor.fetchall()
    
    cursor.close()
    connection.close()
    
    return jsonify(logs)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
