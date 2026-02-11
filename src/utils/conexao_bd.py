import mysql.connector
from mysql.connector import Error

def conectar():
    try:
        connection = mysql.connector.connect(
            host='45.179.90.60',
            port=7513,
            user='lille',
            password='lille@datime2026',
            database='lille'
        )

        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Conectado ao servidor MySQL versão: {db_info}")
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print(f"Você está conectado ao banco: {record}")

    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexão encerrada.")

if __name__ == "__main__":
    conectar()