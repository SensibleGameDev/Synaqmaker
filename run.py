from app import app, socketio
import configparser

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')
    
    # Считываем хост и порт, если они есть, иначе используем defaults
    HOST = config.get('server', 'HOST', fallback='0.0.0.0')
    PORT = config.getint('server', 'PORT', fallback=5000)
    
    print(f"INFO: Запуск SocketIO сервера на http://{HOST}:{PORT}")
    
    # ### ИЗМЕНЕНИЕ: Используем socketio.run вместо app.run ###
    socketio.run(app, 
                 host=HOST, 
                 port=PORT, 
                 debug=False, 
                 allow_unsafe_werkzeug=True # Необходимо для reloader в режиме debug
                 )