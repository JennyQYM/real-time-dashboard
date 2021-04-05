from flask import Flask, render_template
from flask_socketio import SocketIO,emit
from threading import Lock
import time
from kafka import KafkaConsumer
import json
import random


consumer = KafkaConsumer('result', bootstrap_servers=['localhost:9092'], api_version='4.2.0')
async_mode = None
app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app)
thread = None
thread_lock = Lock()

@app.route('/')
def index():
    return render_template('index.html',cors_allowed_origins="*")

@socketio.on('connect', namespace='/test')
def test_connect():
    global thread
    with thread_lock:
        if thread is None:
            thread = socketio.start_background_task(target=background_thread)

def background_thread():
    for msg in consumer:
        cpus = json.loads(msg.value.decode('utf8'))['TotalMale']
        t = time.strftime('%M:%S', time.localtime())
        if cpus != 0:
            socketio.emit('server_response',
                          {'data':[t,cpus]},namespace='/test')

    # while True:
    #     socketio.sleep(2)
    #     timestamp = time.strftime('%M:%S', time.localtime())
    #     t = psutil.cpu_percent(interval=None, percpu=True)
    #     socketio.emit('server_response',
    #                   {'data': [timestamp,t]},namespace='/test')

if __name__ == '__main__':
    socketio.run(app, debug=True)
