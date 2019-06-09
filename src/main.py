import atexit
import datetime
import flask
from message import message_pb2
import psycopg2
import pytz
import queue
import threading
from typing import List


class SubRow:
    def __init__(self, time: datetime.datetime, idx: int, subs: int):
        self.time: datetime.datetime = time
        self.idx: int = idx
        self.subs: int = subs


class SubStore:
    def __init__(self):
        self.store: List[SubRow] = []

    def append(self, sub: SubRow):
        self.store.append(sub)

    def len(self):
        return len(self.store)


app: flask.Flask = flask.Flask(__name__)
message_queue: queue.Queue = queue.Queue()
user: str = 'admin'
password: str = ''
pg_host: str = 'localhost'
pg_port: str = '5432'
database: str = 'youtube'
server_host: str = '0.0.0.0'
server_port: str = '8081'
tz = pytz.timezone('America/Los_Angeles')
write_threshold: int = 500
store: SubStore = SubStore()


def init_ack_message() -> str:
    msg_obj: message_pb2.Ack = message_pb2.Ack()
    msg_obj.ok = True
    return msg_obj.SerializeToString()


message_ack = init_ack_message()


@app.route('/post', methods=['POST'])
def hello() -> str:
    sub_message: message_pb2.SubMessage = message_pb2.SubMessage()
    sub_message.ParseFromString(flask.request.data)
    print('Got message', sub_message.SerializeToString())

    message_queue.put_nowait(sub_message)

    return message_ack


def connect() -> psycopg2:
    print('Connecting to db')
    conn: psycopg2 = psycopg2.connect(
        user=user,
        password=password,
        host=pg_host,
        port=pg_port,
        database=database)

    def exit_func() -> None:
        print('Closing connection')
        conn.close()

    atexit.register(exit_func)
    return conn


def append_to_store(payload: message_pb2.SubMessage) -> None:
    length: int = len(payload.idx)
    time: datetime.datetime = datetime.datetime.fromtimestamp(payload.time, tz)

    for i in range(length):
        idx: int = payload.idx[i]
        sub: int = payload.subs[i]

        row: SubRow = SubRow(time, idx, sub)
        store.append(row)


def write_payload_handler(payload: message_pb2.SubMessage) -> None:
    append_to_store(payload)
    print('Size of store is ', store.len())


def write_daemon() -> None:
    while True:
        print('Write daemon - waiting for message')
        payload: message_pb2.SubMessage = message_queue.get(block=True)
        print('Write daemon - Got message')
        write_payload_handler(payload)


def main() -> None:
    conn: psycopg2 = connect()
    threading.Thread(target=write_daemon, daemon=False).start()

    app.run(host=server_host, port=server_port, debug=True)


if __name__ == '__main__':
    main()
