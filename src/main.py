import atexit
import flask
import message
import psycopg2
import queue

app: flask.Flask = flask.Flask(__name__)
message_queue: queue.Queue = queue.Queue()
user: str = 'admin'
password: str = ''
pg_host: str = 'localhost'
pg_port: str = '5432'
database: str = 'youtube'
server_host: str = '0.0.0.0'
server_port: str = '8081'


def init_ack_message() -> str:
    msg_obj: message.message_pb2.Ack = message.message_pb2.Ack()
    msg_obj.ok = True
    return msg_obj.SerializeToString()


message_ack = init_ack_message()


@app.route('/load', methods=['POST'])
def hello() -> str:
    sub_message: message.message_pb2.SubMessage = message.message_pb2.SubMessage()
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


def main() -> None:
    conn: psycopg2 = connect()
    app.run(host=server_host, port=server_port, debug=True)


if __name__ == '__main__':
    main()
