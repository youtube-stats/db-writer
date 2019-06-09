import atexit
import flask
from message import message_pb2
import psycopg2

app: flask.Flask = flask.Flask(__name__)
user: str = 'admin'
password: str = ''
pg_host: str = 'localhost'
pg_port: str = '5432'
database: str = 'youtube'
server_host: str = '0.0.0.0'
server_port: str = '8081'


def init_ack_message() -> str:
    msg_obj: message_pb2.Ack = message_pb2.Ack()
    msg_obj.ok = True
    return msg_obj.SerializeToString()


message = init_ack_message()


@app.route('/load', methods=['POST'])
def hello() -> str:
    return message


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
