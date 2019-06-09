import atexit
import psycopg2


def connect() -> psycopg2:
    print('Connecting to db')
    user: str = 'admin'
    password: str = ''
    host: str = 'localhost'
    port: str = '5432'
    database: str = 'youtube'

    return psycopg2.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database)


def main() -> None:
    conn: psycopg2 = connect()

    def exit_func() -> None:
        print('Closing connection')
        conn.close()

    atexit.register(exit_func)


if __name__ == '__main__':
    main()
