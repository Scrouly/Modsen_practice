from mysql.connector import connect


def connect_to_db():
    """"Соединение с MySQL."""
    sql = connect(
        host="localhost",
        user='root',
        password='root',
        database='documents'
    )
    if sql.is_connected():
        print('MySQL is connected')
        return sql
    else:
        print('MySQL could not connect!')
        return None
