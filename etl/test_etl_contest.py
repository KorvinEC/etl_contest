import pymysql
from .utils import get_src_dst_max_dt
from datetime import timedelta


def test_data_transfer(mysql_source_image,
                       mysql_destination_image):
    """
    :param mysql_source_image: Контейнер mysql-источника с исходными данными
    :param mysql_destination_image: Контейнер mysql-назначения
    :return:
    """

    src_conn = pymysql.connect(**mysql_source_image,
                               cursorclass=pymysql.cursors.DictCursor)

    dst_conn = pymysql.connect(**mysql_destination_image,
                               cursorclass=pymysql.cursors.DictCursor)

    src_max_dt, dst_max_dt = get_src_dst_max_dt(mysql_source_image, mysql_destination_image)

    with src_conn, dst_conn:
        with src_conn.cursor() as c_src, dst_conn.cursor() as c_dst:
            while src_max_dt > dst_max_dt - timedelta(milliseconds=1):
                new_dst_max_dt = dst_max_dt + timedelta(hours=1)

                src_query = """
                    SELECT
                        t.id, dt, idoper, move, amount, name as name_oper
                    FROM transactions t
                        JOIN operation_types ot ON t.idoper = ot.id
                    WHERE
                        dt >= %s AND dt <= %s
                """

                c_src.execute(src_query, (dst_max_dt, new_dst_max_dt - timedelta(milliseconds=1)))
                src_result = c_src.fetchall()

                dst_query = """
                    INSERT INTO
                        transactions_denormalized
                        (id, dt, idoper, move, amount, name_oper)
                    VALUES
                        (%(id)s, %(dt)s, %(idoper)s, %(move)s, %(amount)s, %(name_oper)s)
                """

                c_dst.executemany(dst_query, src_result)

                dst_max_dt = new_dst_max_dt

            src_query = """
                SELECT
                    t.id, dt, idoper, move, amount, name as name_oper
                FROM transactions t
                    JOIN operation_types ot ON t.idoper = ot.id
                ORDER BY t.id

            """

            c_src.execute(src_query)
            src_result = c_src.fetchall()

            dst_query = """
                SELECT
                    *
                FROM transactions_denormalized
                ORDER BY id
            """

            c_dst.execute(dst_query)
            dst_result = c_dst.fetchall()

    assert src_result == dst_result
