import datetime
import pymysql


def get_src_dst_max_dt(mysql_source_image, mysql_destination_image):
    src_conn = pymysql.connect(**mysql_source_image,
                               cursorclass=pymysql.cursors.DictCursor)

    dst_conn = pymysql.connect(**mysql_destination_image,
                               cursorclass=pymysql.cursors.DictCursor)

    with src_conn:
        with src_conn.cursor() as c:
            src_query = """
                SELECT
                    MAX(dt) as src_max_dt,
                    MIN(dt) as src_min_dt
                FROM transactions
            """

            c.execute(src_query)
            src_result = c.fetchone()

    with dst_conn:
        with dst_conn.cursor() as c:
            dst_query = """
                SELECT
                    MAX(dt) as dst_max_dt
                FROM transactions_denormalized
            """

            c.execute(dst_query)
            dst_result = c.fetchone()

    src_max_result = src_result['src_max_dt'] if src_result['src_max_dt'] else datetime.datetime.min
    src_min_result = src_result['src_min_dt'] if src_result['src_min_dt'] else datetime.datetime.min

    dst_result = dst_result['dst_max_dt'] if dst_result['dst_max_dt'] else datetime.datetime.min
    dst_result = dst_result if dst_result > src_min_result else src_min_result

    return src_max_result, dst_result

