import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import io
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'

def getopenconnection(user='postgres', password='712204', dbname='postgres', host='localhost'):
    connection = psycopg2.connect(
        host=host,
        database=dbname,
        user=user,
        password=password
    )
    connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
    return connection


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    con = openconnection
    cur = con.cursor()
    
    # Tạo bảng với cấu trúc giống dữ liệu trong rating.dat
    cur.execute(f"""
    DROP TABLE IF EXISTS {ratingstablename} CASCADE;
    
    CREATE TABLE {ratingstablename} (
        userid INTEGER, 
        extra1 CHAR, 
        movieid INTEGER, 
        extra2 CHAR, 
        rating FLOAT, 
        extra3 CHAR, 
        timestamp BIGINT
    )
    """)
        
    # Copy data từ file
    cur.copy_from(open(ratingsfilepath, 'r'), ratingstablename, sep=':')
        
    # Drop các cột :
    cur.execute(f"""
    ALTER TABLE {ratingstablename}
    DROP COLUMN extra1,
    DROP COLUMN extra2,
    DROP COLUMN extra3,
    DROP COLUMN timestamp
    """)
        
    con.commit()
    cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    
    try:
        for i in range(numberofpartitions):
            cur.execute(f"DROP TABLE IF EXISTS {RANGE_TABLE_PREFIX}{i} CASCADE;")
        
        range_size = 5.0 / numberofpartitions
        
        for i in range(numberofpartitions):
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
            cur.execute(f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
            """)

        for i in range(numberofpartitions):
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
            min_rating = i * range_size
            max_rating = min_rating +range_size
            
            if i == 0:
                cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM {ratingstablename}
                WHERE rating >= {min_rating} AND rating <= {max_rating}
                """)
            else:
                cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM {ratingstablename}
                WHERE rating > {min_rating} AND rating <= {max_rating}
                """)
        
        con.commit()
        
    except Exception:
        con.rollback()
    finally:
        cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    
    try:
        # Xóa các bảng phân mảnh cũ
        for i in range(numberofpartitions):
            cur.execute(f"DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}{i} CASCADE;")
        
        # Tạo các bảng phân mảnh mới
        for i in range(numberofpartitions):
            table_name = f"{RROBIN_TABLE_PREFIX}{i}"
            cur.execute(f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
            """)
        
        # Phân mảnh dữ liệu
        for i in range(numberofpartitions):
            cur.execute(f"""
            INSERT INTO {RROBIN_TABLE_PREFIX}{i} (userid, movieid, rating)
            SELECT userid, movieid, rating
            FROM (
                SELECT *, ROW_NUMBER() OVER() as rnum
                FROM {ratingstablename}
            ) as temp
            WHERE MOD(temp.rnum - 1, {numberofpartitions}) = {i}
            """)
        
        # Đếm tổng số bản ghi trong bảng gốc
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
        total_records = cur.fetchone()[0]
        
        # Tạo bảng counter và đặt giá trị bằng số bản ghi đã phân mảnh
        cur.execute("""
        CREATE TABLE IF NOT EXISTS round_robin_counter (
            counter BIGINT DEFAULT 0
        );
        """)
        cur.execute("DELETE FROM round_robin_counter;")
        cur.execute("INSERT INTO round_robin_counter (counter) VALUES (%s);", (total_records,))
        
        con.commit()
        
    except Exception:
        con.rollback()
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Chèn record mới vào bảng gốc và phân mảnh Round Robin tiếp theo
    Sử dụng biến đếm toàn cục để đảm bảo phân phối tuần tự, nhất quán với roundrobinpartition và tester
    """
    con = openconnection
    cur = con.cursor()
    
    try:
        # Kiểm tra bảng gốc có tồn tại không
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables 
            WHERE table_name = %s
        """, (ratingstablename,))
        if cur.fetchone()[0] == 0:
            raise Exception(f"Bảng {ratingstablename} không tồn tại")
        
        # Tạo bảng counter nếu chưa tồn tại
        cur.execute("""
        CREATE TABLE IF NOT EXISTS round_robin_counter (
            counter BIGINT DEFAULT 0
        );
        INSERT INTO round_robin_counter (counter) 
        SELECT 0 
        WHERE NOT EXISTS (SELECT 1 FROM round_robin_counter);
        """)
        
        # Xác định số lượng partition
        cur.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables 
            WHERE table_name LIKE %s
        """, (RROBIN_TABLE_PREFIX + '%',))
        numberofpartitions = cur.fetchone()[0]
        
        if numberofpartitions == 0:
            raise Exception("Không tìm thấy bảng phân mảnh Round Robin")
        
        # Chèn vào bảng gốc
        cur.execute(f"""
        INSERT INTO {ratingstablename} (userid, movieid, rating)
        VALUES (%s, %s, %s)
        """, (userid, itemid, rating))
        
        # Lấy và tăng counter
        cur.execute("SELECT counter FROM round_robin_counter FOR UPDATE")
        counter = cur.fetchone()[0]
        partition_index = counter % numberofpartitions
        table_name = f"{RROBIN_TABLE_PREFIX}{partition_index}"
        
        # Chèn vào partition tương ứng
        cur.execute(f"""
        INSERT INTO {table_name} (userid, movieid, rating)
        VALUES (%s, %s, %s)
        """, (userid, itemid, rating))
        
        # Tăng counter
        cur.execute("UPDATE round_robin_counter SET counter = counter + 1")
        
        con.commit()
        
    except Exception:
        con.rollback()
        raise
    finally:
        cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()

    try:
        # Đếm số partition
        numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
        delta = 5.0 / numberofpartitions
        index = int(rating / delta)
        if rating % delta == 0 and index != 0:
            index = index - 1
        table_name = RANGE_TABLE_PREFIX + str(index)

        # Chèn vào bảng chính
        cur.execute(
            "INSERT INTO %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (ratingstablename, userid, itemid, rating))

        # Chèn vào partition tương ứng
        cur.execute("INSERT INTO %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (table_name, userid, itemid, rating))

        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        cur.close()



def create_db(dbname):
    """
    Tạo database mới nếu chưa tồn tại
    """
    try:
        # Kết nối đến database postgres mặc định
        conn = psycopg2.connect(
            host='localhost',
            database='postgres',
            user='postgres',
            password='712204'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Kiểm tra database đã tồn tại chưa
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (dbname,))
        exists = cur.fetchone()

        if not exists:
            # Tạo database mới
            cur.execute(f"CREATE DATABASE {dbname}")
            print(f"Database {dbname} đã được tạo thành công")
        else:
            print(f"Database {dbname} đã tồn tại")

    except Exception as e:
        print(f"Lỗi khi tạo database: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def count_partitions(prefix, openconnection):
    con = openconnection
    cur = con.cursor()
    try:
        cur.execute("""
        SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE %s
        """, (prefix + '%',))
        count = cur.fetchone()[0]
        return count
    finally:
        cur.close()