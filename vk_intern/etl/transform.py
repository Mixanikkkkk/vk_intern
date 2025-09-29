import psycopg2
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': 'localhost',
    'database': 'intern_vk_db',
    'user': 'postgres',
    'password': 'postgres',
    'port': 5432
}

def test_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка подключения к БД: {e}")
        return False
    
def transform_data():
    try:
        logger.info("Трансформация данных")
        
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("DELETE FROM top_users_by_posts")
        
        cur.execute("""
            SELECT 
                user_id,
                COUNT(*) as post_count
            FROM users_by_posts 
            GROUP BY user_id
            ORDER BY COUNT(*) DESC
        """)
        
        results = cur.fetchall()

        for user_id, post_count in results:
            cur.execute("""
                INSERT INTO top_users_by_posts (user_id, post_count, calculated_at)
                VALUES (%s, %s, %s)
            """, (user_id,  post_count, datetime.now()))
        
        conn.commit()
        
        logger.info(f"Витрина обновлена. Обработано {len(results)} пользователей")
        
        cur.execute("""
            SELECT user_id, post_count 
            FROM top_users_by_posts 
            ORDER BY post_count DESC 
        """)
        top_users = cur.fetchall()
        
        logger.info("Топ пользователей по постам:")
        for i, (user_id, count) in enumerate(top_users, 1):
            logger.info(f"{i}. {user_id}: {count} постов")
        
        cur.close()
        conn.close()
        
    except psycopg2.Error as e:
        logger.error(f"Ошибка базы данных: {e}")
    except Exception as e:
        logger.error(f"Ошибка: {e}")

if __name__ == "__main__":
    transform_data()