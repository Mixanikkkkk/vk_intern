import requests
import psycopg2
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - EXTRACT - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

api_url = "https://jsonplaceholder.typicode.com/posts"

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

def extract_posts():
    try:
        logger.info("Извлекаем данные из API")
        
        if not test_db_connection():
            logger.error("Нет подключения к БД")
            return

        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        posts = response.json()
        
        logger.info(f"Получено {len(posts)} постов из API")
        
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM users_by_posts WHERE id > 0")
        existing_count = cur.fetchone()[0]
        
        inserted_count = 0

        for post in posts:
            try:
                cur.execute("""
                    INSERT INTO users_by_posts (id, user_id, title, body, added_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    post['id'],
                    post['userId'],
                    post['title'],  
                    post['body'],
                    datetime.now()
                ))
                inserted_count += 1
                
            except psycopg2.IntegrityError:
                conn.rollback()
                logger.info(f"Пост {post['id']} уже существует, пропускаем") 
                            
            except Exception as e:
                logger.error(f"Ошибка при обработке поста {post['id']}: {e}")
                continue
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Успешно обработано: {inserted_count} новых постов")
        logger.info(f"Всего в БД: {existing_count + inserted_count} постов")
        
    except requests.RequestException as e:
        logger.error(f"Ошибка API запроса: {e}")
    except psycopg2.Error as e:
        logger.error(f"Ошибка базы данных: {e}")
    except Exception as e:
        logger.error(f"Ошибка: {e}")

if __name__ == "__main__":
    extract_posts()