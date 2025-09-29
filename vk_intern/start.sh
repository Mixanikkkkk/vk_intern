echo "Запуск ETL проекта"

echo "Запуск PostgreSQL..."
service postgresql start

sleep 5
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';"

echo "Инициализация базы данных..."
sudo -u postgres psql -f /app/bd.sql

echo "Запуск cron..."
service cron start

echo "Выполняем первичную загрузку данных..."
cd /app && python3 /app/etl/extract.py
cd /app && python3 /app/etl/transform.py

echo "ETL проект запущен!"
echo "Логи cron: tail -f /var/log/cron.log"

tail -f /var/log/cron.log
