FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    postgresql-contrib \
    cron \
    sudo \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app/
WORKDIR /app

RUN chmod +x /app/etl/*.py /app/start.sh

COPY crontab /etc/cron.d/etl-cron
RUN chmod 0644 /etc/cron.d/etl-cron && \
    crontab /etc/cron.d/etl-cron

RUN touch /var/log/cron.log /var/log/etl.log

CMD ["/bin/bash", "/app/start.sh"]