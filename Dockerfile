FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn
COPY . .
CMD exec gunicorn --bind :$PORT --workers 1 --threads 2 --timeout 120 app:app
