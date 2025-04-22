FROM python:3.10

ENV CI=true

WORKDIR /app

COPY requirements.txt ./requirements.txt

COPY backend/ ./

RUN pip install --no-cache-dir --upgrade -r requirements.txt

EXPOSE 8000

CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]