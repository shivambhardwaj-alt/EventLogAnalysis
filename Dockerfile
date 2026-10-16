FROM python:3.12-slim
WORKDIR /app
COPY . .

EXPOSE 8050
CMD ["sh", "-c", "python run_all.py && python -m funnel serve --host 0.0.0.0 --port 8050"]
