FROM python:3.12-slim
WORKDIR /app
COPY . .
# No dependencies to install - the project is standard-library only.
# Secrets are passed at runtime:  docker run --env-file .env -p 8050:8050 funnel-analytics
EXPOSE 8050
CMD ["sh", "-c", "python run_all.py && python -m funnel serve --host 0.0.0.0 --port 8050"]
