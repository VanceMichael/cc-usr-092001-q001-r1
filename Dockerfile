FROM python:3.12-slim
WORKDIR /app
COPY . .
ENV PORT=8080 DATABASE_PATH=/data/app.sqlite3
EXPOSE 8080
CMD ["sh", "-c", "python -m scripts.migrate && exec python -m src.app"]
