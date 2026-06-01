FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -e . && pip install --no-cache-dir starlette uvicorn
EXPOSE 8000
CMD ["uvicorn", "src.sse_server:app", "--host", "0.0.0.0", "--port", "8000"]
