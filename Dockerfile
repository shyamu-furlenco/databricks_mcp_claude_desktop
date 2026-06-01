FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir \
    "mcp>=1.2.0" \
    "databricks-sql-connector>=3.0.0" \
    "databricks-sdk>=0.20.0" \
    starlette \
    uvicorn \
 && useradd -u 1000 -m mcp \
 && chown -R mcp:mcp /app
USER mcp
EXPOSE 8000
CMD ["uvicorn", "src.sse_server:app", "--host", "0.0.0.0", "--port", "8000"]
