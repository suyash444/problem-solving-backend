FROM python:3.12-slim

WORKDIR /app

# OS deps for pyodbc + SQL Server ODBC driver
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gnupg2 ca-certificates \
    unixodbc unixodbc-dev \
    gcc g++ \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
 && echo "deb [signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/microsoft-prod.list \
 && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 9000

CMD ["uvicorn", "services.api_gateway.main:app", "--host", "0.0.0.0", "--port", "9000"]
