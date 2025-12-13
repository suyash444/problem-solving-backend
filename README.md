# Problem Solving Tracker - Backend API

Backend microservices for tracking and resolving missing items in warehouse problem-solving baskets.

## 📋 Project Overview

This system helps warehouse operators:
1. Import order and position data from CSV files and APIs
2. Detect missing items when baskets are shipped
3. Create missions to find missing items
4. Guide operators through position checks
5. Track resolution of missing items

## 🏗️ Architecture
```
problem-solving-backend/
├── config/                    # Configuration settings
├── shared/
│   ├── database/              # SQLAlchemy models & connection
│   ├── schemas/               # Pydantic schemas
│   └── utils/                 # Common utilities
├── services/
│   ├── ingestion_service/     # Import DumpTrack, Monitor, API data
│   ├── mission_service/       # Create and manage missions
│   ├── position_service/      # Position checking logic
│   └── api_gateway/           # FastAPI main gateway
└── tests/                     # Unit tests
```

## 🚀 Setup Instructions

### 1. Prerequisites
- Python 3.9+
- SQL Server (with ProblemSolvingTrackerDB created)
- Access to file paths: `H:/tek/MSBD/DumpTrack/old` and `H:/tek/MSBD/Monitor/old`

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment
```bash
# Copy example env file
cp .env.example .env

# Edit .env with your actual credentials (already configured)
```

### 4. Test Database Connection
```python
from shared.database import test_connection
test_connection()  # Should return True
```

## 📊 Database Schema

### Import Tables
- `import_dumptrack` - Raw DumpTrack CSV data
- `import_monitor` - Raw Monitor CSV data
- `import_prelievo_powersort` - PrelievoPowerSort API data
- `import_spedito` - GetSpedito2 API data

### Business Tables
- `orders` & `order_items` - Order management
- `picking_events` - Picking history
- `udc_inventory` - What's in each UDC
- `udc_locations` - UDC warehouse positions
- `shipped_items` - Actually shipped items

### Mission Tables
- `missions` - Problem-solving missions
- `mission_items` - Missing items per mission
- `position_checks` - Positions to check

## 🔄 Data Flow

### Daily Import (Automatic at 5:30 AM)
1. **DumpTrack Import**: Import latest file → Update orders
2. **Monitor Import**: Import yesterday's file → Update UDC positions

### Mission Creation (On-Demand)
1. Operator enters basket code (cesta)
2. System calls GetSpedito2 API
3. Compares with DumpTrack data
4. Identifies missing items
5. Creates mission with positions to check

### Position Checking
1. Operator selects mission
2. System shows positions in alphabetical order
3. Operator marks: Found / Not Found
4. System auto-updates when all items found

## 🛠️ Development

### Running Services
```bash
# Run API Gateway
python -m services.api_gateway.main

# Run Data Ingestion Service
python -m services.ingestion_service.main
```

## 📝 API Endpoints

### Missions
- `POST /api/missions/create` - Create new mission from cesta
- `GET /api/missions/{mission_id}` - Get mission details
- `GET /api/missions/list` - List all missions
- `PUT /api/missions/{mission_id}/status` - Update mission status

### Position Checks
- `GET /api/missions/{mission_id}/route` - Get checking route
- `PUT /api/checks/{check_id}/update` - Update check status

### Data Import
- `POST /api/import/dumptrack` - Trigger DumpTrack import
- `POST /api/import/monitor` - Trigger Monitor import
- `GET /api/import/status` - Get import status

## 🔐 Security
- Bearer token authentication for external APIs
- SQL Server trusted connection
- CORS configured for frontend

## 📌 Notes
- DumpTrack files are cumulative (latest has all data)
- Monitor files are daily (one file per day)
- No files generated on weekends/holidays
- System handles gaps automatically

## 👥 Team
Backend development for warehouse problem-solving system.
