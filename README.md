# Flight Intelligence Platform

A comprehensive, production-ready platform for flight tracking and analysis using real-time data from OpenSky Network API.

## Features

- **Real-time Flight Tracking**: Monitor flights worldwide with data updated every 5 minutes
- **Advanced Analytics**: Comprehensive statistics and visualizations
- **Data Export**: Export flight data to Excel format
- **Filtering & Search**: Advanced filtering by airline, country, date, and airports
- **RESTful API**: Full-featured API built with FastAPI
- **Responsive Dashboard**: Modern React-based frontend
- **Production Ready**: Docker containers, automated deployment, and monitoring

## Architecture

```
flight-intelligence/
├── backend/          # FastAPI Backend
│   ├── app/         # Application code
│   ├── alembic/     # Database migrations
│   └── requirements.txt
├── frontend/        # React Dashboard
│   ├── src/        # Source code
│   └── package.json
├── worker/         # Celery Workers
│   ├── opensky_client.py
│   ├── data_processor.py
│   ├── ingestion_service.py
│   ├── celery_app.py
│   └── tasks.py
├── docker/         # Docker configurations
│   ├── Dockerfile.backend
│   ├── Dockerfile.worker
│   ├── Dockerfile.frontend
│   ├── docker-compose.yml
│   └── nginx.conf
└── infra/          # Infrastructure configs
    └── railway.json
```

## Tech Stack

### Backend
- **Python 3.11**
- **FastAPI** - Modern web framework
- **SQLAlchemy 2.0** - ORM
- **Alembic** - Database migrations
- **PostgreSQL** - Database
- **Pandas** - Data processing

### Frontend
- **React 18** - UI framework
- **TypeScript** - Type safety
- **Tailwind CSS** - Styling
- **shadcn/ui** - UI components
- **Recharts** - Data visualization
- **Axios** - HTTP client

### Worker & Scheduler
- **Celery** - Distributed task queue
- **Redis** - Message broker
- **Celery Beat** - Periodic task scheduler

### Deployment
- **Docker** - Containerization
- **Railway** - Cloud deployment
- **Nginx** - Reverse proxy

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Node.js 20+ (for local frontend development)
- Python 3.11+ (for local backend development)

### Using Docker Compose

1. Clone the repository:
```bash
git clone <repository-url>
cd flight-intelligence
```

2. Start all services:
```bash
cd docker
docker-compose up -d
```

3. Access the application:
- Frontend: http://localhost
- Backend API: http://localhost:8000
- API Docs: http://localhost:8000/docs

### Local Development

#### Backend

1. Create virtual environment:
```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

4. Run database migrations:
```bash
alembic upgrade head
```

5. Start the server:
```bash
uvicorn app.main:app --reload
```

#### Frontend

1. Install dependencies:
```bash
cd frontend
npm install
```

2. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

3. Start development server:
```bash
npm run dev
```

#### Worker

1. Start Redis:
```bash
docker run -d -p 6379:6379 redis:7-alpine
```

2. Start Celery worker:
```bash
cd worker
celery -A celery_app worker -l info
```

3. Start Celery beat (scheduler):
```bash
celery -A celery_app beat -l info
```

## API Endpoints

### Flights
- `GET /flights` - List all flights (paginated)
- `GET /flights/filter` - Filter flights
- `GET /flights/{id}` - Get flight by ID
- `GET /flights/export/excel` - Export to Excel

### Airlines
- `GET /airlines` - List all airlines
- `GET /airlines/{id}` - Get airline by ID
- `GET /airlines/icao/{icao24}` - Get airline by ICAO24

### Statistics
- `GET /stats` - Get comprehensive statistics
- `GET /stats/airlines` - Get airline statistics
- `GET /stats/health` - Health check

## Data Ingestion

The platform automatically fetches flight data from OpenSky Network API every 5 minutes using Celery Beat scheduler.

### Manual Ingestion

You can also trigger manual ingestion:

```python
from worker.ingestion_service import FlightIngestionService

with FlightIngestionService() as service:
    stats = service.ingest_recent_flights(hours=2)
    print(f"Ingested: {stats}")
```

## Deployment

### Railway

1. Install Railway CLI:
```bash
npm install -g @railway/cli
```

2. Login and create project:
```bash
railway login
railway init
```

3. Deploy:
```bash
railway up
```

### Environment Variables

Required environment variables:

- `DATABASE_URL` - PostgreSQL connection string
- `REDIS_URL` - Redis connection string
- `OPENSKY_USERNAME` - OpenSky API username (optional)
- `OPENSKY_PASSWORD` - OpenSky API password (optional)

## Database Schema

### Tables

1. **countries** - Country information
   - id, name, iso_code, created_at

2. **airlines** - Airline information
   - id, icao24, name, callsign_prefix, country_id

3. **flights** - Flight tracking data
   - id, icao24, callsign, airline_id, origin_country
   - first_seen, last_seen
   - est_departure_airport, est_arrival_airport
   - unique_flight_id (for deduplication)

## Monitoring

- Health check endpoint: `GET /health`
- Database status: `GET /stats/health`
- Celery monitoring: Flower (optional)

## License

MIT License - See LICENSE file for details

## Acknowledgments

- Data provided by [OpenSky Network](https://opensky-network.org/)
- Built with [FastAPI](https://fastapi.tiangolo.com/) and [React](https://react.dev/)
