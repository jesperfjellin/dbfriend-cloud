# dbfriend-cloud

**Topology Watchdog for PostGIS** - An automated geometry quality monitoring framework

**Development Status**: This is an active development framework. APIs and features are evolving.

## What is dbfriend-cloud?

dbfriend-cloud monitors your PostGIS databases for geometry quality issues and changes. Think of it as a **continuous integration system for spatial data** - it watches your geometries, runs comprehensive quality checks, and flags problems for review. It creates an in-map queue system where you can easily fix geometric issues and write the results directly to your database. 

### Key Features

**Automated Quality Checks**
- OGC validity testing (ST_IsValid, topology checks)
- Duplicate geometry detection
- Size anomaly detection (zero area, suspicious dimensions)
- Ring orientation validation for polygons

**Change Detection**
- Git-style diff workflow for geometry changes
- NEW/UPDATED/DELETED geometry tracking
- Confidence scoring for flagged issues
- Accept/reject queue for human review

**Spatial Visualization**
- Interactive maps for geometry inspection
- Context-aware geometry viewer
- Side-by-side diff visualization

**Performance Optimized**
- Handles large datasets (tested with 2m+ geometries)
- Efficient hash-based change detection
- Smart indexing for fast queries

## Architecture

- **Backend**: FastAPI + PostgreSQL + PostGIS
- **Frontend**: Next.js + TypeScript + Mapbox/OpenLayers
- **Database**: SQLAlchemy with async support
- **Quality Engine**: Configurable spatial test framework

## Quick Start

### Prerequisites
- Python 3.9+
- Node.js 18+
- PostgreSQL with PostGIS
- Redis (for background tasks)

### Backend Setup
```bash
cd backend
pip install -r requirements.txt
python _reset_db.py  # Initialize database
python main.py       # Start FastAPI server
```

### Worker Setup
```bash
cd backend
python worker_dev.py
```

### Frontend Setup
```bash
cd frontend
npm install
npm run dev         # Start Next.js development server
```

### Configuration
Create `.env` files with your database connections:

```bash
# backend/.env
DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/dbfriend_cloud
REDIS_URL=redis://localhost:6379/0

# frontend/.env.local
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN=your_token_here
```

## Usage

1. **Connect Your PostGIS Database**
   - Add dataset connections via the web interface
   - Configure monitoring intervals and quality check settings

2. **Monitor Changes**
   - The system establishes a baseline of your geometries
   - Continuously monitors for NEW/UPDATED/DELETED features
   - Flags suspicious changes based on configurable confidence thresholds

3. **Review Quality Issues**
   - View flagged geometries in the diff queue
   - Inspect spatial context and quality test results
   - Accept or reject changes through the web interface

## Development

This framework is designed to be:
- **Configurable**: Adjust quality test thresholds via `test_config.py`
- **Extensible**: Add new spatial quality tests easily
- **Scalable**: Handles large datasets with optimized database design

### Key Components

- `backend/services/spatial_tests.py` - Quality test implementations
- `backend/services/geometry_service.py` - Core geometry processing
- `frontend/src/components/TestMap.tsx` - Spatial visualization
- `backend/api/v1/` - REST API endpoints

## Contributing

This is an evolving framework. Areas of active development:
- Performance optimization for very large datasets
- Additional spatial quality tests
- Real-time monitoring capabilities
- Advanced visualization features

## License

[License details to be added]

---

**Built for spatial data professionals who need reliable geometry quality monitoring**

