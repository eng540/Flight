"""CRUD operations for the Flight Intelligence database."""
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import func, and_, desc, text
from sqlalchemy.exc import IntegrityError
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
import logging

from app import models, schemas

logger = logging.getLogger(__name__)


# ── Country ──────────────────────────────────────────────────────────────────
class CountryCRUD:
    @staticmethod
    def get_by_name(db: Session, name: str) -> Optional[models.Country]:
        return db.query(models.Country).filter(
            func.lower(models.Country.name) == func.lower(name)
        ).first()

    @staticmethod
    def get_or_create(db: Session, name: str) -> models.Country:
        country = CountryCRUD.get_by_name(db, name)
        if not country:
            country = models.Country(name=name)
            db.add(country)
            try:
                db.commit(); db.refresh(country)
            except IntegrityError:
                db.rollback()
                country = CountryCRUD.get_by_name(db, name)
        return country

    @staticmethod
    def get_all(db: Session, skip: int = 0, limit: int = 100) -> List[models.Country]:
        return db.query(models.Country).offset(skip).limit(limit).all()

    @staticmethod
    def create(db: Session, data: schemas.CountryCreate) -> models.Country:
        obj = models.Country(**data.model_dump())
        db.add(obj); db.commit(); db.refresh(obj)
        return obj


# ── Airline ───────────────────────────────────────────────────────────────────
class AirlineCRUD:
    @staticmethod
    def get_by_id(db: Session, airline_id: int) -> Optional[models.Airline]:
        return db.query(models.Airline).options(
            joinedload(models.Airline.country)
        ).filter(models.Airline.id == airline_id).first()

    @staticmethod
    def get_by_icao24(db: Session, icao24: str) -> Optional[models.Airline]:
        return db.query(models.Airline).options(
            joinedload(models.Airline.country)
        ).filter(models.Airline.icao24 == icao24.lower()).first()

    @staticmethod
    def get_or_create(db: Session, icao24: str, name: Optional[str] = None,
                      country_name: Optional[str] = None) -> models.Airline:
        airline = AirlineCRUD.get_by_icao24(db, icao24)
        if not airline:
            country_id = None
            if country_name:
                country = CountryCRUD.get_or_create(db, country_name)
                country_id = country.id
            airline = models.Airline(icao24=icao24.lower(), name=name, country_id=country_id)
            db.add(airline)
            try:
                db.commit(); db.refresh(airline)
            except IntegrityError:
                db.rollback()
                airline = AirlineCRUD.get_by_icao24(db, icao24)
        return airline

    @staticmethod
    def get_all(db: Session, skip: int = 0, limit: int = 100) -> List[models.Airline]:
        return db.query(models.Airline).options(
            joinedload(models.Airline.country)
        ).offset(skip).limit(limit).all()

    @staticmethod
    def get_most_active(db: Session, limit: int = 10) -> List[Dict[str, Any]]:
        results = db.query(
            models.Airline.icao24, models.Airline.name,
            func.count(models.Flight.id).label('flight_count')
        ).join(models.Flight, models.Airline.id == models.Flight.airline_id, isouter=True
        ).group_by(models.Airline.id, models.Airline.icao24, models.Airline.name
        ).order_by(desc('flight_count')).limit(limit).all()
        return [{"airline_icao24": r.icao24, "airline_name": r.name,
                 "flight_count": r.flight_count} for r in results]

    @staticmethod
    def create(db: Session, data: schemas.AirlineCreate) -> models.Airline:
        obj = models.Airline(**data.model_dump())
        db.add(obj); db.commit(); db.refresh(obj)
        return obj


# ── Flight ─────────────────────────────────────────────────────────────────────
class FlightCRUD:
    @staticmethod
    def get_by_id(db: Session, flight_id: int) -> Optional[models.Flight]:
        return db.query(models.Flight).options(
            joinedload(models.Flight.airline)
        ).filter(models.Flight.id == flight_id).first()

    @staticmethod
    def get_by_unique_id(db: Session, uid: str) -> Optional[models.Flight]:
        return db.query(models.Flight).filter(
            models.Flight.unique_flight_id == uid
        ).first()

    @staticmethod
    def exists(db: Session, uid: str) -> bool:
        return db.query(models.Flight.id).filter(
            models.Flight.unique_flight_id == uid
        ).first() is not None

    @staticmethod
    def get_all(
        db: Session,
        skip: int = 0, limit: int = 100,
        airline_id: Optional[int] = None,
        country: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        departure_airport: Optional[str] = None,
        arrival_airport: Optional[str] = None,
        region_key: Optional[str] = None,
        begin_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        lamin: Optional[float] = None,
        lomin: Optional[float] = None,
        lamax: Optional[float] = None,
        lomax: Optional[float] = None,
    ) -> Tuple[List[models.Flight], int]:
        query = db.query(models.Flight).options(joinedload(models.Flight.airline))

        if airline_id:
            query = query.filter(models.Flight.airline_id == airline_id)
        if country:
            query = query.filter(func.lower(models.Flight.origin_country) == func.lower(country))
        if date_from:
            try:
                ts = int(datetime.strptime(date_from, "%Y-%m-%d").timestamp())
                query = query.filter(models.Flight.first_seen >= ts)
            except ValueError:
                pass
        if date_to:
            try:
                ts = int(datetime.strptime(date_to, "%Y-%m-%d").timestamp()) + 86400
                query = query.filter(models.Flight.last_seen <= ts)
            except ValueError:
                pass
        if departure_airport:
            query = query.filter(
                func.upper(models.Flight.est_departure_airport) == departure_airport.upper())
        if arrival_airport:
            query = query.filter(
                func.upper(models.Flight.est_arrival_airport) == arrival_airport.upper())
        if region_key:
            query = query.filter(models.Flight.region_key == region_key)
        if begin_ts is not None:
            query = query.filter(models.Flight.first_seen >= begin_ts)
        if end_ts is not None:
            query = query.filter(models.Flight.last_seen <= end_ts)
        if lamin is not None:
            query = query.filter(models.Flight.latitude >= lamin)
        if lomin is not None:
            query = query.filter(models.Flight.longitude >= lomin)
        if lamax is not None:
            query = query.filter(models.Flight.latitude <= lamax)
        if lomax is not None:
            query = query.filter(models.Flight.longitude <= lomax)

        total = query.count()
        flights = query.order_by(desc(models.Flight.first_seen)).offset(skip).limit(limit).all()
        return flights, total

    @staticmethod
    def create_or_update(db: Session, data: schemas.FlightCreate) -> Optional[models.Flight]:
        existing = FlightCRUD.get_by_unique_id(db, data.unique_flight_id)
        if existing:
            for k, v in data.model_dump(exclude={'unique_flight_id'}).items():
                if v is not None:
                    setattr(existing, k, v)
            db.commit(); db.refresh(existing)
            return existing
        flight = models.Flight(**data.model_dump())
        db.add(flight)
        try:
            db.commit(); db.refresh(flight)
            return flight
        except IntegrityError:
            db.rollback()
            return None

    @staticmethod
    def bulk_create(db: Session, flights_data: List[schemas.FlightCreate]) -> Dict[str, int]:
        created = updated = skipped = 0
        for fd in flights_data:
            try:
                existing = FlightCRUD.get_by_unique_id(db, fd.unique_flight_id)
                if existing:
                    for k, v in fd.model_dump(exclude={'unique_flight_id'}).items():
                        if v is not None:
                            setattr(existing, k, v)
                    updated += 1
                else:
                    db.add(models.Flight(**fd.model_dump()))
                    created += 1
                if (created + updated) % 100 == 0:
                    db.commit()
            except Exception as e:
                logger.error(f"Error processing flight {fd.unique_flight_id}: {e}")
                skipped += 1
        db.commit()
        return {"created": created, "updated": updated, "skipped": skipped}

    @staticmethod
    def get_statistics(db: Session) -> Dict[str, Any]:
        now = datetime.utcnow()
        today_start = int(datetime(now.year, now.month, now.day).timestamp())
        week_start = int((now - timedelta(days=7)).timestamp())
        month_start = int((now - timedelta(days=30)).timestamp())

        total_flights = db.query(models.Flight).count()
        flights_today = db.query(models.Flight).filter(
            models.Flight.first_seen >= today_start).count()
        flights_this_week = db.query(models.Flight).filter(
            models.Flight.first_seen >= week_start).count()
        flights_this_month = db.query(models.Flight).filter(
            models.Flight.first_seen >= month_start).count()

        daily_stats = []
        for i in range(7):
            day = now - timedelta(days=i)
            ds = int(datetime(day.year, day.month, day.day).timestamp())
            de = ds + 86400
            cnt = db.query(models.Flight).filter(
                and_(models.Flight.first_seen >= ds, models.Flight.first_seen < de)
            ).count()
            daily_stats.append({"date": day.strftime("%Y-%m-%d"), "flight_count": cnt})
        daily_stats.reverse()

        top_airlines = db.query(
            models.Airline.icao24, models.Airline.name,
            func.count(models.Flight.id).label('flight_count')
        ).join(models.Flight, models.Airline.id == models.Flight.airline_id
        ).group_by(models.Airline.id, models.Airline.icao24, models.Airline.name
        ).order_by(desc('flight_count')).limit(10).all()

        top_countries = db.query(
            models.Flight.origin_country,
            func.count(models.Flight.id).label('flight_count')
        ).group_by(models.Flight.origin_country
        ).order_by(desc('flight_count')).limit(10).all()

        return {
            "total_flights": total_flights,
            "daily_stats": daily_stats,
            "top_airlines": [{"airline_icao24": a.icao24, "airline_name": a.name,
                               "flight_count": a.flight_count} for a in top_airlines],
            "top_countries": [{"country_name": c.origin_country or "Unknown",
                                "flight_count": c.flight_count} for c in top_countries],
            "flights_today": flights_today,
            "flights_this_week": flights_this_week,
            "flights_this_month": flights_this_month,
        }

    @staticmethod
    def delete_old_flights(db: Session, days: int = 30) -> int:
        if days <= 0:
            return 0
        cutoff = int((datetime.utcnow() - timedelta(days=days)).timestamp())
        result = db.query(models.Flight).filter(
            models.Flight.last_seen < cutoff
        ).delete(synchronize_session=False)
        db.commit()
        return result


# ── Analytics ─────────────────────────────────────────────────────────────────
class AnalyticsCRUD:
    @staticmethod
    def _apply_filters(query, begin_ts=None, end_ts=None, region_key=None,
                       lamin=None, lomin=None, lamax=None, lomax=None):
        if begin_ts:
            query = query.filter(models.Flight.first_seen >= begin_ts)
        if end_ts:
            query = query.filter(models.Flight.last_seen <= end_ts)
        if region_key:
            query = query.filter(models.Flight.region_key == region_key)
        if lamin is not None:
            query = query.filter(models.Flight.latitude >= lamin)
        if lomin is not None:
            query = query.filter(models.Flight.longitude >= lomin)
        if lamax is not None:
            query = query.filter(models.Flight.latitude <= lamax)
        if lomax is not None:
            query = query.filter(models.Flight.longitude <= lomax)
        return query

    @staticmethod
    def get_top_countries(db: Session, limit: int = 15, **kw) -> List[Dict]:
        q = db.query(models.Flight.origin_country,
                     func.count(models.Flight.id).label('flight_count'))
        q = AnalyticsCRUD._apply_filters(q, **kw)
        q = q.filter(models.Flight.origin_country.isnot(None))
        results = q.group_by(models.Flight.origin_country
                  ).order_by(desc('flight_count')).limit(limit).all()
        return [{"country_name": r.origin_country, "flight_count": r.flight_count}
                for r in results]

    @staticmethod
    def get_daily_trend(db: Session, begin_ts: int, end_ts: int, **kw) -> List[Dict]:
        results = []
        cursor = begin_ts
        while cursor < end_ts:
            next_c = cursor + 86400
            q = db.query(func.count(models.Flight.id)).filter(
                and_(models.Flight.first_seen >= cursor,
                     models.Flight.first_seen < next_c))
            q = AnalyticsCRUD._apply_filters(q, **kw)
            cnt = q.scalar() or 0
            results.append({
                "date": datetime.utcfromtimestamp(cursor).strftime("%Y-%m-%d"),
                "flight_count": cnt
            })
            cursor = next_c
        return results

    @staticmethod
    def get_hourly_distribution(db: Session, **kw) -> List[Dict]:
        q = db.query(
            func.extract('hour', func.to_timestamp(models.Flight.first_seen)).label('hour'),
            func.count(models.Flight.id).label('flight_count')
        )
        q = AnalyticsCRUD._apply_filters(q, **kw)
        results = q.group_by('hour').order_by('hour').all()
        hour_map = {int(r.hour): r.flight_count for r in results}
        return [{"hour": h, "flight_count": hour_map.get(h, 0)} for h in range(24)]

    @staticmethod
    def get_top_airports(db: Session, limit: int = 15, **kw) -> List[Dict]:
        q_dep = db.query(models.Flight.est_departure_airport.label('airport'),
                         func.count(models.Flight.id).label('cnt')
                         ).filter(models.Flight.est_departure_airport.isnot(None))
        q_dep = AnalyticsCRUD._apply_filters(q_dep, **kw)
        dep = {r.airport: r.cnt for r in q_dep.group_by('airport').all()}

        q_arr = db.query(models.Flight.est_arrival_airport.label('airport'),
                         func.count(models.Flight.id).label('cnt')
                         ).filter(models.Flight.est_arrival_airport.isnot(None))
        q_arr = AnalyticsCRUD._apply_filters(q_arr, **kw)
        arr = {r.airport: r.cnt for r in q_arr.group_by('airport').all()}

        all_ap = set(dep.keys()) | set(arr.keys())
        combined = [{"airport_icao": ap,
                     "as_departure": dep.get(ap, 0),
                     "as_arrival": arr.get(ap, 0),
                     "flight_count": dep.get(ap, 0) + arr.get(ap, 0)}
                    for ap in all_ap]
        return sorted(combined, key=lambda x: x['flight_count'], reverse=True)[:limit]

    @staticmethod
    def get_top_routes(db: Session, limit: int = 20, **kw) -> List[Dict]:
        q = db.query(
            models.Flight.est_departure_airport,
            models.Flight.est_arrival_airport,
            func.count(models.Flight.id).label('flight_count')
        ).filter(
            models.Flight.est_departure_airport.isnot(None),
            models.Flight.est_arrival_airport.isnot(None)
        )
        q = AnalyticsCRUD._apply_filters(q, **kw)
        results = q.group_by(
            models.Flight.est_departure_airport, models.Flight.est_arrival_airport
        ).order_by(desc('flight_count')).limit(limit).all()
        return [{"departure": r.est_departure_airport, "arrival": r.est_arrival_airport,
                 "flight_count": r.flight_count} for r in results]

    @staticmethod
    def get_summary(db: Session, **kw) -> Dict:
        q_total = AnalyticsCRUD._apply_filters(
            db.query(func.count(models.Flight.id)), **kw)
        total = q_total.scalar() or 0

        q_countries = AnalyticsCRUD._apply_filters(
            db.query(func.count(func.distinct(models.Flight.origin_country))), **kw)
        unique_countries = q_countries.scalar() or 0

        q_airports = AnalyticsCRUD._apply_filters(
            db.query(func.count(func.distinct(models.Flight.est_departure_airport))), **kw)
        unique_airports = q_airports.scalar() or 0

        top_countries = AnalyticsCRUD.get_top_countries(db, limit=5, **kw)
        return {"total_flights": total, "unique_countries": unique_countries,
                "unique_airports": unique_airports, "top_countries": top_countries}


# ── IngestionJob ───────────────────────────────────────────────────────────────
class IngestionJobCRUD:
    @staticmethod
    def get_by_id(db: Session, job_id: int) -> Optional[models.IngestionJob]:
        return db.query(models.IngestionJob).filter(
            models.IngestionJob.id == job_id).first()

    @staticmethod
    def get_by_date_region(db: Session, date_str: str,
                           region_key: str) -> Optional[models.IngestionJob]:
        return db.query(models.IngestionJob).filter(
            and_(models.IngestionJob.date_str == date_str,
                 models.IngestionJob.region_key == region_key)
        ).first()

    @staticmethod
    def is_completed(db: Session, date_str: str, region_key: str) -> bool:
        job = IngestionJobCRUD.get_by_date_region(db, date_str, region_key)
        return job is not None and job.status == "completed"

    @staticmethod
    def get_all(db: Session, skip: int = 0, limit: int = 100,
                status: Optional[str] = None,
                region_key: Optional[str] = None) -> Tuple[List[models.IngestionJob], int]:
        q = db.query(models.IngestionJob)
        if status:
            q = q.filter(models.IngestionJob.status == status)
        if region_key:
            q = q.filter(models.IngestionJob.region_key == region_key)
        total = q.count()
        jobs = q.order_by(desc(models.IngestionJob.created_at)).offset(skip).limit(limit).all()
        return jobs, total

    @staticmethod
    def create(db: Session, date_str: str, region_key: str,
               lamin: float, lomin: float, lamax: float, lomax: float,
               begin_ts: int, end_ts: int, chunks_total: int = 0) -> models.IngestionJob:
        job = models.IngestionJob(
            date_str=date_str, region_key=region_key,
            lamin=lamin, lomin=lomin, lamax=lamax, lomax=lomax,
            begin_ts=begin_ts, end_ts=end_ts,
            status="pending", chunks_total=chunks_total)
        db.add(job); db.commit(); db.refresh(job)
        return job

    @staticmethod
    def update_status(db: Session, job_id: int, status: str,
                      flights_ingested: int = None, chunks_done: int = None,
                      error_message: str = None) -> Optional[models.IngestionJob]:
        job = IngestionJobCRUD.get_by_id(db, job_id)
        if not job:
            return None
        job.status = status
        if status == "running" and not job.started_at:
            job.started_at = datetime.utcnow()
        if status in ("completed", "failed"):
            job.completed_at = datetime.utcnow()
        if flights_ingested is not None:
            job.flights_ingested = flights_ingested
        if chunks_done is not None:
            job.chunks_done = chunks_done
        if error_message is not None:
            job.error_message = error_message
        db.commit(); db.refresh(job)
        return job

    @staticmethod
    def delete(db: Session, job_id: int) -> bool:
        job = IngestionJobCRUD.get_by_id(db, job_id)
        if not job:
            return False
        db.delete(job); db.commit()
        return True
