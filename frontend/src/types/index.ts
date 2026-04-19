// ── Existing types (preserved) ─────────────────────────────────────────────

export interface Flight {
  id: number;
  icao24: string;
  callsign: string | null;
  airline_id: number | null;
  airline?: Airline;
  origin_country: string | null;
  first_seen: number | null;
  last_seen: number | null;
  est_departure_airport: string | null;
  est_arrival_airport: string | null;
  est_departure_time: number | null;
  est_arrival_time: number | null;
  ingestion_time: string;
  duration_seconds: number | null;
  duration_minutes: number | null;
  duration_hours: number | null;
  // New geo fields
  latitude?: number | null;
  longitude?: number | null;
  altitude?: number | null;
  velocity?: number | null;
  heading?: number | null;
  on_ground?: boolean | null;
  region_key?: string | null;
  trajectory?: TrajectoryPoint[] | null;
}

export interface TrajectoryPoint {
  ts: number; lat: number; lon: number;
  alt?: number; vel?: number; hdg?: number;
}

export interface Airline {
  id: number;
  icao24: string;
  name: string | null;
  callsign_prefix: string | null;
  country_id: number | null;
  country?: Country;
  created_at: string;
  flight_count?: number;
}

export interface Country {
  id: number;
  name: string;
  iso_code: string | null;
  created_at: string;
}

export interface FlightListResponse {
  total: number;
  page: number;
  page_size: number;
  pages: number;
  data: Flight[];
}

export interface FlightFilterParams {
  airline_id?: number;
  country?: string;
  date_from?: string;
  date_to?: string;
  departure_airport?: string;
  arrival_airport?: string;
  region_key?: string;
  begin_ts?: number;
  end_ts?: number;
  lamin?: number;
  lomin?: number;
  lamax?: number;
  lomax?: number;
  page?: number;
  page_size?: number;
}

export interface DailyFlightStats { date: string; flight_count: number }
export interface AirlineActivityStats { airline_icao24: string; airline_name: string | null; flight_count: number }
export interface CountryActivityStats { country_name: string; flight_count: number }

export interface FlightStatistics {
  total_flights: number;
  daily_stats: DailyFlightStats[];
  top_airlines: AirlineActivityStats[];
  top_countries: CountryActivityStats[];
  flights_today: number;
  flights_this_week: number;
  flights_this_month: number;
}

export interface HealthCheck {
  status: string; timestamp: string; database: string; version: string;
}

export interface ApiResponse<T> { data: T; message?: string; error?: string }

// ── New types ──────────────────────────────────────────────────────────────

export interface GeoRegion {
  key: string; name: string; name_ar: string;
  lamin: number; lomin: number; lamax: number; lomax: number;
  center_lat: number; center_lon: number;
}

export interface IngestionJob {
  id: number;
  date_str: string;
  region_key: string;
  lamin: number; lomin: number; lamax: number; lomax: number;
  begin_ts: number; end_ts: number;
  status: 'pending' | 'running' | 'completed' | 'failed';
  flights_ingested: number;
  chunks_total: number;
  chunks_done: number;
  error_message?: string | null;
  created_at?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
}

export interface IngestionJobListResponse { total: number; data: IngestionJob[] }

export interface CountryStats { country_name: string; flight_count: number }
export interface DailyStats   { date: string; flight_count: number }
export interface HourlyStats  { hour: number; flight_count: number }
export interface AirportStats { airport_icao: string; flight_count: number; as_departure: number; as_arrival: number }
export interface RouteStats   { departure: string; arrival: string; flight_count: number }
export interface AnalyticsSummary {
  total_flights: number; unique_countries: number; unique_airports: number;
  top_countries: CountryStats[];
}
