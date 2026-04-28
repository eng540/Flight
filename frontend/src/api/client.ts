import axios, { AxiosInstance, AxiosError } from 'axios';
import {
  FlightListResponse, FlightFilterParams,
  FlightStatistics, HealthCheck, Airline,
} from '@/types';

const API_BASE_URL = '';

const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
});

apiClient.interceptors.request.use(
  (config) => { console.log(`API: ${config.method?.toUpperCase()} ${config.url}`); return config; },
  (error) => Promise.reject(error)
);

apiClient.interceptors.response.use(
  (r) => r,
  (error: AxiosError) => {
    console.error('API Error:', error.response?.data || error.message);
    return Promise.reject(error);
  }
);

// ── Flights ────────────────────────────────────────────────────────────────
export const flightsApi = {
  // SRE FIX: Direct mapping to the Live Proxy API for immediate results
  getFlights: async (bounds: string = "63.0,12.0,25.0,42.0"): Promise<any> =>
    (await apiClient.get('/flights/live', { params: { bounds } })).data,

  filterFlights: async (params: FlightFilterParams): Promise<FlightListResponse> =>
    (await apiClient.get('/flights/filter', { params })).data,

  getFlight: async (id: number) =>
    (await apiClient.get(`/flights/${id}`)).data,

  // SRE FIX: Export Excel directly from the Live API
  exportFlights: async (bounds: string = "63.0,12.0,25.0,42.0"): Promise<Blob> =>
    (await apiClient.get('/flights/live', { params: { bounds, export: true }, responseType: 'blob' })).data,
};

// ── Airlines ───────────────────────────────────────────────────────────────
export const airlinesApi = {
  getAirlines: async (skip = 0, limit = 100): Promise<Airline[]> =>
    (await apiClient.get('/airlines', { params: { skip, limit } })).data,
  getAirline: async (id: number): Promise<Airline> =>
    (await apiClient.get(`/airlines/${id}`)).data,
};

// ── Statistics ─────────────────────────────────────────────────────────────
export const statsApi = {
  getStatistics: async (): Promise<FlightStatistics> =>
    (await apiClient.get('/stats')).data,
  getAirlineStats: async (limit = 10) =>
    (await apiClient.get('/stats/airlines', { params: { limit } })).data,
  healthCheck: async (): Promise<HealthCheck> =>
    (await apiClient.get('/stats/health')).data,
};

// ── Analytics (new) ────────────────────────────────────────────────────────
export const analyticsApi = {
  getTopCountries: async (params: Record<string, unknown> = {}) =>
    (await apiClient.get('/analytics/top_countries', { params })).data,
  getDailyTrend: async (params: Record<string, unknown>) =>
    (await apiClient.get('/analytics/daily_trend', { params })).data,
  getHourlyDistribution: async (params: Record<string, unknown> = {}) =>
    (await apiClient.get('/analytics/hourly_distribution', { params })).data,
  getTopAirports: async (params: Record<string, unknown> = {}) =>
    (await apiClient.get('/analytics/top_airports', { params })).data,
  getTopRoutes: async (params: Record<string, unknown> = {}) =>
    (await apiClient.get('/analytics/top_routes', { params })).data,
  getSummary: async (params: Record<string, unknown> = {}) =>
    (await apiClient.get('/analytics/summary', { params })).data,
};

// ── Regions (new) ──────────────────────────────────────────────────────────
export const regionsApi = {
  listRegions: async () => (await apiClient.get('/regions')).data,
  getRegion:   async (key: string) => (await apiClient.get(`/regions/${key}`)).data,
};

// ── Ingestion (new) ────────────────────────────────────────────────────────
export const ingestionApi = {
  listJobs: async (params: Record<string, unknown> = {}) =>
    (await apiClient.get('/ingestion/jobs', { params })).data,
  getJob: async (id: number) =>
    (await apiClient.get(`/ingestion/jobs/${id}`)).data,
  startIngestion: async (body: object) =>
    (await apiClient.post('/ingestion/start', body)).data,
  retryJob: async (id: number) =>
    (await apiClient.post(`/ingestion/jobs/${id}/retry`)).data,
  deleteJob: async (id: number) =>
    (await apiClient.delete(`/ingestion/jobs/${id}`)).data,
};

export default apiClient;