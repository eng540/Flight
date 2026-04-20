import { useState, useEffect, useCallback } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, LineChart, Line, Cell, PieChart, Pie, Legend,
} from 'recharts';
import { analyticsApi, regionsApi } from '@/api/client';
import { GeoRegion, CountryStats, HourlyStats, AirportStats, RouteStats, DailyStats } from '@/types';

const COLORS = ['#3b82f6','#10b981','#f59e0b','#ef4444','#8b5cf6',
                 '#06b6d4','#ec4899','#84cc16','#f97316','#6366f1'];

export function AnalyticsSection() {
  const [regions, setRegions] = useState<GeoRegion[]>([]);
  const [regionKey, setRegionKey] = useState<string>('all');
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo]     = useState('');

  const [topCountries, setTopCountries]   = useState<CountryStats[]>([]);
  const [hourlyDist,   setHourlyDist]     = useState<HourlyStats[]>([]);
  const [topAirports,  setTopAirports]    = useState<AirportStats[]>([]);
  const [topRoutes,    setTopRoutes]      = useState<RouteStats[]>([]);
  const [dailyTrend,   setDailyTrend]     = useState<DailyStats[]>([]);
  const [loading,      setLoading]        = useState(false);

  useEffect(() => {
    regionsApi.listRegions().then(setRegions).catch(console.error);
  }, []);

  const buildParams = useCallback(() => {
    const p: Record<string, unknown> = {};
    if (regionKey && regionKey !== 'all') p.region_key = regionKey;
    if (dateFrom) p.begin_ts = Math.floor(new Date(dateFrom).getTime() / 1000);
    if (dateTo)   p.end_ts   = Math.floor(new Date(dateTo).getTime()   / 1000) + 86399;
    return p;
  }, [regionKey, dateFrom, dateTo]);

  const runAnalysis = useCallback(async () => {
    setLoading(true);
    const p = buildParams();
    try {
      const [c, h, a, r] = await Promise.all([
        analyticsApi.getTopCountries({ ...p, limit: 15 }),
        analyticsApi.getHourlyDistribution(p),
        analyticsApi.getTopAirports({ ...p, limit: 15 }),
        analyticsApi.getTopRoutes({ ...p, limit: 20 }),
      ]);
      setTopCountries(c || []);
      setHourlyDist(h || []);
      setTopAirports(a || []);
      setTopRoutes(r || []);

      if (p.begin_ts && p.end_ts) {
        const d = await analyticsApi.getDailyTrend(p);
        setDailyTrend(d || []);
      } else {
        setDailyTrend([]);
      }
    } catch (e) { console.error(e); }
    setLoading(false);
  }, [buildParams]);

  useEffect(() => { runAnalysis(); }, []);

  return (
    <div className="space-y-6">
      {/* Filter bar */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base">📊 Analytics Filters</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <div className="space-y-2">
              <Label>Region</Label>
              <Select value={regionKey} onValueChange={setRegionKey}>
                <SelectTrigger><SelectValue placeholder="All regions" /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Regions</SelectItem>
                  {regions.map(r => (
                    <SelectItem key={r.key} value={r.key}>
                      {r.name_ar} – {r.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>From Date</Label>
              <Input type="date" value={dateFrom}
                onChange={e => setDateFrom(e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label>To Date</Label>
              <Input type="date" value={dateTo}
                onChange={e => setDateTo(e.target.value)} />
            </div>
            <div className="flex items-end">
              <Button onClick={runAnalysis} disabled={loading} className="w-full">
                {loading ? '⏳ Analyzing…' : '📈 Analyze'}
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      <div className="grid gap-6 lg:grid-cols-2">
        {/* Top countries */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-semibold">🌐 Top Countries</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={topCountries.slice(0,12)} layout="vertical" margin={{ left: 8 }}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis type="number" tick={{ fontSize: 10 }} />
                <YAxis type="category" dataKey="country_name" tick={{ fontSize: 10 }} width={100} />
                <Tooltip formatter={(v: number) => [v.toLocaleString(), 'Flights']} />
                <Bar dataKey="flight_count" radius={[0,4,4,0]}>
                  {topCountries.slice(0,12).map((_,i) =>
                    <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* Hourly distribution */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-semibold">🕐 Flights by Hour (UTC)</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={hourlyDist}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="hour" tick={{ fontSize: 9 }} tickFormatter={h => `${h}h`} />
                <YAxis tick={{ fontSize: 10 }} />
                <Tooltip formatter={(v: number) => [v.toLocaleString(), 'Flights']}
                         labelFormatter={h => `${h}:00 UTC`} />
                <Bar dataKey="flight_count" fill="#8b5cf6" radius={[3,3,0,0]} />
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        {/* Daily trend (only when date range provided) */}
        {dailyTrend.length > 0 && (
          <Card className="lg:col-span-2">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-semibold">📈 Daily Flight Trend</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={dailyTrend}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" tick={{ fontSize: 9 }}
                         tickFormatter={d => d.slice(5)} interval="preserveStartEnd" />
                  <YAxis tick={{ fontSize: 10 }} />
                  <Tooltip formatter={(v: number) => [v.toLocaleString(), 'Flights']} />
                  <Line type="monotone" dataKey="flight_count"
                        stroke="#10b981" strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        )}

        {/* Top airports */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-semibold">🛫 Top Airports</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2 max-h-64 overflow-y-auto">
              {topAirports.slice(0,15).map((a, i) => (
                <div key={a.airport_icao} className="flex items-center gap-2">
                  <span className="text-xs text-muted-foreground w-5">{i+1}</span>
                  <span className="font-mono text-sm font-semibold text-primary w-12">
                    {a.airport_icao}
                  </span>
                  <div className="flex-1 bg-muted rounded-full h-2">
                    <div className="h-2 rounded-full"
                         style={{
                           width: `${topAirports[0]?.flight_count
                             ? (a.flight_count / topAirports[0].flight_count * 100) : 0}%`,
                           backgroundColor: COLORS[i % COLORS.length],
                         }} />
                  </div>
                  <span className="text-xs text-muted-foreground w-14 text-right">
                    {a.flight_count.toLocaleString()}
                  </span>
                </div>
              ))}
              {topAirports.length === 0 && (
                <p className="text-sm text-muted-foreground text-center py-4">No data</p>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Top routes */}
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-semibold">🛤️ Top Routes</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2 max-h-64 overflow-y-auto">
              {topRoutes.slice(0,15).map((r, i) => (
                <div key={`${r.departure}-${r.arrival}`} className="flex items-center gap-2">
                  <span className="text-xs text-muted-foreground w-5">{i+1}</span>
                  <span className="font-mono text-sm">{r.departure || '??'}</span>
                  <span className="text-muted-foreground text-xs">→</span>
                  <span className="font-mono text-sm">{r.arrival || '??'}</span>
                  <div className="flex-1 bg-muted rounded-full h-2">
                    <div className="h-2 rounded-full"
                         style={{
                           width: `${topRoutes[0]?.flight_count
                             ? (r.flight_count / topRoutes[0].flight_count * 100) : 0}%`,
                           backgroundColor: COLORS[i % COLORS.length],
                         }} />
                  </div>
                  <span className="text-xs text-muted-foreground w-12 text-right">
                    {r.flight_count.toLocaleString()}
                  </span>
                </div>
              ))}
              {topRoutes.length === 0 && (
                <p className="text-sm text-muted-foreground text-center py-4">No data</p>
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
