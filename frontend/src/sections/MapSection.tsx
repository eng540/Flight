import { useEffect, useRef, useState, useCallback } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { flightsApi, regionsApi } from '@/api/client';
import { GeoRegion, Flight } from '@/types';

/* Leaflet loaded from CDN at runtime (avoids SSR / bundler issues) */
declare global {
  interface Window { L: typeof import('leaflet') }
}

export function MapSection() {
  const mapDiv    = useRef<HTMLDivElement>(null);
  const mapRef    = useRef<unknown>(null);
  const markersRef = useRef<unknown>(null);
  const regionsRef = useRef<unknown>(null);

  const [mapReady, setMapReady] = useState(false);
  const [regions, setRegions]   = useState<GeoRegion[]>([]);
  const [flights, setFlights]   = useState<Flight[]>([]);
  const [loading, setLoading]   = useState(false);
  const [selected, setSelected] = useState<Flight | null>(null);
  const [showBoxes, setShowBoxes] = useState(true);

  // Filter state
  const [regionKey, setRegionKey] = useState<string>('all');
  const [dateFrom,  setDateFrom]  = useState('');
  const [dateTo,    setDateTo]    = useState('');
  const [country,   setCountry]   = useState('');

  // Load Leaflet from CDN
  useEffect(() => {
    if (window.L) { setMapReady(true); return; }
    const css = document.createElement('link');
    css.rel = 'stylesheet';
    css.href = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css';
    document.head.appendChild(css);

    const js = document.createElement('script');
    js.src = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js';
    js.onload = () => setMapReady(true);
    document.head.appendChild(js);
  }, []);

  // Init map
  useEffect(() => {
    if (!mapReady || !mapDiv.current || mapRef.current) return;
    const L = window.L;
    const map = L.map(mapDiv.current).setView([28, 44], 4);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© <a href="https://openstreetmap.org">OpenStreetMap</a>',
      maxZoom: 18,
    }).addTo(map);
    mapRef.current  = map;
    markersRef.current = L.layerGroup().addTo(map);
    regionsRef.current = L.layerGroup().addTo(map);

    // Load regions
    regionsApi.listRegions().then((data: GeoRegion[]) => setRegions(data)).catch(console.error);
  }, [mapReady]);

  // Draw region rectangles
  useEffect(() => {
    if (!mapReady || !regionsRef.current) return;
    const L = window.L;
    const layer = regionsRef.current as ReturnType<typeof L.layerGroup>;
    layer.clearLayers();
    if (!showBoxes) return;
    regions.forEach(r => {
      L.rectangle([[r.lamin, r.lomin], [r.lamax, r.lomax]], {
        color: '#3b82f6', weight: 1.5, fillOpacity: 0.04, dashArray: '5 5',
      }).bindTooltip(`<b>${r.name_ar}</b><br><small>${r.name}</small>`, { sticky: true })
        .addTo(layer);
    });
  }, [mapReady, regions, showBoxes]);

  // Draw flight markers
  useEffect(() => {
    if (!mapReady || !markersRef.current) return;
    const L = window.L;
    const layer = markersRef.current as ReturnType<typeof L.layerGroup>;
    layer.clearLayers();

    const withPos = flights.filter(f => f.latitude != null && f.longitude != null);
    withPos.forEach(f => {
      const heading = f.heading ?? 0;
      const icon = L.divIcon({
        html: `<div style="transform:rotate(${heading}deg);font-size:16px;line-height:1">✈️</div>`,
        className: '', iconSize: [20, 20], iconAnchor: [10, 10],
      });
      L.marker([f.latitude!, f.longitude!], { icon })
        .bindPopup(`
          <b>${f.callsign || f.icao24}</b><br>
          <small>${f.origin_country || '—'} ${f.region_key ? `· ${f.region_key}` : ''}</small><br>
          ${f.est_departure_airport ? `🛫 ${f.est_departure_airport}` : ''}
          ${f.est_arrival_airport   ? ` → 🛬 ${f.est_arrival_airport}` : ''}<br>
          ${f.altitude ? `Alt: ${Math.round(f.altitude)}m ` : ''}
          ${f.velocity ? `Spd: ${Math.round(f.velocity * 1.944)}kt` : ''}
        `)
        .on('click', () => setSelected(f))
        .addTo(layer);

      // Draw trajectory if available
      if (f.trajectory && f.trajectory.length >= 2) {
        const latlngs = f.trajectory.map(p => [p.lat, p.lon] as [number, number]);
        L.polyline(latlngs, { color: '#ef4444', weight: 1.5, opacity: 0.6 }).addTo(layer);
      }
    });
  }, [mapReady, flights]);

  const flyToRegion = (r: GeoRegion) => {
    if (!mapRef.current) return;
    const L = window.L;
    (mapRef.current as ReturnType<typeof L.map>)
      .fitBounds([[r.lamin, r.lomin], [r.lamax, r.lomax]]);
  };

  const loadFlights = useCallback(async () => {
    setLoading(true);
    try {
      const params: Record<string, unknown> = { page_size: 500 };
      if (regionKey && regionKey !== 'all') params.region_key = regionKey;
      if (dateFrom) params.date_from = dateFrom;
      if (dateTo)   params.date_to   = dateTo;
      if (country)  params.country   = country;
      const res = await flightsApi.filterFlights(params as never);
      setFlights(res.data || []);
    } catch (e) { console.error(e); }
    setLoading(false);
  }, [regionKey, dateFrom, dateTo, country]);

  const flightsWithPos = flights.filter(f => f.latitude != null).length;

  return (
    <div className="space-y-4">
      {/* Controls */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="text-base flex items-center gap-2">
            🗺️ Interactive Map
            {flightsWithPos > 0 && (
              <Badge variant="secondary">{flightsWithPos} on map</Badge>
            )}
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4 mb-3">
            <div className="space-y-1">
              <Label className="text-xs">Region</Label>
              <Select value={regionKey} onValueChange={setRegionKey}>
                <SelectTrigger><SelectValue placeholder="All" /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Regions</SelectItem>
                  {regions.map(r => (
                    <SelectItem key={r.key} value={r.key}>{r.name_ar}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1">
              <Label className="text-xs">From Date</Label>
              <Input type="date" value={dateFrom}
                onChange={e => setDateFrom(e.target.value)} />
            </div>
            <div className="space-y-1">
              <Label className="text-xs">To Date</Label>
              <Input type="date" value={dateTo}
                onChange={e => setDateTo(e.target.value)} />
            </div>
            <div className="space-y-1">
              <Label className="text-xs">Country</Label>
              <Input placeholder="e.g. Saudi Arabia" value={country}
                onChange={e => setCountry(e.target.value)} />
            </div>
          </div>
          <div className="flex gap-2 flex-wrap items-center">
            <Button onClick={loadFlights} disabled={loading}>
              {loading ? '⏳ Loading…' : '🗺️ Load Flights'}
            </Button>
            <label className="flex items-center gap-1.5 text-sm text-muted-foreground cursor-pointer">
              <input type="checkbox" checked={showBoxes}
                onChange={e => setShowBoxes(e.target.checked)} className="rounded" />
              Show region boxes
            </label>
            <div className="ml-auto flex gap-1.5 flex-wrap">
              {regions.map(r => (
                <Button key={r.key} variant="outline" size="sm"
                  onClick={() => flyToRegion(r)} className="text-xs h-7">
                  📍 {r.name_ar}
                </Button>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Map */}
      <div className="relative rounded-lg overflow-hidden border bg-muted"
           style={{ height: 520 }}>
        <div ref={mapDiv} className="w-full h-full" />
        {!mapReady && (
          <div className="absolute inset-0 flex items-center justify-center text-muted-foreground">
            Loading map…
          </div>
        )}
        {loading && (
          <div className="absolute top-3 right-3 bg-background rounded-lg px-3 py-1.5 text-xs shadow border">
            Loading flights…
          </div>
        )}
      </div>

      {/* Selected flight detail */}
      {selected && (
        <Card>
          <CardContent className="pt-4">
            <div className="flex items-start justify-between">
              <div className="space-y-2">
                <div className="flex items-center gap-2">
                  <span className="text-lg">✈️</span>
                  <span className="font-bold">{selected.callsign || selected.icao24}</span>
                  {selected.region_key && (
                    <Badge variant="outline">{selected.region_key}</Badge>
                  )}
                </div>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-x-6 gap-y-1 text-sm">
                  {[
                    ['ICAO24',    <code key="i">{selected.icao24}</code>],
                    ['Country',   selected.origin_country || '—'],
                    ['Departure', selected.est_departure_airport || '—'],
                    ['Arrival',   selected.est_arrival_airport   || '—'],
                    ['Altitude',  selected.altitude ? `${Math.round(selected.altitude)}m` : '—'],
                    ['Speed',     selected.velocity ? `${Math.round(selected.velocity * 1.944)}kt` : '—'],
                    ['Heading',   selected.heading  ? `${Math.round(selected.heading)}°`  : '—'],
                    ['Duration',  selected.duration_hours ? `${selected.duration_hours.toFixed(1)}h` : '—'],
                  ].map(([label, value]) => (
                    <div key={String(label)}>
                      <span className="text-muted-foreground text-xs">{label}: </span>
                      <span className="font-medium">{value}</span>
                    </div>
                  ))}
                </div>
              </div>
              <Button variant="ghost" size="sm" onClick={() => setSelected(null)}>✕</Button>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
