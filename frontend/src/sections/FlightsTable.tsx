import { useState, useEffect, useCallback } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table';
import { Plane, Download, MapPin, Loader2, Calendar, ArrowUp, Gauge } from 'lucide-react';
import { flightsApi } from '@/api/client';
import { toast } from 'sonner';

export function FlightsTable() {
  const [flights, setFlights] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [exporting, setExporting] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  
  const defaultBounds = "63.0,12.0,25.0,42.0";
  const pageSize = 50;

  const loadData = useCallback(async () => {
    try {
      setLoading(true);
      // SRE FIX: Fetching paginated data directly from backend
      const res = await flightsApi.getFlights(defaultBounds, searchTerm, page, pageSize);
      
      setFlights(res?.data || []);
      setTotal(res?.total || 0);
      setTotalPages(res?.pages || 1);
    } catch (e) {
      toast.error('حدث خطأ أثناء جلب البيانات الحية');
    } finally {
      setLoading(false);
    }
  }, [searchTerm, page]);

  // Reset to page 1 when search term changes
  useEffect(() => {
    setPage(1);
  }, [searchTerm]);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 30000);
    return () => clearInterval(interval);
  }, [loadData]);

  const handleExport = async () => {
    try {
      setExporting(true);
      const blob = await flightsApi.exportFlights(defaultBounds, searchTerm);
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      const dateStr = new Date().toISOString().split('T')[0];
      link.setAttribute('download', `الرحلات_الحية_${dateStr}.xlsx`);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
      toast.success('تم تصدير تقرير الرحلات بنجاح');
    } catch (error) {
      toast.error('فشل في تصدير البيانات');
    } finally {
      setExporting(false);
    }
  };

  const formatAltitude = (meters: number | null | undefined) => {
    if (meters == null) return '-';
    return `${Math.round(meters).toLocaleString()} م`;
  };

  const formatSpeed = (kmh: number | null | undefined) => {
    if (kmh == null) return '-';
    return `${Math.round(kmh).toLocaleString()} كم/س`;
  };

  const formatTimestamp = (ts: number | string | null | undefined) => {
    if (!ts) return '-';
    const date = typeof ts === 'number' ? new Date(ts * 1000) : new Date(ts);
    return date.toLocaleString('ar-SA', {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
    });
  };

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <CardTitle className="flex items-center gap-2">
            <Plane className="h-5 w-5" />
            الرحلات الحية
            <Badge variant="secondary">{total} رحلة</Badge>
          </CardTitle>

          <div className="flex flex-col sm:flex-row gap-2">
            <div className="flex gap-2">
              <Input
                placeholder="ابحث برمز النداء أو ICAO..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-64"
              />
            </div>

            <Button variant="outline" onClick={handleExport} disabled={exporting || total === 0}>
              <Download className="h-4 w-4 ml-2" />
              {exporting ? 'جارٍ التصدير...' : 'تصدير Excel'}
            </Button>
          </div>
        </div>
      </CardHeader>

      <CardContent>
        <div className="rounded-md border relative min-h-[400px]">
          {loading && (
            <div className="absolute inset-0 bg-background/50 backdrop-blur-sm flex items-center justify-center z-10">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
            </div>
          )}
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="text-right">رقم الرحلة</TableHead>
                <TableHead className="text-right">الهوية (ICAO24)</TableHead>
                <TableHead className="text-right">مطار الإقلاع</TableHead>
                <TableHead className="text-right">مطار الوصول</TableHead>
                <TableHead className="text-right">الارتفاع</TableHead>
                <TableHead className="text-right">السرعة</TableHead>
                <TableHead className="text-right">آخر رصد</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {flights.length === 0 && !loading ? (
                <TableRow>
                  <TableCell colSpan={7} className="text-center py-8 text-muted-foreground">
                    لا توجد رحلات مطابقة للبحث
                  </TableCell>
                </TableRow>
              ) : (
                flights.map((flight: any, idx: number) => (
                  <TableRow key={flight.id || idx}>
                    <TableCell><Badge variant="outline" className="font-mono">{flight.callsign || 'N/A'}</Badge></TableCell>
                    <TableCell className="font-mono text-xs">{flight.icao24?.toUpperCase() || 'N/A'}</TableCell>
                    <TableCell>
                      {flight.est_departure_airport ? (
                        <Badge variant="secondary" className="font-mono"><MapPin className="h-3 w-3 ml-1" />{flight.est_departure_airport}</Badge>
                      ) : '-'}
                    </TableCell>
                    <TableCell>
                      {flight.est_arrival_airport ? (
                        <Badge variant="secondary" className="font-mono"><MapPin className="h-3 w-3 ml-1" />{flight.est_arrival_airport}</Badge>
                      ) : '-'}
                    </TableCell>
                    <TableCell><div className="flex items-center gap-1"><ArrowUp className="h-3 w-3" />{formatAltitude(flight.altitude)}</div></TableCell>
                    <TableCell><div className="flex items-center gap-1"><Gauge className="h-3 w-3" />{formatSpeed(flight.velocity)}</div></TableCell>
                    <TableCell><div className="flex items-center gap-1 text-sm"><Calendar className="h-3 w-3" />{formatTimestamp(flight.last_seen)}</div></TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>

        {/* Pagination Controls */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between mt-4">
            <Button 
              variant="outline" 
              size="sm" 
              disabled={page === 1 || loading} 
              onClick={() => setPage(p => p - 1)}
            >
              السابق
            </Button>
            <span className="text-sm text-muted-foreground">
              صفحة {page} من {totalPages}
            </span>
            <Button 
              variant="outline" 
              size="sm" 
              disabled={page === totalPages || loading} 
              onClick={() => setPage(p => p + 1)}
            >
              التالي
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
}