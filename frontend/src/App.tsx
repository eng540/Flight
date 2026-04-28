import { useState, useEffect, useCallback } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table';
import { Plane, Download, MapPin, Loader2, Calendar, ArrowUp, Gauge, Search } from 'lucide-react';
import { flightsApi } from '@/api/client';
import { toast } from 'sonner';

export function FlightsTable() {
  const [flights, setFlights] = useState<any[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [exporting, setExporting] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  
  // Bounds الافتراضية (الشرق الأوسط وشمال أفريقيا)
  const defaultBounds = "63.0,12.0,25.0,42.0";

  const loadData = useCallback(async () => {
    try {
      // نمرر الـ bounds و الـ callsign للبحث في قاعدة البيانات مباشرة
      const res = await flightsApi.getFlights(defaultBounds);
      
      // فلترة محلية سريعة إذا كان هناك نص بحث (أو يمكن إرسالها للـ API)
      let filteredData = res?.data || [];
      if (searchTerm.trim()) {
        const term = searchTerm.toLowerCase();
        filteredData = filteredData.filter((f: any) => 
          f.callsign?.toLowerCase().includes(term) || 
          f.icao24?.toLowerCase().includes(term)
        );
      }
      
      setFlights(filteredData);
      setTotal(filteredData.length);
    } catch (e) {
      toast.error('حدث خطأ أثناء جلب البيانات الحية');
    } finally {
      setLoading(false);
    }
  }, [searchTerm]);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 30000); // تحديث كل 30 ثانية
    return () => clearInterval(interval);
  }, [loadData]);

  const handleExport = async () => {
    try {
      setExporting(true);
      const blob = await flightsApi.exportFlights(defaultBounds);
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

  if (loading && flights.length === 0) {
    return (
      <Card>
        <CardHeader><div className="h-6 w-48 bg-muted rounded animate-pulse" /></CardHeader>
        <CardContent>
          <div className="h-96 bg-muted rounded animate-pulse flex items-center justify-center">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        </CardContent>
      </Card>
    );
  }

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

            <Button variant="outline" onClick={handleExport} disabled={exporting || flights.length === 0}>
              <Download className="h-4 w-4 ml-2" />
              {exporting ? 'جارٍ التصدير...' : 'تصدير Excel'}
            </Button>
          </div>
        </div>
      </CardHeader>

      <CardContent>
        <div className="rounded-md border">
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
              {flights.length === 0 ? (
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
      </CardContent>
    </Card>
  );
}