import { useState, useEffect, useMemo } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import {
  Plane,
  Download,
  Search,
  MapPin,
  Loader2,
  Calendar,
  ArrowUp,
  Gauge,
} from 'lucide-react';
import { flightsApi } from '@/api/client';
import { toast } from 'sonner';

export function FlightsTable() {
  const [allData, setAllData] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [exporting, setExporting] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');

  const loadData = async () => {
    try {
      const res = await flightsApi.getFlights();
      // نتوقع أن يكون الرد كائنًا يحتوي على قائمة في res.data
      setAllData(Array.isArray(res?.data) ? res.data : Array.isArray(res) ? res : []);
    } catch (e) {
      toast.error('حدث خطأ أثناء جلب البيانات الحية');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 30000);
    return () => clearInterval(interval);
  }, []);

  // فلترة محلية حسب رمز النداء (callsign)
  const data = useMemo(() => {
    if (!searchTerm.trim()) return allData;
    const term = searchTerm.toLowerCase();
    return allData.filter(
      (f: any) =>
        f.callsign?.toLowerCase().includes(term) ||
        f.icao24?.toLowerCase().includes(term)
    );
  }, [allData, searchTerm]);

  const handleExport = async () => {
    try {
      setExporting(true);
      const blob = await flightsApi.exportFlights();
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      const dateStr = new Date().toISOString().split('T')[0];
      link.setAttribute('download', `Live_Flights_Report_${dateStr}.xlsx`);
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

  const formatAltitude = (meters: number | null) => {
    if (meters == null) return '-';
    return `${Math.round(meters).toLocaleString()} م`;
  };

  const formatSpeed = (kmh: number | null) => {
    if (kmh == null) return '-';
    return `${Math.round(kmh).toLocaleString()} كم/س`;
  };

  const formatTimestamp = (ts: number | null) =>
    ts
      ? new Date(ts * 1000).toLocaleString('ar-SA', {
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
        })
      : '-';

  if (loading) {
    return (
      <Card>
        <CardHeader>
          <div className="h-6 w-48 bg-muted rounded animate-pulse" />
        </CardHeader>
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
            <Badge variant="secondary">
              {data.length} طائرة
            </Badge>
          </CardTitle>

          <div className="flex flex-col sm:flex-row gap-2">
            <div className="flex gap-2">
              <Input
                placeholder="ابحث برمز النداء..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-48"
              />
              <Button variant="outline" size="icon">
                <Search className="h-4 w-4" />
              </Button>
            </div>

            <Button
              variant="outline"
              onClick={handleExport}
              disabled={exporting || data.length === 0}
            >
              <Download className="h-4 w-4 mr-2" />
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
                <TableHead>رقم الرحلة</TableHead>
                <TableHead>الهوية (ICAO24)</TableHead>
                <TableHead>دولة التسجيل</TableHead>
                <TableHead>مطار الإقلاع</TableHead>
                <TableHead>مطار الوصول</TableHead>
                <TableHead>الارتفاع (متر)</TableHead>
                <TableHead>السرعة (كم/س)</TableHead>
                <TableHead>آخر رصد</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={8} className="text-center py-8 text-muted-foreground">
                    لا توجد رحلات حالياً
                  </TableCell>
                </TableRow>
              ) : (
                data.map((flight: any, idx: number) => (
                  <TableRow key={flight.id || idx}>
                    <TableCell>
                      <Badge variant="outline" className="font-mono">
                        {flight.callsign || 'N/A'}
                      </Badge>
                    </TableCell>
                    <TableCell className="font-mono text-xs">
                      {flight.icao24?.toUpperCase() || 'N/A'}
                    </TableCell>
                    <TableCell>{flight.origin_country || 'غير معروف'}</TableCell>
                    <TableCell>
                      {flight.est_departure_airport ? (
                        <Badge variant="secondary" className="font-mono">
                          <MapPin className="h-3 w-3 ml-1" />
                          {flight.est_departure_airport}
                        </Badge>
                      ) : (
                        '-'
                      )}
                    </TableCell>
                    <TableCell>
                      {flight.est_arrival_airport ? (
                        <Badge variant="secondary" className="font-mono">
                          <MapPin className="h-3 w-3 ml-1" />
                          {flight.est_arrival_airport}
                        </Badge>
                      ) : (
                        '-'
                      )}
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-1">
                        <ArrowUp className="h-3 w-3" />
                        {formatAltitude(flight.altitude)}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-1">
                        <Gauge className="h-3 w-3" />
                        {formatSpeed(flight.velocity)}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center gap-1 text-sm">
                        <Calendar className="h-3 w-3" />
                        {formatTimestamp(flight.last_contact || flight.first_seen)}
                      </div>
                    </TableCell>
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