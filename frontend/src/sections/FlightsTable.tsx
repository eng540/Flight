import { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  Table, TableBody, TableCell, TableHead, TableHeader, TableRow,
} from '@/components/ui/table';
import { Plane, Download, MapPin, Loader2, Calendar, ArrowUp, Gauge } from 'lucide-react';
import { flightsApi } from '@/api/client';
import { toast } from 'sonner';
import { FlightListResponse, FlightFilterParams } from '@/types';

// SRE FIX: إعادة تعريف الـ Props ليتوافق مع App.tsx
interface FlightsTableProps {
  data: FlightListResponse | null;
  loading: boolean;
  filters: FlightFilterParams;
  onFilterChange: (filters: FlightFilterParams) => void;
  onPageChange: (page: number) => void;
}

export function FlightsTable({ data, loading, filters, onPageChange }: FlightsTableProps) {
  const [exporting, setExporting] = useState(false);

  const flights = data?.data || [];
  const totalPages = data?.pages || 1;
  const currentPage = filters.page || 1;

  const handleExport = async () => {
    try {
      setExporting(true);
      const blob = await flightsApi.exportFlights();
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      const dateStr = new Date().toISOString().split('T')[0];
      link.setAttribute('download', `تقرير_الرحلات_${dateStr}.xlsx`);
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

  if (loading) {
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
            سجل الرحلات
            <Badge variant="secondary">{data?.total || 0} رحلة</Badge>
          </CardTitle>
          <Button variant="outline" onClick={handleExport} disabled={exporting || flights.length === 0}>
            <Download className="h-4 w-4 ml-2" />
            {exporting ? 'جارٍ التصدير...' : 'تصدير Excel'}
          </Button>
        </div>
      </CardHeader>

      <CardContent>
        <div className="rounded-md border">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="text-right">رقم الرحلة</TableHead>
                <TableHead className="text-right">الهوية (ICAO24)</TableHead>
                <TableHead className="text-right">دولة التسجيل</TableHead>
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
                  <TableCell colSpan={8} className="text-center py-8 text-muted-foreground">
                    لا توجد رحلات مطابقة للبحث
                  </TableCell>
                </TableRow>
              ) : (
                flights.map((flight: any, idx: number) => (
                  <TableRow key={flight.id || idx}>
                    <TableCell><Badge variant="outline" className="font-mono">{flight.callsign || 'N/A'}</Badge></TableCell>
                    <TableCell className="font-mono text-xs">{flight.icao24?.toUpperCase() || 'N/A'}</TableCell>
                    <TableCell>{flight.origin_country || 'غير معروف'}</TableCell>
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
                    <TableCell><div className="flex items-center gap-1"><ArrowUp className="h-3 w-3" />{formatAltitude(flight.altitude || flight.max_altitude_m)}</div></TableCell>
                    <TableCell><div className="flex items-center gap-1"><Gauge className="h-3 w-3" />{formatSpeed(flight.velocity || flight.velocity_kmh)}</div></TableCell>
                    <TableCell><div className="flex items-center gap-1 text-sm"><Calendar className="h-3 w-3" />{formatTimestamp(flight.last_seen_ts || flight.last_seen)}</div></TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between mt-4">
            <Button variant="outline" size="sm" disabled={currentPage === 1} onClick={() => onPageChange(currentPage - 1)}>
              السابق
            </Button>
            <span className="text-sm text-muted-foreground">صفحة {currentPage} من {totalPages}</span>
            <Button variant="outline" size="sm" disabled={currentPage === totalPages} onClick={() => onPageChange(currentPage + 1)}>
              التالي
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
}