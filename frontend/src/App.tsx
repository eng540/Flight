import { useState } from 'react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Toaster } from '@/components/ui/sonner';
import { Header } from '@/sections/Header';
import { StatsCards } from '@/sections/StatsCards';
import { ChartsSection } from '@/sections/ChartsSection';
import { FlightsTable } from '@/sections/FlightsTable';
import { AnalyticsSection } from '@/sections/AnalyticsSection';
import { MapSection } from '@/sections/MapSection';
import { IngestionSection } from '@/sections/IngestionSection';
import { useStatistics } from '@/hooks/useStatistics';
import './App.css';

function App() {
  const { data: stats, loading: statsLoading, refetch: refetchStats } = useStatistics();

  const handleRefresh = () => {
    refetchStats();
    // تم إزالة refetchFlights لأن FlightsTable يدير بياناته ذاتياً
  };

  return (
    <div className="min-h-screen bg-background">
      <Toaster position="top-right" richColors />

      <Header onRefresh={handleRefresh} loading={statsLoading} />

      <main className="container mx-auto px-4 py-6">
        <Tabs defaultValue="dashboard" className="space-y-6">
          <TabsList className="grid w-full grid-cols-4 lg:w-auto lg:inline-flex">
            <TabsTrigger value="dashboard">📊 لوحة القيادة</TabsTrigger>
            <TabsTrigger value="analytics">📈 التحليلات</TabsTrigger>
            <TabsTrigger value="map">🗺️ الخريطة الحية</TabsTrigger>
            <TabsTrigger value="ingestion">📥 جلب البيانات</TabsTrigger>
          </TabsList>

          {/* ── Dashboard ── */}
          <TabsContent value="dashboard" className="space-y-6">
            <StatsCards stats={stats} loading={statsLoading} />
            <ChartsSection stats={stats} loading={statsLoading} />
            {/* تم إزالة FilterSection لأن البحث أصبح مدمجاً داخل الجدول الحي */}
            <FlightsTable />
          </TabsContent>

          {/* ── Analytics ── */}
          <TabsContent value="analytics">
            <AnalyticsSection />
          </TabsContent>

          {/* ── Map ── */}
          <TabsContent value="map">
            <MapSection />
          </TabsContent>

          {/* ── Ingestion ── */}
          <TabsContent value="ingestion">
            <IngestionSection />
          </TabsContent>
        </Tabs>
      </main>

      <footer className="border-t mt-12 py-6">
        <div className="container mx-auto px-4 text-center text-sm text-muted-foreground">
          <p>منصة استخبارات الطيران الإصدار 4.0 &copy; {new Date().getFullYear()}</p>
          <p className="mt-1">
            المناطق المدعومة: الشرق الأوسط · شمال أفريقيا · آسيا الوسطى
          </p>
        </div>
      </footer>
    </div>
  );
}

export default App;