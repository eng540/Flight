import { useState } from 'react';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Toaster } from '@/components/ui/sonner';
import { Header } from '@/sections/Header';
import { StatsCards } from '@/sections/StatsCards';
import { ChartsSection } from '@/sections/ChartsSection';
import { FlightsTable } from '@/sections/FlightsTable';
import { FilterSection } from '@/sections/FilterSection';
import { AnalyticsSection } from '@/sections/AnalyticsSection';
import { MapSection } from '@/sections/MapSection';
import { IngestionSection } from '@/sections/IngestionSection';
import { useStatistics } from '@/hooks/useStatistics';
import { useFilteredFlights } from '@/hooks/useFlights';
import { FlightFilterParams } from '@/types';
import './App.css';

function App() {
  const [filters, setFilters] = useState<FlightFilterParams>({ page: 1, page_size: 50 });

  const { data: stats, loading: statsLoading, refetch: refetchStats } = useStatistics();
  const { data: flightsData, loading: flightsLoading, refetch: refetchFlights } =
    useFilteredFlights(filters);

  const handleRefresh = () => { refetchStats(); refetchFlights(); };

  const handleFilterChange = (f: FlightFilterParams) =>
    setFilters({ ...f, page: 1 });

  const handlePageChange = (page: number) =>
    setFilters(prev => ({ ...prev, page }));

  return (
    <div className="min-h-screen bg-background">
      <Toaster position="top-right" richColors />

      <Header onRefresh={handleRefresh} loading={statsLoading || flightsLoading} />

      <main className="container mx-auto px-4 py-6">
        <Tabs defaultValue="dashboard" className="space-y-6">
          <TabsList className="grid w-full grid-cols-4 lg:w-auto lg:inline-flex">
            <TabsTrigger value="dashboard">📊 Dashboard</TabsTrigger>
            <TabsTrigger value="analytics">📈 Analytics</TabsTrigger>
            <TabsTrigger value="map">🗺️ Map</TabsTrigger>
            <TabsTrigger value="ingestion">📥 Ingestion</TabsTrigger>
          </TabsList>

          {/* ── Dashboard (original view) ── */}
          <TabsContent value="dashboard" className="space-y-6">
            <StatsCards stats={stats} loading={statsLoading} />
            <ChartsSection stats={stats} loading={statsLoading} />
            <FilterSection filters={filters} onFilterChange={handleFilterChange} />
            <FlightsTable
              data={flightsData}
              loading={flightsLoading}
              filters={filters}
              onFilterChange={handleFilterChange}
              onPageChange={handlePageChange}
            />
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
          <p>Flight Intelligence v2 &copy; {new Date().getFullYear()}</p>
          <p className="mt-1">
            Data: <a href="https://opensky-network.org" target="_blank" rel="noreferrer"
              className="underline">OpenSky Network</a>
            {' · '}Regions: Middle East · North Africa · Central Asia · East Africa · South Asia
          </p>
        </div>
      </footer>
    </div>
  );
}

export default App;
