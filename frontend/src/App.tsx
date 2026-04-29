import { useState } from 'react';
import { Toaster } from '@/components/ui/sonner';
import { 
  SidebarProvider, 
  Sidebar, 
  SidebarContent, 
  SidebarGroup, 
  SidebarGroupContent, 
  SidebarGroupLabel, 
  SidebarMenu, 
  SidebarMenuButton, 
  SidebarMenuItem,
  SidebarHeader as SidebarHeaderUI
} from '@/components/ui/sidebar';
import { 
  LayoutDashboard, 
  Map, 
  BarChart3, 
  Database, 
  Search, 
  FileText, 
  Lightbulb,
  Plane
} from 'lucide-react';

// استيراد الأقسام
import { Header } from '@/sections/Header';
import { StatsCards } from '@/sections/StatsCards';
import { ChartsSection } from '@/sections/ChartsSection';
import { FlightsTable } from '@/sections/FlightsTable';
import { AnalyticsSection } from '@/sections/AnalyticsSection';
import { MapSection } from '@/sections/MapSection';
import { IngestionSection } from '@/sections/IngestionSection';

// استيراد الـ Hooks
import { useStatistics } from '@/hooks/useStatistics';

import './App.css';

// تعريف عناصر القائمة الجانبية
const menuItems = [
  { id: 'dashboard', title: 'لوحة القيادة', icon: LayoutDashboard, group: 'الرئيسية' },
  { id: 'map', title: 'الخريطة الحية', icon: Map, group: 'الرئيسية' },
  { id: 'analytics', title: 'التحليلات', icon: BarChart3, group: 'الرئيسية' },
  
  { id: 'search', title: 'البحث المتقدم', icon: Search, group: 'الاستخبارات والتقارير' },
  { id: 'reports', title: 'منشئ التقارير', icon: FileText, group: 'الاستخبارات والتقارير' },
  { id: 'insights', title: 'الأسئلة الذكية (Q&A)', icon: Lightbulb, group: 'الاستخبارات والتقارير' },
  
  { id: 'ingestion', title: 'إدارة البيانات', icon: Database, group: 'النظام' },
];

function App() {
  const [activeView, setActiveView] = useState('dashboard');
  const { data: stats, loading: statsLoading, refetch: refetchStats } = useStatistics();

  const handleRefresh = () => { 
    refetchStats(); 
    // ملاحظة: FlightsTable يجلب بياناته بنفسه الآن
  };

  // دالة لعرض المحتوى بناءً على الاختيار
  const renderContent = () => {
    switch (activeView) {
      case 'dashboard':
        return (
          <div className="space-y-6 animate-in fade-in duration-500">
            <StatsCards stats={stats} loading={statsLoading} />
            <ChartsSection stats={stats} loading={statsLoading} />
            <FlightsTable />
          </div>
        );
      case 'map':
        return <div className="animate-in fade-in duration-500"><MapSection /></div>;
      case 'analytics':
        return <div className="animate-in fade-in duration-500"><AnalyticsSection /></div>;
      case 'ingestion':
        return <div className="animate-in fade-in duration-500"><IngestionSection /></div>;
      
      // الأقسام الجديدة (قيد التطوير)
      case 'search':
      case 'reports':
      case 'insights':
        return (
          <div className="flex flex-col items-center justify-center h-[60vh] text-center animate-in zoom-in-95 duration-500">
            <div className="bg-muted p-6 rounded-full mb-4">
              <Plane className="h-12 w-12 text-muted-foreground opacity-50" />
            </div>
            <h2 className="text-2xl font-bold mb-2">قريباً...</h2>
            <p className="text-muted-foreground max-w-md">
              هذا القسم قيد التطوير حالياً. سيتم إضافة أدوات استخبارات الطيران المتقدمة هنا في المرحلة القادمة.
            </p>
          </div>
        );
      default:
        return null;
    }
  };

  return (
    <SidebarProvider>
      <div className="flex min-h-screen w-full bg-background text-foreground" dir="rtl">
        <Toaster position="top-right" richColors />
        
        {/* الشريط الجانبي */}
        <Sidebar side="right" variant="inset">
          <SidebarHeaderUI className="p-4 border-b">
            <div className="flex items-center gap-2 font-bold text-lg text-primary">
              <Plane className="h-6 w-6" />
              <span>استخبارات الطيران</span>
            </div>
          </SidebarHeaderUI>
          
          <SidebarContent>
            {/* تجميع العناصر حسب الـ Group */}
            {Array.from(new Set(menuItems.map(item => item.group))).map(group => (
              <SidebarGroup key={group}>
                <SidebarGroupLabel>{group}</SidebarGroupLabel>
                <SidebarGroupContent>
                  <SidebarMenu>
                    {menuItems.filter(item => item.group === group).map((item) => (
                      <SidebarMenuItem key={item.id}>
                        <SidebarMenuButton 
                          isActive={activeView === item.id}
                          onClick={() => setActiveView(item.id)}
                          tooltip={item.title}
                        >
                          <item.icon className="h-4 w-4" />
                          <span>{item.title}</span>
                        </SidebarMenuButton>
                      </SidebarMenuItem>
                    ))}
                  </SidebarMenu>
                </SidebarGroupContent>
              </SidebarGroup>
            ))}
          </SidebarContent>
        </Sidebar>

        {/* المحتوى الرئيسي */}
        <div className="flex flex-col flex-1 w-full overflow-hidden">
          <Header onRefresh={handleRefresh} loading={statsLoading} />
          
          <main className="flex-1 overflow-y-auto p-4 md:p-6">
            <div className="container mx-auto max-w-7xl">
              {renderContent()}
            </div>
          </main>
        </div>
      </div>
    </SidebarProvider>
  );
}

export default App;