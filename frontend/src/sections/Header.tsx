import { Plane, RefreshCw, Activity } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { SidebarTrigger } from '@/components/ui/sidebar';
import { useHealthCheck } from '@/hooks/useStatistics';
import { toast } from 'sonner';

interface HeaderProps {
  onRefresh: () => void;
  loading?: boolean;
}

export function Header({ onRefresh, loading }: HeaderProps) {
  const { healthy, loading: healthLoading } = useHealthCheck();

  const handleRefresh = () => {
    onRefresh();
    toast.info('جارٍ تحديث البيانات...');
  };

  return (
    <header className="border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 sticky top-0 z-10">
      <div className="flex h-16 items-center px-4 gap-4">
        {/* زر القائمة الجانبية */}
        <SidebarTrigger />
        
        <div className="flex flex-1 items-center justify-between">
          {/* الشعار */}
          <div className="flex items-center gap-3">
            <div className="bg-primary p-2 rounded-lg hidden sm:block">
              <Plane className="h-5 w-5 text-primary-foreground" />
            </div>
            <div>
              <h1 className="text-lg sm:text-xl font-bold">منصة استخبارات الطيران</h1>
              <p className="text-xs text-muted-foreground hidden sm:block">تتبع وتحليل الرحلات الجوية في الوقت الفعلي</p>
            </div>
          </div>

          {/* الحالة والأزرار */}
          <div className="flex items-center gap-3">
            {!healthLoading && (
              <Badge 
                variant={healthy ? "default" : "destructive"}
                className="hidden md:flex items-center gap-1"
              >
                <Activity className="h-3 w-3" />
                {healthy ? 'النظام متصل' : 'النظام غير متصل'}
              </Badge>
            )}

            <Button 
              variant="outline" 
              size="sm" 
              onClick={handleRefresh}
              disabled={loading}
            >
              <RefreshCw className={`h-4 w-4 ml-2 ${loading ? 'animate-spin' : ''}`} />
              <span className="hidden sm:inline">تحديث البيانات</span>
              <span className="sm:hidden">تحديث</span>
            </Button>
          </div>
        </div>
      </div>
    </header>
  );
}