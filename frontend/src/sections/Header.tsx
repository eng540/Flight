import { Plane, RefreshCw, Activity } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
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
    toast.info('Refreshing data...');
  };

  return (
    <header className="border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container mx-auto px-4 py-4">
        <div className="flex items-center justify-between">
          {/* Logo */}
          <div className="flex items-center gap-3">
            <div className="bg-primary p-2 rounded-lg">
              <Plane className="h-6 w-6 text-primary-foreground" />
            </div>
            <div>
              <h1 className="text-xl font-bold">Flight Intelligence</h1>
              <p className="text-xs text-muted-foreground">Real-time Flight Tracking & Analytics</p>
            </div>
          </div>

          {/* Status & Actions */}
          <div className="flex items-center gap-3">
            {/* Health Status */}
            {!healthLoading && (
              <Badge 
                variant={healthy ? "default" : "destructive"}
                className="hidden sm:flex items-center gap-1"
              >
                <Activity className="h-3 w-3" />
                {healthy ? 'System Online' : 'System Offline'}
              </Badge>
            )}

            {/* Refresh Button */}
            <Button 
              variant="outline" 
              size="sm" 
              onClick={handleRefresh}
              disabled={loading}
            >
              <RefreshCw className={`h-4 w-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
          </div>
        </div>
      </div>
    </header>
  );
}
