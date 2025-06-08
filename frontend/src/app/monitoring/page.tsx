'use client'

import { useQuery } from '@tanstack/react-query'
import { datasetApi } from '@/lib/api'

export default function MonitoringPage() {
  const { data: datasets, isLoading } = useQuery({
    queryKey: ['datasets'],
    queryFn: datasetApi.list,
  })

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-3 text-gray-600">Loading monitoring data...</p>
        </div>
      </div>
    )
  }

  const activeDatasets = datasets?.filter(d => d.is_active) || []
  const totalDatasets = datasets?.length || 0

  // Mock health metrics for now
  const systemHealth = {
    api_status: 'healthy',
    database_status: 'healthy',
    last_check: new Date().toISOString(),
    uptime_hours: 24.5
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 py-8">
        
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-3xl font-bold text-gray-900">System Monitoring</h1>
          <p className="mt-2 text-gray-600 max-w-2xl">
            Monitor system health, dataset connections, and spatial data quality metrics across all your PostGIS databases.
          </p>
        </div>

        {/* System Health Overview */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <div className="w-8 h-8 bg-green-100 rounded-lg flex items-center justify-center">
                  <div className="w-3 h-3 bg-green-600 rounded-full"></div>
                </div>
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600">API Status</p>
                <p className="text-2xl font-semibold text-gray-900 capitalize">{systemHealth.api_status}</p>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <div className="w-8 h-8 bg-green-100 rounded-lg flex items-center justify-center">
                  <div className="w-3 h-3 bg-green-600 rounded-full"></div>
                </div>
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600">Database</p>
                <p className="text-2xl font-semibold text-gray-900 capitalize">{systemHealth.database_status}</p>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <div className="w-8 h-8 bg-blue-100 rounded-lg flex items-center justify-center">
                  <div className="w-3 h-3 bg-blue-600 rounded-full"></div>
                </div>
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600">Uptime</p>
                <p className="text-2xl font-semibold text-gray-900">{systemHealth.uptime_hours}h</p>
              </div>
            </div>
          </div>

          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <div className="flex items-center">
              <div className="flex-shrink-0">
                <div className="w-8 h-8 bg-indigo-100 rounded-lg flex items-center justify-center">
                  <div className="w-3 h-3 bg-indigo-600 rounded-full"></div>
                </div>
              </div>
              <div className="ml-4">
                <p className="text-sm font-medium text-gray-600">Active Datasets</p>
                <p className="text-2xl font-semibold text-gray-900">{activeDatasets.length}</p>
              </div>
            </div>
          </div>
        </div>

        {/* Dataset Connection Status */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 mb-8">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">Dataset Connection Status</h2>
            <p className="text-sm text-gray-600 mt-1">Monitor the health of all PostGIS database connections</p>
          </div>
          
          <div className="p-6">
            {totalDatasets === 0 ? (
              <div className="text-center py-8">
                <p className="text-gray-500">No datasets configured yet.</p>
                <a 
                  href="/datasets" 
                  className="text-blue-600 hover:text-blue-800 font-medium mt-2 inline-block"
                >
                  Add your first dataset →
                </a>
              </div>
            ) : (
              <div className="space-y-4">
                {datasets?.map((dataset) => (
                  <div key={dataset.id} className="flex items-center justify-between p-4 border border-gray-200 rounded-lg">
                    <div className="flex items-center">
                      <div className={`w-3 h-3 rounded-full mr-3 ${
                        dataset.connection_status === 'success' ? 'bg-green-500' :
                        dataset.connection_status === 'failed' ? 'bg-red-500' :
                        dataset.connection_status === 'testing' ? 'bg-yellow-500' :
                        'bg-gray-400'
                      }`}></div>
                      <div>
                        <h3 className="font-medium text-gray-900">{dataset.name}</h3>
                        <p className="text-sm text-gray-600">
                          {dataset.host}:{dataset.port}/{dataset.database} → {dataset.schema_name}.{dataset.table_name}
                        </p>
                      </div>
                    </div>
                    
                    <div className="text-right">
                      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                        dataset.connection_status === 'success' ? 'bg-green-100 text-green-800' :
                        dataset.connection_status === 'failed' ? 'bg-red-100 text-red-800' :
                        dataset.connection_status === 'testing' ? 'bg-yellow-100 text-yellow-800' :
                        'bg-gray-100 text-gray-800'
                      }`}>
                        {dataset.connection_status || 'unknown'}
                      </span>
                      {dataset.last_check_at && (
                        <p className="text-xs text-gray-500 mt-1">
                          Last checked: {new Date(dataset.last_check_at).toLocaleString()}
                        </p>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Quick Actions */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-3">System Logs</h3>
            <p className="text-gray-600 text-sm mb-4">View detailed system logs and error reports</p>
            <button className="text-blue-600 hover:text-blue-800 font-medium text-sm">
              View Logs →
            </button>
          </div>

          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-3">Performance Metrics</h3>
            <p className="text-gray-600 text-sm mb-4">Monitor query performance and resource usage</p>
            <button className="text-blue-600 hover:text-blue-800 font-medium text-sm">
              View Metrics →
            </button>
          </div>

          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-3">Alert Settings</h3>
            <p className="text-gray-600 text-sm mb-4">Configure notifications for system events</p>
            <button className="text-blue-600 hover:text-blue-800 font-medium text-sm">
              Configure →
            </button>
          </div>
        </div>
      </div>
    </div>
  )
} 