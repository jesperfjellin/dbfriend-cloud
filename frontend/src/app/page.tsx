/**
 * Main Dashboard Page with Real Data
 */

'use client'

import { useQuery } from '@tanstack/react-query'
import { datasetApi, diffsApi } from '@/lib/api'
import Link from 'next/link'

export default function HomePage() {
  // Fetch data from backend (like requests.get() but reactive)
  const { data: datasets, isLoading: datasetsLoading } = useQuery({
    queryKey: ['datasets'],
    queryFn: datasetApi.list,
  })

  const { data: pendingCount, isLoading: pendingLoading } = useQuery({
    queryKey: ['pending-count'],
    queryFn: () => diffsApi.getPendingCount(),
  })

  // Loading state (like waiting for requests to complete)
  if (datasetsLoading || pendingLoading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading dashboard...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* This is like using a Django template */}
      <header className="bg-white shadow">
        <div className="max-w-7xl mx-auto px-4 py-6">
          <h1 className="text-3xl font-bold text-gray-900">
            dbfriend-cloud
          </h1>
          <p className="text-gray-600 mt-2">
            PostGIS geometry diff checker - like Git but for spatial data
          </p>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 py-8">
        {/* Dashboard Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          
          {/* Pending Diffs Card */}
          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center">
              <div className="ml-4">
                <h2 className="text-lg font-medium text-gray-900">
                  Pending Reviews
                </h2>
                                 <p className="text-2xl font-bold text-yellow-600">
                   {pendingCount?.pending_count || 0}
                 </p>
              </div>
            </div>
            <p className="text-sm text-gray-500 mt-2">
              Geometry changes waiting for review
            </p>
          </div>

          {/* Active Datasets Card */}
          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center">
              <div className="ml-4">
                <h2 className="text-lg font-medium text-gray-900">
                  Active Datasets
                </h2>
                                 <p className="text-2xl font-bold text-blue-600">
                   {datasets?.length || 0}
                 </p>
              </div>
            </div>
            <p className="text-sm text-gray-500 mt-2">
              Spatial tables being monitored
            </p>
          </div>

          {/* Recent Activity Card */}
          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center">
              <div className="ml-4">
                <h2 className="text-lg font-medium text-gray-900">
                  Last Check
                </h2>
                <p className="text-lg font-bold text-green-600">
                  Never {/* We'll make this dynamic later */}
                </p>
              </div>
            </div>
            <p className="text-sm text-gray-500 mt-2">
              Most recent geometry import
            </p>
          </div>
        </div>

        {/* Quick Actions */}
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">
            Quick Actions
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <Link 
              href="/datasets"
              className="flex items-center p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
            >
              <div>
                <div className="font-medium text-gray-900">Manage Databases</div>
                <div className="text-sm text-gray-500">Configure connections</div>
              </div>
            </Link>

            <Link 
              href="/monitoring"
              className="flex items-center p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
            >
              <div>
                <div className="font-medium text-gray-900">Change Monitor</div>
                <div className="text-sm text-gray-500">Track data changes</div>
              </div>
            </Link>
            
            <Link 
              href="/diffs"
              className="flex items-center p-4 border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
            >
              <div>
                <div className="font-medium text-gray-900">Review Diffs</div>
                <div className="text-sm text-gray-500">
                  {pendingCount?.pending_count && pendingCount.pending_count > 0 
                    ? `${pendingCount.pending_count} pending` 
                    : 'No pending reviews'
                  }
                </div>
              </div>
            </Link>
          </div>
        </div>

      </main>
    </div>
  )
} 