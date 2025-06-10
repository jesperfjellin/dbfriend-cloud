/**
 * Quality Tests Page - Map-Centric View
 * 
 * Map-focused interface for reviewing spatial quality tests
 * Left sidebar shows test list, right side shows map with test geometries
 */

'use client'

import { useState, useEffect } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { testsApi, datasetApi, geometryApi } from '@/lib/api'
import { SpatialCheck, Dataset } from '@/types/api'
import TestMap from '@/components/TestMap'
import { useGeometryContext } from '@/lib/hooks/useGeometryContext'

export default function TestsPage() {
  const [selectedTest, setSelectedTest] = useState<SpatialCheck | null>(null)
  const [filter, setFilter] = useState<'all' | 'pass' | 'fail' | 'warning'>('fail')
  const [selectedDataset, setSelectedDataset] = useState<string>('all')
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize] = useState(50) // Smaller page size for sidebar
  
  const queryClient = useQueryClient()

  // Geometry context hook for spatial analysis
  const { contextData, loading: contextLoading, fetchContext, clearContext } = useGeometryContext()

  // Fetch available datasets for filtering
  const { data: datasets } = useQuery({
    queryKey: ['datasets'],
    queryFn: () => datasetApi.list(),
  })

  // Fetch spatial checks (quality tests) with pagination
  const { data: tests, isLoading } = useQuery({
    queryKey: ['tests', filter, selectedDataset, currentPage],
    queryFn: () => testsApi.list({ 
      check_result: filter === 'all' ? undefined : filter.toUpperCase(),
      dataset_id: selectedDataset === 'all' ? undefined : selectedDataset,
      skip: (currentPage - 1) * pageSize,
      limit: pageSize
    }),
  })

  // Fetch test statistics
  const { data: stats } = useQuery({
    queryKey: ['test-stats', selectedDataset],
    queryFn: () => testsApi.getStats(selectedDataset === 'all' ? undefined : selectedDataset),
  })

  // Fetch geometry data for selected test
  const { data: geometryData, error: geometryError, isLoading: geometryLoading } = useQuery<{
    snapshot_id: string;
    geometry: any;
    attributes?: any;
  } | null>({
    queryKey: ['test-geometry', selectedTest?.snapshot_id],
    queryFn: async () => {
      if (!selectedTest) return null
      try {
        const data = await geometryApi.getGeoJSON(selectedTest.snapshot_id)
        console.log('Geometry data received:', data) // Debug log
        return data as { snapshot_id: string; geometry: any; attributes?: any }
      } catch (error) {
        console.error('Failed to fetch geometry:', error)
        throw error
      }
    },
    enabled: !!selectedTest?.snapshot_id,
    retry: 1,
  })

  // Auto-select first failed test when page loads
  useEffect(() => {
    if (tests && tests.length > 0 && !selectedTest) {
      setSelectedTest(tests[0])
    }
  }, [tests, selectedTest])

  // Fetch spatial context for any selected test to provide spatial context
  useEffect(() => {
    if (selectedTest && geometryData) {
      console.log('Fetching spatial context for test:', selectedTest.check_type, selectedTest.snapshot_id)
      fetchContext(selectedTest.snapshot_id, 500) // 500m buffer
    } else {
      clearContext()
    }
  }, [selectedTest, geometryData, fetchContext, clearContext])

  // Reset to page 1 when filters change
  const handleFilterChange = (newFilter: typeof filter) => {
    setFilter(newFilter)
    setCurrentPage(1)
    setSelectedTest(null)
  }

  const handleDatasetChange = (newDataset: string) => {
    setSelectedDataset(newDataset)
    setCurrentPage(1)
    setSelectedTest(null)
  }

  // Calculate total count based on current filter
  const getTotalCount = () => {
    if (!stats) return 0
    
    let total = 0
    Object.values(stats.check_stats).forEach(results => {
      if (filter === 'all') {
        total += Object.values(results).reduce((sum, count) => sum + count, 0)
      } else {
        const filterKey = filter.toUpperCase()
        total += results[filterKey] || 0
      }
    })
    
    return total
  }

  const totalCount = getTotalCount()
  const totalPages = Math.ceil(totalCount / pageSize)

  // Calculate if there might be more pages
  const hasNextPage = currentPage < totalPages
  const hasPrevPage = currentPage > 1

  const getCheckTypeColor = (checkType: string) => {
    switch (checkType.toLowerCase()) {
      case 'validity': return 'bg-blue-100 text-blue-800'
      case 'topology': return 'bg-green-100 text-green-800'
      case 'duplicate': return 'bg-yellow-100 text-yellow-800'
      case 'area': return 'bg-purple-100 text-purple-800'
      default: return 'bg-gray-100 text-gray-800'
    }
  }

  const getResultColor = (result: string) => {
    switch (result.toLowerCase()) {
      case 'pass': return 'bg-green-100 text-green-800'
      case 'fail': return 'bg-red-100 text-red-800'
      case 'warning': return 'bg-yellow-100 text-yellow-800'
      default: return 'bg-gray-100 text-gray-800'
    }
  }

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-purple-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading quality tests...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="h-[calc(100vh-4rem-2px)] bg-gray-50 flex flex-col overflow-hidden">
      {/* Header */}
      <header className="bg-white flex-shrink-0" style={{ borderBottom: '1px solid rgb(229 231 235)', boxSizing: 'border-box' }}>
        <div className="w-full px-4 py-4">
          <div className="flex justify-between items-center">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">
                Quality Tests Monitor
              </h1>
              <p className="text-sm text-gray-600">
                {totalCount.toLocaleString()} tests • Inspect spatial data quality issues
              </p>
            </div>
            
            {/* Filter controls */}
            <div className="flex space-x-4">
              {/* Dataset filter */}
              <select
                value={selectedDataset}
                onChange={(e) => handleDatasetChange(e.target.value)}
                className="px-3 py-2 text-sm border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-purple-500"
              >
                <option value="all">All Datasets</option>
                {datasets?.map((dataset) => (
                  <option key={dataset.id} value={dataset.id}>
                    {dataset.name}
                  </option>
                ))}
              </select>

              {/* Result filter */}
              <div className="flex space-x-1">
                {(['all', 'fail', 'warning', 'pass'] as const).map((status) => (
                  <button
                    key={status}
                    onClick={() => handleFilterChange(status)}
                    className={`px-3 py-2 text-sm rounded-lg capitalize transition-colors ${
                      filter === status
                        ? 'bg-purple-600 text-white'
                        : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                    }`}
                  >
                    {status}
                  </button>
                ))}
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* Main Content - Sidebar + Map */}
      <div className="flex flex-1 min-h-0 overflow-hidden">
        
        {/* Left Sidebar - Tests List */}
        <div className="w-80 bg-white border-r border-gray-200 flex flex-col flex-shrink-0">
          
          {/* Sidebar Header */}
          <div className="p-4 border-b border-gray-200 flex-shrink-0">
            <div className="flex justify-between items-center">
              <h2 className="font-semibold text-gray-900">
                Tests ({(currentPage - 1) * pageSize + 1}-{Math.min(currentPage * pageSize, totalCount)} of {totalCount.toLocaleString()})
              </h2>
              {totalPages > 1 && (
                <span className="text-xs text-gray-500">
                  Page {currentPage}/{totalPages}
                </span>
              )}
            </div>
          </div>
          
          {/* Tests List */}
          <div className="flex-1 overflow-y-auto">
            {tests?.length === 0 ? (
              <div className="h-full flex items-center justify-center text-gray-500 p-4">
                <div className="text-center">
                  <p>No {filter === 'all' ? '' : filter} tests found</p>
                  <p className="text-xs mt-1">Try adjusting your filters</p>
                </div>
              </div>
            ) : (
              tests?.map((test) => (
                <div
                  key={test.id}
                  onClick={() => setSelectedTest(test)}
                  className={`p-4 border-b border-gray-100 cursor-pointer hover:bg-gray-50 transition-colors ${
                    selectedTest?.id === test.id ? 'bg-purple-50 border-purple-200' : ''
                  }`}
                >
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center space-x-2">
                        <span className={`px-2 py-1 text-xs rounded-full font-medium ${getCheckTypeColor(test.check_type)}`}>
                          {test.check_type}
                        </span>
                        <span className={`px-2 py-1 text-xs rounded-full font-medium ${getResultColor(test.check_result)}`}>
                          {test.check_result}
                        </span>
                      </div>
                      <div className="text-xs text-gray-500">
                        {datasets?.find(d => d.id === test.dataset_id)?.name || 'Unknown'}
                      </div>
                    </div>
                    
                    {test.error_message && (
                      <p className="text-sm text-gray-700 leading-tight">
                        {test.error_message}
                      </p>
                    )}
                    
                    <div className="flex justify-between items-center text-xs text-gray-500">
                      <span>ID: {test.snapshot_id.slice(0, 8)}...</span>
                      <span>{new Date(test.created_at).toLocaleDateString()}</span>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>

          {/* Pagination Controls */}
          {totalPages > 1 && (
            <div className="p-3 border-t border-gray-200 bg-gray-50 flex justify-between items-center flex-shrink-0">
              <button
                onClick={() => setCurrentPage(prev => prev - 1)}
                disabled={!hasPrevPage}
                className={`px-3 py-1 text-sm rounded transition-colors ${
                  hasPrevPage
                    ? 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-50'
                    : 'bg-gray-100 text-gray-400 cursor-not-allowed'
                }`}
              >
                ← Prev
              </button>
              
              <span className="text-xs text-gray-600">
                {currentPage} of {totalPages}
              </span>
              
              <button
                onClick={() => setCurrentPage(prev => prev + 1)}
                disabled={!hasNextPage}
                className={`px-3 py-1 text-sm rounded transition-colors ${
                  hasNextPage
                    ? 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-50'
                    : 'bg-gray-100 text-gray-400 cursor-not-allowed'
                }`}
              >
                Next →
              </button>
            </div>
          )}
        </div>

        {/* Right Side - Map */}
        <div className="flex-1 relative min-w-0">
          {selectedTest ? (
            <>
              <TestMap
                className="h-full"
                geometry={!contextData ? geometryData?.geometry : undefined}
                geometries={contextData?.geometries.map(item => ({
                  geometry: item.geometry,
                  isPrimary: item.is_primary,
                  id: item.geometry_id,
                  attributes: item.attributes
                }))}
                highlightError={selectedTest.check_result === 'FAIL'}
              />
              
              {/* Map Overlay - Test Details */}
              <div className="absolute top-4 right-4 bg-white rounded-lg shadow-lg p-4 max-w-sm">
                <h3 className="font-semibold text-gray-900 mb-2">Test Details</h3>
                <div className="space-y-2 text-sm">
                  <div className="flex items-center space-x-2">
                    <span className={`px-2 py-1 text-xs rounded-full ${getCheckTypeColor(selectedTest.check_type)}`}>
                      {selectedTest.check_type}
                    </span>
                    <span className={`px-2 py-1 text-xs rounded-full ${getResultColor(selectedTest.check_result)}`}>
                      {selectedTest.check_result}
                    </span>
                  </div>
                  
                  {selectedTest.error_message && (
                    <div>
                      <span className="font-medium text-gray-700">Issue:</span>
                      <p className="text-gray-600 mt-1">{selectedTest.error_message}</p>
                    </div>
                  )}
                  
                  <div className="pt-2 border-t border-gray-200 space-y-1 text-xs text-gray-500">
                    <div>Dataset: {datasets?.find(d => d.id === selectedTest.dataset_id)?.name}</div>
                    <div>Snapshot: {selectedTest.snapshot_id.slice(0, 8)}...</div>
                    <div>Detected: {new Date(selectedTest.created_at).toLocaleString()}</div>
                    {contextLoading && (
                      <div className="text-blue-600 flex items-center">
                        <div className="animate-spin rounded-full h-3 w-3 border-b border-blue-600 mr-1"></div>
                        Loading spatial context...
                      </div>
                    )}
                    {contextData && (
                      <div className="text-green-600">
                        Found {contextData.total_found} nearby geometries
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </>
          ) : (
            <div className="h-full flex items-center justify-center bg-gray-100">
              <div className="text-center text-gray-500">
                <div className="text-6xl mb-4">🗺️</div>
                <h3 className="text-xl font-medium mb-2">Select a Test to View</h3>
                <p className="text-gray-400">Click on a test from the sidebar to see its geometry on the map</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
} 