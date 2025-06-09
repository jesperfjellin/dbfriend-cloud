/**
 * Quality Tests Page
 * 
 * Monitor persistent topology and validity tests on spatial data
 * Similar to diffs but shows quality check results instead of changes
 */

'use client'

import { useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { testsApi, datasetApi } from '@/lib/api'
import { SpatialCheck, Dataset } from '@/types/api'

export default function TestsPage() {
  const [selectedTest, setSelectedTest] = useState<SpatialCheck | null>(null)
  const [filter, setFilter] = useState<'all' | 'pass' | 'fail' | 'warning'>('fail')
  const [selectedDataset, setSelectedDataset] = useState<string>('all')
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize] = useState(100) // Items per page
  
  const queryClient = useQueryClient()

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

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow">
        <div className="max-w-7xl mx-auto px-4 py-6">
          <div className="flex justify-between items-center">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">
                Quality Tests Monitor
              </h1>
              <p className="text-gray-600 mt-2">
                Persistent topology and validity checks on your spatial data
              </p>
            </div>
            
            {/* Filter controls */}
            <div className="flex space-x-4">
              {/* Dataset filter */}
              <select
                value={selectedDataset}
                onChange={(e) => handleDatasetChange(e.target.value)}
                className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-purple-500"
              >
                <option value="all">All Datasets</option>
                {datasets?.map((dataset) => (
                  <option key={dataset.id} value={dataset.id}>
                    {dataset.name}
                  </option>
                ))}
              </select>

              {/* Result filter */}
              <div className="flex space-x-2">
                {(['all', 'fail', 'warning', 'pass'] as const).map((status) => (
                  <button
                    key={status}
                    onClick={() => handleFilterChange(status)}
                    className={`px-4 py-2 rounded-lg capitalize transition-colors ${
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

      <div className="max-w-7xl mx-auto px-4 py-8">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          
          {/* Tests List */}
          <div className="bg-white rounded-lg shadow flex flex-col h-[600px]">
            <div className="p-6 border-b flex-shrink-0">
              <div className="flex justify-between items-center">
                <h2 className="text-xl font-semibold">
                  Quality Tests ({totalCount.toLocaleString()})
                </h2>
                <div className="text-sm text-gray-600">
                  {totalPages > 0 ? `Page ${currentPage} of ${totalPages}` : 'No results'}
                </div>
              </div>
            </div>
            
            <div className="flex-1 overflow-y-auto">
              {tests?.length === 0 ? (
                <div className="h-full flex items-center justify-center text-gray-500">
                  No {filter === 'all' ? '' : filter} tests found
                </div>
              ) : (
                tests?.map((test) => (
                  <div
                    key={test.id}
                    onClick={() => setSelectedTest(test)}
                    className={`p-4 border-b cursor-pointer hover:bg-gray-50 transition-colors ${
                      selectedTest?.id === test.id ? 'bg-purple-50 border-purple-200' : ''
                    }`}
                  >
                    <div className="flex justify-between items-start">
                      <div>
                        <div className="flex items-center space-x-2">
                          <span className={`px-2 py-1 text-xs rounded-full ${getCheckTypeColor(test.check_type)}`}>
                            {test.check_type}
                          </span>
                          <span className={`px-2 py-1 text-xs rounded-full ${getResultColor(test.check_result)}`}>
                            {test.check_result}
                          </span>
                        </div>
                        {test.error_message && (
                          <p className="text-sm text-gray-600 mt-1">
                            {test.error_message}
                          </p>
                        )}
                        <p className="text-xs text-gray-500 mt-1">
                          {new Date(test.created_at).toLocaleDateString()} {new Date(test.created_at).toLocaleTimeString()}
                        </p>
                      </div>
                      
                      <div className="text-right">
                        <div className="text-xs text-gray-500">Dataset</div>
                        <div className="text-sm font-medium">
                          {datasets?.find(d => d.id === test.dataset_id)?.name || 'Unknown'}
                        </div>
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>

            {/* Pagination Controls */}
            {totalPages > 1 && (
              <div className="p-4 border-t bg-gray-50 flex justify-between items-center flex-shrink-0">
                <button
                  onClick={() => setCurrentPage(prev => prev - 1)}
                  disabled={!hasPrevPage}
                  className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                    hasPrevPage
                      ? 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-50'
                      : 'bg-gray-100 text-gray-400 cursor-not-allowed'
                  }`}
                >
                  ← Previous
                </button>
                
                <div className="flex items-center space-x-4">
                  <span className="text-sm text-gray-600">
                    Page {currentPage} of {totalPages}
                  </span>
                  <span className="text-xs text-gray-500">
                    ({((currentPage - 1) * pageSize + 1).toLocaleString()}-{Math.min(currentPage * pageSize, totalCount).toLocaleString()} of {totalCount.toLocaleString()})
                  </span>
                </div>
                
                <button
                  onClick={() => setCurrentPage(prev => prev + 1)}
                  disabled={!hasNextPage}
                  className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
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

          {/* Test Details & Statistics */}
          <div className="bg-white rounded-lg shadow">
            <div className="p-6 border-b">
              <h2 className="text-xl font-semibold">
                {selectedTest ? 'Test Details' : 'Test Statistics'}
              </h2>
            </div>
            
            <div className="p-6">
              {selectedTest ? (
                <div>
                  {/* Test Details */}
                  <div className="space-y-4">
                    <div>
                      <h3 className="text-lg font-medium mb-2">Check Information</h3>
                      <div className="grid grid-cols-2 gap-4 text-sm">
                        <div>
                          <span className="text-gray-500">Type:</span>
                          <span className={`ml-2 px-2 py-1 rounded-full text-xs ${getCheckTypeColor(selectedTest.check_type)}`}>
                            {selectedTest.check_type}
                          </span>
                        </div>
                        <div>
                          <span className="text-gray-500">Result:</span>
                          <span className={`ml-2 px-2 py-1 rounded-full text-xs ${getResultColor(selectedTest.check_result)}`}>
                            {selectedTest.check_result}
                          </span>
                        </div>
                        <div>
                          <span className="text-gray-500">Snapshot ID:</span>
                          <span className="ml-2 font-mono text-xs">{selectedTest.snapshot_id.slice(0, 8)}...</span>
                        </div>
                        <div>
                          <span className="text-gray-500">Created:</span>
                          <span className="ml-2">{new Date(selectedTest.created_at).toLocaleString()}</span>
                        </div>
                      </div>
                    </div>

                    {selectedTest.error_message && (
                      <div>
                        <h3 className="text-lg font-medium mb-2">Error Details</h3>
                        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                          <p className="text-red-800">{selectedTest.error_message}</p>
                          {selectedTest.error_details && (
                            <pre className="mt-2 text-xs text-red-600 overflow-auto">
                              {JSON.stringify(selectedTest.error_details, null, 2)}
                            </pre>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              ) : (
                <div>
                  {/* Statistics */}
                  {stats && (
                    <div className="space-y-6">
                      <h3 className="text-lg font-medium">Test Results Summary</h3>
                      
                      {Object.entries(stats.check_stats).map(([checkType, results]) => (
                        <div key={checkType} className="border rounded-lg p-4">
                          <h4 className={`font-medium mb-3 px-2 py-1 rounded inline-block ${getCheckTypeColor(checkType)}`}>
                            {checkType} Tests
                          </h4>
                          <div className="grid grid-cols-3 gap-4">
                            {Object.entries(results).map(([result, count]) => (
                              <div key={result} className="text-center">
                                <div className={`text-2xl font-bold mb-1 ${
                                  result === 'PASS' ? 'text-green-600' :
                                  result === 'FAIL' ? 'text-red-600' :
                                  'text-yellow-600'
                                }`}>
                                  {count}
                                </div>
                                <div className="text-sm text-gray-600 capitalize">{result.toLowerCase()}</div>
                              </div>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
} 