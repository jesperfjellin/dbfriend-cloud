'use client'

import { useEffect, useState } from 'react'
import { monitoringApi, DatasetMonitoringStatus, QualityCheckStatus } from '@/lib/api'

export default function MonitoringPage() {
  const [datasets, setDatasets] = useState<DatasetMonitoringStatus[]>([])
  const [qualityStatuses, setQualityStatuses] = useState<Record<string, QualityCheckStatus>>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [runningChecks, setRunningChecks] = useState<Set<string>>(new Set())

  // Load datasets monitoring status function
  const loadDatasetsStatus = async () => {
    try {
      setLoading(true)
      const datasetsData = await monitoringApi.getDatasetsStatus()
      setDatasets(datasetsData)
      
      // Load quality check status for each dataset
      const statusPromises = datasetsData.map(async (dataset) => {
        try {
          const status = await monitoringApi.getQualityCheckStatus(dataset.dataset_id)
          return { datasetId: dataset.dataset_id, status }
        } catch (err) {
          console.error(`Failed to load quality status for ${dataset.dataset_name}:`, err)
          return null
        }
      })
      
      const statuses = await Promise.all(statusPromises)
      const statusMap: Record<string, QualityCheckStatus> = {}
      statuses.forEach(item => {
        if (item) {
          statusMap[item.datasetId] = item.status
        }
      })
      setQualityStatuses(statusMap)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load monitoring data')
    } finally {
      setLoading(false)
    }
  }

  // Load datasets monitoring status
  useEffect(() => {
    loadDatasetsStatus()
  }, [])

  // Handle starting quality checks
  const handleStartQualityChecks = async (datasetId: string, datasetName: string) => {
    try {
      setRunningChecks(prev => new Set(prev).add(datasetId))
      
      const response = await monitoringApi.startQualityChecks(datasetId)
      console.log('Quality checks started:', response)
      
      // Poll for status updates
      const pollStatus = async () => {
        try {
          const status = await monitoringApi.getQualityCheckStatus(datasetId)
          setQualityStatuses(prev => ({
            ...prev,
            [datasetId]: status
          }))
          
          // Continue polling if still running
          if (status.status === 'running') {
            setTimeout(pollStatus, 5000) // Poll every 5 seconds (less aggressive)
          } else {
            // Quality checks completed or failed - stop polling
            setRunningChecks(prev => {
              const newSet = new Set(prev)
              newSet.delete(datasetId)
              return newSet
            })
            
            // Show completion message and reload main status
            if (status.status === 'completed') {
              console.log(`Quality checks completed for ${datasetName}`)
              // Reload the main datasets status to refresh everything
              loadDatasetsStatus()
            } else if (status.status === 'failed') {
              console.error(`Quality checks failed for ${datasetName}: ${status.error_message}`)
              // Still reload to ensure consistency
              loadDatasetsStatus()
            }
            
            // Clear the individual status after completion to avoid stale data
            setTimeout(() => {
              setQualityStatuses(prev => {
                const updated = { ...prev }
                delete updated[datasetId]
                return updated
              })
            }, 10000) // Clear after 10 seconds
          }
        } catch (err) {
          console.error('Error polling quality check status:', err)
          setRunningChecks(prev => {
            const newSet = new Set(prev)
            newSet.delete(datasetId)
            return newSet
          })
        }
      }
      
      // Start polling after a short delay
      setTimeout(pollStatus, 1000)
      
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to start quality checks')
      setRunningChecks(prev => {
        const newSet = new Set(prev)
        newSet.delete(datasetId)
        return newSet
      })
    }
  }

  const formatDateTime = (dateString?: string) => {
    if (!dateString) return 'Never'
    return new Date(dateString).toLocaleString()
  }

  const getConnectionStatusColor = (status: string) => {
    switch (status) {
      case 'success': return 'text-green-600 bg-green-50'
      case 'failed': return 'text-red-600 bg-red-50'
      case 'testing': return 'text-yellow-600 bg-yellow-50'
      default: return 'text-gray-600 bg-gray-50'
    }
  }

  const getQualityCheckSummary = (status: QualityCheckStatus) => {
    // Show progress if running
    if (status.status === 'running' && status.progress) {
      const { current, total, phase, percentage } = status.progress
      return `${phase}: ${current.toLocaleString()}/${total.toLocaleString()} (${percentage || 0}%)`
    }
    
    if (!status.check_results) return 'No checks run'
    
    const totalChecks = Object.values(status.check_results).reduce((sum, count) => sum + count, 0)
    const failedChecks = Object.entries(status.check_results)
      .filter(([key]) => key.includes('fail'))
      .reduce((sum, [, count]) => sum + count, 0)
    
    return `${totalChecks} checks (${failedChecks} failed)`
  }

  if (loading) {
    return (
      <div className="p-6">
        <h1 className="text-2xl font-bold mb-6">System Monitoring</h1>
        <div className="animate-pulse">
          <div className="h-4 bg-gray-200 rounded mb-4"></div>
          <div className="h-32 bg-gray-200 rounded"></div>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="p-6">
        <h1 className="text-2xl font-bold mb-6">System Monitoring</h1>
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <p className="text-red-600">Error: {error}</p>
        </div>
      </div>
    )
  }

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">System Monitoring</h1>
        <p className="text-gray-600 mt-1">Monitor dataset health and control quality checks</p>
      </div>

      {/* Overview Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        <div className="bg-white rounded-lg shadow p-4">
          <h3 className="text-sm font-medium text-gray-500">Total Datasets</h3>
          <p className="text-2xl font-bold text-gray-900">{datasets.length}</p>
        </div>
        <div className="bg-white rounded-lg shadow p-4">
          <h3 className="text-sm font-medium text-gray-500">Snapshots Complete</h3>
          <p className="text-2xl font-bold text-green-600">
            {datasets.filter(d => d.snapshots_complete).length}
          </p>
        </div>
        <div className="bg-white rounded-lg shadow p-4">
          <h3 className="text-sm font-medium text-gray-500">Total Snapshots</h3>
          <p className="text-2xl font-bold text-blue-600">
            {datasets.reduce((sum, d) => sum + d.snapshot_count, 0).toLocaleString()}
          </p>
        </div>
        <div className="bg-white rounded-lg shadow p-4">
          <h3 className="text-sm font-medium text-gray-500">Pending Diffs</h3>
          <p className="text-2xl font-bold text-orange-600">
            {datasets.reduce((sum, d) => sum + d.pending_diffs, 0)}
          </p>
        </div>
      </div>

      {/* Datasets Table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="px-6 py-4 border-b border-gray-200">
          <h2 className="text-lg font-medium text-gray-900">Dataset Monitoring</h2>
        </div>
        
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Dataset
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Connection
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Snapshots
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Quality Checks
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Last Updates
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {datasets.map((dataset) => {
                const qualityStatus = qualityStatuses[dataset.dataset_id]
                const isRunning = runningChecks.has(dataset.dataset_id)
                
                return (
                  <tr key={dataset.dataset_id}>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div>
                        <div className="text-sm font-medium text-gray-900">
                          {dataset.dataset_name}
                        </div>
                        <div className="text-sm text-gray-500">
                          {dataset.pending_diffs > 0 && (
                            <span className="text-orange-600">
                              {dataset.pending_diffs} pending diffs
                            </span>
                          )}
                        </div>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${getConnectionStatusColor(dataset.connection_status)}`}>
                        {dataset.connection_status}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center">
                        {dataset.snapshots_complete ? (
                          <div className="flex items-center">
                            <div className="w-2 h-2 bg-green-400 rounded-full mr-2"></div>
                            <span className="text-sm text-gray-900">
                              {dataset.snapshot_count.toLocaleString()} complete
                            </span>
                          </div>
                        ) : (
                          <div className="flex items-center">
                            <div className="w-2 h-2 bg-yellow-400 rounded-full animate-pulse mr-2"></div>
                            <span className="text-sm text-gray-600">
                              {dataset.snapshot_count.toLocaleString()} in progress
                            </span>
                          </div>
                        )}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      {qualityStatus ? (
                        <div className="text-sm">
                          {qualityStatus.status === 'running' ? (
                            <div className="space-y-2">
                              <div className="flex items-center text-blue-600">
                                <svg className="animate-spin h-4 w-4 mr-2" fill="none" viewBox="0 0 24 24">
                                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                                </svg>
                                Running checks...
                              </div>
                              {qualityStatus.progress && (
                                <div className="w-full">
                                  <div className="flex justify-between text-xs text-gray-600 mb-1">
                                    <span>{qualityStatus.progress.phase}</span>
                                    <span>{qualityStatus.progress.percentage || 0}%</span>
                                  </div>
                                  <div className="w-full bg-gray-200 rounded-full h-2">
                                    <div 
                                      className="bg-blue-600 h-2 rounded-full transition-all duration-300" 
                                      style={{ width: `${qualityStatus.progress.percentage || 0}%` }}
                                    ></div>
                                  </div>
                                  <div className="text-xs text-gray-500 mt-1">
                                    {qualityStatus.progress.current.toLocaleString()} / {qualityStatus.progress.total.toLocaleString()} geometries
                                  </div>
                                </div>
                              )}
                            </div>
                          ) : (
                            <>
                              <div className="text-gray-900">
                                {getQualityCheckSummary(qualityStatus)}
                              </div>
                              <div className="text-gray-500">
                                {formatDateTime(qualityStatus.last_check_at)}
                              </div>
                            </>
                          )}
                        </div>
                      ) : (
                        <span className="text-sm text-gray-500">No checks run</span>
                      )}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      <div>
                        <div>Changes: {formatDateTime(dataset.last_change_check)}</div>
                        <div>Quality: {formatDateTime(dataset.last_quality_check)}</div>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      {dataset.snapshots_complete ? (
                        <button
                          onClick={() => handleStartQualityChecks(dataset.dataset_id, dataset.dataset_name)}
                          disabled={isRunning}
                          className={`inline-flex items-center px-3 py-2 border border-transparent text-sm leading-4 font-medium rounded-md ${
                            isRunning
                              ? 'text-gray-400 bg-gray-100 cursor-not-allowed'
                              : 'text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500'
                          }`}
                        >
                          {isRunning ? (
                            <>
                              <svg className="animate-spin -ml-1 mr-2 h-4 w-4 text-gray-400" fill="none" viewBox="0 0 24 24">
                                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                              </svg>
                              Running...
                            </>
                          ) : (
                            'Run Quality Checks'
                          )}
                        </button>
                      ) : (
                        <div className="text-sm text-gray-500">
                          <div className="flex items-center">
                            <svg className="animate-spin h-4 w-4 text-gray-400 mr-2" fill="none" viewBox="0 0 24 24">
                              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                            </svg>
                            Waiting for snapshots
                          </div>
                        </div>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* Instructions */}
      <div className="mt-8 bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div className="flex">
          <svg className="flex-shrink-0 h-5 w-5 text-blue-400 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
          </svg>
          <div className="ml-3">
            <h3 className="text-sm font-medium text-blue-800">How Quality Checks Work</h3>
            <div className="mt-2 text-sm text-blue-700">
              <ul className="list-disc pl-5 space-y-1">
                <li>Quality checks can only run after initial snapshots are complete</li>
                <li>Snapshots are created automatically when datasets are first monitored</li>
                <li>Click "Run Quality Checks" to manually start comprehensive spatial validation</li>
                <li>Results will show validation, topology, duplicate, and area checks</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
} 