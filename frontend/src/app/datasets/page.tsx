'use client'

import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { datasetApi } from '@/lib/api'
import { DatasetConnectionForm } from '@/components/DatasetConnectionForm'
import { Dataset } from '@/types/api'

export default function DatasetsPage() {
  const [showForm, setShowForm] = useState(false)
  const [editingDataset, setEditingDataset] = useState<Dataset | null>(null)

  const { data: datasets, isLoading, refetch } = useQuery({
    queryKey: ['datasets'],
    queryFn: datasetApi.list,
  })

  const handleAddNew = () => {
    setEditingDataset(null)
    setShowForm(true)
  }

  const handleEdit = (dataset: Dataset) => {
    setEditingDataset(dataset)
    setShowForm(true)
  }

  const handleFormClose = () => {
    setShowForm(false)
    setEditingDataset(null)
    refetch()
  }

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-3 text-gray-600">Loading datasets...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 py-8">
        
        {/* Header */}
        <div className="mb-8">
          <div className="flex justify-between items-start">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">Database Connections</h1>
              <p className="mt-2 text-gray-600 max-w-2xl">
                Connect to your PostGIS databases to monitor data quality and commit approved fixes. 
                Each connection represents a spatial dataset that will be continuously monitored for changes.
              </p>
            </div>
            <button
              onClick={handleAddNew}
              className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors font-medium shadow-sm"
            >
              Add Connection
            </button>
          </div>
        </div>

        {/* Connection Form Modal */}
        {showForm && (
          <DatasetConnectionForm
            dataset={editingDataset}
            onClose={handleFormClose}
          />
        )}

        {/* Datasets List or Empty State */}
        {datasets && datasets.length > 0 ? (
          <div className="space-y-4">
            {datasets.map((dataset) => (
              <div key={dataset.id} className="bg-white rounded-lg shadow-sm border border-gray-200 hover:shadow-md transition-shadow">
                <div className="p-6">
                  <div className="flex justify-between items-start">
                    <div className="flex-1">
                      <div className="flex items-center gap-3 mb-3">
                        <h3 className="text-xl font-semibold text-gray-900">{dataset.name}</h3>
                        <span className={`px-3 py-1 rounded-full text-xs font-medium ${
                          dataset.is_active 
                            ? 'bg-green-100 text-green-800' 
                            : 'bg-gray-100 text-gray-800'
                        }`}>
                          {dataset.is_active ? 'Active' : 'Inactive'}
                        </span>
                      </div>
                      
                      {dataset.description && (
                        <p className="text-gray-600 mb-4 text-sm leading-relaxed">{dataset.description}</p>
                      )}
                      
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-6 text-sm">
                        <div>
                          <div className="font-medium text-gray-900 mb-1">Schema</div>
                          <div className="text-gray-600 font-mono text-xs">{dataset.schema_name}</div>
                        </div>
                        <div>
                          <div className="font-medium text-gray-900 mb-1">Table</div>
                          <div className="text-gray-600 font-mono text-xs">{dataset.table_name}</div>
                        </div>
                        <div>
                          <div className="font-medium text-gray-900 mb-1">Geometry Column</div>
                          <div className="text-gray-600 font-mono text-xs">{dataset.geometry_column}</div>
                        </div>
                        <div>
                          <div className="font-medium text-gray-900 mb-1">Check Interval</div>
                          <div className="text-gray-600">{dataset.check_interval_minutes} minutes</div>
                        </div>
                      </div>
                      
                      {dataset.last_check_at && (
                        <div className="mt-4 text-xs text-gray-500">
                          Last checked: {new Date(dataset.last_check_at).toLocaleString()}
                        </div>
                      )}
                    </div>
                    
                    <div className="flex gap-3 ml-6">
                      <button
                        onClick={() => handleEdit(dataset)}
                        className="text-blue-600 hover:text-blue-800 font-medium text-sm px-3 py-1 hover:bg-blue-50 rounded transition-colors"
                      >
                        Edit
                      </button>
                      <button className="text-red-600 hover:text-red-800 font-medium text-sm px-3 py-1 hover:bg-red-50 rounded transition-colors">
                        Delete
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        ) : (
          /* Empty State */
          <div className="bg-white rounded-lg shadow-sm border border-gray-200">
            <div className="text-center py-16 px-8">
              <div className="max-w-md mx-auto">
                <h3 className="text-lg font-medium text-gray-900 mb-2">No database connections</h3>
                <p className="text-gray-600 mb-8 leading-relaxed">
                  Get started by connecting your first PostGIS database. Once connected, 
                  dbfriend-cloud will automatically monitor your spatial data for changes 
                  and help you maintain data quality.
                </p>
                <button
                  onClick={handleAddNew}
                  className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors font-medium shadow-sm"
                >
                  Add Your First Connection
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
} 