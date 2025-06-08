'use client'

import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { datasetApi } from '@/lib/api'
import { Dataset, DatasetCreate, DatasetUpdate, DatasetConnectionTest } from '@/types/api'

interface DatasetConnectionFormProps {
  dataset?: Dataset | null
  onClose: () => void
}

export function DatasetConnectionForm({ dataset, onClose }: DatasetConnectionFormProps) {
  const isEditing = !!dataset
  
  const [formData, setFormData] = useState({
    name: dataset?.name || '',
    description: dataset?.description || '',
    host: dataset?.host || 'localhost',
    port: dataset?.port || 5432,
    database: dataset?.database || '',
    username: '',
    password: '',
    schema_name: dataset?.schema_name || 'public',
    table_name: dataset?.table_name || '',
    geometry_column: dataset?.geometry_column || 'geom',
    check_interval_minutes: dataset?.check_interval_minutes || 60,
    ssl_mode: dataset?.ssl_mode || 'prefer',
    read_only: dataset?.read_only ?? true,
  })

  const [connectionTest, setConnectionTest] = useState<{
    status: 'idle' | 'testing' | 'success' | 'error'
    message: string
    details?: any
  }>({ status: 'idle', message: '' })

  // Connection test mutation
  const testMutation = useMutation({
    mutationFn: (testData: DatasetConnectionTest) => datasetApi.testConnection(testData),
    onSuccess: (result) => {
      if (result.success) {
        setConnectionTest({
          status: 'success',
          message: result.message,
          details: {
            postgis_version: result.postgis_version,
            permissions: result.permissions,
            schema_info: result.schema_info
          }
        })
      } else {
        setConnectionTest({
          status: 'error',
          message: result.message
        })
      }
    },
    onError: (error: any) => {
      setConnectionTest({
        status: 'error',
        message: error.message || 'Connection test failed'
      })
    }
  })

  const createMutation = useMutation({
    mutationFn: (data: DatasetCreate) => datasetApi.create(data),
    onSuccess: () => {
      onClose()
    },
  })

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: string, data: DatasetUpdate }) => 
      datasetApi.update(id, data),
    onSuccess: () => {
      onClose()
    },
  })

  const handleTestConnection = () => {
    setConnectionTest({ status: 'testing', message: 'Testing connection...' })
    
    testMutation.mutate({
      host: formData.host,
      port: formData.port,
      database: formData.database,
      username: formData.username,
      password: formData.password,
      ssl_mode: formData.ssl_mode
    })
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    
    if (isEditing && dataset) {
      updateMutation.mutate({
        id: dataset.id,
        data: {
          name: formData.name,
          description: formData.description,
          check_interval_minutes: formData.check_interval_minutes,
        }
      })
    } else {
      createMutation.mutate(formData)
    }
  }

  const handleChange = (field: string, value: any) => {
    setFormData(prev => ({ ...prev, [field]: value }))
    // Reset connection test when connection details change
    if (['host', 'port', 'database', 'username', 'password'].includes(field)) {
      setConnectionTest({ status: 'idle', message: '' })
    }
  }

  const isLoading = createMutation.isPending || updateMutation.isPending

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg max-w-3xl w-full mx-4 max-h-[90vh] overflow-y-auto">
        <div className="p-6">
          <div className="flex justify-between items-center mb-6">
            <h2 className="text-xl font-semibold text-gray-900">
              {isEditing ? 'Edit Dataset' : 'Add Database Connection'}
            </h2>
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-gray-600 transition-colors"
            >
              <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          <form onSubmit={handleSubmit} className="space-y-6">
            {/* Basic Info */}
            <div className="bg-gray-50 p-4 rounded-lg">
              <h3 className="font-medium text-gray-900 mb-4">Dataset Information</h3>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="md:col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Dataset Name *
                  </label>
                  <input
                    type="text"
                    value={formData.name}
                    onChange={(e) => handleChange('name', e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    required
                  />
                </div>

                <div className="md:col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Description
                  </label>
                  <textarea
                    value={formData.description}
                    onChange={(e) => handleChange('description', e.target.value)}
                    rows={2}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="Optional description of this spatial dataset"
                  />
                </div>
              </div>
            </div>

            {/* Connection Details */}
            {!isEditing && (
              <>
                <div className="bg-blue-50 p-4 rounded-lg">
                  <h3 className="font-medium text-gray-900 mb-4">Database Connection</h3>
                  
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        Host *
                      </label>
                      <input
                        type="text"
                        value={formData.host}
                        onChange={(e) => handleChange('host', e.target.value)}
                        placeholder="localhost or your-server.com"
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                        required
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        Port *
                      </label>
                      <input
                        type="number"
                        value={formData.port}
                        onChange={(e) => handleChange('port', parseInt(e.target.value))}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                        required
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        Database Name *
                      </label>
                      <input
                        type="text"
                        value={formData.database}
                        onChange={(e) => handleChange('database', e.target.value)}
                        placeholder="your_database_name"
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                        required
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        SSL Mode
                      </label>
                      <select
                        value={formData.ssl_mode}
                        onChange={(e) => handleChange('ssl_mode', e.target.value)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                      >
                        <option value="disable">Disable</option>
                        <option value="allow">Allow</option>
                        <option value="prefer">Prefer (recommended)</option>
                        <option value="require">Require</option>
                      </select>
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        Username *
                      </label>
                      <input
                        type="text"
                        value={formData.username}
                        onChange={(e) => handleChange('username', e.target.value)}
                        placeholder="database_user"
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                        required
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        Password *
                      </label>
                      <input
                        type="password"
                        value={formData.password}
                        onChange={(e) => handleChange('password', e.target.value)}
                        placeholder="••••••••"
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                        required
                      />
                    </div>
                  </div>

                  {/* Connection Test */}
                  <div className="border-t pt-4">
                    <div className="flex items-center justify-between mb-3">
                      <span className="text-sm font-medium text-gray-700">Connection Test</span>
                      <button
                        type="button"
                        onClick={handleTestConnection}
                        disabled={testMutation.isPending || !formData.host || !formData.database || !formData.username || !formData.password}
                        className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed text-sm"
                      >
                        {testMutation.isPending ? 'Testing...' : 'Test Connection'}
                      </button>
                    </div>

                    {connectionTest.status !== 'idle' && (
                      <div className={`p-3 rounded-md text-sm ${
                        connectionTest.status === 'success' ? 'bg-green-50 text-green-800' :
                        connectionTest.status === 'error' ? 'bg-red-50 text-red-800' :
                        'bg-blue-50 text-blue-800'
                      }`}>
                        <div className="font-medium">{connectionTest.message}</div>
                        {connectionTest.details && (
                          <div className="mt-2 text-xs">
                            {connectionTest.details.postgis_version && (
                              <div>PostGIS: {connectionTest.details.postgis_version}</div>
                            )}
                            {connectionTest.details.permissions && connectionTest.details.permissions.length > 0 && (
                              <div>Permissions: {connectionTest.details.permissions.join(', ')}</div>
                            )}
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                </div>

                {/* Spatial Table Details */}
                <div className="bg-green-50 p-4 rounded-lg">
                  <h3 className="font-medium text-gray-900 mb-4">Spatial Table Configuration</h3>
                  
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        Schema Name *
                      </label>
                      <input
                        type="text"
                        value={formData.schema_name}
                        onChange={(e) => handleChange('schema_name', e.target.value)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                        required
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        Table Name *
                      </label>
                      <input
                        type="text"
                        value={formData.table_name}
                        onChange={(e) => handleChange('table_name', e.target.value)}
                        placeholder="your_spatial_table"
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                        required
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-2">
                        Geometry Column *
                      </label>
                      <input
                        type="text"
                        value={formData.geometry_column}
                        onChange={(e) => handleChange('geometry_column', e.target.value)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                        required
                      />
                    </div>
                  </div>
                </div>
              </>
            )}

            {/* Settings */}
            <div className="bg-gray-50 p-4 rounded-lg">
              <h3 className="font-medium text-gray-900 mb-4">Monitoring Settings</h3>
              
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Check Interval (minutes)
                  </label>
                  <input
                    type="number"
                    value={formData.check_interval_minutes}
                    onChange={(e) => handleChange('check_interval_minutes', parseInt(e.target.value))}
                    min="1"
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  />
                </div>

                {!isEditing && (
                  <div className="flex items-center">
                    <input
                      type="checkbox"
                      id="read_only"
                      checked={formData.read_only}
                      onChange={(e) => handleChange('read_only', e.target.checked)}
                      className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                    />
                    <label htmlFor="read_only" className="ml-2 block text-sm text-gray-700">
                      Read-only access (recommended)
                    </label>
                  </div>
                )}
              </div>
            </div>

            {/* Security Warning */}
            {!isEditing && (
              <div className="bg-yellow-50 border border-yellow-200 p-4 rounded-lg">
                <div className="text-sm text-yellow-800">
                  <strong>Security Best Practices:</strong>
                  <ul className="mt-2 list-disc list-inside space-y-1">
                    <li>Create a dedicated database user for dbfriend-cloud</li>
                    <li>Grant only necessary permissions (SELECT for read-only, or SELECT/UPDATE for data fixes)</li>
                    <li>Use SSL connections in production</li>
                    <li>Credentials will be encrypted and stored securely</li>
                  </ul>
                </div>
              </div>
            )}

            {/* Submit Buttons */}
            <div className="flex justify-end gap-3 pt-4 border-t">
              <button
                type="button"
                onClick={onClose}
                className="px-6 py-2 text-gray-700 border border-gray-300 rounded-md hover:bg-gray-50 transition-colors"
                disabled={isLoading}
              >
                Cancel
              </button>
              <button
                type="submit"
                className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 transition-colors"
                disabled={isLoading || (!isEditing && connectionTest.status !== 'success')}
              >
                {isLoading ? 'Saving...' : (isEditing ? 'Update Dataset' : 'Add Dataset')}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  )
} 