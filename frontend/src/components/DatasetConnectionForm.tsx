'use client'

import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { datasetApi } from '@/lib/api'
import { Dataset, DatasetCreate, DatasetUpdate } from '@/types/api'

interface DatasetConnectionFormProps {
  dataset?: Dataset | null
  onClose: () => void
}

export function DatasetConnectionForm({ dataset, onClose }: DatasetConnectionFormProps) {
  const isEditing = !!dataset
  
  const [formData, setFormData] = useState({
    name: dataset?.name || '',
    description: dataset?.description || '',
    connection_string: dataset?.connection_string || '',
    schema_name: dataset?.schema_name || 'public',
    table_name: dataset?.table_name || '',
    geometry_column: dataset?.geometry_column || 'geom',
    check_interval_minutes: dataset?.check_interval_minutes || 60,
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
  }

  const isLoading = createMutation.isPending || updateMutation.isPending

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg max-w-2xl w-full mx-4 max-h-[90vh] overflow-y-auto">
        <div className="p-6">
          <div className="flex justify-between items-center mb-6">
            <h2 className="text-xl font-semibold text-gray-900">
              {isEditing ? 'Edit Dataset' : 'Add Database Connection'}
            </h2>
            <button
              onClick={onClose}
              className="text-gray-400 hover:text-gray-600"
            >
              <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
              </svg>
            </button>
          </div>

          <form onSubmit={handleSubmit} className="space-y-6">
            {/* Basic Info */}
            <div>
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

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Description
              </label>
              <textarea
                value={formData.description}
                onChange={(e) => handleChange('description', e.target.value)}
                rows={3}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            {/* Connection Details */}
            {!isEditing && (
              <>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    PostgreSQL Connection String *
                  </label>
                  <input
                    type="text"
                    value={formData.connection_string}
                    onChange={(e) => handleChange('connection_string', e.target.value)}
                    placeholder="postgresql://user:password@host:5432/database"
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    required
                  />
                </div>

                <div className="grid grid-cols-2 gap-4">
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
                      className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                      required
                    />
                  </div>
                </div>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Geometry Column Name *
                  </label>
                  <input
                    type="text"
                    value={formData.geometry_column}
                    onChange={(e) => handleChange('geometry_column', e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                    required
                  />
                </div>
              </>
            )}

            {/* Settings */}
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

            {/* Submit Buttons */}
            <div className="flex justify-end gap-3 pt-4">
              <button
                type="button"
                onClick={onClose}
                className="px-4 py-2 text-gray-700 border border-gray-300 rounded-md hover:bg-gray-50"
                disabled={isLoading}
              >
                Cancel
              </button>
              <button
                type="submit"
                className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
                disabled={isLoading}
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