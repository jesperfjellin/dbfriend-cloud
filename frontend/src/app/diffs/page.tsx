/**
 * Diffs Review Page
 * 
 * Git-style accept/reject queue for geometry changes
 * Like a pull request review but for spatial data
 */

'use client'

import { useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { diffsApi } from '@/lib/api'
import { GeometryDiff } from '@/types/api'
import { Map } from '@/components/Map'

export default function DiffsPage() {
  const [selectedDiff, setSelectedDiff] = useState<GeometryDiff | null>(null)
  const [filter, setFilter] = useState<'all' | 'pending' | 'accepted' | 'rejected'>('pending')
  
  const queryClient = useQueryClient()

  // Fetch diffs (like requests.get with filters)
  const { data: diffs, isLoading } = useQuery({
    queryKey: ['diffs', filter],
    queryFn: () => diffsApi.list({ 
      status: filter === 'all' ? undefined : filter.toUpperCase() 
    }),
  })

  // Mutation for reviewing diffs (like requests.put)
  const reviewMutation = useMutation({
    mutationFn: ({ diffId, status, reviewedBy }: {
      diffId: string
      status: 'ACCEPTED' | 'REJECTED'
      reviewedBy: string
    }) => diffsApi.review(diffId, { status, reviewed_by: reviewedBy }),
    
    onSuccess: () => {
      // Refresh data after review (like re-fetching)
      queryClient.invalidateQueries({ queryKey: ['diffs'] })
      queryClient.invalidateQueries({ queryKey: ['pending-count'] })
      setSelectedDiff(null)
    }
  })

  const handleReview = (status: 'ACCEPTED' | 'REJECTED') => {
    if (!selectedDiff) return
    
    reviewMutation.mutate({
      diffId: selectedDiff.id,
      status,
      reviewedBy: 'Demo User' // In real app, this would be logged-in user
    })
  }

  if (isLoading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading diffs...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow">
        <div className="max-w-7xl mx-auto px-4 py-6">
          <div className="flex justify-between items-center">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">
                Geometry Diffs Review
              </h1>
              <p className="text-gray-600 mt-2">
                Review and approve geometry changes - like Git but for spatial data
              </p>
            </div>
            
            {/* Filter buttons */}
            <div className="flex space-x-2">
              {(['all', 'pending', 'accepted', 'rejected'] as const).map((status) => (
                <button
                  key={status}
                  onClick={() => setFilter(status)}
                  className={`px-4 py-2 rounded-lg capitalize transition-colors ${
                    filter === status
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                  }`}
                >
                  {status}
                </button>
              ))}
            </div>
          </div>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 py-8">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          
          {/* Diffs List */}
          <div className="bg-white rounded-lg shadow">
            <div className="p-6 border-b">
              <h2 className="text-xl font-semibold">
                Diffs Queue ({diffs?.length || 0})
              </h2>
            </div>
            
            <div className="max-h-96 overflow-y-auto">
              {diffs?.length === 0 ? (
                <div className="p-6 text-center text-gray-500">
                  No {filter === 'all' ? '' : filter} diffs found
                </div>
              ) : (
                diffs?.map((diff) => (
                  <div
                    key={diff.id}
                    onClick={() => setSelectedDiff(diff)}
                    className={`p-4 border-b cursor-pointer hover:bg-gray-50 transition-colors ${
                      selectedDiff?.id === diff.id ? 'bg-blue-50 border-blue-200' : ''
                    }`}
                  >
                    <div className="flex justify-between items-start">
                      <div>
                        <div className="flex items-center space-x-2">
                          <span className={`px-2 py-1 text-xs rounded-full ${
                            diff.diff_type === 'NEW' ? 'bg-green-100 text-green-800' :
                            diff.diff_type === 'UPDATED' ? 'bg-yellow-100 text-yellow-800' :
                            'bg-red-100 text-red-800'
                          }`}>
                            {diff.diff_type}
                          </span>
                          <span className={`px-2 py-1 text-xs rounded-full ${
                            diff.status === 'PENDING' ? 'bg-yellow-100 text-yellow-800' :
                            diff.status === 'ACCEPTED' ? 'bg-green-100 text-green-800' :
                            'bg-red-100 text-red-800'
                          }`}>
                            {diff.status}
                          </span>
                        </div>
                        <p className="text-sm text-gray-600 mt-1">
                          {diff.geometry_changed && diff.attributes_changed ? 'Geometry + Attributes' :
                           diff.geometry_changed ? 'Geometry Only' :
                           'Attributes Only'}
                        </p>
                        <p className="text-xs text-gray-500 mt-1">
                          {new Date(diff.created_at).toLocaleDateString()}
                        </p>
                      </div>
                      
                      {diff.confidence_score && (
                        <div className="text-right">
                          <div className="text-xs text-gray-500">Confidence</div>
                          <div className="text-sm font-medium">
                            {Math.round(diff.confidence_score * 100)}%
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>

          {/* Map Viewer */}
          <div className="bg-white rounded-lg shadow">
            <div className="p-6 border-b">
              <div className="flex justify-between items-center">
                <h2 className="text-xl font-semibold">
                  Geometry Diff Viewer
                </h2>
                
                {selectedDiff?.status === 'PENDING' && (
                  <div className="flex space-x-2">
                    <button
                      onClick={() => handleReview('REJECTED')}
                      disabled={reviewMutation.isPending}
                      className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50"
                    >
                      Reject
                    </button>
                    <button
                      onClick={() => handleReview('ACCEPTED')}
                      disabled={reviewMutation.isPending}
                      className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50"
                    >
                      Accept
                    </button>
                  </div>
                )}
              </div>
            </div>
            
            <div className="p-6">
              {selectedDiff ? (
                <div>
                  {/* Diff Details */}
                  <div className="mb-4 text-sm text-gray-600">
                    <p><strong>Type:</strong> {selectedDiff.diff_type}</p>
                    <p><strong>Status:</strong> {selectedDiff.status}</p>
                    {selectedDiff.reviewed_by && (
                      <p><strong>Reviewed by:</strong> {selectedDiff.reviewed_by}</p>
                    )}
                  </div>
                  
                  {/* Map */}
                  <div className="h-96">
                    <Map
                      className="h-full"
                      // We'll fetch the actual GeoJSON next
                      oldGeometry={null} // TODO: fetch from API
                      newGeometry={null} // TODO: fetch from API
                    />
                  </div>
                </div>
              ) : (
                <div className="h-96 flex items-center justify-center text-gray-500">
                  Select a diff to view geometry changes
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
} 