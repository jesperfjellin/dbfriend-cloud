import { useState, useCallback } from 'react'
import { getGeometryContext, GeometryContextResponse } from '../api'

export interface UseGeometryContextReturn {
  contextData: GeometryContextResponse | null
  loading: boolean
  error: string | null
  fetchContext: (geometryId: string, bufferMeters?: number) => Promise<void>
  clearContext: () => void
}

/**
 * Hook for managing geometry context state
 * 
 * Usage:
 * const { contextData, loading, fetchContext } = useGeometryContext()
 * 
 * // When user clicks on a flagged geometry
 * await fetchContext('geometry-id-123', 500)
 * 
 * // Pass contextData.geometries to TestMap component
 * <TestMap geometries={contextData?.geometries.map(item => ({
 *   geometry: item.geometry,
 *   isPrimary: item.is_primary,
 *   id: item.geometry_id,
 *   attributes: item.attributes
 * }))} />
 */
export function useGeometryContext(): UseGeometryContextReturn {
  const [contextData, setContextData] = useState<GeometryContextResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const fetchContext = useCallback(async (geometryId: string, bufferMeters: number = 500) => {
    setLoading(true)
    setError(null)
    
    try {
      console.log(`Fetching context for geometry ${geometryId} with ${bufferMeters}m buffer`)
      const response = await getGeometryContext(geometryId, bufferMeters)
      console.log(`Found ${response.total_found} context geometries`)
      
      setContextData(response)
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to fetch geometry context'
      console.error('Error fetching geometry context:', err)
      setError(errorMessage)
    } finally {
      setLoading(false)
    }
  }, [])

  const clearContext = useCallback(() => {
    setContextData(null)
    setError(null)
  }, [])

  return {
    contextData,
    loading,
    error,
    fetchContext,
    clearContext
  }
} 