/**
 * TestMap Component for Quality Test Visualization
 * 
 * This displays individual problematic geometries from spatial tests
 * Unlike the diff Map, this shows a single geometry with error highlighting
 */

'use client'

import { useEffect, useRef } from 'react'
import maplibregl from 'maplibre-gl'

interface TestMapProps {
  // Test geometry data from the API
  geometryData?: {
    snapshot_id: string
    geometry: any // GeoJSON geometry
    attributes?: any
  } | null
  testInfo?: {
    check_type: string
    check_result: string
    error_message?: string
  }
  isLoading?: boolean
  error?: any
  className?: string
}

export function TestMap({ geometryData, testInfo, isLoading, error, className = '' }: TestMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)

  useEffect(() => {
    if (!mapContainer.current) return

    // Initialize map
    map.current = new maplibregl.Map({
      container: mapContainer.current,
      style: 'https://demotiles.maplibre.org/style.json',
      center: [16.3738, 48.2082], // Default to Vienna, Austria (central Europe)
      zoom: 9,
    })

    // Add navigation controls
    map.current.addControl(new maplibregl.NavigationControl(), 'top-right')

    // Cleanup
    return () => {
      if (map.current) {
        map.current.remove()
      }
    }
  }, [])

  // Update geometry when data changes
  useEffect(() => {
    if (!map.current || !geometryData?.geometry) return

    const updateGeometry = () => {
      if (!map.current || !geometryData?.geometry) return

      // Remove existing layers
      try {
        if (map.current.getLayer('test-geometry')) map.current.removeLayer('test-geometry')
        if (map.current.getLayer('test-geometry-outline')) map.current.removeLayer('test-geometry-outline')
        if (map.current.getSource('test-geometry')) map.current.removeSource('test-geometry')
      } catch (e) {
        // Layers might not exist yet
      }

      // Create GeoJSON feature
      const feature = {
        type: 'Feature',
        geometry: geometryData.geometry,
        properties: geometryData.attributes || {}
      }

      // Add source
      map.current.addSource('test-geometry', {
        type: 'geojson',
        data: feature
      })

      // Determine color based on test result
      const getGeometryColor = () => {
        if (!testInfo) return '#ef4444' // Default red
        
        switch (testInfo.check_result.toLowerCase()) {
          case 'fail': return '#ef4444' // Red for failures
          case 'warning': return '#f59e0b' // Orange for warnings  
          case 'pass': return '#10b981' // Green for passes
          default: return '#6b7280' // Gray for unknown
        }
      }

      const fillColor = getGeometryColor()
      const outlineColor = fillColor

      // Add fill layer
      map.current.addLayer({
        id: 'test-geometry',
        type: 'fill',
        source: 'test-geometry',
        paint: {
          'fill-color': fillColor,
          'fill-opacity': 0.6
        }
      })

      // Add outline layer
      map.current.addLayer({
        id: 'test-geometry-outline',
        type: 'line',
        source: 'test-geometry',
        paint: {
          'line-color': outlineColor,
          'line-width': 2
        }
      })

      // Fit map to geometry
      const bounds = new maplibregl.LngLatBounds()
      addGeometryToBounds(bounds, geometryData.geometry)

      if (!bounds.isEmpty()) {
        map.current.fitBounds(bounds, { padding: 50 })
      }
    }

    if (map.current.isStyleLoaded()) {
      updateGeometry()
    } else {
      map.current.on('load', updateGeometry)
    }
  }, [geometryData, testInfo])

  // Helper function to add geometry coordinates to bounds
  function addGeometryToBounds(bounds: maplibregl.LngLatBounds, geometry: any) {
    if (geometry.type === 'Point') {
      bounds.extend(geometry.coordinates as [number, number])
    } else if (geometry.type === 'Polygon') {
      geometry.coordinates[0].forEach((coord: number[]) => {
        bounds.extend(coord as [number, number])
      })
    } else if (geometry.type === 'MultiPolygon') {
      geometry.coordinates.forEach((polygon: number[][][]) => {
        polygon[0].forEach((coord: number[]) => {
          bounds.extend(coord as [number, number])
        })
      })
    } else if (geometry.type === 'LineString') {
      geometry.coordinates.forEach((coord: number[]) => {
        bounds.extend(coord as [number, number])
      })
    } else if (geometry.type === 'MultiLineString') {
      geometry.coordinates.forEach((line: number[][]) => {
        line.forEach((coord: number[]) => {
          bounds.extend(coord as [number, number])
        })
      })
    }
  }

  return (
    <div className={`relative ${className}`}>
      <div ref={mapContainer} className="w-full h-full rounded-lg" />
      
      {/* Map Status Overlay */}
      <div className="absolute top-4 left-4 bg-white rounded-lg shadow-lg p-3 text-sm max-w-xs">
        {isLoading ? (
          <div className="flex items-center space-x-2">
            <div className="w-4 h-4 border-2 border-purple-600 border-t-transparent rounded-full animate-spin"></div>
            <span>Loading geometry...</span>
          </div>
        ) : error ? (
          <div>
            <h3 className="font-semibold text-red-700 mb-1">Geometry Error</h3>
            <p className="text-red-600 text-xs">{error.message || 'Failed to load geometry'}</p>
          </div>
        ) : geometryData?.geometry ? (
          <div>
            <h3 className="font-semibold mb-2">Test Geometry</h3>
            <div className="flex items-center mb-1">
              <div 
                className="w-4 h-4 rounded mr-2 opacity-60" 
                style={{ 
                  backgroundColor: testInfo?.check_result.toLowerCase() === 'fail' ? '#ef4444' :
                                   testInfo?.check_result.toLowerCase() === 'warning' ? '#f59e0b' :
                                   testInfo?.check_result.toLowerCase() === 'pass' ? '#10b981' : '#6b7280'
                }}
              ></div>
              <span>
                {testInfo?.check_result || 'Unknown'} - {testInfo?.check_type || 'Test'}
              </span>
            </div>
            <div className="text-xs text-gray-500">
              Type: {geometryData.geometry.type}
            </div>
          </div>
        ) : (
          <div>
            <h3 className="font-semibold text-gray-600 mb-1">No Geometry</h3>
            <p className="text-gray-500 text-xs">No spatial data available for this test</p>
          </div>
        )}
      </div>
    </div>
  )
} 