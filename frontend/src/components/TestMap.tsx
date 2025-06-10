/**
 * TestMap Component for Quality Test Visualization
 * Displays individual problematic geometries with 3D globe effect using Mapbox GL JS
 */

'use client'

import { useEffect, useRef, useState } from 'react'
import mapboxgl from 'mapbox-gl'
import 'mapbox-gl/dist/mapbox-gl.css'

interface GeometryContext {
  geometry: GeoJSON.Geometry | GeoJSON.Feature
  isPrimary?: boolean // The main flagged geometry
  id?: string
  attributes?: any
}

interface TestMapProps {
  className?: string
  geometry?: GeoJSON.Geometry | GeoJSON.Feature // Single geometry (backward compatibility)
  geometries?: GeometryContext[] // Multiple geometries with context
  highlightError?: boolean
}

export default function TestMap({ className = '', geometry, geometries, highlightError = true }: TestMapProps) {
  const mapRef = useRef<HTMLDivElement>(null)
  const mapInstance = useRef<mapboxgl.Map | null>(null)
  const [mapLoaded, setMapLoaded] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [geometryType, setGeometryType] = useState<string | null>(null)

  useEffect(() => {
    if (!mapRef.current || mapInstance.current) return

    try {
      console.log('Initializing Mapbox map with globe projection...')
      
      // Set the access token
      mapboxgl.accessToken = process.env.NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN || ''
      
      if (!mapboxgl.accessToken) {
        throw new Error('Mapbox access token is required. Please set NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN in your .env.local file.')
      }
      
      // Disable telemetry to avoid CORS issues
      // @ts-ignore
      mapboxgl.config.REQUIRE_ACCESS_TOKEN = false
      
      const map = new mapboxgl.Map({
        container: mapRef.current,
        style: {
          version: 8,
          sources: {
            osm: {
              type: 'raster',
              tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
              tileSize: 256,
              attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
            },
          },
          layers: [
            {
              id: 'osm-layer',
              type: 'raster',
              source: 'osm',
            },
          ],
        },
        center: [-122.4194, 37.7749], // San Francisco
        zoom: 2, // Wider view for globe effect
        projection: 'globe', // Enable 3D globe
        preserveDrawingBuffer: true, // Help with WebGL context stability
        antialias: true
      })

      map.on('load', () => {
        console.log('Mapbox map loaded successfully')
        setMapLoaded(true)
        setError(null)
      })

      map.on('error', (e) => {
        console.error('Mapbox map error:', e)
        setError(`Map error: ${e.error?.message || 'Unknown error'}`)
      })

      // Handle WebGL context loss (common in Firefox)
      map.on('webglcontextlost', () => {
        console.warn('WebGL context lost, attempting to restore...')
        setError('WebGL context lost. Try refreshing the page.')
      })

      map.on('webglcontextrestored', () => {
        console.log('WebGL context restored')
        setError(null)
      })

      mapInstance.current = map

    } catch (err) {
      console.error('Error initializing map:', err)
      setError(`Failed to initialize map: ${err instanceof Error ? err.message : 'Unknown error'}`)
    }

    return () => {
      if (mapInstance.current) {
        mapInstance.current.remove()
        mapInstance.current = null
      }
    }
  }, [])

  // Helper function to normalize geometry data
  const normalizeGeometry = (geom: GeoJSON.Geometry | GeoJSON.Feature | undefined): GeoJSON.Feature | null => {
    if (!geom) return null
    
    // If it has a geometry property, it's a Feature
    if ('geometry' in geom) {
      return geom as GeoJSON.Feature
    }
    
    // If it has coordinates, it's a direct geometry - wrap it in a Feature
    if ('coordinates' in geom) {
      return {
        type: 'Feature',
        geometry: geom as GeoJSON.Geometry,
        properties: {}
      }
    }
    
    return null
  }

  // Helper function to add geometry layers based on type
  const addGeometryLayers = (map: mapboxgl.Map, prefix: string, geomType: string | undefined, fillColor: string, strokeColor: string) => {
    const sourceId = `${prefix}-source`
    const geomPrefix = geomType?.toLowerCase().replace('multi', '') || 'unknown'
    
    if (geomType === 'Polygon' || geomType === 'MultiPolygon') {
      // Add fill layer for polygons
      map.addLayer({
        id: `${prefix}-${geomPrefix}-fill`,
        type: 'fill',
        source: sourceId,
        filter: ['in', ['geometry-type'], ['literal', ['Polygon', 'MultiPolygon']]],
        paint: {
          'fill-color': fillColor,
          'fill-opacity': prefix === 'primary' ? 0.3 : 0.15,
        },
      })
      
      // Add outline
      map.addLayer({
        id: `${prefix}-${geomPrefix}-line`,
        type: 'line',
        source: sourceId,
        filter: ['in', ['geometry-type'], ['literal', ['Polygon', 'MultiPolygon']]],
        paint: {
          'line-color': strokeColor,
          'line-width': prefix === 'primary' ? 3 : 2,
        },
      })
    } else if (geomType === 'LineString' || geomType === 'MultiLineString') {
      // Add line layer
      map.addLayer({
        id: `${prefix}-${geomPrefix}-line`,
        type: 'line',
        source: sourceId,
        filter: ['in', ['geometry-type'], ['literal', ['LineString', 'MultiLineString']]],
        paint: {
          'line-color': strokeColor,
          'line-width': prefix === 'primary' ? 4 : 2,
        },
      })
    } else if (geomType === 'Point' || geomType === 'MultiPoint') {
      // Add point layer
      map.addLayer({
        id: `${prefix}-${geomPrefix}-point`,
        type: 'circle',
        source: sourceId,
        filter: ['in', ['geometry-type'], ['literal', ['Point', 'MultiPoint']]],
        paint: {
          'circle-color': fillColor,
          'circle-radius': prefix === 'primary' ? 10 : 6,
          'circle-stroke-color': strokeColor,
          'circle-stroke-width': prefix === 'primary' ? 3 : 2,
          'circle-opacity': prefix === 'primary' ? 0.8 : 0.6,
          'circle-stroke-opacity': 1.0,
        },
      })
    }
  }

  // Helper function to extract coordinates recursively
  const extractCoordinates = (coords: any[]): [number, number][] => {
    const result: [number, number][] = []
    
    const extract = (coord: any) => {
      if (typeof coord[0] === 'number' && typeof coord[1] === 'number') {
        // This is a coordinate pair [lon, lat] (ignore elevation if present)
        result.push([coord[0], coord[1]])
      } else if (Array.isArray(coord)) {
        // This is nested, recurse
        coord.forEach(extract)
      }
    }
    
    // Handle Point coordinates directly
    if (coords.length >= 2 && typeof coords[0] === 'number' && typeof coords[1] === 'number') {
      result.push([coords[0], coords[1]])
    } else {
      coords.forEach(extract)
    }
    
    return result
  }

  // Update geometry when data changes
  useEffect(() => {
    if (!mapInstance.current || !mapLoaded) return
    
    // Handle both single geometry and multiple geometries
    const geometryData = geometries || (geometry ? [{ geometry, isPrimary: true }] : [])
    
    if (geometryData.length === 0) return

    const map = mapInstance.current
    
    console.log('Processing geometry data:', geometryData)
    
    // Normalize all geometries
    const normalizedGeometries = geometryData.map((item, index) => {
      const normalized = normalizeGeometry(item.geometry)
      if (!normalized) {
        console.error(`Could not normalize geometry ${index}`)
        return null
      }
      return {
        feature: normalized,
        isPrimary: item.isPrimary || false,
        id: item.id || `geometry-${index}`,
        attributes: item.attributes
      }
    }).filter(Boolean) as Array<{
      feature: GeoJSON.Feature
      isPrimary: boolean
      id: string
      attributes?: any
    }>
    
    if (normalizedGeometries.length === 0) return
    
    console.log('Normalized geometries:', normalizedGeometries)
    
    // Update the geometry type state for the legend (use primary geometry type)
    const primaryGeometry = normalizedGeometries.find(g => g.isPrimary) || normalizedGeometries[0]
    setGeometryType(primaryGeometry.feature.geometry?.type || null)

    // Remove existing geometry layers (including geometry-type specific ones)
    const layerPrefixes = ['primary', 'context']
    const geomTypes = ['polygon', 'linestring', 'point']
    const layerSuffixes = ['fill', 'line', 'point']
    
    layerPrefixes.forEach(prefix => {
      geomTypes.forEach(geomType => {
        layerSuffixes.forEach(suffix => {
          const layerId = `${prefix}-${geomType}-${suffix}`
          if (map.getLayer(layerId)) {
            map.removeLayer(layerId)
          }
        })
      })
    })
    
    // Remove existing sources
    if (map.getSource('primary-source')) {
      map.removeSource('primary-source')
    }
    if (map.getSource('context-source')) {
      map.removeSource('context-source')
    }

    try {
      // Separate primary and context geometries
      const primaryGeometries = normalizedGeometries.filter(g => g.isPrimary)
      const contextGeometries = normalizedGeometries.filter(g => !g.isPrimary)
      
      console.log(`Adding ${primaryGeometries.length} primary and ${contextGeometries.length} context geometries`)

      // Style colors
      const primaryFillColor = highlightError ? '#ef4444' : '#3b82f6'
      const primaryStrokeColor = highlightError ? '#dc2626' : '#2563eb'
      const contextFillColor = '#6b7280' // Gray for context
      const contextStrokeColor = '#4b5563' // Darker gray

      // Add primary geometries source and layers
      if (primaryGeometries.length > 0) {
        const primaryFeatureCollection: GeoJSON.FeatureCollection = {
          type: 'FeatureCollection',
          features: primaryGeometries.map(g => g.feature)
        }
        
        map.addSource('primary-source', {
          type: 'geojson',
          data: primaryFeatureCollection,
        })
        
        const primaryGeomType = primaryGeometry.feature.geometry?.type
        console.log('Adding layers for primary geometry type:', primaryGeomType)
        addGeometryLayers(map, 'primary', primaryGeomType, primaryFillColor, primaryStrokeColor)
      }
      
      // Add context geometries source and layers
      if (contextGeometries.length > 0) {
        const contextFeatureCollection: GeoJSON.FeatureCollection = {
          type: 'FeatureCollection',
          features: contextGeometries.map(g => g.feature)
        }
        
        map.addSource('context-source', {
          type: 'geojson',
          data: contextFeatureCollection,
        })
        
        // Only add layers for geometry types that actually exist in context data
        const geomTypeSet = new Set(contextGeometries.map(g => g.feature.geometry?.type).filter(Boolean))
        const contextGeomTypes = Array.from(geomTypeSet)
        console.log('Adding context layers for geometry types:', contextGeomTypes)
        
        contextGeomTypes.forEach(geomType => {
          if (geomType === 'Polygon' || geomType === 'MultiPolygon') {
            addGeometryLayers(map, 'context', geomType, contextFillColor, contextStrokeColor)
          } else if (geomType === 'LineString' || geomType === 'MultiLineString') {
            addGeometryLayers(map, 'context', geomType, contextFillColor, contextStrokeColor)
          } else if (geomType === 'Point' || geomType === 'MultiPoint') {
            addGeometryLayers(map, 'context', geomType, contextFillColor, contextStrokeColor)
          }
        })
      }



      // Calculate bounds for all geometries
      try {
        const bounds = new mapboxgl.LngLatBounds()
        let hasValidCoords = false
        
        normalizedGeometries.forEach((item, index) => {
          const geomCoords = (item.feature.geometry as any)?.coordinates || []
          console.log(`Geometry ${index} coordinates:`, geomCoords)
          const coords = extractCoordinates(geomCoords)
          
          coords.forEach(([lon, lat]) => {
            if (typeof lon === 'number' && typeof lat === 'number' && 
                lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90) {
              bounds.extend([lon, lat])
              hasValidCoords = true
            }
          })
        })
        
        if (hasValidCoords && !bounds.isEmpty()) {
          console.log('Fitting bounds to show all geometries')
          map.fitBounds(bounds, { 
            padding: 100, // More padding for multiple geometries
            maxZoom: 16,
            duration: 1000
          })
        }
      } catch (boundsError) {
        console.warn('Could not calculate bounds for geometries:', boundsError)
        // Fallback to a reasonable view
        map.flyTo({ center: [-122.4194, 37.7749], zoom: 9 })
      }

    } catch (geometryError) {
      console.error('Error adding geometry to map:', geometryError)
      setError(`Failed to display geometry: ${geometryError instanceof Error ? geometryError.message : 'Unknown error'}`)
    }
  }, [geometry, geometries, highlightError, mapLoaded])

  if (error) {
    return (
      <div className={`p-4 bg-red-50 border border-red-200 rounded-lg ${className}`}>
        <h3 className="text-red-800 font-medium">Map Error</h3>
        <p className="text-red-600 mt-1">{error}</p>
        <details className="mt-2">
          <summary className="text-red-700 cursor-pointer">Debug Info</summary>
          <pre className="text-xs mt-1 text-red-600">
            Browser: {navigator.userAgent}
            Mapbox GL JS: {mapboxgl.version}
          </pre>
        </details>
      </div>
    )
  }

  return (
    <div className={`relative ${className}`}>
      <div 
        ref={mapRef} 
        className="w-full h-full bg-gray-100"
        style={{ 
          position: 'relative',
          overflow: 'hidden'
        }}
      />
      {!mapLoaded && (
        <div className="absolute inset-0 flex items-center justify-center bg-gray-100 rounded-lg">
          <div className="text-center">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500 mx-auto"></div>
            <p className="mt-2 text-gray-600">Loading globe map...</p>
          </div>
        </div>
      )}
      
      {/* Map legend/info */}
      {(geometry || (geometries && geometries.length > 0)) && mapLoaded && (
        <div className="absolute top-4 left-4 bg-white rounded-lg shadow-lg p-3 text-sm">
          <h3 className="font-semibold mb-2">
            {geometries && geometries.length > 1 ? 'Geometry Context' : 'Test Geometry'}
          </h3>
          
          {/* Primary geometry indicator */}
          <div className="flex items-center mb-1">
            <div className={`w-4 h-4 rounded mr-2 ${
              highlightError ? 'bg-red-500 opacity-60' : 'bg-blue-500 opacity-60'
            }`}></div>
            <span>{highlightError ? 'Flagged Geometry' : 'Primary Geometry'}</span>
          </div>
          
          {/* Context geometries indicator */}
          {geometries && geometries.length > 1 && (
            <div className="flex items-center mb-1">
              <div className="w-4 h-4 rounded mr-2 bg-gray-500 opacity-60"></div>
              <span>Context ({geometries.length - 1})</span>
            </div>
          )}
          
          <div className="text-xs text-gray-500">
            Type: {geometryType || 'Unknown'}
            {geometries && geometries.length > 1 && (
              <div className="mt-1">
                500m buffer context
              </div>
            )}
          </div>
        </div>
      )}
      
      {/* Globe controls info */}
      {mapLoaded && (
        <div className="absolute bottom-4 right-4 bg-white rounded-lg shadow-lg p-2 text-xs text-gray-600">
          <div>🌍 Globe projection</div>
          <div>Drag to rotate • Scroll to zoom</div>
        </div>
      )}
    </div>
  )
}