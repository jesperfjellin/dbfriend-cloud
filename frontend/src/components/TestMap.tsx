/**
 * TestMap Component for Quality Test Visualization
 * Displays individual problematic geometries with 3D globe effect using Mapbox GL JS
 */

'use client'

import { useEffect, useRef, useState } from 'react'
import mapboxgl from 'mapbox-gl'
import 'mapbox-gl/dist/mapbox-gl.css'

interface TestMapProps {
  className?: string
  geometry?: GeoJSON.Geometry | GeoJSON.Feature // Support both direct geometry and Feature
  highlightError?: boolean
}

export default function TestMap({ className = '', geometry, highlightError = true }: TestMapProps) {
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
    if (!mapInstance.current || !mapLoaded || !geometry) return

    const map = mapInstance.current
    
    console.log('Processing geometry data:', geometry)
    
    // Normalize the geometry data
    const normalizedGeometry = normalizeGeometry(geometry)
    if (!normalizedGeometry) {
      console.error('Could not normalize geometry data')
      return
    }
    
    console.log('Normalized geometry:', normalizedGeometry)
    console.log('Geometry type:', normalizedGeometry.geometry?.type)
    console.log('Geometry coordinates:', (normalizedGeometry.geometry as any)?.coordinates)
    
    // Update the geometry type state for the legend
    setGeometryType(normalizedGeometry.geometry?.type || null)

    // Remove existing geometry layers
    const layersToRemove = ['geometry-fill', 'geometry-line', 'geometry-point']
    layersToRemove.forEach(layerId => {
      if (map.getLayer(layerId)) {
        map.removeLayer(layerId)
      }
    })
    
    if (map.getSource('geometry-source')) {
      map.removeSource('geometry-source')
    }

    try {
      // Add the geometry source
      map.addSource('geometry-source', {
        type: 'geojson',
        data: normalizedGeometry,
      })

      const fillColor = highlightError ? '#ef4444' : '#3b82f6'
      const strokeColor = highlightError ? '#dc2626' : '#2563eb'

      // Add layers based on geometry type
      const geomType = normalizedGeometry.geometry?.type
      console.log('Adding layers for geometry type:', geomType)

      if (geomType === 'Polygon' || geomType === 'MultiPolygon') {
        // Add fill layer for polygons
        map.addLayer({
          id: 'geometry-fill',
          type: 'fill',
          source: 'geometry-source',
          paint: {
            'fill-color': fillColor,
            'fill-opacity': 0.3,
          },
        })
        
        // Add outline
        map.addLayer({
          id: 'geometry-line',
          type: 'line',
          source: 'geometry-source',
          paint: {
            'line-color': strokeColor,
            'line-width': 2,
          },
        })
      } else if (geomType === 'LineString' || geomType === 'MultiLineString') {
        // Add line layer
        map.addLayer({
          id: 'geometry-line',
          type: 'line',
          source: 'geometry-source',
          paint: {
            'line-color': strokeColor,
            'line-width': 3,
          },
        })
      } else if (geomType === 'Point' || geomType === 'MultiPoint') {
        // Add point layer
        console.log('Creating Point layer with colors:', { fillColor, strokeColor })
        map.addLayer({
          id: 'geometry-point',
          type: 'circle',
          source: 'geometry-source',
          paint: {
            'circle-color': fillColor,
            'circle-radius': 8, // Make larger to ensure visibility
            'circle-stroke-color': strokeColor,
            'circle-stroke-width': 3,
            'circle-opacity': 0.8,
            'circle-stroke-opacity': 1.0,
          },
        })
        console.log('Point layer added successfully')
      }

      // Calculate bounds more robustly
      try {
        const geomCoords = (normalizedGeometry.geometry as any)?.coordinates || []
        console.log('Raw geometry coordinates for bounds:', geomCoords)
        const coords = extractCoordinates(geomCoords)
        console.log('Extracted coordinates for bounds:', coords)
        
        if (coords.length > 0) {
          const bounds = new mapboxgl.LngLatBounds()
          coords.forEach(([lon, lat]) => {
            if (typeof lon === 'number' && typeof lat === 'number' && 
                lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90) {
              bounds.extend([lon, lat])
              console.log('Added to bounds:', [lon, lat])
            }
          })
          
          if (!bounds.isEmpty()) {
            // For Points, use a different zoom strategy
            if (geomType === 'Point') {
              const [lon, lat] = coords[0]
              console.log('Flying to Point:', [lon, lat])
              map.flyTo({ 
                center: [lon, lat], 
                zoom: 15, // Good zoom level for Points
                duration: 1000 
              })
            } else {
              map.fitBounds(bounds, { 
                padding: 50, 
                maxZoom: 18,
                duration: 1000 // Smooth animation
              })
            }
          }
        }
      } catch (boundsError) {
        console.warn('Could not calculate bounds for geometry:', boundsError)
        // Fallback to a reasonable view
        map.flyTo({ center: [-122.4194, 37.7749], zoom: 9 })
      }

    } catch (geometryError) {
      console.error('Error adding geometry to map:', geometryError)
      setError(`Failed to display geometry: ${geometryError instanceof Error ? geometryError.message : 'Unknown error'}`)
    }
  }, [geometry, highlightError, mapLoaded])

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
      {geometry && mapLoaded && (
        <div className="absolute top-4 left-4 bg-white rounded-lg shadow-lg p-3 text-sm">
          <h3 className="font-semibold mb-2">Test Geometry</h3>
          <div className="flex items-center mb-1">
            <div className={`w-4 h-4 rounded mr-2 ${
              highlightError ? 'bg-red-500 opacity-60' : 'bg-blue-500 opacity-60'
            }`}></div>
            <span>{highlightError ? 'Error Geometry' : 'Test Geometry'}</span>
          </div>
          <div className="text-xs text-gray-500">
            Type: {geometryType || 'Unknown'}
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