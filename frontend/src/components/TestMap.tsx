/**
 * TestMap Component for Quality Test Visualization
 * 
 * This displays individual problematic geometries from spatial tests
 * Unlike the diff Map, this shows a single geometry with error highlighting
 */

'use client'

import { useEffect, useRef, useState } from 'react'
import 'ol/ol.css'
import { Map as OLMap, View } from 'ol'
import { Tile as TileLayer, Vector as VectorLayer } from 'ol/layer'
import { OSM } from 'ol/source'
import { Vector as VectorSource } from 'ol/source'
import { GeoJSON } from 'ol/format'
import { Style, Fill, Stroke, Circle } from 'ol/style'
import { defaults as defaultControls } from 'ol/control'
import { fromLonLat } from 'ol/proj'
import Feature from 'ol/Feature'
import Geometry from 'ol/geom/Geometry'

interface TestMapProps {
  className?: string
  geometry?: any // GeoJSON geometry to display
  highlightError?: boolean // Whether to highlight as error (red) or normal (blue)
}

export default function TestMap({ className = '', geometry, highlightError = true }: TestMapProps) {
  const mapRef = useRef<HTMLDivElement>(null)
  const mapInstance = useRef<OLMap | null>(null)
  const geometryLayerRef = useRef<VectorLayer<any> | null>(null)
  const [mapLoaded, setMapLoaded] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!mapRef.current || mapInstance.current) return

    try {
      console.log('Initializing OpenLayers map...')
      
      const map = new OLMap({
        target: mapRef.current,
        layers: [
          // Base tile layer (OpenStreetMap)
          new TileLayer({
            source: new OSM(),
          }),
        ],
        view: new View({
          center: fromLonLat([-122.4194, 37.7749]), // San Francisco
          zoom: 9,
        }),
        controls: defaultControls(),
      })

      mapInstance.current = map

      // Set loaded state after a short delay to ensure map is ready
      setTimeout(() => {
        console.log('OpenLayers map loaded successfully')
        setMapLoaded(true)
        setError(null)
      }, 100)

    } catch (err) {
      console.error('Error initializing map:', err)
      setError(`Failed to initialize map: ${err instanceof Error ? err.message : 'Unknown error'}`)
    }

    return () => {
      if (mapInstance.current) {
        mapInstance.current.setTarget(undefined)
        mapInstance.current = null
      }
    }
  }, [])

  // Update geometry when data changes
  useEffect(() => {
    if (!mapInstance.current || !geometry) return

    const map = mapInstance.current

    // Remove existing geometry layer
    if (geometryLayerRef.current) {
      map.removeLayer(geometryLayerRef.current)
      geometryLayerRef.current = null
    }

    try {
      // Create geometry source from GeoJSON
      const geometrySource = new VectorSource({
        features: new GeoJSON().readFeatures(geometry, {
          featureProjection: 'EPSG:3857', // Web Mercator
          dataProjection: 'EPSG:4326',    // WGS84
        }),
      })

      // Create style based on error highlighting
      const geometryStyle = new Style({
        fill: new Fill({
          color: highlightError 
            ? 'rgba(239, 68, 68, 0.3)'   // Red for errors
            : 'rgba(59, 130, 246, 0.3)', // Blue for normal
        }),
        stroke: new Stroke({
          color: highlightError ? '#dc2626' : '#2563eb',
          width: 2,
        }),
        image: new Circle({
          radius: 6,
          fill: new Fill({
            color: highlightError ? '#dc2626' : '#2563eb',
          }),
        }),
      })

      const geometryLayer = new VectorLayer({
        source: geometrySource,
        style: geometryStyle,
      })

      map.addLayer(geometryLayer)
      geometryLayerRef.current = geometryLayer

      // Fit map to show the geometry
      const extent = geometrySource.getExtent()
      if (extent && !isNaN(extent[0])) {
        map.getView().fit(extent, {
          padding: [50, 50, 50, 50],
          maxZoom: 18,
        })
      }

    } catch (geometryError) {
      console.error('Error adding geometry to map:', geometryError)
      setError(`Failed to display geometry: ${geometryError instanceof Error ? geometryError.message : 'Unknown error'}`)
    }
  }, [geometry, highlightError])

  if (error) {
    return (
      <div className={`p-4 bg-red-50 border border-red-200 rounded-lg ${className}`}>
        <h3 className="text-red-800 font-medium">Map Error</h3>
        <p className="text-red-600 mt-1">{error}</p>
        <details className="mt-2">
          <summary className="text-red-700 cursor-pointer">Debug Info</summary>
          <pre className="text-xs mt-1 text-red-600">
            Browser: {navigator.userAgent}
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
            <p className="mt-2 text-gray-600">Loading map...</p>
          </div>
        </div>
      )}
      
      {/* Map legend/info */}
      {geometry && mapLoaded && (
        <div className="absolute top-4 left-4 bg-white rounded-lg shadow-lg p-3 text-sm">
          <h3 className="font-semibold mb-2">Test Geometry</h3>
          <div className="flex items-center">
            <div className={`w-4 h-4 rounded mr-2 ${
              highlightError ? 'bg-red-500 opacity-60' : 'bg-blue-500 opacity-60'
            }`}></div>
            <span>{highlightError ? 'Error Geometry' : 'Test Geometry'}</span>
          </div>
        </div>
      )}
    </div>
  )
} 