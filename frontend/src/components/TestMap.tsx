/**
 * TestMap Component for Quality Test Visualization
 * 
 * This displays individual problematic geometries from spatial tests
 * Unlike the diff Map, this shows a single geometry with error highlighting
 */

'use client'

import { useEffect, useRef, useState } from 'react'
import maplibregl from 'maplibre-gl'

interface TestMapProps {
  className?: string
}

export default function TestMap({ className = '' }: TestMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const [mapLoaded, setMapLoaded] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!mapContainer.current || map.current) return

    try {
      console.log('Initializing MapLibre GL map...')
      
      map.current = new maplibregl.Map({
        container: mapContainer.current,
        style: {
          version: 8,
          sources: {},
          layers: []
        },
        center: [-122.4194, 37.7749], // San Francisco
        zoom: 9,
        antialias: true
      })

      map.current.on('load', () => {
        console.log('Map loaded successfully')
        setMapLoaded(true)
        setError(null)
      })

      map.current.on('error', (e) => {
        console.error('Map error:', e)
        setError(`Map error: ${e.error?.message || 'Unknown error'}`)
      })

    } catch (err) {
      console.error('Error initializing map:', err)
      setError(`Failed to initialize map: ${err instanceof Error ? err.message : 'Unknown error'}`)
    }

    return () => {
      if (map.current) {
        map.current.remove()
        map.current = null
      }
    }
  }, [])

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
        ref={mapContainer} 
        className="w-full h-96 bg-gray-100 rounded-lg"
        style={{ 
          minHeight: '400px',
          // Add basic MapLibre styles inline to avoid import issues
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
    </div>
  )
} 