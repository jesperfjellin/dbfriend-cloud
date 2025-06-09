/**
 * Map Component for Geometry Visualization
 * 
 * This displays spatial data and diffs using MapLibre GL
 * Think of it like a visual diff tool but for geometries
 */

'use client'

import { useEffect, useRef } from 'react'
import maplibregl from 'maplibre-gl'
// CSS imported in layout.tsx

interface MapProps {
  // GeoJSON data to display (like your backend's GeoJSON responses)
  oldGeometry?: any  // GeoJSON for "before" state
  newGeometry?: any  // GeoJSON for "after" state  
  onMapLoad?: (map: maplibregl.Map) => void
  className?: string
}

export function Map({ oldGeometry, newGeometry, onMapLoad, className = '' }: MapProps) {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)

  useEffect(() => {
    if (!mapContainer.current) return

    // Initialize map (like setting up a matplotlib plot)
    map.current = new maplibregl.Map({
      container: mapContainer.current,
      // Free tile server (like using free base layers)
      style: 'https://demotiles.maplibre.org/style.json',
      center: [16.3738, 48.2082], // Default to Vienna, Austria (central Europe)
      zoom: 9,
    })

    // Add navigation controls
    map.current.addControl(new maplibregl.NavigationControl(), 'top-right')

    map.current.on('load', () => {
      if (onMapLoad && map.current) {
        onMapLoad(map.current)
      }
    })

    // Cleanup (like closing matplotlib figures)
    return () => {
      if (map.current) {
        map.current.remove()
      }
    }
  }, [onMapLoad])

  // Update geometries when data changes (like updating a plot)
  useEffect(() => {
    if (!map.current) return

    map.current.on('load', () => {
      addGeometryLayers()
    })

    if (map.current.isStyleLoaded()) {
      addGeometryLayers()
    }

    function addGeometryLayers() {
      if (!map.current) return

      // Remove existing layers (clean slate)
      try {
        if (map.current.getLayer('old-geometry')) map.current.removeLayer('old-geometry')
        if (map.current.getLayer('new-geometry')) map.current.removeLayer('new-geometry')
        if (map.current.getSource('old-geometry')) map.current.removeSource('old-geometry')
        if (map.current.getSource('new-geometry')) map.current.removeSource('new-geometry')
      } catch (e) {
        // Layers might not exist yet
      }

      // Add "before" geometry in red
      if (oldGeometry) {
        map.current.addSource('old-geometry', {
          type: 'geojson',
          data: oldGeometry
        })

        map.current.addLayer({
          id: 'old-geometry',
          type: 'fill',
          source: 'old-geometry',
          paint: {
            'fill-color': '#ef4444', // Red for "removed/old"
            'fill-opacity': 0.6,
            'fill-outline-color': '#dc2626'
          }
        })
      }

      // Add "after" geometry in green  
      if (newGeometry) {
        map.current.addSource('new-geometry', {
          type: 'geojson',
          data: newGeometry
        })

        map.current.addLayer({
          id: 'new-geometry',
          type: 'fill',
          source: 'new-geometry',
          paint: {
            'fill-color': '#10b981', // Green for "added/new"
            'fill-opacity': 0.6,
            'fill-outline-color': '#059669'
          }
        })
      }

      // Fit map to show all geometries (like plt.xlim, plt.ylim)
      if (oldGeometry || newGeometry) {
        const bounds = new maplibregl.LngLatBounds()
        
        if (oldGeometry && oldGeometry.geometry) {
          addGeometryToBounds(bounds, oldGeometry.geometry)
        }
        if (newGeometry && newGeometry.geometry) {
          addGeometryToBounds(bounds, newGeometry.geometry)
        }

        if (!bounds.isEmpty()) {
          map.current.fitBounds(bounds, { padding: 50 })
        }
      }
    }
  }, [oldGeometry, newGeometry])

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
    }
  }

  return (
    <div className={`relative ${className}`}>
      <div ref={mapContainer} className="w-full h-full rounded-lg" />
      
      {/* Legend (like a matplotlib legend) */}
      <div className="absolute top-4 left-4 bg-white rounded-lg shadow-lg p-3 text-sm">
        <h3 className="font-semibold mb-2">Geometry Diff</h3>
        {oldGeometry && (
          <div className="flex items-center mb-1">
            <div className="w-4 h-4 bg-red-500 rounded mr-2 opacity-60"></div>
            <span>Old Geometry</span>
          </div>
        )}
        {newGeometry && (
          <div className="flex items-center">
            <div className="w-4 h-4 bg-green-500 rounded mr-2 opacity-60"></div>
            <span>New Geometry</span>
          </div>
        )}
        {!oldGeometry && !newGeometry && (
          <p className="text-gray-500">No geometry data</p>
        )}
      </div>
    </div>
  )
} 