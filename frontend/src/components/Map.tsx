/**
 * Map Component for Geometry Visualization
 * 
 * This displays spatial data and diffs using OpenLayers
 * Think of it like a visual diff tool but for geometries
 */

'use client'

import { useEffect, useRef } from 'react'
import 'ol/ol.css'
import { Map as OLMap, View } from 'ol'
import { Tile as TileLayer, Vector as VectorLayer } from 'ol/layer'
import { OSM } from 'ol/source'
import { Vector as VectorSource } from 'ol/source'
import { GeoJSON } from 'ol/format'
import { Style, Fill, Stroke } from 'ol/style'
import { defaults as defaultControls } from 'ol/control'
import { fromLonLat } from 'ol/proj'

interface MapProps {
  // GeoJSON data to display (like your backend's GeoJSON responses)
  oldGeometry?: any  // GeoJSON for "before" state
  newGeometry?: any  // GeoJSON for "after" state  
  onMapLoad?: (map: OLMap) => void
  className?: string
}

export function Map({ oldGeometry, newGeometry, onMapLoad, className = '' }: MapProps) {
  const mapRef = useRef<HTMLDivElement>(null)
  const mapInstance = useRef<OLMap | null>(null)
  const oldLayerRef = useRef<VectorLayer<any> | null>(null)
  const newLayerRef = useRef<VectorLayer<any> | null>(null)

  useEffect(() => {
    if (!mapRef.current || mapInstance.current) return

    // Initialize OpenLayers map
    const map = new OLMap({
      target: mapRef.current,
      layers: [
        // Base tile layer (OpenStreetMap)
        new TileLayer({
          source: new OSM(),
        }),
      ],
      view: new View({
        center: fromLonLat([16.3738, 48.2082]), // Vienna, Austria (central Europe)
        zoom: 9,
      }),
      controls: defaultControls(),
    })

    mapInstance.current = map

    if (onMapLoad) {
      onMapLoad(map)
    }

    // Cleanup
    return () => {
      if (mapInstance.current) {
        mapInstance.current.setTarget(undefined)
        mapInstance.current = null
      }
    }
  }, [onMapLoad])

  // Update geometries when data changes
  useEffect(() => {
    if (!mapInstance.current) return

    const map = mapInstance.current

    // Remove existing geometry layers
    if (oldLayerRef.current) {
      map.removeLayer(oldLayerRef.current)
      oldLayerRef.current = null
    }
    if (newLayerRef.current) {
      map.removeLayer(newLayerRef.current)
      newLayerRef.current = null
    }

    const extent: number[] = []

    // Add "before" geometry in red
    if (oldGeometry) {
      const oldSource = new VectorSource({
        features: new GeoJSON().readFeatures(oldGeometry, {
          featureProjection: 'EPSG:3857', // Web Mercator
          dataProjection: 'EPSG:4326',    // WGS84
        }),
      })

      const oldLayer = new VectorLayer({
        source: oldSource,
        style: new Style({
          fill: new Fill({
            color: 'rgba(239, 68, 68, 0.6)', // Red for "removed/old"
          }),
          stroke: new Stroke({
            color: '#dc2626',
            width: 2,
          }),
        }),
      })

      map.addLayer(oldLayer)
      oldLayerRef.current = oldLayer

      // Add to extent
      const sourceExtent = oldSource.getExtent()
      if (extent.length === 0) {
        extent.push(...sourceExtent)
      } else {
        extent[0] = Math.min(extent[0], sourceExtent[0])
        extent[1] = Math.min(extent[1], sourceExtent[1])
        extent[2] = Math.max(extent[2], sourceExtent[2])
        extent[3] = Math.max(extent[3], sourceExtent[3])
      }
    }

    // Add "after" geometry in green
    if (newGeometry) {
      const newSource = new VectorSource({
        features: new GeoJSON().readFeatures(newGeometry, {
          featureProjection: 'EPSG:3857', // Web Mercator
          dataProjection: 'EPSG:4326',    // WGS84
        }),
      })

      const newLayer = new VectorLayer({
        source: newSource,
        style: new Style({
          fill: new Fill({
            color: 'rgba(16, 185, 129, 0.6)', // Green for "added/new"
          }),
          stroke: new Stroke({
            color: '#059669',
            width: 2,
          }),
        }),
      })

      map.addLayer(newLayer)
      newLayerRef.current = newLayer

      // Add to extent
      const sourceExtent = newSource.getExtent()
      if (extent.length === 0) {
        extent.push(...sourceExtent)
      } else {
        extent[0] = Math.min(extent[0], sourceExtent[0])
        extent[1] = Math.min(extent[1], sourceExtent[1])
        extent[2] = Math.max(extent[2], sourceExtent[2])
        extent[3] = Math.max(extent[3], sourceExtent[3])
      }
    }

    // Fit map to show all geometries
    if (extent.length > 0 && !isNaN(extent[0])) {
      map.getView().fit(extent, {
        padding: [50, 50, 50, 50],
        maxZoom: 18,
      })
    }
  }, [oldGeometry, newGeometry])

  return (
    <div className={`relative ${className}`}>
      <div 
        ref={mapRef} 
        className="w-full h-full rounded-lg"
      />
      
      {/* Legend */}
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