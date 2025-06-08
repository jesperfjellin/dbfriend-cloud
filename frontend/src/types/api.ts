/**
 * TypeScript Types for API
 * 
 * These mirror the Pydantic models from our backend
 * Think of these like Python's type hints but for JavaScript
 */

// Like your Pydantic BaseModel for Dataset
export interface Dataset {
  id: string
  name: string
  description?: string
  connection_string: string
  schema_name: string
  table_name: string
  geometry_column: string
  check_interval_minutes: number
  created_at: string
  updated_at: string
  is_active: boolean
  last_check_at?: string
}

// For creating new datasets (like DatasetCreate Pydantic model)
export interface DatasetCreate {
  name: string
  description?: string
  connection_string: string
  schema_name: string
  table_name: string
  geometry_column: string
  check_interval_minutes: number
}

// For updating existing datasets (like DatasetUpdate Pydantic model)
export interface DatasetUpdate {
  name?: string
  description?: string
  check_interval_minutes?: number
  is_active?: boolean
}

// Like your GeometryDiff Pydantic model
export interface GeometryDiff {
  id: string
  dataset_id: string
  diff_type: 'NEW' | 'UPDATED' | 'DELETED'
  confidence_score?: number
  old_snapshot_id?: string
  new_snapshot_id?: string
  geometry_changed: boolean
  attributes_changed: boolean
  changes_summary?: Record<string, any>
  status: 'PENDING' | 'ACCEPTED' | 'REJECTED'
  reviewed_at?: string
  reviewed_by?: string
  created_at: string
}

// Statistics (like your DiffStats Pydantic model)
export interface DiffStats {
  total_diffs: number
  pending_diffs: number
  accepted_diffs: number
  rejected_diffs: number
  new_geometries: number
  updated_geometries: number
  deleted_geometries: number
}

export interface DatasetStats {
  dataset_id: string
  total_snapshots: number
  last_check_at?: string
  diff_stats: DiffStats
  spatial_checks: Record<string, number>
}

// For reviewing diffs (like DiffReview Pydantic model)
export interface DiffReview {
  status: 'ACCEPTED' | 'REJECTED'
  reviewed_by: string
}

// GeoJSON type (for map display)
export interface GeoJSONGeometry {
  type: string
  coordinates: any[]
}

// API Response types
export interface ApiResponse<T> {
  data?: T
  error?: string
  message?: string
} 