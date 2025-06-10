/**
 * API Client for dbfriend-cloud
 * 
 * This is like Python's requests library - it handles HTTP calls to our FastAPI backend
 * Each function here corresponds to an endpoint in your backend
 */

import { 
  Dataset, 
  DatasetCreate, 
  DatasetUpdate,
  DatasetConnectionTest,
  DatasetConnectionTestResponse,
  DatasetStats, 
  GeometryDiff, 
  DiffReview,
  SpatialCheck,
  SpatialCheckStats
} from '@/types/api'

// Base API URL (like setting a base URL in requests)
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

// Helper function like requests.get() in Python
async function apiRequest<T>(
  endpoint: string, 
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE_URL}/api/v1${endpoint}`
  
  const response = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
    ...options,
  })

  if (!response.ok) {
    throw new Error(`API Error: ${response.status} ${response.statusText}`)
  }

  return response.json()
}

// Dataset API functions
export const datasetApi = {
  // GET /api/v1/datasets/ - list all datasets
  list: async (): Promise<Dataset[]> => {
    return apiRequest<Dataset[]>('/datasets/')
  },

  // POST /api/v1/datasets/ - create new dataset
  create: async (dataset: DatasetCreate): Promise<Dataset> => {
    return apiRequest<Dataset>('/datasets/', {
      method: 'POST',
      body: JSON.stringify(dataset),
    })
  },

  // GET /api/v1/datasets/{id} - get specific dataset
  get: async (id: string): Promise<Dataset> => {
    return apiRequest<Dataset>(`/datasets/${id}`)
  },

  // PUT /api/v1/datasets/{id} - update dataset
  update: async (id: string, updates: DatasetUpdate): Promise<Dataset> => {
    return apiRequest<Dataset>(`/datasets/${id}`, {
      method: 'PUT',
      body: JSON.stringify(updates),
    })
  },

  // DELETE /api/v1/datasets/{id} - delete dataset
  delete: async (id: string): Promise<void> => {
    return apiRequest<void>(`/datasets/${id}`, {
      method: 'DELETE',
    })
  },

  // POST /api/v1/datasets/test-connection - test database connection
  testConnection: async (testData: DatasetConnectionTest): Promise<DatasetConnectionTestResponse> => {
    return apiRequest<DatasetConnectionTestResponse>('/datasets/test-connection', {
      method: 'POST',
      body: JSON.stringify(testData),
    })
  },

  // POST /api/v1/datasets/{id}/import - trigger geometry import
  import: async (id: string, forceReimport: boolean = false) => {
    return apiRequest(`/datasets/${id}/import`, {
      method: 'POST',
      body: JSON.stringify({ force_reimport: forceReimport }),
    })
  },

  // GET /api/v1/datasets/{id}/stats - like requests.get()
  getStats: async (id: string): Promise<DatasetStats> => {
    return apiRequest<DatasetStats>(`/datasets/${id}/stats`)
  },
}

// Diffs API functions 
export const diffsApi = {
  // GET /api/v1/diffs/ - like requests.get() with query params
  list: async (params: {
    dataset_id?: string
    status?: string
    skip?: number
    limit?: number
  } = {}): Promise<GeometryDiff[]> => {
    const searchParams = new URLSearchParams()
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined) searchParams.append(key, value.toString())
    })
    
    const query = searchParams.toString()
    return apiRequest<GeometryDiff[]>(`/diffs/${query ? '?' + query : ''}`)
  },

  // GET /api/v1/diffs/{id} - get detailed diff
  get: async (id: string) => {
    return apiRequest(`/diffs/${id}`)
  },

  // PUT /api/v1/diffs/{id}/review - review a diff
  review: async (id: string, review: DiffReview) => {
    return apiRequest(`/diffs/${id}/review`, {
      method: 'PUT',
      body: JSON.stringify(review),
    })
  },

  // GET /api/v1/diffs/pending/count - get pending count
  getPendingCount: async (dataset_id?: string): Promise<{ pending_count: number }> => {
    const query = dataset_id ? `?dataset_id=${dataset_id}` : ''
    return apiRequest<{ pending_count: number }>(`/diffs/pending/count${query}`)
  },
}

// Health check (like a simple requests.get() to test connection)
export const healthApi = {
  check: async (): Promise<{ status: string }> => {
    return apiRequest<{ status: string }>('/health')
  }
}

// Tests API functions (Spatial Checks)
export const testsApi = {
  // GET /api/v1/geometry/spatial-checks/ - list spatial checks with filtering
  list: async (params: {
    dataset_id?: string
    check_type?: string
    check_result?: string
    skip?: number
    limit?: number
  } = {}): Promise<SpatialCheck[]> => {
    const searchParams = new URLSearchParams()
    Object.entries(params).forEach(([key, value]) => {
      if (value !== undefined) searchParams.append(key, value.toString())
    })
    
    const query = searchParams.toString()
    return apiRequest<SpatialCheck[]>(`/geometry/spatial-checks/${query ? '?' + query : ''}`)
  },

  // GET /api/v1/geometry/spatial-checks/stats - get spatial check statistics
  getStats: async (dataset_id?: string): Promise<SpatialCheckStats> => {
    const query = dataset_id ? `?dataset_id=${dataset_id}` : ''
    return apiRequest<SpatialCheckStats>(`/geometry/spatial-checks/stats${query}`)
  },

  // POST /api/v1/geometry/snapshots/{snapshot_id}/spatial-checks - run spatial checks on a snapshot
  runChecks: async (snapshot_id: string): Promise<SpatialCheck[]> => {
    return apiRequest<SpatialCheck[]>(`/geometry/snapshots/${snapshot_id}/spatial-checks`, {
      method: 'POST',
    })
  },
}

// Geometry API functions
export const geometryApi = {
  // GET /api/v1/geometry/snapshots/{snapshot_id}/geojson - get geometry as GeoJSON
  getGeoJSON: async (snapshot_id: string) => {
    return apiRequest(`/geometry/snapshots/${snapshot_id}/geojson`)
  },
}

export interface GeometryContextItem {
  geometry_id: string
  geometry: GeoJSON.Geometry
  attributes: any
  is_primary: boolean
  distance_meters?: number
}

export interface GeometryContextResponse {
  geometries: GeometryContextItem[]
  buffer_geometry: GeoJSON.Geometry
  total_found: number
}

/**
 * Get spatial context for a geometry - finds other geometries within a buffer
 */
export async function getGeometryContext(
  geometryId: string, 
  bufferMeters: number = 500,
  maxResults: number = 50
): Promise<GeometryContextResponse> {
  const response = await fetch(
    `${API_BASE_URL}/api/v1/spatial/geometry-context/${geometryId}?buffer_meters=${bufferMeters}&max_results=${maxResults}`
  )
  
  if (!response.ok) {
    throw new Error(`Failed to get geometry context: ${response.statusText}`)
  }
  
  return response.json()
} 