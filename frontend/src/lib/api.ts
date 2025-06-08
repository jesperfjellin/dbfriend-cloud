/**
 * API Client for dbfriend-cloud
 * 
 * This is like Python's requests library - it handles HTTP calls to our FastAPI backend
 * Each function here corresponds to an endpoint in your backend
 */

import { Dataset, DatasetCreate, DatasetStats, GeometryDiff, DiffReview } from '@/types/api'

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

// Dataset API functions (correspond to your FastAPI endpoints)
export const datasetApi = {
  // GET /api/v1/datasets/ - like requests.get()
  list: async (): Promise<Dataset[]> => {
    return apiRequest<Dataset[]>('/datasets/')
  },

  // GET /api/v1/datasets/{id} - like requests.get(f'/datasets/{id}')
  get: async (id: string): Promise<Dataset> => {
    return apiRequest<Dataset>(`/datasets/${id}`)
  },

  // POST /api/v1/datasets/ - like requests.post()
  create: async (dataset: DatasetCreate): Promise<Dataset> => {
    return apiRequest<Dataset>('/datasets/', {
      method: 'POST',
      body: JSON.stringify(dataset),
    })
  },

  // PUT /api/v1/datasets/{id} - like requests.put()
  update: async (id: string, dataset: Partial<DatasetCreate>): Promise<Dataset> => {
    return apiRequest<Dataset>(`/datasets/${id}`, {
      method: 'PUT',
      body: JSON.stringify(dataset),
    })
  },

  // GET /api/v1/datasets/{id}/stats - like requests.get()
  getStats: async (id: string): Promise<DatasetStats> => {
    return apiRequest<DatasetStats>(`/datasets/${id}/stats`)
  },

  // POST /api/v1/datasets/{id}/import - trigger diff detection
  triggerImport: async (id: string, force_reimport: boolean = false) => {
    return apiRequest(`/datasets/${id}/import`, {
      method: 'POST',
      body: JSON.stringify({ dataset_id: id, force_reimport }),
    })
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