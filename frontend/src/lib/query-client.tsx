/**
 * React Query Setup
 * 
 * This is like having automatic caching for your API calls
 * Think of it as smart requests that automatically refetch when data changes
 */

'use client'

import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { useState } from 'react'

// Create a client (like setting up a requests session in Python)
export function QueryProvider({ children }: { children: React.ReactNode }) {
  const [queryClient] = useState(() => new QueryClient({
    defaultOptions: {
      queries: {
        // Refetch data when window gets focus (like auto-refresh)
        refetchOnWindowFocus: false,
        // Keep data fresh for 5 minutes
        staleTime: 5 * 60 * 1000,
        // Retry failed requests 3 times
        retry: 3,
      },
    },
  }))

  return (
    <QueryClientProvider client={queryClient}>
      {children}
    </QueryClientProvider>
  )
} 