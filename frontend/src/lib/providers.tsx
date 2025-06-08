'use client'

import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ReactQueryDevtools } from '@tanstack/react-query-devtools'
import { useState } from 'react'

export function Providers({ children }: { children: React.ReactNode }) {
  // Create a client instance (like creating a database connection pool)
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            // How long to keep data fresh (like Django's cache timeout)
            staleTime: 60 * 1000, // 1 minute
            // How long to keep data in memory
            gcTime: 10 * 60 * 1000, // 10 minutes
          },
        },
      })
  )

  return (
    <QueryClientProvider client={queryClient}>
      {children}
      {/* Dev tools for debugging queries (like Django Debug Toolbar) */}
      <ReactQueryDevtools initialIsOpen={false} />
    </QueryClientProvider>
  )
} 