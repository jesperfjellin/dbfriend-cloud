/**
 * Root Layout Component
 */

import './globals.css'
import type { Metadata } from 'next'
import { Providers } from '@/lib/providers'
import { Navigation } from '@/components/Navigation'

// Metadata for SEO (like Django's <title> and <meta> tags)
export const metadata: Metadata = {
  title: 'dbfriend-cloud | Topology Watchdog',
  description: 'SaaS for geometry-diff QA on PostGIS: automated checks + Git-style accept/reject queue',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode  // This is like Django's {% block content %}
}) {
  return (
    <html lang="en">
      <body>
        {/* Wrap everything with React Query provider */}
        <Providers>
          {/* Navigation header - appears on all pages */}
          <Navigation />
          
          {/* Main content area */}
          <main>
            {/* This is where all our pages will be rendered */}
            {/* Like Django's {% block content %}...{% endblock %} */}
            {children}
          </main>
        </Providers>
      </body>
    </html>
  )
} 