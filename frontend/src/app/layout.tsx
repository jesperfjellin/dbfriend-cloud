/**
 * Root Layout Component
 */

import './globals.css'
// TODO: Fix MapLibre CSS import issue
// import 'maplibre-gl/dist/maplibre-gl.css'
import type { Metadata } from 'next'
import { Providers } from '@/lib/providers'

// Metadata for SEO (like Django's <title> and <meta> tags)
export const metadata: Metadata = {
  title: 'dbfriend-cloud | PostGIS Diff Checker',
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
          {/* This is where all our pages will be rendered */}
          {/* Like Django's {% block content %}...{% endblock %} */}
          {children}
        </Providers>
      </body>
    </html>
  )
} 