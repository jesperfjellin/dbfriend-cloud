/**
 * Main Dashboard Page with Real Data
 */

'use client'

import Link from 'next/link'
import { useQuery } from '@tanstack/react-query'

// Simple dashboard overview
export default function HomePage() {
  // We can add real API calls later, for now just a nice landing page
  
  const features = [
    {
      title: 'Database Connections',
      description: 'Connect to your PostGIS databases to monitor spatial data quality and detect changes.',
      href: '/datasets',
      color: 'bg-blue-50 text-blue-700 border-blue-200'
    },
    {
      title: 'Diff Queue',
      description: 'Review detected geometry changes with Git-style accept/reject workflow.',
      href: '/diffs',
      color: 'bg-orange-50 text-orange-700 border-orange-200'
    },
    {
      title: 'Quality Tests',
      description: 'Monitor persistent topology and validity tests running on your spatial data.',
      href: '/tests',
      color: 'bg-purple-50 text-purple-700 border-purple-200'
    },
    {
      title: 'System Monitoring',
      description: 'Monitor storage usage, system health, and performance metrics.',
      href: '/monitoring',
      color: 'bg-green-50 text-green-700 border-green-200'
    }
  ]

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 py-12">
        
        {/* Hero Section */}
        <div className="text-center mb-16">
          <h1 className="text-4xl font-bold text-gray-900 mb-4">
            Welcome to dbfriend-cloud
          </h1>
          <p className="text-xl text-gray-600 max-w-3xl mx-auto leading-relaxed">
            SaaS for geometry-diff QA on PostGIS: automated checks + Git-style accept/reject queue 
            that writes back to your database. Monitor spatial data quality and detect changes automatically.
          </p>
        </div>

        {/* Feature Cards */}
        <div className="grid md:grid-cols-3 gap-8 mb-16">
          {features.map((feature) => (
            <Link 
              key={feature.title}
              href={feature.href}
              className="group"
            >
              <div className={`p-8 rounded-xl border-2 transition-all hover:shadow-lg hover:scale-105 ${feature.color}`}>
                <h3 className="text-xl font-semibold mb-3">{feature.title}</h3>
                <p className="text-sm leading-relaxed opacity-80 group-hover:opacity-100 transition-opacity">
                  {feature.description}
                </p>
                <div className="mt-4 text-sm font-medium opacity-60 group-hover:opacity-100 transition-opacity">
                  Get started →
                </div>
              </div>
            </Link>
          ))}
        </div>

        {/* Quick Stats */}
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-8">
          <h2 className="text-2xl font-bold text-gray-900 mb-6">System Overview</h2>
          <div className="grid md:grid-cols-4 gap-6">
            <div className="text-center">
              <div className="text-3xl font-bold text-blue-600 mb-2">-</div>
              <div className="text-sm text-gray-600">Active Datasets</div>
            </div>
            <div className="text-center">
              <div className="text-3xl font-bold text-orange-600 mb-2">-</div>
              <div className="text-sm text-gray-600">Pending Diffs</div>
            </div>
            <div className="text-center">
              <div className="text-3xl font-bold text-green-600 mb-2">-</div>
              <div className="text-sm text-gray-600">Snapshots Stored</div>
            </div>
            <div className="text-center">
              <div className="text-3xl font-bold text-purple-600 mb-2">-</div>
              <div className="text-sm text-gray-600">Storage Used</div>
            </div>
          </div>
          <div className="mt-6 text-center">
            <Link 
              href="/monitoring" 
              className="text-blue-600 hover:text-blue-700 font-medium text-sm"
            >
              View detailed metrics →
            </Link>
          </div>
        </div>

        {/* Getting Started */}
        <div className="mt-16 text-center">
          <h2 className="text-2xl font-bold text-gray-900 mb-4">Getting Started</h2>
          <p className="text-gray-600 mb-8 max-w-2xl mx-auto">
            Connect your first PostGIS database to start monitoring spatial data quality and detecting changes automatically.
          </p>
          <Link 
            href="/datasets"
            className="bg-blue-600 text-white px-8 py-3 rounded-lg hover:bg-blue-700 transition-colors font-medium"
          >
            Add Your First Database
          </Link>
        </div>
      </div>
    </div>
  )
} 