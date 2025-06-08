/** @type {import('next').NextConfig} */
const nextConfig = {
  // Enable TypeScript strict mode (like Python's type hints)
  typescript: {
    ignoreBuildErrors: false,
  },
  
  // Configure environment variables (like Python's os.environ)
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000',
  },
  
  // Configure webpack for map libraries
  webpack: (config) => {
    config.resolve.alias = {
      ...config.resolve.alias,
      // Fix for maplibre-gl
      'maplibre-gl': 'maplibre-gl/dist/maplibre-gl.js',
    }
    return config
  },
}

module.exports = nextConfig 