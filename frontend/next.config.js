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
    // OpenLayers configuration
    config.resolve.fallback = {
      ...config.resolve.fallback,
      fs: false,
    }
    return config
  },
}

module.exports = nextConfig 