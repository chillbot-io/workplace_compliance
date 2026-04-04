import Link from "next/link";

export function Footer() {
  return (
    <footer className="border-t border-gray-200 bg-gray-50">
      <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8 py-12">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
          <div>
            <h3 className="text-sm font-semibold text-gray-900">Product</h3>
            <ul className="mt-4 space-y-2">
              <li><Link href="/search" className="text-sm text-gray-600 hover:text-gray-900">Search</Link></li>
              <li><Link href="/upload" className="text-sm text-gray-600 hover:text-gray-900">CSV Upload</Link></li>
              <li><Link href="/pricing" className="text-sm text-gray-600 hover:text-gray-900">Pricing</Link></li>
            </ul>
          </div>
          <div>
            <h3 className="text-sm font-semibold text-gray-900">Developers</h3>
            <ul className="mt-4 space-y-2">
              <li><Link href="/docs" className="text-sm text-gray-600 hover:text-gray-900">API Docs</Link></li>
              <li><a href="https://api.fastdol.com/v1/health" target="_blank" rel="noopener noreferrer" className="text-sm text-gray-600 hover:text-gray-900">API Status</a></li>
            </ul>
          </div>
          <div>
            <h3 className="text-sm font-semibold text-gray-900">Company</h3>
            <ul className="mt-4 space-y-2">
              <li><Link href="/terms" className="text-sm text-gray-600 hover:text-gray-900">Terms of Service</Link></li>
              <li><Link href="/privacy" className="text-sm text-gray-600 hover:text-gray-900">Privacy Policy</Link></li>
            </ul>
          </div>
          <div>
            <h3 className="text-sm font-semibold text-gray-900">Data Sources</h3>
            <ul className="mt-4 space-y-2">
              <li className="text-sm text-gray-600">OSHA Inspections</li>
              <li className="text-sm text-gray-600">OSHA Violations</li>
              <li className="text-sm text-gray-600">WHD Enforcement</li>
            </ul>
          </div>
        </div>
        <div className="mt-8 pt-8 border-t border-gray-200">
          <p className="text-sm text-gray-500">
            Data sourced from publicly available federal enforcement records. Not legal advice.
            &copy; {new Date().getFullYear()} FastDOL. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  );
}
