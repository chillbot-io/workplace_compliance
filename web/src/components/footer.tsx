import Link from "next/link";

export function Footer() {
  return (
    <footer className="border-t border-slate-800 bg-[#0b1120]">
      <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8 py-12">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
          <div>
            <h3 className="text-sm font-semibold text-white">Product</h3>
            <ul className="mt-4 space-y-2">
              <li><Link href="/search" className="text-sm text-slate-500 hover:text-slate-300">Search</Link></li>
              <li><Link href="/upload" className="text-sm text-slate-500 hover:text-slate-300">CSV Upload</Link></li>
              <li><Link href="/pricing" className="text-sm text-slate-500 hover:text-slate-300">Pricing</Link></li>
            </ul>
          </div>
          <div>
            <h3 className="text-sm font-semibold text-white">Developers</h3>
            <ul className="mt-4 space-y-2">
              <li><Link href="/docs" className="text-sm text-slate-500 hover:text-slate-300">API Docs</Link></li>
              <li><a href="https://api.fastdol.com/v1/health" target="_blank" rel="noopener noreferrer" className="text-sm text-slate-500 hover:text-slate-300">API Status</a></li>
            </ul>
          </div>
          <div>
            <h3 className="text-sm font-semibold text-white">Company</h3>
            <ul className="mt-4 space-y-2">
              <li><Link href="/terms" className="text-sm text-slate-500 hover:text-slate-300">Terms of Service</Link></li>
              <li><Link href="/privacy" className="text-sm text-slate-500 hover:text-slate-300">Privacy Policy</Link></li>
            </ul>
          </div>
          <div>
            <h3 className="text-sm font-semibold text-white">Data Sources</h3>
            <ul className="mt-4 space-y-2">
              <li className="text-sm text-slate-500">OSHA Inspections</li>
              <li className="text-sm text-slate-500">OSHA Violations</li>
              <li className="text-sm text-slate-500">WHD Enforcement</li>
              <li className="text-sm text-slate-500">MSHA Mine Safety</li>
            </ul>
          </div>
        </div>
        <div className="mt-8 pt-8 border-t border-slate-800">
          <p className="text-sm text-slate-600">
            Data sourced from publicly available federal enforcement records. Not legal advice.
            &copy; {new Date().getFullYear()} FastDOL. All rights reserved.
          </p>
        </div>
      </div>
    </footer>
  );
}
