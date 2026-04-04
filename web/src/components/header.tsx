"use client";

import Link from "next/link";
import { useState, useEffect } from "react";
import { useRouter, usePathname } from "next/navigation";

export function Header() {
  const [mobileOpen, setMobileOpen] = useState(false);
  const [loggedIn, setLoggedIn] = useState(false);
  const router = useRouter();
  const pathname = usePathname();

  // Check auth state by looking for cookie presence via a lightweight check
  useEffect(() => {
    // If we're on a protected page, we must be logged in (middleware redirects otherwise)
    const protectedRoutes = ["/search", "/upload", "/account", "/employers"];
    setLoggedIn(protectedRoutes.some((r) => pathname.startsWith(r)));
  }, [pathname]);

  async function handleLogout() {
    await fetch("/api/auth/logout", { method: "POST" });
    router.push("/");
  }

  return (
    <header className="border-b border-gray-200 bg-white">
      <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
        <div className="flex h-16 items-center justify-between">
          <Link href="/" className="text-xl font-bold text-gray-900">
            Fast<span className="text-blue-600">DOL</span>
          </Link>

          {/* Desktop nav */}
          <nav className="hidden md:flex items-center gap-8">
            <Link href="/pricing" className="text-sm text-gray-600 hover:text-gray-900">
              Pricing
            </Link>
            <Link href="/docs" className="text-sm text-gray-600 hover:text-gray-900">
              Docs
            </Link>
            {loggedIn ? (
              <>
                <Link href="/search" className="text-sm text-gray-600 hover:text-gray-900">
                  Search
                </Link>
                <Link href="/account" className="text-sm text-gray-600 hover:text-gray-900">
                  Account
                </Link>
                <button onClick={handleLogout} className="text-sm text-gray-600 hover:text-gray-900">
                  Log out
                </button>
              </>
            ) : (
              <>
                <Link href="/login" className="text-sm text-gray-600 hover:text-gray-900">
                  Log in
                </Link>
                <Link
                  href="/signup"
                  className="rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
                >
                  Get Started Free
                </Link>
              </>
            )}
          </nav>

          {/* Mobile hamburger */}
          <button
            className="md:hidden p-2"
            onClick={() => setMobileOpen(!mobileOpen)}
            aria-label="Toggle navigation menu"
            aria-expanded={mobileOpen}
          >
            <svg className="h-6 w-6" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" aria-hidden="true">
              {mobileOpen ? (
                <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
              ) : (
                <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 6.75h16.5M3.75 12h16.5m-16.5 5.25h16.5" />
              )}
            </svg>
          </button>
        </div>

        {/* Mobile menu */}
        {mobileOpen && (
          <div className="md:hidden pb-4 space-y-2">
            <Link href="/pricing" className="block py-2 text-sm text-gray-600">Pricing</Link>
            <Link href="/docs" className="block py-2 text-sm text-gray-600">Docs</Link>
            {loggedIn ? (
              <>
                <Link href="/search" className="block py-2 text-sm text-gray-600">Search</Link>
                <Link href="/upload" className="block py-2 text-sm text-gray-600">CSV Upload</Link>
                <Link href="/account" className="block py-2 text-sm text-gray-600">Account</Link>
                <button onClick={handleLogout} className="block py-2 text-sm text-gray-600">Log out</button>
              </>
            ) : (
              <>
                <Link href="/login" className="block py-2 text-sm text-gray-600">Log in</Link>
                <Link href="/signup" className="block py-2 text-sm font-medium text-blue-600">Get Started Free</Link>
              </>
            )}
          </div>
        )}
      </div>
    </header>
  );
}
