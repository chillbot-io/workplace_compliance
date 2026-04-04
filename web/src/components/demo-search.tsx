"use client";

import { useState } from "react";
import Link from "next/link";

interface DemoResult {
  employer_name: string;
  employer_id: string;
  state: string;
  city: string;
  zip: string;
  risk_tier: string;
  risk_score: number;
  osha_inspections_5yr: number;
  osha_violations_5yr: number;
  osha_total_penalties: number;
  location_count: number;
  parent_name: string | null;
  risk_note?: string;
}

const TIER_COLORS: Record<string, string> = {
  HIGH: "bg-red-100 text-red-800",
  ELEVATED: "bg-orange-100 text-orange-800",
  MEDIUM: "bg-yellow-100 text-yellow-800",
  LOW: "bg-green-100 text-green-800",
};

export function DemoSearch() {
  const [query, setQuery] = useState("");
  const [zip, setZip] = useState("");
  const [results, setResults] = useState<DemoResult[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function handleSearch(e: React.FormEvent) {
    e.preventDefault();
    if (!query.trim()) return;

    setLoading(true);
    setError("");
    setResults([]);

    try {
      const params = new URLSearchParams({ name: query.trim() });
      if (zip.trim()) params.set("zip", zip.trim());
      params.set("limit", "3");

      const res = await fetch(`/api/demo-search?${params}`);
      const data = await res.json();

      if (!res.ok) {
        setError(data.error || "Search failed");
        return;
      }

      setResults(data.results || []);
      setTotalCount(data.total_count || 0);
    } catch {
      setError("Something went wrong. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div>
      <form onSubmit={handleSearch} className="flex flex-col sm:flex-row gap-3">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Employer name (e.g., Walmart, Amazon)"
          className="flex-1 rounded-md border border-gray-300 px-4 py-3 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none"
        />
        <input
          type="text"
          value={zip}
          onChange={(e) => setZip(e.target.value)}
          placeholder="ZIP (optional)"
          className="w-full sm:w-28 rounded-md border border-gray-300 px-4 py-3 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none"
          maxLength={5}
        />
        <button
          type="submit"
          disabled={loading}
          className="rounded-md bg-blue-600 px-6 py-3 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
        >
          {loading ? "Searching..." : "Search"}
        </button>
      </form>

      {error && (
        <div className="mt-4 rounded-md bg-red-50 border border-red-200 p-4 text-sm text-red-700">
          {error}
        </div>
      )}

      {results.length > 0 && (
        <div className="mt-6 space-y-3">
          {results.map((r) => (
            <div
              key={r.employer_id}
              className="rounded-lg border border-gray-200 p-4 hover:border-gray-300 transition-colors"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <div className="font-semibold text-gray-900 truncate">
                    {r.employer_name}
                  </div>
                  <div className="text-sm text-gray-500 mt-1">
                    {[r.city, r.state, r.zip].filter(Boolean).join(", ")}
                    {r.parent_name && (
                      <span className="ml-2 text-xs text-blue-600">
                        ({r.parent_name})
                      </span>
                    )}
                  </div>
                  <div className="flex gap-4 mt-2 text-xs text-gray-500">
                    <span>{r.osha_inspections_5yr} inspections (5yr)</span>
                    <span>{r.osha_violations_5yr} violations</span>
                    <span>${(r.osha_total_penalties || 0).toLocaleString()} penalties</span>
                    {r.location_count > 1 && (
                      <span>{r.location_count} locations</span>
                    )}
                  </div>
                </div>
                <div className="flex flex-col items-end gap-1 shrink-0">
                  <span className={`inline-block rounded-full px-2.5 py-0.5 text-xs font-medium ${TIER_COLORS[r.risk_tier] || "bg-gray-100 text-gray-800"}`}>
                    {r.risk_tier}
                  </span>
                  <span className="text-sm font-bold text-gray-700">
                    {r.risk_score}
                  </span>
                </div>
              </div>
              {r.risk_note && (
                <div className="mt-2 text-xs text-gray-400 italic">{r.risk_note}</div>
              )}
            </div>
          ))}

          {totalCount > 3 && (
            <div className="text-center pt-2">
              <Link href="/signup" className="text-sm text-blue-600 hover:underline">
                Sign up free to see all {totalCount} results
              </Link>
            </div>
          )}
        </div>
      )}

      {results.length === 0 && !loading && !error && query && (
        <div className="mt-6 text-center text-sm text-gray-500">
          No results. Try a different name or remove the ZIP filter.
        </div>
      )}
    </div>
  );
}
