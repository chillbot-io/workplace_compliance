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
  osha_inspections: number;
  osha_violations: number;
  osha_total_penalties: number;
  location_count: number;
  parent_name: string | null;
  risk_note?: string;
}

const TIER_COLORS: Record<string, string> = {
  HIGH: "bg-red-500/20 text-red-400 border border-red-500/30",
  ELEVATED: "bg-orange-500/20 text-orange-400 border border-orange-500/30",
  MEDIUM: "bg-yellow-500/20 text-yellow-400 border border-yellow-500/30",
  LOW: "bg-emerald-500/20 text-emerald-400 border border-emerald-500/30",
};

export function DemoSearch() {
  const [query, setQuery] = useState("");
  const [zip, setZip] = useState("");
  const [results, setResults] = useState<DemoResult[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [searched, setSearched] = useState(false);

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
      setSearched(true);
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
          aria-label="Employer name"
          className="flex-1 rounded-md border border-slate-600 bg-slate-800 px-4 py-3 text-sm text-white placeholder-slate-500 focus:border-violet-500 focus:ring-1 focus:ring-violet-500 outline-none"
        />
        <input
          type="text"
          value={zip}
          onChange={(e) => setZip(e.target.value)}
          placeholder="ZIP (optional)"
          aria-label="ZIP code"
          className="w-full sm:w-28 rounded-md border border-slate-600 bg-slate-800 px-4 py-3 text-sm text-white placeholder-slate-500 focus:border-violet-500 focus:ring-1 focus:ring-violet-500 outline-none"
          maxLength={5}
        />
        <button
          type="submit"
          disabled={loading}
          className="rounded-md bg-violet-500 px-6 py-3 text-sm font-medium text-white hover:bg-violet-600 disabled:opacity-50 transition-colors"
        >
          {loading ? "Searching..." : "Search"}
        </button>
      </form>

      {error && (
        <div className="mt-4 rounded-md bg-red-500/10 border border-red-500/20 p-4 text-sm text-red-400">
          {error}
        </div>
      )}

      {results.length > 0 && (
        <div className="mt-6 space-y-3">
          {results.map((r) => (
            <Link
              key={r.employer_id}
              href={`/employers/${r.employer_id}`}
              className="block rounded-lg border border-slate-700 bg-slate-800/50 p-4 hover:border-violet-500/50 transition-colors"
            >
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <div className="font-semibold text-white truncate">
                    {r.employer_name}
                  </div>
                  <div className="text-sm text-slate-500 mt-1">
                    {[r.city, r.state, r.zip].filter(Boolean).join(", ")}
                    {r.parent_name && (
                      <span className="ml-2 text-xs text-violet-400">
                        ({r.parent_name})
                      </span>
                    )}
                  </div>
                  <div className="flex gap-4 mt-2 text-xs text-slate-500">
                    <span>{r.osha_inspections} inspections</span>
                    <span>{r.osha_violations} violations</span>
                    <span>${(r.osha_total_penalties || 0).toLocaleString()} penalties</span>
                    {r.location_count > 1 && (
                      <span>{r.location_count} locations</span>
                    )}
                  </div>
                </div>
                <div className="flex flex-col items-end gap-1 shrink-0">
                  <span className={`inline-block rounded-full px-2.5 py-0.5 text-xs font-medium ${TIER_COLORS[r.risk_tier] || "bg-slate-700 text-slate-400"}`}>
                    {r.risk_tier}
                  </span>
                  <span className="text-sm font-bold text-white">
                    {r.risk_score}
                  </span>
                </div>
              </div>
              {r.risk_note && (
                <div className="mt-2 text-xs text-slate-600 italic">{r.risk_note}</div>
              )}
            </Link>
          ))}

          {totalCount > 3 && (
            <div className="text-center pt-2">
              <Link href="/signup" className="text-sm text-violet-500 hover:underline">
                Sign up free to see all {totalCount} results
              </Link>
            </div>
          )}
        </div>
      )}

      {results.length === 0 && !loading && !error && searched && (
        <div className="mt-6 text-center text-sm text-slate-500">
          No results. Try a different name or remove the ZIP filter.
        </div>
      )}
    </div>
  );
}
