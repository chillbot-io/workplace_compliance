"use client";

import { useState } from "react";
import Link from "next/link";

interface Employer {
  employer_id: string;
  employer_name: string;
  state: string;
  city: string;
  zip: string;
  risk_tier: string;
  risk_score: number;
  osha_inspections_5yr: number;
  osha_violations_5yr: number;
  osha_total_penalties: number;
  whd_cases_5yr: number;
  whd_backwages_total: number;
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

const US_STATES = [
  "", "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
  "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
  "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
  "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "PR",
  "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "VI", "WA",
  "WV", "WI", "WY",
];

export default function SearchPage() {
  const [name, setName] = useState("");
  const [zip, setZip] = useState("");
  const [state, setState] = useState("");
  const [results, setResults] = useState<Employer[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [searched, setSearched] = useState(false);
  const limit = 20;

  async function doSearch(newOffset = 0) {
    if (!name.trim()) return;
    setLoading(true);
    setError("");

    try {
      const params = new URLSearchParams({ name: name.trim(), limit: String(limit), offset: String(newOffset) });
      if (zip.trim()) params.set("zip", zip.trim());
      if (state) params.set("state", state);

      const res = await fetch(`/api/search?${params}`);
      const data = await res.json();

      if (!res.ok) {
        setError(data.detail?.message || data.error || "Search failed");
        setResults([]);
        return;
      }

      setResults(data.results || []);
      setTotalCount(data.total_count || 0);
      setOffset(newOffset);
      setSearched(true);
    } catch {
      setError("Something went wrong.");
    } finally {
      setLoading(false);
    }
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    doSearch(0);
  }

  return (
    <div className="mx-auto max-w-6xl px-4 sm:px-6 lg:px-8 py-8">
      <title>Employer Search - FastDOL</title>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Employer Search</h1>
        <Link href="/upload" className="rounded-md bg-gray-100 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-200">
          CSV Upload
        </Link>
      </div>

      <form onSubmit={handleSubmit} className="flex flex-col sm:flex-row gap-3 mb-8">
        <input
          type="text"
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="Employer name"
          className="flex-1 rounded-md border border-gray-300 px-4 py-2.5 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none"
        />
        <select
          value={state}
          onChange={(e) => setState(e.target.value)}
          className="rounded-md border border-gray-300 px-3 py-2.5 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none"
        >
          <option value="">All states</option>
          {US_STATES.filter(Boolean).map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
        <input
          type="text"
          value={zip}
          onChange={(e) => setZip(e.target.value)}
          placeholder="ZIP"
          maxLength={5}
          className="w-24 rounded-md border border-gray-300 px-3 py-2.5 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 outline-none"
        />
        <button
          type="submit"
          disabled={loading}
          className="rounded-md bg-blue-600 px-6 py-2.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
        >
          {loading ? "Searching..." : "Search"}
        </button>
      </form>

      {error && (
        <div className="rounded-md bg-red-50 border border-red-200 p-4 text-sm text-red-700 mb-6">{error}</div>
      )}

      {searched && results.length === 0 && !loading && (
        <div className="text-center py-12 text-gray-500">No employers found. Try a different search.</div>
      )}

      {results.length > 0 && (
        <>
          <div className="text-sm text-gray-500 mb-4">{totalCount.toLocaleString()} results</div>

          <div className="space-y-3">
            {results.map((r) => (
              <Link
                key={r.employer_id}
                href={`/employers/${r.employer_id}`}
                className="block rounded-lg border border-gray-200 p-4 hover:border-blue-300 hover:shadow-sm transition-all"
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="min-w-0">
                    <div className="font-semibold text-gray-900">{r.employer_name}</div>
                    <div className="text-sm text-gray-500 mt-1">
                      {[r.city, r.state, r.zip].filter(Boolean).join(", ")}
                      {r.parent_name && (
                        <span className="ml-2 text-xs text-blue-600">({r.parent_name})</span>
                      )}
                    </div>
                    <div className="flex flex-wrap gap-x-4 gap-y-1 mt-2 text-xs text-gray-500">
                      <span>{r.osha_inspections_5yr} inspections</span>
                      <span>{r.osha_violations_5yr} violations</span>
                      <span>${(r.osha_total_penalties || 0).toLocaleString()} penalties</span>
                      {r.whd_cases_5yr > 0 && <span>{r.whd_cases_5yr} WHD cases</span>}
                      {r.whd_backwages_total > 0 && <span>${r.whd_backwages_total.toLocaleString()} back wages</span>}
                      {r.location_count > 1 && <span>{r.location_count} locations</span>}
                    </div>
                  </div>
                  <div className="flex flex-col items-end gap-1 shrink-0">
                    <span className={`rounded-full px-2.5 py-0.5 text-xs font-medium ${TIER_COLORS[r.risk_tier] || "bg-gray-100"}`}>
                      {r.risk_tier}
                    </span>
                    <span className="text-lg font-bold text-gray-700">{r.risk_score}</span>
                  </div>
                </div>
              </Link>
            ))}
          </div>

          {/* Pagination */}
          {totalCount > limit && (
            <div className="flex justify-center gap-4 mt-8">
              <button
                disabled={offset === 0}
                onClick={() => doSearch(offset - limit)}
                className="rounded-md border border-gray-300 px-4 py-2 text-sm disabled:opacity-30"
              >
                Previous
              </button>
              <span className="text-sm text-gray-500 py-2">
                {offset + 1}–{Math.min(offset + limit, totalCount)} of {totalCount.toLocaleString()}
              </span>
              <button
                disabled={offset + limit >= totalCount}
                onClick={() => doSearch(offset + limit)}
                className="rounded-md border border-gray-300 px-4 py-2 text-sm disabled:opacity-30"
              >
                Next
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
