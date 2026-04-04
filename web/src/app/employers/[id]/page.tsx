"use client";

import { useEffect, useState } from "react";
import { use } from "react";
import Link from "next/link";

const TIER_COLORS: Record<string, string> = {
  HIGH: "bg-red-100 text-red-800 border-red-200",
  ELEVATED: "bg-orange-100 text-orange-800 border-orange-200",
  MEDIUM: "bg-yellow-100 text-yellow-800 border-yellow-200",
  LOW: "bg-green-100 text-green-800 border-green-200",
};

export default function EmployerDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const [employer, setEmployer] = useState<Record<string, unknown> | null>(null);
  const [inspections, setInspections] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    async function load() {
      try {
        const [empRes, inspRes] = await Promise.all([
          fetch(`/api/employers/${id}`),
          fetch(`/api/employers/${id}/inspections`),
        ]);

        if (!empRes.ok) {
          setError("Employer not found");
          return;
        }

        const empData = await empRes.json();
        setEmployer(empData.match || empData);

        if (inspRes.ok) {
          const inspData = await inspRes.json();
          setInspections(inspData.data || []);
        }
      } catch {
        setError("Failed to load employer data");
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [id]);

  if (loading) return <div className="p-8 text-center text-gray-500">Loading...</div>;
  if (error) return <div className="p-8 text-center text-red-600">{error}</div>;
  if (!employer) return null;

  const e = employer as Record<string, string | number | boolean | null>;

  return (
    <div className="mx-auto max-w-4xl px-4 sm:px-6 lg:px-8 py-8">
      <Link href="/search" className="text-sm text-blue-600 hover:underline mb-4 inline-block">&larr; Back to search</Link>

      {/* Header */}
      <div className="flex items-start justify-between gap-4 mb-8">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">{e.employer_name as string}</h1>
          <p className="text-gray-500 mt-1">
            {[e.address, e.city, e.state, e.zip].filter(Boolean).join(", ")}
          </p>
          {e.parent_name && (
            <Link href={`/search?parent=${encodeURIComponent(e.parent_name as string)}`}
              className="text-sm text-blue-600 hover:underline mt-1 inline-block">
              Parent: {e.parent_name as string} ({e.location_count as number} locations)
            </Link>
          )}
          {e.naics_description && (
            <p className="text-sm text-gray-400 mt-1">{e.naics_code as string} — {e.naics_description as string}</p>
          )}
        </div>
        <div className="text-right shrink-0">
          <span className={`inline-block rounded-full px-3 py-1 text-sm font-semibold ${TIER_COLORS[e.risk_tier as string] || "bg-gray-100"}`}>
            {e.risk_tier as string}
          </span>
          <div className="text-3xl font-bold text-gray-900 mt-2">{e.risk_score as number}</div>
          <div className="text-xs text-gray-500">risk score</div>
        </div>
      </div>

      {e.risk_note && (
        <div className="rounded-md bg-blue-50 border border-blue-200 p-3 text-sm text-blue-700 mb-6">
          {e.risk_note as string}
        </div>
      )}

      {/* Stats grid */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-8">
        {[
          { label: "OSHA Inspections (5yr)", value: e.osha_inspections_5yr },
          { label: "OSHA Violations (5yr)", value: e.osha_violations_5yr },
          { label: "Total Penalties", value: `$${((e.osha_total_penalties as number) || 0).toLocaleString()}` },
          { label: "Trend", value: e.trend_signal || "STABLE" },
          { label: "WHD Cases (5yr)", value: e.whd_cases_5yr || 0 },
          { label: "Back Wages Owed", value: `$${((e.whd_backwages_total as number) || 0).toLocaleString()}` },
          { label: "Confidence", value: e.confidence_tier || "—" },
          { label: "SVEP Flag", value: e.svep_flag ? "Yes" : "No" },
        ].map((stat) => (
          <div key={stat.label} className="rounded-lg border border-gray-200 p-4">
            <div className="text-xs text-gray-500">{stat.label}</div>
            <div className="text-lg font-semibold text-gray-900 mt-1">{String(stat.value)}</div>
          </div>
        ))}
      </div>

      {/* Inspection history */}
      {inspections.length > 0 && (
        <div>
          <h2 className="text-lg font-semibold text-gray-900 mb-4">Inspection History</h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200 text-left text-xs text-gray-500">
                  <th className="pb-2 pr-4">Date</th>
                  <th className="pb-2 pr-4">Type</th>
                  <th className="pb-2 pr-4">Violations</th>
                  <th className="pb-2">Penalties</th>
                </tr>
              </thead>
              <tbody>
                {inspections.map((insp, i) => (
                  <tr key={i} className="border-b border-gray-100">
                    <td className="py-2 pr-4">{(insp.inspection_date as string) || "—"}</td>
                    <td className="py-2 pr-4">{(insp.insp_type_label as string) || "—"}</td>
                    <td className="py-2 pr-4">
                      {insp.violations ? JSON.stringify(insp.violations) : "—"}
                    </td>
                    <td className="py-2">—</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
