"use client";

import { useEffect, useState } from "react";
import { use } from "react";
import Link from "next/link";

interface Employer {
  employer_id: string;
  employer_name: string;
  address: string;
  city: string;
  state: string;
  zip: string;
  parent_name: string | null;
  location_count: number;
  naics_code: string | null;
  naics_description: string | null;
  risk_tier: string;
  risk_score: number;
  risk_note: string | null;
  osha_inspections_5yr: number;
  osha_violations_5yr: number;
  osha_total_penalties: number;
  whd_cases_5yr: number;
  whd_backwages_total: number;
  trend_signal: string | null;
  confidence_tier: string | null;
  svep_flag: boolean;
}

interface ViolationDetail {
  type?: string;
  count?: number;
  penalty?: number;
  description?: string;
  [key: string]: unknown;
}

interface Inspection {
  inspection_date: string | null;
  insp_type_label: string | null;
  violations: ViolationDetail[] | Record<string, number> | string | null;
  penalties: number | null;
}

const TIER_COLORS: Record<string, string> = {
  HIGH: "bg-red-100 text-red-800 border-red-200",
  ELEVATED: "bg-orange-100 text-orange-800 border-orange-200",
  MEDIUM: "bg-yellow-100 text-yellow-800 border-yellow-200",
  LOW: "bg-green-100 text-green-800 border-green-200",
};

function renderViolations(rawViolations: Inspection["violations"]): React.ReactNode {
  if (!rawViolations) return "—";

  let violations: ViolationDetail[] | Record<string, number> | null = null;

  // If it's a string, try parsing it as JSON first
  if (typeof rawViolations === "string") {
    try {
      violations = JSON.parse(rawViolations);
    } catch {
      return <span>{rawViolations}</span>;
    }
  } else {
    violations = rawViolations;
  }

  if (!violations) return "—";

  // Array of violation objects: summarize by type with counts
  if (Array.isArray(violations)) {
    if (violations.length === 0) return "None";
    const countsByType: Record<string, number> = {};
    for (const v of violations) {
      const label = v.type || "Other";
      countsByType[label] = (countsByType[label] || 0) + (v.count ?? 1);
    }
    return (
      <span className="flex flex-wrap gap-1.5">
        {Object.entries(countsByType).map(([type, count]) => (
          <span
            key={type}
            className={`inline-block rounded px-1.5 py-0.5 text-xs font-medium ${
              type.toLowerCase() === "willful"
                ? "bg-red-100 text-red-700"
                : type.toLowerCase() === "repeat"
                  ? "bg-orange-100 text-orange-700"
                  : type.toLowerCase() === "serious"
                    ? "bg-yellow-100 text-yellow-700"
                    : "bg-gray-100 text-gray-600"
            }`}
          >
            {count} {type}
          </span>
        ))}
      </span>
    );
  }

  // Plain object with type keys mapping to counts, e.g. { "Serious": 3, "Willful": 1 }
  if (typeof violations === "object" && violations !== null) {
    const entries = Object.entries(violations);
    if (entries.length === 0) return "None";
    return (
      <span className="flex flex-wrap gap-1.5">
        {entries.map(([type, count]) => (
          <span
            key={type}
            className={`inline-block rounded px-1.5 py-0.5 text-xs font-medium ${
              type.toLowerCase() === "willful"
                ? "bg-red-100 text-red-700"
                : type.toLowerCase() === "repeat"
                  ? "bg-orange-100 text-orange-700"
                  : type.toLowerCase() === "serious"
                    ? "bg-yellow-100 text-yellow-700"
                    : "bg-gray-100 text-gray-600"
            }`}
          >
            {count} {type}
          </span>
        ))}
      </span>
    );
  }

  return "—";
}

export default function EmployerDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const [employer, setEmployer] = useState<Employer | null>(null);
  const [inspections, setInspections] = useState<Inspection[]>([]);
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

  return (
    <div className="mx-auto max-w-4xl px-4 sm:px-6 lg:px-8 py-8">
      <title>{`${employer.employer_name} - FastDOL`}</title>
      <Link href="/search" className="text-sm text-blue-600 hover:underline mb-4 inline-block">&larr; Back to search</Link>

      {/* Header */}
      <div className="flex items-start justify-between gap-4 mb-8">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">{employer.employer_name}</h1>
          <p className="text-gray-500 mt-1">
            {[employer.address, employer.city, employer.state, employer.zip].filter(Boolean).join(", ")}
          </p>
          {employer.parent_name && (
            <Link href={`/search?parent=${encodeURIComponent(employer.parent_name)}`}
              className="text-sm text-blue-600 hover:underline mt-1 inline-block">
              Parent: {employer.parent_name} ({employer.location_count} locations)
            </Link>
          )}
          {employer.naics_description && (
            <p className="text-sm text-gray-400 mt-1">{employer.naics_code} — {employer.naics_description}</p>
          )}
        </div>
        <div className="text-right shrink-0">
          <span className={`inline-block rounded-full px-3 py-1 text-sm font-semibold ${TIER_COLORS[employer.risk_tier] || "bg-gray-100"}`}>
            {employer.risk_tier}
          </span>
          <div className="text-3xl font-bold text-gray-900 mt-2">{employer.risk_score}</div>
          <div className="text-xs text-gray-500">risk score</div>
        </div>
      </div>

      {employer.risk_note && (
        <div className="rounded-md bg-blue-50 border border-blue-200 p-3 text-sm text-blue-700 mb-6">
          {employer.risk_note}
        </div>
      )}

      {/* Stats grid */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-8">
        {[
          { label: "OSHA Inspections (5yr)", value: employer.osha_inspections_5yr },
          { label: "OSHA Violations (5yr)", value: employer.osha_violations_5yr },
          { label: "Total Penalties", value: `$${(employer.osha_total_penalties || 0).toLocaleString()}` },
          { label: "Trend", value: employer.trend_signal || "STABLE" },
          { label: "WHD Cases (5yr)", value: employer.whd_cases_5yr || 0 },
          { label: "Back Wages Owed", value: `$${(employer.whd_backwages_total || 0).toLocaleString()}` },
          { label: "Confidence", value: employer.confidence_tier || "—" },
          { label: "SVEP Flag", value: employer.svep_flag ? "Yes" : "No" },
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
                    <td className="py-2 pr-4">{insp.inspection_date || "—"}</td>
                    <td className="py-2 pr-4">{insp.insp_type_label || "—"}</td>
                    <td className="py-2 pr-4">
                      {renderViolations(insp.violations)}
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
