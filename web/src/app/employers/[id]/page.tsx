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
  whd_ee_violated_total: number;
  trend_signal: string | null;
  confidence_tier: string | null;
  svep_flag: boolean;
}

interface InspectionRecord {
  activity_nr: string;
  employer_name: string;
  site_address: string;
  site_city: string;
  site_state: string;
  zip5: string;
  open_date: string | null;
  close_case_date: string | null;
  insp_type: string | null;
  violation_count: number;
  serious_count: number;
  willful_count: number;
  repeat_count: number;
  other_count: number;
  total_penalties: number;
  avg_gravity: number | null;
}

interface Violation {
  citation_id: string;
  viol_type: string;
  viol_type_label: string;
  gravity: number;
  nr_instances: number;
  initial_penalty: number;
  current_penalty: number;
  abate_date: string | null;
  issuance_date: string | null;
}

const TIER_COLORS: Record<string, string> = {
  HIGH: "bg-red-500/10 text-red-400 border-red-500/20",
  ELEVATED: "bg-orange-500/10 text-orange-400 border-orange-500/20",
  MEDIUM: "bg-yellow-500/10 text-yellow-400 border-yellow-500/20",
  LOW: "bg-emerald-500/10 text-emerald-400 border-emerald-500/20",
};

const VIOL_COLORS: Record<string, string> = {
  W: "bg-red-500/10 text-red-400",
  R: "bg-orange-500/10 text-orange-400",
  S: "bg-yellow-500/10 text-yellow-400",
  O: "bg-slate-700 text-slate-400",
  U: "bg-slate-700 text-slate-400",
};

const INSP_TYPES: Record<string, string> = {
  A: "Accident", B: "Complaint", C: "Referral", D: "Monitoring",
  E: "Variance", F: "Follow-up", G: "Unprog Related",
  H: "Planned", I: "Unprog Other", J: "Prog Related",
  K: "Prog Other", L: "Other",
};

export default function EmployerDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = use(params);
  const [employer, setEmployer] = useState<Employer | null>(null);
  const [inspections, setInspections] = useState<InspectionRecord[]>([]);
  const [expandedInsp, setExpandedInsp] = useState<string | null>(null);
  const [violations, setViolations] = useState<Record<string, Violation[]>>({});
  const [loadingViol, setLoadingViol] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    async function load() {
      try {
        const [empRes, inspRes] = await Promise.all([
          fetch(`/api/employers/${id}`),
          fetch(`/api/employers/${id}/inspections?limit=100`),
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

  async function toggleInspection(activityNr: string) {
    if (expandedInsp === activityNr) {
      setExpandedInsp(null);
      return;
    }

    setExpandedInsp(activityNr);

    // Load violations if not already cached
    if (!violations[activityNr]) {
      setLoadingViol(activityNr);
      try {
        const res = await fetch(`/api/inspections/${activityNr}/violations`);
        if (res.ok) {
          const data = await res.json();
          setViolations((prev) => ({ ...prev, [activityNr]: data.violations || [] }));
        }
      } catch {
        // Silently fail — just show no violations
      } finally {
        setLoadingViol(null);
      }
    }
  }

  if (loading) return <div className="p-8 text-center text-slate-400">Loading...</div>;
  if (error) return <div className="p-8 text-center text-red-400">{error}</div>;
  if (!employer) return null;

  return (
    <div className="mx-auto max-w-4xl px-4 sm:px-6 lg:px-8 py-8">
      <title>{`${employer.employer_name} - FastDOL`}</title>
      <Link href="/search" className="text-sm text-violet-500 hover:underline mb-4 inline-block">&larr; Back to search</Link>

      {/* Header */}
      <div className="flex items-start justify-between gap-4 mb-8">
        <div>
          <h1 className="text-2xl font-bold text-white">{employer.employer_name}</h1>
          <p className="text-slate-400 mt-1">
            {[employer.address, employer.city, employer.state, employer.zip].filter(Boolean).join(", ")}
          </p>
          {employer.parent_name && (
            <p className="text-sm text-violet-500 mt-1">
              Parent: {employer.parent_name} ({employer.location_count} locations)
            </p>
          )}
          {employer.naics_description && (
            <p className="text-sm text-slate-500 mt-1">{employer.naics_code} — {employer.naics_description}</p>
          )}
        </div>
        <div className="text-right shrink-0">
          <span className={`inline-block rounded-full px-3 py-1 text-sm font-semibold border ${TIER_COLORS[employer.risk_tier] || "bg-slate-700"}`}>
            {employer.risk_tier}
          </span>
          <div className="text-3xl font-bold text-white mt-2">{employer.risk_score}</div>
          <div className="text-xs text-slate-500">risk score</div>
        </div>
      </div>

      {employer.risk_note && (
        <div className="rounded-md bg-violet-500/10 border border-violet-500/20 p-3 text-sm text-violet-400 mb-6">
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
          <div key={stat.label} className="rounded-lg bg-slate-800/50 border border-slate-700 p-4">
            <div className="text-xs text-slate-500">{stat.label}</div>
            <div className="text-lg font-semibold text-white mt-1">{String(stat.value)}</div>
          </div>
        ))}
      </div>

      {/* Inspection history — clickable cards */}
      <div>
        <h2 className="text-lg font-semibold text-white mb-4">
          Inspection History
          {inspections.length > 0 && <span className="text-slate-500 font-normal ml-2">({inspections.length})</span>}
        </h2>

        {inspections.length === 0 && (
          <p className="text-slate-500 text-sm">No inspections found in the last 5 years.</p>
        )}

        <div className="space-y-3">
          {inspections.map((insp) => (
            <div key={insp.activity_nr} className="rounded-lg bg-slate-800/50 border border-slate-700 overflow-hidden">
              {/* Inspection summary — clickable */}
              <button
                onClick={() => toggleInspection(insp.activity_nr)}
                className="w-full text-left p-4 hover:bg-slate-800 transition-colors"
              >
                <div className="flex items-center justify-between gap-4">
                  <div className="flex items-center gap-3 min-w-0">
                    <div className="text-sm font-medium text-white">
                      {insp.open_date || "Unknown date"}
                    </div>
                    <span className="text-xs text-slate-500 bg-slate-700 rounded px-2 py-0.5">
                      {INSP_TYPES[insp.insp_type || ""] || insp.insp_type || "—"}
                    </span>
                  </div>
                  <div className="flex items-center gap-4 shrink-0">
                    {/* Violation type badges */}
                    <div className="flex gap-1.5">
                      {insp.willful_count > 0 && (
                        <span className="text-xs rounded px-1.5 py-0.5 bg-red-500/10 text-red-400">
                          {insp.willful_count}W
                        </span>
                      )}
                      {insp.repeat_count > 0 && (
                        <span className="text-xs rounded px-1.5 py-0.5 bg-orange-500/10 text-orange-400">
                          {insp.repeat_count}R
                        </span>
                      )}
                      {insp.serious_count > 0 && (
                        <span className="text-xs rounded px-1.5 py-0.5 bg-yellow-500/10 text-yellow-400">
                          {insp.serious_count}S
                        </span>
                      )}
                      {insp.other_count > 0 && (
                        <span className="text-xs rounded px-1.5 py-0.5 bg-slate-700 text-slate-400">
                          {insp.other_count}O
                        </span>
                      )}
                      {insp.violation_count === 0 && (
                        <span className="text-xs text-slate-600">No violations</span>
                      )}
                    </div>
                    <div className="text-sm font-medium text-white w-24 text-right">
                      ${(insp.total_penalties || 0).toLocaleString()}
                    </div>
                    <svg
                      className={`w-4 h-4 text-slate-500 transition-transform ${expandedInsp === insp.activity_nr ? "rotate-180" : ""}`}
                      fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor"
                    >
                      <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
                    </svg>
                  </div>
                </div>
              </button>

              {/* Expanded violation detail */}
              {expandedInsp === insp.activity_nr && (
                <div className="border-t border-slate-700 bg-slate-900/50 p-4">
                  <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs mb-4">
                    <div>
                      <span className="text-slate-500">Activity #</span>
                      <div className="text-slate-300 font-mono">{insp.activity_nr}</div>
                    </div>
                    <div>
                      <span className="text-slate-500">Opened</span>
                      <div className="text-slate-300">{insp.open_date || "—"}</div>
                    </div>
                    <div>
                      <span className="text-slate-500">Closed</span>
                      <div className="text-slate-300">{insp.close_case_date || "Open"}</div>
                    </div>
                    <div>
                      <span className="text-slate-500">Avg Gravity</span>
                      <div className="text-slate-300">{insp.avg_gravity || "—"}</div>
                    </div>
                  </div>

                  {loadingViol === insp.activity_nr && (
                    <div className="text-sm text-slate-500">Loading violations...</div>
                  )}

                  {violations[insp.activity_nr] && violations[insp.activity_nr].length > 0 && (
                    <div>
                      <h4 className="text-xs font-semibold text-slate-400 mb-2 uppercase tracking-wide">Violations</h4>
                      <div className="space-y-2">
                        {violations[insp.activity_nr].map((v, i) => (
                          <div key={i} className="rounded bg-slate-800 border border-slate-700 p-3">
                            <div className="flex items-center justify-between gap-3">
                              <div className="flex items-center gap-2">
                                <span className={`text-xs rounded px-2 py-0.5 font-medium ${VIOL_COLORS[v.viol_type] || "bg-slate-700 text-slate-400"}`}>
                                  {v.viol_type_label}
                                </span>
                                <span className="text-xs text-slate-500">
                                  Citation {v.citation_id}
                                </span>
                                {v.gravity > 0 && (
                                  <span className="text-xs text-slate-500">
                                    Gravity: {v.gravity}
                                  </span>
                                )}
                                {v.nr_instances > 1 && (
                                  <span className="text-xs text-slate-500">
                                    {v.nr_instances} instances
                                  </span>
                                )}
                              </div>
                              <div className="text-right shrink-0">
                                <div className="text-sm font-medium text-white">
                                  ${(v.current_penalty || 0).toLocaleString()}
                                </div>
                                {v.initial_penalty !== v.current_penalty && v.initial_penalty > 0 && (
                                  <div className="text-xs text-slate-600 line-through">
                                    ${(v.initial_penalty || 0).toLocaleString()}
                                  </div>
                                )}
                              </div>
                            </div>
                            <div className="flex gap-4 mt-2 text-xs text-slate-500">
                              {v.issuance_date && <span>Issued: {v.issuance_date}</span>}
                              {v.abate_date && <span>Abate by: {v.abate_date}</span>}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {violations[insp.activity_nr] && violations[insp.activity_nr].length === 0 && (
                    <div className="text-sm text-slate-600">No violation details available for this inspection.</div>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
