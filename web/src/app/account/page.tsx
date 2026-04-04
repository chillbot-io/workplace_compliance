"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

interface ApiKey {
  key_prefix: string;
  status: string;
  current_usage: number;
  monthly_limit: number;
  created_at?: string;
  plan?: string;
}

export default function AccountPage() {
  const [keys, setKeys] = useState<ApiKey[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    fetch("/api/account/keys")
      .then(async (res) => {
        if (!res.ok) {
          setError("Please log in to view your account.");
          return;
        }
        const data = await res.json();
        setKeys(data.keys || data || []);
      })
      .catch(() => setError("Failed to load account data."))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <div className="p-8 text-center text-slate-400">Loading...</div>;
  if (error) {
    return (
      <div className="flex min-h-[60vh] items-center justify-center px-4">
        <div className="text-center">
          <p className="text-slate-400 mb-4">{error}</p>
          <Link href="/login" className="rounded-md bg-rose-500 px-6 py-2.5 text-sm font-medium text-white hover:bg-rose-600">Log in</Link>
        </div>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-3xl px-4 sm:px-6 lg:px-8 py-8">
      <title>Account - FastDOL</title>
      <h1 className="text-2xl font-bold text-white mb-8">Account</h1>

      {/* API Keys */}
      <div className="rounded-lg bg-slate-800/50 border border-slate-700 p-6 mb-6">
        <h2 className="font-semibold text-white mb-4">API Keys</h2>
        {Array.isArray(keys) && keys.length > 0 ? (
          <div className="space-y-3">
            {keys.map((key, i) => (
              <div key={i} className="flex items-center justify-between bg-slate-800 border border-slate-600 rounded p-3">
                <div>
                  <code className="text-sm font-mono text-white">{key.key_prefix || "***"}...</code>
                  <span className={`ml-2 text-xs px-2 py-0.5 rounded-full ${
                    key.status === "active" ? "bg-emerald-500/10 text-emerald-400" : "bg-slate-700 text-slate-400"
                  }`}>
                    {key.status}
                  </span>
                </div>
                <div className="text-xs text-slate-400">
                  {key.current_usage}/{key.monthly_limit} lookups
                </div>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-sm text-slate-400">No API keys yet. They are created when you verify your email.</p>
        )}
      </div>

      {/* Quick links */}
      <div className="grid sm:grid-cols-3 gap-4">
        <Link href="/search" className="rounded-lg bg-slate-800/50 border border-slate-700 p-4 text-center hover:border-rose-500/50">
          <div className="font-semibold text-white">Search</div>
          <div className="text-sm text-slate-400">Look up employers</div>
        </Link>
        <Link href="/upload" className="rounded-lg bg-slate-800/50 border border-slate-700 p-4 text-center hover:border-rose-500/50">
          <div className="font-semibold text-white">CSV Upload</div>
          <div className="text-sm text-slate-400">Bulk lookup</div>
        </Link>
        <Link href="/pricing" className="rounded-lg bg-slate-800/50 border border-slate-700 p-4 text-center hover:border-rose-500/50">
          <div className="font-semibold text-white">Upgrade</div>
          <div className="text-sm text-slate-400">Get more lookups</div>
        </Link>
      </div>
    </div>
  );
}
