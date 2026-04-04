"use client";

import { useEffect, useState } from "react";
import Link from "next/link";

export default function AccountPage() {
  const [keys, setKeys] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [copied, setCopied] = useState(false);

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

  if (loading) return <div className="p-8 text-center text-gray-500">Loading...</div>;
  if (error) {
    return (
      <div className="flex min-h-[60vh] items-center justify-center px-4">
        <div className="text-center">
          <p className="text-gray-600 mb-4">{error}</p>
          <Link href="/login" className="rounded-md bg-blue-600 px-6 py-2.5 text-sm font-medium text-white hover:bg-blue-700">Log in</Link>
        </div>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-3xl px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-2xl font-bold text-gray-900 mb-8">Account</h1>

      {/* API Keys */}
      <div className="rounded-lg border border-gray-200 p-6 mb-6">
        <h2 className="font-semibold text-gray-900 mb-4">API Keys</h2>
        {Array.isArray(keys) && keys.length > 0 ? (
          <div className="space-y-3">
            {keys.map((key, i) => (
              <div key={i} className="flex items-center justify-between bg-gray-50 rounded p-3">
                <div>
                  <code className="text-sm font-mono">{(key.key_prefix as string) || "***"}...</code>
                  <span className={`ml-2 text-xs px-2 py-0.5 rounded-full ${
                    key.status === "active" ? "bg-green-100 text-green-700" : "bg-gray-100 text-gray-600"
                  }`}>
                    {key.status as string}
                  </span>
                </div>
                <div className="text-xs text-gray-500">
                  {key.current_usage as number}/{key.monthly_limit as number} lookups
                </div>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-sm text-gray-500">No API keys yet. They are created when you verify your email.</p>
        )}
      </div>

      {/* Quick links */}
      <div className="grid sm:grid-cols-3 gap-4">
        <Link href="/search" className="rounded-lg border border-gray-200 p-4 text-center hover:border-blue-300">
          <div className="font-semibold text-gray-900">Search</div>
          <div className="text-sm text-gray-500">Look up employers</div>
        </Link>
        <Link href="/upload" className="rounded-lg border border-gray-200 p-4 text-center hover:border-blue-300">
          <div className="font-semibold text-gray-900">CSV Upload</div>
          <div className="text-sm text-gray-500">Bulk lookup</div>
        </Link>
        <Link href="/pricing" className="rounded-lg border border-gray-200 p-4 text-center hover:border-blue-300">
          <div className="font-semibold text-gray-900">Upgrade</div>
          <div className="text-sm text-gray-500">Get more lookups</div>
        </Link>
      </div>
    </div>
  );
}
