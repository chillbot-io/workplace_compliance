"use client";

import Link from "next/link";
import { useState } from "react";
import { useSearchParams } from "next/navigation";
import { Suspense } from "react";

function ResetContent() {
  const searchParams = useSearchParams();
  const token = searchParams.get("token");
  const [password, setPassword] = useState("");
  const [confirm, setConfirm] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (password !== confirm) {
      setError("Passwords don't match.");
      return;
    }
    setLoading(true);
    setError("");

    try {
      const res = await fetch("/api/auth/reset-password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token, password }),
      });
      const data = await res.json();

      if (!res.ok) {
        setError(data.detail?.message || "Reset failed. Link may have expired.");
        return;
      }
      setSuccess(true);
    } catch {
      setError("Something went wrong.");
    } finally {
      setLoading(false);
    }
  }

  if (!token) {
    return (
      <div className="text-center">
        <h1 className="text-2xl font-bold text-white mb-4">Invalid link</h1>
        <p className="text-slate-400">
          <Link href="/forgot-password" className="text-rose-500 hover:underline">Request a new reset link</Link>
        </p>
      </div>
    );
  }

  if (success) {
    return (
      <div className="text-center">
        <h1 className="text-2xl font-bold text-white mb-4">Password reset!</h1>
        <p className="text-slate-300 mb-6">Your password has been updated.</p>
        <Link href="/login" className="rounded-md bg-rose-500 px-6 py-2.5 text-sm font-medium text-white hover:bg-rose-600">
          Log in
        </Link>
      </div>
    );
  }

  return (
    <div>
      <h1 className="text-2xl font-bold text-white text-center mb-8">Set new password</h1>
      <form onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label htmlFor="password" className="block text-sm font-medium text-slate-300 mb-1">New password</label>
          <input id="password" type="password" required minLength={8} value={password} onChange={(e) => setPassword(e.target.value)}
            className="w-full rounded-md border border-slate-600 bg-slate-800 px-4 py-2.5 text-sm text-white placeholder-slate-500 focus:border-rose-500 focus:ring-1 focus:ring-rose-500 outline-none" />
        </div>
        <div>
          <label htmlFor="confirm" className="block text-sm font-medium text-slate-300 mb-1">Confirm password</label>
          <input id="confirm" type="password" required minLength={8} value={confirm} onChange={(e) => setConfirm(e.target.value)}
            className="w-full rounded-md border border-slate-600 bg-slate-800 px-4 py-2.5 text-sm text-white placeholder-slate-500 focus:border-rose-500 focus:ring-1 focus:ring-rose-500 outline-none" />
        </div>
        {error && <div className="rounded-md bg-red-500/10 border border-red-500/20 p-3 text-sm text-red-400">{error}</div>}
        <button type="submit" disabled={loading}
          className="w-full rounded-md bg-rose-500 py-2.5 text-sm font-medium text-white hover:bg-rose-600 disabled:opacity-50">
          {loading ? "Resetting..." : "Reset password"}
        </button>
      </form>
    </div>
  );
}

export default function ResetPasswordPage() {
  return (
    <div className="flex min-h-[60vh] items-center justify-center px-4">
      <div className="w-full max-w-md">
        <Suspense fallback={<div className="text-center text-slate-400">Loading...</div>}>
          <ResetContent />
        </Suspense>
      </div>
    </div>
  );
}
