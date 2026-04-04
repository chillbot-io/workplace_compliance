"use client";

import { useEffect, useState } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { Suspense } from "react";

function VerifyContent() {
  const searchParams = useSearchParams();
  const token = searchParams.get("token");
  const [status, setStatus] = useState<"loading" | "success" | "error">("loading");
  const [apiKey, setApiKey] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    if (!token) {
      setStatus("error");
      setError("Missing verification token.");
      return;
    }

    fetch(`/api/auth/verify?token=${encodeURIComponent(token)}`)
      .then(async (res) => {
        const data = await res.json();
        if (res.ok) {
          setStatus("success");
          setApiKey(data.api_key || "");
        } else {
          setStatus("error");
          setError(data.detail?.message || "Verification failed.");
        }
      })
      .catch(() => {
        setStatus("error");
        setError("Something went wrong.");
      });
  }, [token]);

  if (status === "loading") {
    return (
      <div className="text-center">
        <h1 className="text-2xl font-bold text-gray-900 mb-4">Verifying your email...</h1>
      </div>
    );
  }

  if (status === "error") {
    return (
      <div className="text-center">
        <h1 className="text-2xl font-bold text-gray-900 mb-4">Verification failed</h1>
        <p className="text-gray-600 mb-6">{error}</p>
        <Link href="/signup" className="text-blue-600 hover:underline">Try signing up again</Link>
      </div>
    );
  }

  return (
    <div className="text-center">
      <h1 className="text-2xl font-bold text-gray-900 mb-4">Email verified!</h1>
      <p className="text-gray-600 mb-6">Your account is active. Here is your API key:</p>

      {apiKey && (
        <div className="mx-auto max-w-lg bg-gray-50 border border-gray-200 rounded-lg p-4 mb-6">
          <p className="text-xs text-gray-500 mb-2">Your API key (shown once — copy it now)</p>
          <code className="block text-sm font-mono bg-white border border-gray-300 rounded px-3 py-2 break-all select-all">
            {apiKey}
          </code>
        </div>
      )}

      <Link
        href="/search"
        className="rounded-md bg-blue-600 px-6 py-2.5 text-sm font-medium text-white hover:bg-blue-700"
      >
        Start searching
      </Link>
    </div>
  );
}

export default function VerifyPage() {
  return (
    <div className="flex min-h-[60vh] items-center justify-center px-4">
      <div className="w-full max-w-md">
        <Suspense fallback={<div className="text-center text-gray-600">Loading...</div>}>
          <VerifyContent />
        </Suspense>
      </div>
    </div>
  );
}
