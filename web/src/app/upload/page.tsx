"use client";

import { useState, useRef, useCallback, useEffect } from "react";
import Link from "next/link";

export default function UploadPage() {
  const [file, setFile] = useState<File | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<{ matched: number; total: number; url: string } | null>(null);
  const fileRef = useRef<HTMLInputElement>(null);
  const blobUrlRef = useRef<string | null>(null);

  // Revoke blob URL on unmount to prevent memory leaks
  useEffect(() => {
    return () => {
      if (blobUrlRef.current) {
        URL.revokeObjectURL(blobUrlRef.current);
        blobUrlRef.current = null;
      }
    };
  }, []);

  async function handleUpload() {
    if (!file) return;
    setLoading(true);
    setError("");

    // Revoke any previous blob URL before starting a new upload
    if (blobUrlRef.current) {
      URL.revokeObjectURL(blobUrlRef.current);
      blobUrlRef.current = null;
    }
    setResult(null);

    try {
      const formData = new FormData();
      formData.append("file", file);

      const res = await fetch("/api/upload", {
        method: "POST",
        body: formData,
      });

      if (!res.ok) {
        const data = await res.json();
        setError(data.detail?.message || data.error || "Upload failed");
        return;
      }

      // Response is a CSV file
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const matched = parseInt(res.headers.get("X-Matched") || "0");
      const total = parseInt(res.headers.get("X-Total") || "0");

      blobUrlRef.current = url;
      setResult({ matched, total, url });
    } catch {
      setError("Something went wrong. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  function handleDrop(e: React.DragEvent) {
    e.preventDefault();
    const f = e.dataTransfer.files[0];
    if (f && f.name.toLowerCase().endsWith(".csv")) {
      setFile(f);
    } else {
      setError("Please upload a .csv file");
    }
  }

  return (
    <div className="mx-auto max-w-3xl px-4 sm:px-6 lg:px-8 py-8">
      <title>CSV Bulk Upload - FastDOL</title>
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-2xl font-bold text-white">CSV Bulk Upload</h1>
        <Link href="/search" className="text-sm text-violet-500 hover:underline">&larr; Search</Link>
      </div>

      <p className="text-slate-300 mb-6">
        Upload a CSV with employer names and optional location data. Get back a CSV with risk profiles for each employer.
      </p>

      <div className="rounded-lg bg-slate-800/50 border border-slate-700 p-6 mb-6">
        <h3 className="font-semibold text-white mb-2">CSV format</h3>
        <p className="text-sm text-slate-300 mb-3">
          Your CSV needs at least a <code className="bg-slate-700 px-1 rounded text-slate-300">name</code> or <code className="bg-slate-700 px-1 rounded text-slate-300">company_name</code> column.
          Optional columns: <code className="bg-slate-700 px-1 rounded text-slate-300">state</code>, <code className="bg-slate-700 px-1 rounded text-slate-300">zip</code>, <code className="bg-slate-700 px-1 rounded text-slate-300">city</code>, <code className="bg-slate-700 px-1 rounded text-slate-300">ein</code>.
        </p>
        <div className="bg-slate-800 border border-slate-600 rounded p-3 text-xs font-mono text-slate-300 overflow-x-auto">
          name,state,zip<br />
          Walmart,ID,83669<br />
          Amazon Fulfillment,CA,92408<br />
          Target,MN,55403
        </div>
      </div>

      {/* Drop zone */}
      <div
        onDragOver={(e) => e.preventDefault()}
        onDrop={handleDrop}
        onClick={() => fileRef.current?.click()}
        className="border-2 border-dashed border-slate-600 rounded-lg p-12 text-center cursor-pointer hover:border-violet-500 transition-colors"
      >
        <input
          ref={fileRef}
          type="file"
          accept=".csv"
          className="hidden"
          onChange={(e) => {
            const f = e.target.files?.[0];
            if (f) setFile(f);
          }}
        />
        {file ? (
          <div>
            <p className="font-medium text-white">{file.name}</p>
            <p className="text-sm text-slate-400 mt-1">{(file.size / 1024).toFixed(1)} KB</p>
          </div>
        ) : (
          <div>
            <p className="text-slate-300">Drag & drop a CSV file here, or click to browse</p>
            <p className="text-sm text-slate-500 mt-2">Max 500 rows, 5MB</p>
          </div>
        )}
      </div>

      {error && (
        <div className="mt-4 rounded-md bg-red-500/10 border border-red-500/20 p-4 text-sm text-red-400">{error}</div>
      )}

      {file && !result && (
        <button
          onClick={handleUpload}
          disabled={loading}
          className="mt-4 w-full rounded-md bg-violet-500 py-3 text-sm font-medium text-white hover:bg-violet-600 disabled:opacity-50"
        >
          {loading ? "Processing..." : `Upload & process ${file.name}`}
        </button>
      )}

      {result && (
        <div className="mt-6 rounded-lg border border-emerald-500/20 bg-emerald-500/10 p-6 text-center">
          <p className="font-semibold text-emerald-400">
            Matched {result.matched} of {result.total} employers
          </p>
          <a
            href={result.url}
            download="fastdol_results.csv"
            className="mt-4 inline-block rounded-md bg-emerald-600 px-6 py-2.5 text-sm font-medium text-white hover:bg-emerald-700"
          >
            Download Results CSV
          </a>
          <button
            onClick={() => { if (blobUrlRef.current) { URL.revokeObjectURL(blobUrlRef.current); blobUrlRef.current = null; } setFile(null); setResult(null); }}
            className="mt-2 block mx-auto text-sm text-slate-400 hover:underline"
          >
            Upload another file
          </button>
        </div>
      )}
    </div>
  );
}
