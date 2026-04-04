const API_BASE = process.env.NEXT_PUBLIC_API_URL || "https://api.fastdol.com";

export class ApiError extends Error {
  status: number;
  detail: unknown;
  constructor(status: number, detail: unknown) {
    super(typeof detail === "string" ? detail : JSON.stringify(detail));
    this.status = status;
    this.detail = detail;
  }
}

async function apiFetch(path: string, options: RequestInit = {}) {
  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    credentials: "include", // send JWT cookie
    headers: {
      "Content-Type": "application/json",
      ...options.headers,
    },
  });

  if (!res.ok) {
    const detail = await res.json().catch(() => res.statusText);
    throw new ApiError(res.status, detail);
  }

  return res;
}

// Server-side API calls (Next.js API routes → FastDOL API)
// These forward the API key header
export async function serverFetch(path: string, apiKey: string, options: RequestInit = {}) {
  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      "X-Api-Key": apiKey,
      "Content-Type": "application/json",
      ...options.headers,
    },
  });

  if (!res.ok) {
    const detail = await res.json().catch(() => res.statusText);
    throw new ApiError(res.status, detail);
  }

  return res;
}

// Auth endpoints (no API key needed, use cookies)
export async function signup(email: string, password: string, companyName: string) {
  const res = await apiFetch("/auth/signup", {
    method: "POST",
    body: JSON.stringify({ email, password, company_name: companyName }),
  });
  return res.json();
}

export async function login(email: string, password: string) {
  const res = await apiFetch("/auth/login", {
    method: "POST",
    body: JSON.stringify({ email, password }),
  });
  return res.json();
}

export async function verifyEmail(token: string) {
  const res = await apiFetch(`/auth/verify?token=${encodeURIComponent(token)}`);
  return res.json();
}

export async function forgotPassword(email: string) {
  const res = await apiFetch("/auth/forgot-password", {
    method: "POST",
    body: JSON.stringify({ email }),
  });
  return res.json();
}

export async function resetPassword(token: string, password: string) {
  const res = await apiFetch("/auth/reset-password", {
    method: "POST",
    body: JSON.stringify({ token, password }),
  });
  return res.json();
}

// Employer search (used by demo and authenticated search)
export async function searchEmployers(params: {
  name?: string;
  zip?: string;
  state?: string;
  naics?: string;
  limit?: number;
  offset?: number;
}, apiKey?: string) {
  const query = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== "") query.set(k, String(v));
  }
  const headers: Record<string, string> = {};
  if (apiKey) headers["X-Api-Key"] = apiKey;

  const res = await fetch(`${API_BASE}/v1/employers?${query}`, {
    credentials: "include",
    headers,
  });

  if (!res.ok) {
    const detail = await res.json().catch(() => res.statusText);
    throw new ApiError(res.status, detail);
  }

  return res.json();
}
