import { NextRequest, NextResponse } from "next/server";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";
const DEMO_API_KEY = process.env.DEMO_API_KEY;

// Simple in-memory rate limiter: 10 searches per IP per hour
// NOTE: This is per-instance only. In serverless (Vercel), each cold start
// gets a fresh map. The real rate limiting is in nginx on the API server.
// This is defense-in-depth, not the sole protection.
const rateLimit = new Map<string, { count: number; resetAt: number }>();
const MAX_PER_HOUR = 10;

function checkRateLimit(ip: string): boolean {
  const now = Date.now();
  const entry = rateLimit.get(ip);

  // Clean old entries inline (no setInterval needed)
  if (rateLimit.size > 10000) {
    for (const [key, val] of rateLimit) {
      if (now > val.resetAt) rateLimit.delete(key);
    }
  }

  if (!entry || now > entry.resetAt) {
    rateLimit.set(ip, { count: 1, resetAt: now + 3600_000 });
    return true;
  }

  if (entry.count >= MAX_PER_HOUR) return false;
  entry.count++;
  return true;
}

export async function GET(req: NextRequest) {
  if (!DEMO_API_KEY) {
    return NextResponse.json(
      { error: "Demo search is not configured. Please sign up for an account." },
      { status: 503 }
    );
  }

  // Rate limit by IP — use Vercel's header first, then x-forwarded-for
  const ip = req.headers.get("x-vercel-forwarded-for")
    || req.headers.get("x-forwarded-for")?.split(",")[0]?.trim()
    || req.headers.get("x-real-ip")
    || "unknown";

  if (!checkRateLimit(ip)) {
    return NextResponse.json(
      { error: "Rate limit exceeded. Sign up for free to get 50 lookups/month." },
      { status: 429 }
    );
  }

  const name = req.nextUrl.searchParams.get("name");
  if (!name || name.length > 200) {
    return NextResponse.json({ error: "Valid name parameter required" }, { status: 400 });
  }

  // Validate zip if provided
  const zip = req.nextUrl.searchParams.get("zip");
  if (zip && !/^\d{1,5}$/.test(zip)) {
    return NextResponse.json({ error: "ZIP must be 1-5 digits" }, { status: 400 });
  }

  const params = new URLSearchParams({ name, limit: "3" });
  if (zip) params.set("zip", zip);

  try {
    const res = await fetch(`${API_BASE}/v1/employers?${params}`, {
      headers: { "X-Api-Key": DEMO_API_KEY },
    });

    if (!res.ok) {
      if (res.status === 404) {
        return NextResponse.json({ results: [], total_count: 0 });
      }
      return NextResponse.json({ error: "Search temporarily unavailable" }, { status: 503 });
    }

    const data = await res.json().catch(() => ({ results: [], total_count: 0 }));
    return NextResponse.json({
      results: data.results || [],
      total_count: data.total_count || 0,
    });
  } catch {
    return NextResponse.json({ error: "Search temporarily unavailable" }, { status: 503 });
  }
}
