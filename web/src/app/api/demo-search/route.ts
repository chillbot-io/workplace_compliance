import { NextRequest, NextResponse } from "next/server";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";
const DEMO_API_KEY = process.env.DEMO_API_KEY || "";

// Simple in-memory rate limiter: 10 searches per IP per hour
const rateLimit = new Map<string, { count: number; resetAt: number }>();
const MAX_PER_HOUR = 10;

function checkRateLimit(ip: string): boolean {
  const now = Date.now();
  const entry = rateLimit.get(ip);

  if (!entry || now > entry.resetAt) {
    rateLimit.set(ip, { count: 1, resetAt: now + 3600_000 });
    return true;
  }

  if (entry.count >= MAX_PER_HOUR) {
    return false;
  }

  entry.count++;
  return true;
}

// Clean up old entries every 10 minutes
setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of rateLimit) {
    if (now > entry.resetAt) rateLimit.delete(ip);
  }
}, 600_000);

export async function GET(req: NextRequest) {
  // Rate limit by IP
  const ip = req.headers.get("x-forwarded-for")?.split(",")[0]?.trim()
    || req.headers.get("x-real-ip")
    || "unknown";

  if (!checkRateLimit(ip)) {
    return NextResponse.json(
      { error: "Rate limit exceeded. Sign up for free to get 50 lookups/month." },
      { status: 429 }
    );
  }

  const name = req.nextUrl.searchParams.get("name");
  if (!name) {
    return NextResponse.json({ error: "Name parameter required" }, { status: 400 });
  }

  // Build query to FastDOL API — limited to 3 results for demo
  const params = new URLSearchParams({
    name,
    limit: "3",
  });

  const zip = req.nextUrl.searchParams.get("zip");
  if (zip) params.set("zip", zip);

  try {
    const res = await fetch(`${API_BASE}/v1/employers?${params}`, {
      headers: {
        "X-Api-Key": DEMO_API_KEY,
      },
    });

    if (!res.ok) {
      if (res.status === 404) {
        return NextResponse.json({ results: [], total_count: 0 });
      }
      return NextResponse.json(
        { error: "Search temporarily unavailable" },
        { status: 503 }
      );
    }

    const data = await res.json();

    return NextResponse.json({
      results: data.results || [],
      total_count: data.total_count || 0,
    });
  } catch {
    return NextResponse.json(
      { error: "Search temporarily unavailable" },
      { status: 503 }
    );
  }
}
