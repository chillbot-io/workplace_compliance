import { NextRequest, NextResponse } from "next/server";
import { cookies } from "next/headers";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";
const ALLOWED_PARAMS = ["name", "ein", "state", "zip", "naics", "limit", "offset"];

export async function GET(req: NextRequest) {
  const cookieStore = await cookies();
  const jwt = cookieStore.get("access_token")?.value;

  if (!jwt) {
    return NextResponse.json({ error: "Not authenticated. Please log in." }, { status: 401 });
  }

  // Whitelist query parameters
  const params = new URLSearchParams();
  for (const key of ALLOWED_PARAMS) {
    const val = req.nextUrl.searchParams.get(key);
    if (val) params.set(key, val);
  }

  try {
    const res = await fetch(`${API_BASE}/v1/employers?${params}`, {
      headers: { Cookie: `access_token=${jwt}` },
    });

    const data = await res.json().catch(() => ({}));

    if (!res.ok) {
      return NextResponse.json(
        { error: data.detail?.message || "Search failed" },
        { status: res.status }
      );
    }

    return NextResponse.json(data);
  } catch {
    return NextResponse.json({ error: "Service unavailable" }, { status: 503 });
  }
}
