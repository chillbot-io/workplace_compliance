import { NextRequest, NextResponse } from "next/server";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";

export async function GET(req: NextRequest) {
  const token = req.nextUrl.searchParams.get("token");
  if (!token || token.length > 500) {
    return NextResponse.json({ error: "Invalid token" }, { status: 400 });
  }

  try {
    const res = await fetch(`${API_BASE}/auth/verify?token=${encodeURIComponent(token)}`);
    const data = await res.json().catch(() => ({}));

    const response = NextResponse.json(
      !res.ok ? { error: data.detail?.message || "Verification failed" } : data,
      { status: res.status }
    );

    // Forward Set-Cookie with validation
    const setCookie = res.headers.get("set-cookie");
    if (setCookie && setCookie.includes("access_token=")) {
      response.headers.set("set-cookie", setCookie);
    }

    return response;
  } catch {
    return NextResponse.json({ error: "Service unavailable" }, { status: 503 });
  }
}
