import { NextRequest, NextResponse } from "next/server";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";

export async function POST(req: NextRequest) {
  let body;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "Invalid request body" }, { status: 400 });
  }

  if (!body.token || !body.password) {
    return NextResponse.json({ error: "Token and password are required" }, { status: 400 });
  }

  try {
    const res = await fetch(`${API_BASE}/auth/reset-password`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token: String(body.token).slice(0, 500),
        password: String(body.password).slice(0, 128),
      }),
    });

    const data = await res.json().catch(() => ({}));

    if (!res.ok) {
      return NextResponse.json(
        { error: data.detail?.message || "Reset failed" },
        { status: res.status }
      );
    }

    return NextResponse.json(data);
  } catch {
    return NextResponse.json({ error: "Service unavailable" }, { status: 503 });
  }
}
