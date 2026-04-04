import { NextRequest, NextResponse } from "next/server";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";

export async function POST(req: NextRequest) {
  let body;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "Invalid request body" }, { status: 400 });
  }

  if (!body.email || !body.password) {
    return NextResponse.json({ error: "Email and password are required" }, { status: 400 });
  }

  try {
    const res = await fetch(`${API_BASE}/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        email: String(body.email).slice(0, 254),
        password: String(body.password).slice(0, 128),
      }),
    });

    const data = await res.json().catch(() => ({}));

    const response = NextResponse.json(
      !res.ok ? { error: data.detail?.message || "Login failed" } : data,
      { status: res.status }
    );

    // Forward Set-Cookie with validation — only accept access_token
    const setCookie = res.headers.get("set-cookie");
    if (setCookie && setCookie.includes("access_token=")) {
      response.headers.set("set-cookie", setCookie);
    }

    return response;
  } catch {
    return NextResponse.json({ error: "Service unavailable" }, { status: 503 });
  }
}
