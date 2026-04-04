import { NextRequest, NextResponse } from "next/server";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";

export async function POST(req: NextRequest) {
  let body;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "Invalid request body" }, { status: 400 });
  }

  // Validate expected fields
  if (!body.email || !body.password || !body.company_name) {
    return NextResponse.json({ error: "Email, password, and company_name are required" }, { status: 400 });
  }

  try {
    const res = await fetch(`${API_BASE}/auth/signup`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        email: String(body.email).slice(0, 254),
        password: String(body.password).slice(0, 128),
        company_name: String(body.company_name).slice(0, 200),
      }),
    });

    const data = await res.json().catch(() => ({}));

    if (!res.ok) {
      return NextResponse.json(
        { error: data.detail?.message || "Signup failed" },
        { status: res.status }
      );
    }

    return NextResponse.json(data);
  } catch {
    return NextResponse.json({ error: "Service unavailable" }, { status: 503 });
  }
}
