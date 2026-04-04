import { NextRequest, NextResponse } from "next/server";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";

export async function POST(req: NextRequest) {
  let body;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "Invalid request body" }, { status: 400 });
  }

  if (!body.email) {
    return NextResponse.json({ error: "Email is required" }, { status: 400 });
  }

  try {
    await fetch(`${API_BASE}/auth/forgot-password`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ email: String(body.email).slice(0, 254) }),
    });
  } catch {
    // Ignore errors — always return 202 to prevent email enumeration
  }

  return NextResponse.json({ status: "ok" }, { status: 202 });
}
