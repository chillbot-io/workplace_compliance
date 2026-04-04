import { NextResponse } from "next/server";
import { cookies } from "next/headers";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";

export async function GET() {
  const cookieStore = await cookies();
  const jwt = cookieStore.get("access_token")?.value;

  if (!jwt) {
    return NextResponse.json({ error: "Not authenticated" }, { status: 401 });
  }

  try {
    const res = await fetch(`${API_BASE}/dashboard/keys`, {
      headers: { Cookie: `access_token=${jwt}` },
    });

    if (!res.ok) {
      return NextResponse.json({ error: "Failed to load keys" }, { status: res.status });
    }

    const data = await res.json().catch(() => ({}));
    return NextResponse.json(data);
  } catch {
    return NextResponse.json({ error: "Service unavailable" }, { status: 503 });
  }
}
