import { NextRequest, NextResponse } from "next/server";
import { cookies } from "next/headers";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ activityNr: string }> }
) {
  const { activityNr } = await params;

  // Validate activity_nr is numeric
  if (!/^\d+$/.test(activityNr)) {
    return NextResponse.json({ error: "Invalid activity number" }, { status: 400 });
  }

  const cookieStore = await cookies();
  const jwt = cookieStore.get("access_token")?.value;
  if (!jwt) {
    return NextResponse.json({ error: "Not authenticated" }, { status: 401 });
  }

  try {
    const res = await fetch(`${API_BASE}/v1/inspections/${activityNr}/violations`, {
      headers: { Cookie: `access_token=${jwt}` },
    });
    if (!res.ok) {
      return NextResponse.json({ error: "Failed to load violations" }, { status: res.status });
    }
    const data = await res.json();
    return NextResponse.json(data);
  } catch {
    return NextResponse.json({ error: "Service unavailable" }, { status: 503 });
  }
}
