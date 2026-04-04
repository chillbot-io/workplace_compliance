import { NextRequest, NextResponse } from "next/server";
import { cookies } from "next/headers";
import { isValidUUID } from "@/lib/validate";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";
const DEMO_API_KEY = process.env.DEMO_API_KEY || "";

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;

  if (!isValidUUID(id)) {
    return NextResponse.json({ error: "Invalid employer ID" }, { status: 400 });
  }

  const cookieStore = await cookies();
  const jwt = cookieStore.get("access_token")?.value;

  const headers: Record<string, string> = {};
  if (jwt) {
    headers["Cookie"] = `access_token=${jwt}`;
  } else if (DEMO_API_KEY) {
    headers["X-Api-Key"] = DEMO_API_KEY;
  } else {
    return NextResponse.json({ error: "Not available" }, { status: 401 });
  }

  try {
    const res = await fetch(`${API_BASE}/v1/employers/${id}/inspections?limit=100`, { headers });
    if (!res.ok) {
      return NextResponse.json({ error: "Failed to load inspections" }, { status: res.status });
    }
    const data = await res.json();
    return NextResponse.json(data);
  } catch {
    return NextResponse.json({ error: "Service unavailable" }, { status: 503 });
  }
}
