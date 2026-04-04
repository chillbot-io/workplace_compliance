import { NextRequest, NextResponse } from "next/server";
import { cookies } from "next/headers";
import { isValidUUID } from "@/lib/validate";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";

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
  if (!jwt) {
    return NextResponse.json({ error: "Not authenticated" }, { status: 401 });
  }

  try {
    const res = await fetch(`${API_BASE}/v1/employers/${id}`, {
      headers: { Cookie: `access_token=${jwt}` },
    });
    if (!res.ok) {
      return NextResponse.json({ error: "Employer not found" }, { status: res.status });
    }
    const data = await res.json();
    return NextResponse.json(data);
  } catch {
    return NextResponse.json({ error: "Service unavailable" }, { status: 503 });
  }
}
