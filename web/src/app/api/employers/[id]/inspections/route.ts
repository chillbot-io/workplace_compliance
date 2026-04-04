import { NextRequest, NextResponse } from "next/server";
import { cookies } from "next/headers";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";

export async function GET(
  _req: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const cookieStore = await cookies();
  const jwt = cookieStore.get("access_token")?.value;

  if (!jwt) {
    return NextResponse.json({ error: "Not authenticated" }, { status: 401 });
  }

  const res = await fetch(`${API_BASE}/v1/employers/${id}/inspections`, {
    headers: { Cookie: `access_token=${jwt}` },
  });

  const data = await res.json();
  return NextResponse.json(data, { status: res.status });
}
