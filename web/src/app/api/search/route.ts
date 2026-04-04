import { NextRequest, NextResponse } from "next/server";
import { cookies } from "next/headers";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";

export async function GET(req: NextRequest) {
  // Forward the JWT cookie as auth
  const cookieStore = await cookies();
  const jwt = cookieStore.get("access_token")?.value;

  if (!jwt) {
    return NextResponse.json({ error: "Not authenticated. Please log in." }, { status: 401 });
  }

  // Forward search params to FastDOL API
  const params = req.nextUrl.searchParams.toString();

  const res = await fetch(`${API_BASE}/v1/employers?${params}`, {
    headers: {
      Cookie: `access_token=${jwt}`,
    },
  });

  const data = await res.json();
  return NextResponse.json(data, { status: res.status });
}
