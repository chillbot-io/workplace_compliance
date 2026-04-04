import { NextRequest, NextResponse } from "next/server";
import { cookies } from "next/headers";

const API_BASE = process.env.API_URL || "https://api.fastdol.com";

export async function POST(req: NextRequest) {
  const cookieStore = await cookies();
  const jwt = cookieStore.get("access_token")?.value;

  if (!jwt) {
    return NextResponse.json({ error: "Not authenticated" }, { status: 401 });
  }

  // Forward the multipart form data to FastDOL API
  const formData = await req.formData();

  const res = await fetch(`${API_BASE}/v1/employers/upload-csv`, {
    method: "POST",
    headers: {
      Cookie: `access_token=${jwt}`,
    },
    body: formData,
  });

  if (!res.ok) {
    const data = await res.json().catch(() => ({ error: "Upload failed" }));
    return NextResponse.json(data, { status: res.status });
  }

  // Forward the CSV response back to the client
  const blob = await res.blob();
  const matched = res.headers.get("X-Matched") || "0";
  const total = res.headers.get("X-Total") || "0";

  return new NextResponse(blob, {
    status: 200,
    headers: {
      "Content-Type": "text/csv",
      "Content-Disposition": "attachment; filename=fastdol_results.csv",
      "X-Matched": matched,
      "X-Total": total,
    },
  });
}
