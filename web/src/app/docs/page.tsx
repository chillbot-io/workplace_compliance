export default function DocsPage() {
  return (
    <div className="mx-auto max-w-4xl px-4 sm:px-6 lg:px-8 py-12">
      <h1 className="text-3xl font-bold text-gray-900 mb-4">API Documentation</h1>

      <div className="flex gap-4 mb-8">
        <a href="https://api.fastdol.com/docs" target="_blank"
          className="rounded-md bg-blue-50 border border-blue-200 px-4 py-2 text-sm font-medium text-blue-700 hover:bg-blue-100">
          Interactive API (Swagger)
        </a>
        <a href="https://api.fastdol.com/redoc" target="_blank"
          className="rounded-md bg-gray-50 border border-gray-200 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-100">
          API Reference (ReDoc)
        </a>
      </div>

      <div className="prose prose-gray max-w-none">
        <h2 id="getting-started">Getting Started</h2>
        <ol>
          <li><a href="/signup">Create a free account</a> (50 lookups/month, no credit card)</li>
          <li>Verify your email — you will receive your API key</li>
          <li>Make your first API call:</li>
        </ol>
        <pre className="bg-gray-900 text-green-400 rounded-lg p-4 overflow-x-auto text-sm">
{`curl -H "X-Api-Key: YOUR_API_KEY" \\
  "https://api.fastdol.com/v1/employers?name=walmart&zip=83669"`}
        </pre>

        <h2 id="authentication">Authentication</h2>
        <p>
          Include your API key in the <code>X-Api-Key</code> header with every request.
          Keys are created when you verify your email. You can view your key on the <a href="/account">account page</a>.
        </p>

        <h2 id="endpoints">Endpoints</h2>

        <h3>Search Employers</h3>
        <pre className="bg-gray-50 rounded-lg p-4 overflow-x-auto text-sm">
{`GET /v1/employers?name=walmart&state=ID&zip=83669&limit=20&offset=0`}
        </pre>
        <p><strong>Parameters:</strong></p>
        <table className="w-full text-sm">
          <thead><tr className="border-b"><th className="text-left py-2">Param</th><th className="text-left py-2">Type</th><th className="text-left py-2">Description</th></tr></thead>
          <tbody>
            <tr className="border-b"><td className="py-2"><code>name</code></td><td>string</td><td>Employer name (fuzzy match)</td></tr>
            <tr className="border-b"><td className="py-2"><code>ein</code></td><td>string</td><td>Exact EIN match</td></tr>
            <tr className="border-b"><td className="py-2"><code>state</code></td><td>string</td><td>2-letter state code</td></tr>
            <tr className="border-b"><td className="py-2"><code>zip</code></td><td>string</td><td>5-digit ZIP code</td></tr>
            <tr className="border-b"><td className="py-2"><code>naics</code></td><td>string</td><td>4-digit NAICS prefix</td></tr>
            <tr className="border-b"><td className="py-2"><code>limit</code></td><td>int</td><td>Results per page (1-100, default 20)</td></tr>
            <tr><td className="py-2"><code>offset</code></td><td>int</td><td>Pagination offset</td></tr>
          </tbody>
        </table>

        <h3>Get Employer Detail</h3>
        <pre className="bg-gray-50 rounded-lg p-4 overflow-x-auto text-sm">
{`GET /v1/employers/{employer_id}`}
        </pre>

        <h3>Inspection History</h3>
        <pre className="bg-gray-50 rounded-lg p-4 overflow-x-auto text-sm">
{`GET /v1/employers/{employer_id}/inspections?limit=20&offset=0`}
        </pre>
        <p>Free — not counted against your quota.</p>

        <h3>Risk History</h3>
        <pre className="bg-gray-50 rounded-lg p-4 overflow-x-auto text-sm">
{`GET /v1/employers/{employer_id}/risk-history?limit=90`}
        </pre>

        <h3>Parent Company Rollup</h3>
        <pre className="bg-gray-50 rounded-lg p-4 overflow-x-auto text-sm">
{`GET /v1/employers/parent?name=Amazon&state=CA`}
        </pre>
        <p>Aggregate risk across all locations of a parent company.</p>

        <h3>Batch Lookup</h3>
        <pre className="bg-gray-50 rounded-lg p-4 overflow-x-auto text-sm">
{`POST /v1/employers/batch
Content-Type: application/json

{
  "lookups": [
    {"name": "Walmart", "state": "ID", "zip": "83669"},
    {"name": "Amazon", "state": "CA"},
    {"name": "Target", "zip": "55403"}
  ]
}`}
        </pre>
        <p>Up to 100 items per request. Each item can include <code>name</code>, <code>ein</code>, <code>employer_id</code>, <code>state</code>, <code>zip</code>, <code>city</code>.</p>

        <h3>CSV Upload</h3>
        <pre className="bg-gray-50 rounded-lg p-4 overflow-x-auto text-sm">
{`POST /v1/employers/upload-csv
Content-Type: multipart/form-data

file: employers.csv`}
        </pre>
        <p>Upload a CSV with employer names. Returns a CSV with risk profiles appended. Max 500 rows, 5MB.</p>

        <h3>Industry Benchmarks</h3>
        <pre className="bg-gray-50 rounded-lg p-4 overflow-x-auto text-sm">
{`GET /v1/industries/4451
GET /v1/industries/naics-codes`}
        </pre>

        <h2 id="response-format">Response Format</h2>
        <pre className="bg-gray-50 rounded-lg p-4 overflow-x-auto text-sm">
{`{
  "results": [
    {
      "employer_id": "uuid",
      "employer_name": "WALMART",
      "address": "123 MAIN ST",
      "city": "STAR",
      "state": "ID",
      "zip": "83669",
      "naics_code": "452112",
      "naics_description": "Discount Department Stores",
      "risk_tier": "MEDIUM",
      "risk_score": 12.5,
      "osha_inspections_5yr": 3,
      "osha_violations_5yr": 7,
      "osha_total_penalties": 15000.00,
      "whd_cases_5yr": 1,
      "whd_backwages_total": 5000.00,
      "trend_signal": "STABLE",
      "confidence_tier": "HIGH",
      "svep_flag": false,
      "parent_name": "Walmart Inc.",
      "location_count": 140
    }
  ],
  "total_count": 1,
  "limit": 20,
  "offset": 0,
  "data_notes": {
    "freshness": "OSHA citations typically appear 3-8 months after inspection date.",
    "coverage": "Data includes OSHA inspections/violations and WHD wage enforcement.",
    "scoring": "Risk scores combine OSHA violation severity and WHD back wages."
  }
}`}
        </pre>

        <h2 id="risk-scoring">Risk Scoring</h2>
        <p>Risk scores range from 0-100 and combine:</p>
        <ul>
          <li><strong>OSHA violations</strong> — willful (30 pts), repeat (15 pts), serious (3 pts)</li>
          <li><strong>OSHA penalties</strong> — up to 15 pts based on dollar amount</li>
          <li><strong>WHD enforcement</strong> — back wages (up to 8 pts), cases (up to 4 pts), employees violated (up to 3 pts)</li>
        </ul>
        <p>Risk tiers: <strong>HIGH</strong> (willful violations, &gt;$100k penalties), <strong>ELEVATED</strong>, <strong>MEDIUM</strong>, <strong>LOW</strong>.</p>

        <h2 id="rate-limits">Rate Limits</h2>
        <p>100 requests per minute per API key. If exceeded, you will receive a 429 response. Wait and retry.</p>

        <h2 id="errors">Errors</h2>
        <pre className="bg-gray-50 rounded-lg p-4 overflow-x-auto text-sm">
{`{
  "detail": {
    "error": "error_code",
    "message": "Human-readable description"
  }
}`}
        </pre>
        <table className="w-full text-sm">
          <thead><tr className="border-b"><th className="text-left py-2">Status</th><th className="text-left py-2">Meaning</th></tr></thead>
          <tbody>
            <tr className="border-b"><td className="py-2">400</td><td>Bad request (missing params)</td></tr>
            <tr className="border-b"><td className="py-2">401</td><td>Invalid or missing API key</td></tr>
            <tr className="border-b"><td className="py-2">403</td><td>Insufficient scope</td></tr>
            <tr className="border-b"><td className="py-2">404</td><td>No results found</td></tr>
            <tr className="border-b"><td className="py-2">429</td><td>Rate limit or quota exceeded</td></tr>
            <tr><td className="py-2">503</td><td>Service temporarily unavailable</td></tr>
          </tbody>
        </table>

        <h2 id="code-examples">Code Examples</h2>

        <h3>Python</h3>
        <pre className="bg-gray-900 text-green-400 rounded-lg p-4 overflow-x-auto text-sm">
{`import requests

API_KEY = "your_api_key"
BASE = "https://api.fastdol.com/v1"

# Search
r = requests.get(f"{BASE}/employers",
    headers={"X-Api-Key": API_KEY},
    params={"name": "Walmart", "state": "ID"})
print(r.json()["results"][0]["risk_tier"])

# Batch
r = requests.post(f"{BASE}/employers/batch",
    headers={"X-Api-Key": API_KEY},
    json={"lookups": [
        {"name": "Walmart", "zip": "83669"},
        {"name": "Amazon", "state": "CA"},
    ]})
for item in r.json()["results"]:
    print(item["query"]["name"], "->", item["match"]["risk_tier"] if item["match"] else "no match")`}
        </pre>

        <h3>JavaScript</h3>
        <pre className="bg-gray-900 text-green-400 rounded-lg p-4 overflow-x-auto text-sm">
{`const API_KEY = "your_api_key";
const BASE = "https://api.fastdol.com/v1";

const res = await fetch(
  \`\${BASE}/employers?name=walmart&zip=83669\`,
  { headers: { "X-Api-Key": API_KEY } }
);
const data = await res.json();
console.log(data.results[0].risk_tier);`}
        </pre>

        <h3>curl</h3>
        <pre className="bg-gray-900 text-green-400 rounded-lg p-4 overflow-x-auto text-sm">
{`# Search
curl -H "X-Api-Key: YOUR_KEY" \\
  "https://api.fastdol.com/v1/employers?name=walmart&zip=83669"

# CSV Upload
curl -H "X-Api-Key: YOUR_KEY" \\
  -F "file=@employers.csv" \\
  "https://api.fastdol.com/v1/employers/upload-csv" \\
  -o results.csv`}
        </pre>
      </div>
    </div>
  );
}
