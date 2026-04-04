export default function TermsPage() {
  return (
    <div className="mx-auto max-w-3xl px-4 sm:px-6 lg:px-8 py-12 prose prose-gray">
      <h1>Terms of Service</h1>
      <p className="text-sm text-gray-500">Last updated: April 2026</p>

      <h2>1. Service Description</h2>
      <p>FastDOL provides access to aggregated federal workplace enforcement data through a web interface and API. Data is sourced from publicly available government records including OSHA and WHD enforcement data.</p>

      <h2>2. Data Disclaimer</h2>
      <p>FastDOL data is derived from publicly available federal enforcement records and is provided &ldquo;as is&rdquo; without warranty. FastDOL does not guarantee the accuracy, completeness, or timeliness of the data. This service does not constitute legal advice.</p>

      <h2>3. Acceptable Use</h2>
      <p>You may use FastDOL for lawful business purposes including risk assessment, compliance monitoring, and due diligence. You may not use FastDOL to discriminate against individuals, harass employers, or for any unlawful purpose.</p>

      <h2>4. API Usage</h2>
      <p>API access is subject to rate limits and monthly quotas based on your subscription plan. Exceeding your quota will result in 429 responses until the next billing cycle.</p>

      <h2>5. Payment</h2>
      <p>Paid plans are billed monthly via Stripe. You may cancel at any time. Refunds are not provided for partial months.</p>

      <h2>6. Privacy</h2>
      <p>See our <a href="/privacy">Privacy Policy</a> for details on how we handle your data.</p>

      <h2>7. Contact</h2>
      <p>Questions? Email <a href="mailto:support@fastdol.com">support@fastdol.com</a>.</p>
    </div>
  );
}
