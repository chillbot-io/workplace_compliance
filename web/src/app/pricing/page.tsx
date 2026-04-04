import type { Metadata } from "next";
import Link from "next/link";

export const metadata: Metadata = {
  title: "Pricing - FastDOL",
  description: "Simple, transparent pricing for employer compliance risk lookups. Start free with 50 lookups per month.",
};

const plans = [
  {
    tier: "Free", price: "$0", period: "forever", lookups: "50", perLookup: "—",
    features: ["50 lookups/month", "Single employer search", "Risk tier & score", "API access"],
    cta: "Get Started", ctaStyle: "bg-gray-100 text-gray-700 hover:bg-gray-200",
  },
  {
    tier: "Starter", price: "$79", period: "/month", lookups: "1,000", perLookup: "$0.079",
    features: ["1,000 lookups/month", "Batch lookup (up to 100)", "CSV bulk upload", "Match confidence scores", "Email support"],
    cta: "Start Free Trial", ctaStyle: "bg-gray-100 text-gray-700 hover:bg-gray-200",
  },
  {
    tier: "Growth", price: "$249", period: "/month", lookups: "5,000", perLookup: "$0.050",
    features: ["5,000 lookups/month", "Everything in Starter", "Parent company rollups", "Risk history & trends", "Priority support"],
    cta: "Start Free Trial", ctaStyle: "bg-blue-600 text-white hover:bg-blue-700", popular: true,
  },
  {
    tier: "Pro", price: "$599", period: "/month", lookups: "25,000", perLookup: "$0.024",
    features: ["25,000 lookups/month", "Everything in Growth", "Dedicated account manager", "Custom integrations", "SLA guarantee"],
    cta: "Start Free Trial", ctaStyle: "bg-gray-100 text-gray-700 hover:bg-gray-200",
  },
];

const faqs = [
  { q: "What counts as a lookup?", a: "Each employer search, batch item, or CSV row that returns a result counts as one lookup. Searches that return no results are free." },
  { q: "Can I try before I buy?", a: "Yes. The free tier gives you 50 lookups/month with no credit card required. You can also try the demo search on our homepage without signing up." },
  { q: "What data sources are included?", a: "All plans include OSHA inspections, OSHA violations, and WHD wage enforcement data. We aggregate federal enforcement records from the Department of Labor." },
  { q: "How fresh is the data?", a: "OSHA data is refreshed nightly. WHD data is refreshed weekly. Note that OSHA citations typically appear 3-8 months after the actual inspection date." },
  { q: "Do you offer annual billing?", a: "Yes. Annual plans save 20% (2 months free). Contact us at support@fastdol.com for annual pricing." },
  { q: "What about enterprise needs?", a: "For unlimited lookups, custom SLAs, or data licensing, contact us at enterprise@fastdol.com." },
];

export default function PricingPage() {
  return (
    <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8 py-16">
      <div className="text-center mb-12">
        <h1 className="text-3xl font-bold text-gray-900">Simple, transparent pricing</h1>
        <p className="mt-4 text-lg text-gray-600">Start free. Scale as you grow.</p>
      </div>

      <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-16">
        {plans.map((plan) => (
          <div
            key={plan.tier}
            className={`rounded-lg border p-6 flex flex-col ${
              plan.popular ? "border-blue-600 ring-2 ring-blue-600" : "border-gray-200"
            }`}
          >
            {plan.popular && (
              <div className="text-xs font-semibold text-blue-600 mb-2">MOST POPULAR</div>
            )}
            <div className="text-sm font-medium text-gray-500">{plan.tier}</div>
            <div className="mt-2">
              <span className="text-4xl font-bold text-gray-900">{plan.price}</span>
              <span className="text-sm text-gray-500">{plan.period}</span>
            </div>
            <div className="text-sm text-gray-500 mt-2">
              {plan.lookups} lookups/mo
              {plan.perLookup !== "—" && <span className="text-xs ml-1">({plan.perLookup}/lookup)</span>}
            </div>

            <ul className="mt-6 space-y-3 flex-1">
              {plan.features.map((f) => (
                <li key={f} className="flex items-start gap-2 text-sm text-gray-600">
                  <svg className="w-4 h-4 text-blue-500 mt-0.5 shrink-0" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
                  </svg>
                  {f}
                </li>
              ))}
            </ul>

            <Link
              href="/signup"
              className={`mt-6 block text-center rounded-md px-4 py-2.5 text-sm font-medium ${plan.ctaStyle}`}
            >
              {plan.cta}
            </Link>
          </div>
        ))}
      </div>

      {/* Enterprise */}
      <div className="rounded-lg border border-gray-200 bg-gray-50 p-8 text-center mb-16">
        <h2 className="text-xl font-bold text-gray-900">Enterprise</h2>
        <p className="text-gray-600 mt-2">Unlimited lookups, custom SLAs, data licensing, dedicated support.</p>
        <a href="mailto:enterprise@fastdol.com"
          className="mt-4 inline-block rounded-md bg-gray-900 px-6 py-2.5 text-sm font-medium text-white hover:bg-gray-800">
          Contact Sales
        </a>
      </div>

      {/* FAQ */}
      <div className="max-w-3xl mx-auto">
        <h2 className="text-2xl font-bold text-gray-900 text-center mb-8">Frequently asked questions</h2>
        <div className="space-y-6">
          {faqs.map((faq) => (
            <div key={faq.q}>
              <h3 className="font-semibold text-gray-900">{faq.q}</h3>
              <p className="mt-2 text-sm text-gray-600">{faq.a}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
