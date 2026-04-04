import Link from "next/link";
import { DemoSearch } from "@/components/demo-search";

export default function HomePage() {
  return (
    <>
      {/* Hero */}
      <section className="relative py-24 sm:py-32 overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-b from-slate-900 via-[#0f172a] to-[#1e293b]" />
        <div className="relative mx-auto max-w-7xl px-4 sm:px-6 lg:px-8 text-center">
          <h1 className="text-4xl sm:text-5xl lg:text-6xl font-bold tracking-tight text-white">
            Employer compliance data,{" "}
            <span className="text-violet-500">instantly</span>
          </h1>
          <p className="mt-6 text-lg sm:text-xl text-slate-400 max-w-3xl mx-auto">
            Search OSHA violations, wage enforcement actions, and employer risk profiles.
            One API call replaces hours of manual research on government websites.
          </p>
          <div className="mt-10 flex flex-col sm:flex-row gap-4 justify-center">
            <Link
              href="/signup"
              className="rounded-md bg-violet-500 px-8 py-3 text-base font-medium text-white hover:bg-violet-600 shadow-lg shadow-violet-500/25 transition-all"
            >
              Get Started Free
            </Link>
            <Link
              href="/docs"
              className="rounded-md bg-slate-800 px-8 py-3 text-base font-medium text-slate-300 border border-slate-700 hover:bg-slate-700 hover:text-white transition-all"
            >
              View API Docs
            </Link>
          </div>
        </div>
      </section>

      {/* Demo search */}
      <section className="py-16 bg-[#1e293b]">
        <div className="mx-auto max-w-4xl px-4 sm:px-6 lg:px-8">
          <h2 className="text-2xl font-bold text-center text-white mb-2">
            Try it now
          </h2>
          <p className="text-center text-slate-400 mb-8">
            Search any employer — no signup required
          </p>
          <DemoSearch />
        </div>
      </section>

      {/* How it works */}
      <section className="py-16 bg-[#0f172a]">
        <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
          <h2 className="text-2xl font-bold text-center text-white mb-12">
            How it works
          </h2>
          <div className="grid md:grid-cols-3 gap-8">
            {[
              { step: "1", title: "Search or upload", desc: "Type an employer name, or upload a CSV with hundreds of employers. Filter by state, zip code, or industry." },
              { step: "2", title: "Get risk profiles", desc: "Each employer gets a risk score (0-100) combining OSHA violations, penalties, wage enforcement, and trend analysis." },
              { step: "3", title: "Make better decisions", desc: "Use data-driven risk assessments for underwriting, staffing placements, supply chain compliance, and vendor screening." },
            ].map((item) => (
              <div key={item.step} className="text-center">
                <div className="mx-auto w-12 h-12 bg-violet-500/10 border border-violet-500/20 rounded-full flex items-center justify-center text-violet-500 font-bold text-lg mb-4">
                  {item.step}
                </div>
                <h3 className="font-semibold text-white mb-2">{item.title}</h3>
                <p className="text-sm text-slate-400">{item.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Who it's for */}
      <section className="py-16 bg-[#1e293b]">
        <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
          <h2 className="text-2xl font-bold text-center text-white mb-12">
            Built for
          </h2>
          <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-6">
            {[
              { title: "Insurance Underwriters", desc: "Screen employer safety history before quoting workers comp and commercial policies." },
              { title: "Staffing Agencies", desc: "Evaluate client employer risk before placing workers at their facilities." },
              { title: "Compliance Teams", desc: "Monitor vendor and supply chain compliance status continuously." },
              { title: "Developers", desc: "Embed employer safety data into your platform via REST API." },
            ].map((item) => (
              <div key={item.title} className="rounded-lg border border-slate-700 bg-slate-800/50 p-6 hover:border-slate-600 transition-colors">
                <h3 className="font-semibold text-white mb-2">{item.title}</h3>
                <p className="text-sm text-slate-400">{item.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Data sources */}
      <section className="py-16 bg-[#0f172a]">
        <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8">
          <h2 className="text-2xl font-bold text-center text-white mb-12">
            Federal enforcement data, normalized and scored
          </h2>
          <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-6">
            {[
              { name: "OSHA Inspections", count: "2.5M+", desc: "Workplace safety inspections since 1972" },
              { name: "OSHA Violations", count: "1.8M+", desc: "Citations with severity, penalties, and abatement" },
              { name: "WHD Enforcement", count: "355K+", desc: "Wage & Hour Division compliance actions" },
              { name: "Risk Scoring", count: "190K+", desc: "Employer profiles with risk tier and score" },
              { name: "Entity Resolution", count: "1.1M", desc: "Clusters linking records to canonical employers" },
              { name: "Updated Nightly", count: "24h", desc: "OSHA data refreshed daily, WHD weekly" },
            ].map((source) => (
              <div key={source.name} className="flex items-start gap-4 rounded-lg bg-slate-800/50 border border-slate-700 p-5">
                <div>
                  <div className="text-sm font-semibold text-white">{source.name}</div>
                  <div className="text-xs text-slate-500 mt-1">{source.desc}</div>
                </div>
                <div className="ml-auto text-sm font-bold text-violet-500 whitespace-nowrap">{source.count}</div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Pricing preview */}
      <section className="py-16 bg-[#1e293b]">
        <div className="mx-auto max-w-7xl px-4 sm:px-6 lg:px-8 text-center">
          <h2 className="text-2xl font-bold text-white mb-4">
            Simple, transparent pricing
          </h2>
          <p className="text-slate-400 mb-8">
            Start free. Upgrade when you need more lookups.
          </p>
          <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-6 max-w-5xl mx-auto">
            {[
              { tier: "Free", price: "$0", lookups: "50/mo", cta: "Get Started", popular: false },
              { tier: "Starter", price: "$79", lookups: "1,000/mo", cta: "Start Trial", popular: false },
              { tier: "Growth", price: "$249", lookups: "5,000/mo", cta: "Start Trial", popular: true },
              { tier: "Pro", price: "$599", lookups: "25,000/mo", cta: "Start Trial", popular: false },
            ].map((plan) => (
              <div
                key={plan.tier}
                className={`rounded-lg border p-6 ${
                  plan.popular ? "border-violet-500 ring-2 ring-violet-500 bg-slate-800" : "border-slate-700 bg-slate-800/50"
                }`}
              >
                {plan.popular && (
                  <div className="text-xs font-semibold text-violet-500 mb-2">MOST POPULAR</div>
                )}
                <div className="text-sm font-medium text-slate-400">{plan.tier}</div>
                <div className="text-3xl font-bold text-white mt-1">{plan.price}</div>
                <div className="text-sm text-slate-500 mt-1">{plan.lookups}</div>
                <Link
                  href="/signup"
                  className={`mt-6 block rounded-md px-4 py-2 text-sm font-medium transition-colors ${
                    plan.popular
                      ? "bg-violet-500 text-white hover:bg-violet-600"
                      : "bg-slate-700 text-slate-300 hover:bg-slate-600"
                  }`}
                >
                  {plan.cta}
                </Link>
              </div>
            ))}
          </div>
          <Link href="/pricing" className="mt-8 inline-block text-sm text-violet-500 hover:underline">
            View full pricing details
          </Link>
        </div>
      </section>

      {/* Final CTA */}
      <section className="py-20 bg-gradient-to-r from-violet-600 to-violet-500">
        <div className="mx-auto max-w-3xl px-4 text-center">
          <h2 className="text-3xl font-bold text-white mb-4">
            Stop searching OSHA.gov one employer at a time
          </h2>
          <p className="text-violet-100 mb-8">
            50 free lookups. No credit card required.
          </p>
          <Link
            href="/signup"
            className="rounded-md bg-white px-8 py-3 text-base font-medium text-violet-600 hover:bg-violet-50 shadow-lg transition-all"
          >
            Get Started Free
          </Link>
        </div>
      </section>
    </>
  );
}
