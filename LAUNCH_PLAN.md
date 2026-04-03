# FastDOL Launch Plan — Business & Go-to-Market

**Created:** April 2026

---

## Legal & Finance

| # | Task | Notes | When |
|---|------|-------|------|
| 1 | **EIN from IRS** | Apply at irs.gov/ein — instant if online, free | This week |
| 2 | **Business bank account** | Mercury or Relay (no fees, good API). Take EIN + LLC docs. Avoid big banks | This week |
| 3 | **Connect Stripe to business bank** | Stripe dashboard → Settings → Bank accounts | After bank account |
| 4 | **Accounting software** | QuickBooks Self-Employed ($15/mo) or Wave (free). Connect business bank for auto-import | Before first revenue |
| 5 | **Terms of Service + Privacy Policy** | Required before launch. Use Termly or TermsFeed to generate, then customize. Put on website | During frontend build |
| 6 | **Data usage disclaimer** | "Data sourced from publicly available federal enforcement records. Not legal advice." | On website + API responses |

---

## Marketing & Go-to-Market

### Launch Day Activities

| # | Task | Notes |
|---|------|-------|
| 7 | **Landing page** | Hero section, problem statement, live demo, pricing table, CTA. This IS your marketing |
| 8 | **Product Hunt launch** | Free. Prepare description + screenshots. Launch on a Tuesday for max visibility |
| 9 | **Hacker News "Show HN" post** | "I built an API that aggregates OSHA/WHD enforcement data into employer risk profiles." HN loves data products built on public data |
| 10 | **LinkedIn content** | 3-5 posts explaining the problem you solve. Target: insurance underwriters, staffing firms, compliance consultants |
| 11 | **Cold outreach to 50 prospects** | Personalized email with free tier offer to insurance underwriting teams, staffing agency compliance officers, ESG consultants |

### Week After Launch

| # | Task | Notes |
|---|------|-------|
| 12 | **API marketplace listings** | RapidAPI, API Layer, Datarade (see details below) |
| 13 | **SEO blog posts** | 3-5 articles targeting search traffic (see topics below) |
| 14 | **Cold outreach round 2** | Follow up on first round + 50 new prospects |

### SEO Content Topics

- "How to check OSHA violations for an employer"
- "Employer safety screening for insurance underwriters"
- "OSHA compliance API for staffing agencies"
- "Automated workplace safety due diligence"
- "OSHA violation lookup API — developer guide"

---

## API Marketplaces

| Marketplace | Difficulty | Cost | Audience | Priority |
|---|---|---|---|---|
| **Datarade** | Medium — apply, they review | Free basic listing, paid premium ($500+/mo) | B2B data buyers — insurance, finance, compliance. YOUR market | Launch day — apply now |
| **RapidAPI** | Easy — self-serve, 30 min | Free to list, ~20% revenue share | Largest API marketplace, millions of developers | Launch day |
| **API Layer** | Easy — self-serve | Free to list, revenue share | Smaller but curated, good for data APIs | Launch day |
| **AWS Data Exchange** | Harder — application + technical requirements | AWS handles billing, takes a cut | Enterprise buyers with AWS accounts | Phase 2 |
| **Snowflake Marketplace** | Medium — need Snowflake share packaging | Free to list | Data teams at large companies | Phase 2+ |

**Note:** RapidAPI and API Layer proxy your API through their servers. Customers hit `rapidapi.com/fastdol/...` not `api.fastdol.com`. Some customers prefer this (single billing), some prefer direct. Offer both.

---

## First 10 Customers Strategy

Don't try to get 1000 customers at launch. Get 10 who love it.

| Buyer Segment | How to Reach | What to Say |
|---|---|---|
| **Insurance underwriters** | LinkedIn, InsureTech communities, industry conferences | "You're checking osha.gov one employer at a time. This API does it programmatically with risk scoring." |
| **Staffing/PEO firms** | LinkedIn, ASA (American Staffing Association) member directory | "Screen client employers for safety violations before placing workers." |
| **Compliance consultants** | LinkedIn, search "OSHA consultant" | "Give your clients a data-driven safety risk assessment instead of a checklist." |
| **InsureTech startups** | YC company directory, LinkedIn, AngelList | "Embed employer safety data into your underwriting workflow via API." |
| **Supply chain compliance** | LinkedIn, procurement teams at large manufacturers | "Monitor vendor compliance status continuously instead of annual audits." |

---

## Pricing Strategy

### Tiers

| Tier | Price | Lookups/mo | Per Lookup | Target |
|------|-------|-----------|-----------|--------|
| **Free** | $0 | 50 | — | Developer evaluation |
| **Starter** | $79/mo | 1,000 | $0.079 | Small firms, consultants |
| **Growth** | $249/mo | 5,000 | $0.050 | Mid-market insurance, PEO |
| **Pro** | $599/mo | 25,000 | $0.024 | Large underwriting teams |
| **Enterprise** | Custom | Unlimited | Negotiated | Carrier integrations |

### Pricing Tactics

| Tactic | Why |
|--------|-----|
| Free tier is your acquisition funnel | 50 lookups lets developers build a demo and show their boss |
| Annual discount (2 months free) | Incentivize commitment, improve cash flow. Offer 20% off annual |
| "Contact us" for Enterprise | Never cap your upside. If a carrier wants custom, charge $2k-10k/mo |
| Show per-lookup cost on pricing page | "$0.079/lookup" feels cheap next to "$30/background check" |

### Competitive Positioning

| Competitor | Their Price | Our Advantage |
|---|---|---|
| Manual OSHA lookup (osha.gov) | Free but hours of labor | Automated, risk-scored, API-accessible |
| Background checks (Checkr) | $30-80/check | 100x cheaper per lookup |
| ISNetworld/Avetta | $500-5000/yr per contractor | Screen hundreds of employers for less than one subscription |
| Dun & Bradstreet risk reports | $100-250/report | Real-time API, not PDF reports |

---

## Timeline

| When | What |
|------|------|
| **This week** | EIN, bank account (Mercury/Relay) |
| **During frontend build** | Terms of Service, Privacy Policy, landing page |
| **1 week before launch** | Apply to Datarade, prepare Product Hunt listing, write LinkedIn posts |
| **Launch day** | Product Hunt, HN Show post, LinkedIn post, email 50 prospects, list on RapidAPI + API Layer |
| **Week after launch** | Blog posts, cold outreach round 2, follow up on marketplace leads |
| **Month 1 goal** | 5-10 free tier users, 1-3 paying customers |
| **Month 3 goal** | 50+ free tier, 10+ paying, $1k+ MRR |

---

## What NOT to Do

- **Don't spend money on paid ads** — your market is too niche for Google/Facebook ads to work at this stage
- **Don't build a sales team** — you are the sales team for the first 20 customers
- **Don't incorporate in Delaware** or set up complex corporate structures — your LLC is fine
- **Don't build features nobody asked for** — get 10 users, listen to what they need, build that
- **Don't wait for perfection** — launch with OSHA data, add more sources based on customer demand
