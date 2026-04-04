import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Privacy Policy - FastDOL",
  description: "FastDOL privacy policy. What we collect, how we use your data, and data retention policies.",
};

export default function PrivacyPage() {
  return (
    <div className="mx-auto max-w-3xl px-4 sm:px-6 lg:px-8 py-12 prose prose-gray">
      <h1>Privacy Policy</h1>
      <p className="text-sm text-gray-500">Last updated: April 2026</p>

      <h2>What We Collect</h2>
      <ul>
        <li><strong>Account information:</strong> email address, company name, and hashed password when you sign up.</li>
        <li><strong>Usage data:</strong> API calls, search queries, and endpoints accessed (for quota tracking and service improvement).</li>
        <li><strong>Payment information:</strong> processed by Stripe. We do not store credit card numbers.</li>
      </ul>

      <h2>How We Use Your Data</h2>
      <ul>
        <li>To provide and improve the FastDOL service</li>
        <li>To track API usage and enforce quotas</li>
        <li>To send transactional emails (verification, password reset)</li>
        <li>To process payments via Stripe</li>
      </ul>

      <h2>What We Don&apos;t Do</h2>
      <ul>
        <li>We do not sell your personal data</li>
        <li>We do not share your search queries with third parties</li>
        <li>We do not use your data for advertising</li>
      </ul>

      <h2>Data Retention</h2>
      <p>Account data is retained while your account is active. API usage logs are retained for 12 months. You may request account deletion by emailing <a href="mailto:support@fastdol.com">support@fastdol.com</a>.</p>

      <h2>Employer Data</h2>
      <p>FastDOL serves publicly available federal enforcement data (OSHA, WHD). This data is published by the U.S. Department of Labor and is public record. FastDOL normalizes and aggregates this data but does not create or modify the underlying government records.</p>

      <h2>Contact</h2>
      <p>Questions about privacy? Email <a href="mailto:support@fastdol.com">support@fastdol.com</a>.</p>
    </div>
  );
}
