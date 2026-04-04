import type { Metadata } from "next";
import "./globals.css";
import { Header } from "@/components/header";
import { Footer } from "@/components/footer";

export const metadata: Metadata = {
  title: "FastDOL — Employer Compliance Risk API",
  description: "Search OSHA violations, WHD enforcement actions, and employer risk profiles. API and bulk lookup tools for insurance underwriters, staffing agencies, and compliance teams.",
  openGraph: {
    title: "FastDOL — Employer Compliance Risk API",
    description: "Aggregated federal workplace enforcement data. Search by employer name, get back risk scores.",
    type: "website",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="h-full antialiased">
      <body className="min-h-full flex flex-col bg-[#0f172a] text-slate-200 font-sans">
        <Header />
        <main className="flex-1">{children}</main>
        <Footer />
      </body>
    </html>
  );
}
