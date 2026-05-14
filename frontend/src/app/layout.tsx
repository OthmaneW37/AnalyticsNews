import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import { Providers } from "./providers";
import { Sidebar } from "@/components/Sidebar";
import { ThemeToggle } from "@/components/ThemeToggle";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "AnalyticsNews — Intelligence Dashboard",
  description: "Veille médiatique intelligente multilingue",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="fr"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}
      suppressHydrationWarning
    >
      <body className="min-h-full flex bg-background text-foreground">
        <Providers>
          <Sidebar />
          <main className="ml-60 flex-1 p-8">
            <div className="fixed top-0 right-0 z-50 flex items-center gap-3 px-6 py-4">
              <ThemeToggle />
            </div>
            {children}
          </main>
        </Providers>
      </body>
    </html>
  );
}
