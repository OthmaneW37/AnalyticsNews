"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  Play,
  Newspaper,
  BarChart3,
  ShieldCheck,
  Radio,
  TrendingUp,
} from "lucide-react";

const nav = [
  { href: "/",           label: "Dashboard",   icon: LayoutDashboard },
  { href: "/pipeline",   label: "Pipeline",    icon: Play },
  { href: "/articles",   label: "Articles",    icon: Newspaper },
  { href: "/polymarket", label: "Polymarket",  icon: TrendingUp },
  { href: "/quality",    label: "Qualité",     icon: ShieldCheck },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="fixed left-0 top-0 z-40 flex h-screen w-60 flex-col border-r border-border bg-surface">
      <div className="flex items-center gap-3 px-5 py-6">
        <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-[var(--accent-subtle)] text-accent">
          <Radio className="h-5 w-5" />
        </div>
        <div>
          <h1 className="text-sm font-bold tracking-wide text-foreground">
            AnalyticsNews
          </h1>
          <p className="text-[10px] font-medium text-muted">
            Intelligence
          </p>
        </div>
      </div>

      <nav className="flex-1 space-y-1 px-3">
        {nav.map((item) => {
          const active = pathname === item.href;
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`group relative flex items-center gap-3 px-3 py-2.5 text-sm font-medium transition-colors ${
                active
                  ? "nav-active"
                  : "text-muted hover:bg-surface-hover hover:text-foreground rounded-lg"
              }`}
            >
              <item.icon className="relative z-10 h-4 w-4 shrink-0" />
              <span className="relative z-10">{item.label}</span>
              {item.href === "/polymarket" && !active && (
                <span className="relative z-10 ml-auto rounded bg-[var(--accent-subtle)] px-1.5 py-0.5 text-[9px] font-medium text-accent">
                  NEW
                </span>
              )}
            </Link>
          );
        })}
      </nav>

      <div className="border-t border-border px-5 py-4">
        <p className="text-[10px] text-muted">API : localhost:8001</p>
      </div>
    </aside>
  );
}
