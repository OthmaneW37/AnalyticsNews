"use client";

import { motion } from "framer-motion";
import { LucideIcon } from "lucide-react";

interface KPICardProps {
  label: string;
  value: string | number;
  icon: LucideIcon;
  accent?: "blue" | "green" | "red" | "cyan";
  subtitle?: string;
}

const accentMap = {
  blue: "text-accent bg-[var(--accent-subtle)] border-border",
  green: "text-success bg-success/10 border-border",
  red: "text-danger bg-danger/10 border-border",
  cyan: "text-accent bg-[var(--accent-subtle)] border-border",
};

export function KPICard({ label, value, icon: Icon, accent = "blue", subtitle }: KPICardProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      className="relative overflow-hidden rounded-lg border border-border bg-surface p-4 sm:p-5 shadow-sm"
    >
      <div className="flex items-start justify-between">
        <div>
          <p className="text-xs font-medium tracking-wide text-muted">
            {label}
          </p>
          <p className="mt-2 text-2xl font-bold tabular-nums text-foreground">
            {value}
          </p>
          {subtitle && (
            <p className="mt-1 text-xs text-muted">{subtitle}</p>
          )}
        </div>
        <div
          className={`flex h-10 w-10 items-center justify-center rounded-lg border ${accentMap[accent]}`}
        >
          <Icon className="h-5 w-5" />
        </div>
      </div>
    </motion.div>
  );
}
