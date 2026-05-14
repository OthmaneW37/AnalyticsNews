"use client";

/**
 * ChartWrapper — prevents Recharts "width(-1) height(-1)" SSR errors.
 * Import and use it to wrap any ResponsiveContainer-based chart.
 */

import { useEffect, useState } from "react";

export function ChartWrapper({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}) {
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  if (!mounted) {
    return (
      <div
        className={`flex items-center justify-center text-muted text-xs ${className ?? ""}`}
      >
        Chargement du graphique...
      </div>
    );
  }

  return <div className={className}>{children}</div>;
}
