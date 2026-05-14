"use client";

import { motion } from "framer-motion";
import { PipelineStatus } from "@/lib/types";

interface StatusBadgeProps {
  status: PipelineStatus | undefined;
}

export function StatusBadge({ status }: StatusBadgeProps) {
  if (!status) {
    return (
      <span className="inline-flex items-center gap-2 rounded-full border border-border-subtle bg-surface px-3 py-1 text-xs font-medium text-muted">
        <span className="h-2 w-2 rounded-full bg-muted" />
        Inconnu
      </span>
    );
  }

  const isRunning = status.running;
  const hasError = !!status.error;

  const color = isRunning
    ? "bg-success"
    : hasError
    ? "bg-danger"
    : "bg-muted";

  const label = isRunning
    ? "RUNNING"
    : hasError
    ? "ERROR"
    : "IDLE";

  return (
    <span className="inline-flex items-center gap-2 rounded-full border border-border-subtle bg-surface px-3 py-1 text-xs font-medium text-foreground">
      <motion.span
        className={`h-2 w-2 rounded-full ${color}`}
        animate={isRunning ? { scale: [1, 1.4, 1], opacity: [1, 0.6, 1] } : {}}
        transition={
          isRunning
            ? { repeat: Infinity, duration: 1.5, ease: "easeInOut" }
            : {}
        }
      />
      {label}
      {status.started_at && isRunning && (
        <span className="text-muted">
          ·{" "}
          {new Date(status.started_at).toLocaleTimeString("fr-FR", {
            hour: "2-digit",
            minute: "2-digit",
          })}
        </span>
      )}
    </span>
  );
}
