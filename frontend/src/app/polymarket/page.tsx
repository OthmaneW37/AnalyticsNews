"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { motion, AnimatePresence } from "framer-motion";
import { TrendingUp, AlertTriangle, ExternalLink, ChevronDown, ChevronUp, Target } from "lucide-react";
import { PageHeader } from "@/components/PageHeader";
import { getPolymarket, getAvailableDates } from "@/lib/api";
import { PolymarketGroup, PolymarketBet } from "@/lib/types";

function SignalBar({ value }: { value: number }) {
  const pct = Math.max(0, Math.min(100, value * 100));
  const colorClass = pct >= 70 ? "bg-success" : pct >= 40 ? "bg-warning" : "bg-muted";
  return (
    <div className="mt-2">
      <div className="flex items-center justify-between text-xs mb-1">
        <span className="text-muted">Signal de marché</span>
        <span className="font-bold tabular-nums text-muted">
          {pct.toFixed(1)}%
        </span>
      </div>
      <div className="h-1.5 w-full rounded-full bg-border overflow-hidden">
        <motion.div
          className={`h-1.5 rounded-full ${colorClass}`}
          initial={{ width: 0 }}
          animate={{ width: `${pct}%` }}
          transition={{ duration: 0.7, ease: "easeOut" }}
        />
      </div>
    </div>
  );
}

function BetCard({ bet }: { bet: PolymarketBet }) {
  const pct = Math.round(bet.probability * 100);
  const colorClass = pct >= 70 ? "text-success" : pct >= 40 ? "text-warning" : "text-muted";
  const bgClass = pct >= 70 ? "bg-success/10 border-success/20" : pct >= 40 ? "bg-warning/10 border-warning/20" : "bg-surface-hover border-border";
  return (
    <div className="flex items-start gap-3 rounded-lg bg-background border border-border px-4 py-3">
      <div
        className={`shrink-0 mt-0.5 w-10 h-10 rounded-full flex items-center justify-center text-xs font-bold border ${bgClass} ${colorClass}`}
      >
        {pct}%
      </div>
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium text-foreground leading-snug">
          {bet.question}
        </p>
        {bet.volume_usd > 0 && (
          <p className="mt-0.5 text-[11px] text-muted">
            Volume : ${bet.volume_usd.toLocaleString("fr-FR", { maximumFractionDigits: 0 })} USD
          </p>
        )}
      </div>
      {bet.url && (
        <a
          href={`https://polymarket.com${bet.url}`}
          target="_blank"
          rel="noopener noreferrer"
          className="shrink-0 text-muted hover:text-accent transition-colors"
        >
          <ExternalLink className="h-4 w-4" />
        </a>
      )}
    </div>
  );
}

function GroupCard({ group, index }: { group: PolymarketGroup; index: number }) {
  const [expanded, setExpanded] = useState(false);
  const pct = Math.round(group.signal * 100);
  const colorClass = pct >= 70 ? "text-success" : pct >= 40 ? "text-warning" : "text-muted";

  return (
    <motion.div
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.05 }}
      className="rounded-lg border border-border bg-surface overflow-hidden shadow-sm"
    >
      {/* Header du groupe */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full text-left p-5 hover:bg-surface-hover transition-colors"
      >
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 mb-1">
              <Target className={`h-4 w-4 shrink-0 ${colorClass}`} />
              <h3 className="text-base font-bold text-foreground truncate">
                {group.name}
              </h3>
            </div>
            <div className="flex items-center gap-3 text-xs text-muted">
              <span>{group.article_count} article{group.article_count > 1 ? "s" : ""}</span>
              <span>·</span>
              <span>{group.bets.length} pari{group.bets.length > 1 ? "s" : ""} Polymarket</span>
            </div>
          </div>
          <div className="flex items-center gap-2 shrink-0">
            <span className={`text-sm font-bold tabular-nums ${colorClass}`}>
              {group.signal_pct}
            </span>
            {expanded ? (
              <ChevronUp className="h-4 w-4 text-muted" />
            ) : (
              <ChevronDown className="h-4 w-4 text-muted" />
            )}
          </div>
        </div>

        <SignalBar value={group.signal} />

        {/* Sources */}
        <div className="mt-3 flex flex-wrap gap-1">
          {group.sources.filter(Boolean).slice(0, 4).map((src) => (
            <span
              key={src}
              className="rounded bg-background px-1.5 py-0.5 text-[10px] text-muted"
            >
              {src}
            </span>
          ))}
        </div>
      </button>

      {/* Section expandable : Bets + Articles */}
      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.3, ease: "easeInOut" }}
            className="overflow-hidden border-t border-border"
          >
            <div className="p-5 space-y-5">
              {/* Paris Polymarket */}
              {group.bets.length > 0 ? (
                <div>
                  <h4 className="text-xs font-bold uppercase tracking-widest text-accent mb-3">
                    🎯 Paris Polymarket ({group.bets.length})
                  </h4>
                  <div className="space-y-2">
                    {group.bets.map((bet, i) => (
                      <BetCard key={i} bet={bet} />
                    ))}
                  </div>
                </div>
              ) : (
                <div className="text-xs text-muted italic">
                  Aucun pari Polymarket trouvé pour ce sujet.
                </div>
              )}

              {/* Articles liés */}
              {group.articles.length > 0 && (
                <div>
                  <h4 className="text-xs font-bold uppercase tracking-widest text-muted mb-3">
                    📰 Articles liés ({group.articles.length})
                  </h4>
                  <div className="space-y-1.5">
                    {group.articles.slice(0, 8).map((article, idx) => (
                      <div
                        key={article.id ?? idx}
                        className="flex items-center justify-between rounded-lg bg-background border border-border-subtle px-3 py-2.5 gap-3"
                      >
                        <div className="min-w-0 flex-1">
                          <p className="truncate text-sm text-foreground">
                            {article.titre || "Sans titre"}
                          </p>
                          <p className="text-[11px] text-muted mt-0.5">
                            {article.source}
                            {article.date
                              ? ` · ${new Date(article.date).toLocaleDateString("fr-FR", { day: "2-digit", month: "short" })}`
                              : ""}
                          </p>
                        </div>
                        {article.url && (
                          <a
                            href={article.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="shrink-0 text-muted hover:text-accent"
                          >
                            <ExternalLink className="h-3.5 w-3.5" />
                          </a>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}

export default function PolymarketPage() {
  const [date, setDate] = useState<string>("");

  const { data: response, isLoading } = useQuery({
    queryKey: ["polymarket", date],
    queryFn: () => getPolymarket(date || undefined),
  });

  const { data: datesResponse } = useQuery({
    queryKey: ["available-dates"],
    queryFn: getAvailableDates,
  });

  const groups = response?.groups ?? [];
  const hasPolymarket = response?.has_polymarket ?? false;
  const dates = datesResponse?.dates ?? [];

  const totalArticles = groups.reduce((s, g) => s + g.article_count, 0);
  const totalBets = groups.reduce((s, g) => s + g.bets.length, 0);

  return (
    <div>
      <PageHeader title="Polymarket" subtitle="Signaux de prédiction par entité thématique">
        <div className="flex items-center gap-2">
          {hasPolymarket ? (
            <span className="flex items-center gap-1.5 rounded-full bg-success/10 px-3 py-1 text-xs font-medium text-success">
              <TrendingUp className="h-3 w-3" />
              Données Polymarket actives
            </span>
          ) : (
            <span className="flex items-center gap-1.5 rounded-full bg-warning/10 px-3 py-1 text-xs font-medium text-warning">
              <AlertTriangle className="h-3 w-3" />
              Mode simulation
            </span>
          )}
        </div>
      </PageHeader>

      {/* Filtre date */}
      <div className="mb-6 flex items-center gap-4">
        <select
          value={date}
          onChange={(e) => setDate(e.target.value)}
          className="rounded-lg border border-border bg-surface px-3 py-2 text-sm text-foreground outline-none focus:border-accent"
        >
          <option value="">Aujourd&apos;hui</option>
          {dates.map((d) => (
            <option key={d} value={d}>{d}</option>
          ))}
        </select>
        <div className="flex items-center gap-4 text-xs text-muted">
          <span><strong className="text-foreground">{groups.length}</strong> sujets</span>
          <span><strong className="text-foreground">{totalArticles}</strong> articles</span>
          <span><strong className="text-accent">{totalBets}</strong> paris Polymarket</span>
        </div>
      </div>

      {isLoading && (
        <div className="space-y-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="h-28 animate-pulse rounded-lg bg-border" />
          ))}
        </div>
      )}

      {!isLoading && groups.length === 0 && (
        <div className="flex flex-col items-center justify-center py-20">
          <TrendingUp className="h-12 w-12 text-muted mb-4" />
          <p className="text-sm text-muted">Aucun groupe disponible pour cette date.</p>
          <p className="mt-1 text-xs text-muted">
            Lancez le pipeline pour générer des données.
          </p>
        </div>
      )}

      {/* Grille des groupes par entité */}
      {groups.length > 0 && (
        <div className="space-y-4">
          {groups.map((group, i) => (
            <GroupCard key={group.name} group={group} index={i} />
          ))}
        </div>
      )}
    </div>
  );
}
