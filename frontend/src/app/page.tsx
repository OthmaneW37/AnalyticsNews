"use client";

import { useQuery } from "@tanstack/react-query";
import { motion } from "framer-motion";
import {
  Newspaper,
  Radio,
  BarChart3,
  Clock,
  TrendingUp,
  Globe,
} from "lucide-react";
import { KPICard } from "@/components/KPICard";
import { StatusBadge } from "@/components/StatusBadge";
import { PageHeader } from "@/components/PageHeader";
import { ChartWrapper } from "@/components/ChartWrapper";
import { getPipelineStatus, getData, getAvailableDates, getKeywords } from "@/lib/api";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";

export default function DashboardPage() {
  const { data: status } = useQuery({
    queryKey: ["pipeline-status"],
    queryFn: getPipelineStatus,
    refetchInterval: 30000,
  });

  const { data: dataResponse } = useQuery({
    queryKey: ["data"],
    queryFn: () => getData(),
    refetchInterval: 30000,
  });

  const { data: datesResponse } = useQuery({
    queryKey: ["available-dates"],
    queryFn: getAvailableDates,
    refetchInterval: 30000,
  });

  const { data: keywordsResponse } = useQuery({
    queryKey: ["keywords"],
    queryFn: () => getKeywords(),
    refetchInterval: 30000,
  });

  const articles = dataResponse?.articles ?? [];
  const dates = datesResponse?.dates ?? [];
  const keywords = keywordsResponse?.keywords ?? [];

  const uniqueSources = Array.from(
    new Set(articles.map((a) => a.source))
  ).length;
  const uniqueTopics = Array.from(
    new Set(articles.map((a) => a.topic).filter(Boolean))
  ).length;

  const articlesBySource = articles.reduce<Record<string, number>>(
    (acc, a) => {
      acc[a.source] = (acc[a.source] ?? 0) + 1;
      return acc;
    },
    {}
  );

  const chartData = Object.entries(articlesBySource)
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 8);

  const topTopics = Object.entries(
    articles.reduce<Record<string, number>>((acc, a) => {
      const t = a.topic || "Non classé";
      acc[t] = (acc[t] ?? 0) + 1;
      return acc;
    }, {})
  )
    .map(([topic, count]) => ({ topic, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 6);

  const lastUpdate = status?.finished_at
    ? new Date(status.finished_at).toLocaleString("fr-FR")
    : status?.started_at
    ? new Date(status.started_at).toLocaleString("fr-FR")
    : "—";

  return (
    <div>
      <PageHeader
        title="Dashboard"
        subtitle="Vue d'ensemble de la veille médiatique"
      >
        <StatusBadge status={status} />
      </PageHeader>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <KPICard
          label="Articles collectés"
          value={articles.length}
          icon={Newspaper}
          accent="blue"
          subtitle={`${uniqueSources} sources`}
        />
        <KPICard
          label="Sources actives"
          value={uniqueSources}
          icon={Radio}
          accent="cyan"
        />
        <KPICard
          label="Sujets détectés"
          value={uniqueTopics}
          icon={BarChart3}
          accent="green"
        />
        <KPICard
          label="Dernière MAJ"
          value={lastUpdate === "—" ? "—" : lastUpdate.split(",")[0] ?? lastUpdate}
          icon={Clock}
          accent="blue"
          subtitle={lastUpdate !== "—" ? (lastUpdate.split(",")[1]?.trim() ?? "") : "Aucune exécution"}
        />
      </div>

      <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-3">
        <div className="rounded-lg border border-border bg-surface p-5 lg:col-span-2 shadow-sm">
          <h3 className="mb-4 text-sm font-semibold text-foreground">
            Articles par source
          </h3>
          <ChartWrapper className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData}>
                <XAxis
                  dataKey="name"
                  tick={{ fill: "var(--text-muted)", fontSize: 12 }}
                  axisLine={{ stroke: "var(--bg-border)" }}
                  tickLine={false}
                />
                <YAxis
                  tick={{ fill: "var(--text-muted)", fontSize: 12 }}
                  axisLine={false}
                  tickLine={false}
                />
                <Tooltip
                  contentStyle={{
                    background: "var(--bg-surface)",
                    border: "1px solid var(--bg-border)",
                    borderRadius: "8px",
                    color: "var(--text-primary)",
                  }}
                />
                <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                  {chartData.map((_, i) => (
                    <Cell
                      key={i}
                      fill={i === 0 ? "var(--accent)" : "var(--bg-surface-2)"}
                      stroke={i === 0 ? "var(--accent)" : "var(--bg-border)"}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartWrapper>
        </div>

        <div className="rounded-lg border border-border bg-surface p-5 shadow-sm">
          <h3 className="mb-4 text-sm font-semibold text-foreground">
            Top sujets
          </h3>
          <div className="space-y-3">
            {topTopics.map((t, i) => (
              <motion.div
                key={t.topic}
                initial={{ opacity: 0, x: 10 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: i * 0.05 }}
                className="flex items-center justify-between rounded-lg bg-background px-3 py-2"
              >
                <div className="flex items-center gap-3">
                  <span className="flex h-6 w-6 items-center justify-center rounded bg-[var(--accent-subtle)] text-xs font-bold text-accent">
                    {i + 1}
                  </span>
                  <span className="text-sm text-foreground truncate max-w-[140px]">
                    {t.topic}
                  </span>
                </div>
                <span className="text-xs font-medium tabular-nums text-muted">
                  {t.count}
                </span>
              </motion.div>
            ))}
            {topTopics.length === 0 && (
              <p className="text-sm text-muted">Aucun sujet détecté</p>
            )}
          </div>
        </div>

        <div className="rounded-lg border border-border bg-surface p-5 shadow-sm">
          <h3 className="mb-4 text-sm font-semibold text-foreground">
            Mots-clés fréquents
          </h3>
          <div className="space-y-3">
            {keywords.slice(0, 8).map((k, i) => (
              <motion.div
                key={k.mot}
                initial={{ opacity: 0, x: 10 }}
                animate={{ opacity: 1, x: 0 }}
                transition={{ delay: i * 0.05 }}
                className="flex items-center justify-between rounded-lg bg-background px-3 py-2"
              >
                <div className="flex items-center gap-3">
                  <span className="flex h-6 w-6 items-center justify-center rounded bg-[var(--accent-subtle)] text-xs font-bold text-accent">
                    {i + 1}
                  </span>
                  <span className="text-sm text-foreground truncate max-w-[140px]">
                    {k.mot}
                  </span>
                </div>
                <span className="text-xs font-medium tabular-nums text-muted">
                  {k.frequence}
                </span>
              </motion.div>
            ))}
            {keywords.length === 0 && (
              <p className="text-sm text-muted">Aucun mot-clé disponible</p>
            )}
          </div>
        </div>
      </div>

      <div className="mt-6 rounded-lg border border-border bg-surface p-5 shadow-sm">
        <h3 className="mb-4 text-sm font-semibold text-foreground">
          Live feed — 10 derniers articles
        </h3>
        <div className="space-y-2">
          {articles.slice(0, 10).map((article, i) => (
            <motion.div
              key={article.id ?? i}
              initial={{ opacity: 0, y: 5 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: i * 0.03 }}
              className="flex items-center justify-between rounded-lg border border-border-subtle bg-background px-4 py-3"
            >
              <div className="min-w-0 flex-1">
                <p className="truncate text-sm font-medium text-foreground">
                  {article.titre || "Sans titre"}
                </p>
                <div className="mt-1 flex items-center gap-3 text-xs text-muted">
                  <span className="flex items-center gap-1">
                    <Globe className="h-3 w-3" />
                    {article.source}
                  </span>
                  <span>{article.langue}</span>
                  <span>{article.topic || "—"}</span>
                </div>
              </div>
              <span
                className={`ml-4 rounded-full px-2 py-0.5 text-[10px] font-medium uppercase ${
                  article.quality === "OK"
                    ? "bg-success/10 text-success"
                    : "bg-danger/10 text-danger"
                }`}
              >
                {article.quality}
              </span>
            </motion.div>
          ))}
          {articles.length === 0 && (
            <p className="text-sm text-muted">Aucun article disponible</p>
          )}
        </div>
      </div>

      <div className="mt-6 grid grid-cols-2 gap-4 lg:grid-cols-4">
        {dates.slice(0, 4).map((d) => (
          <div
            key={d}
            className="rounded-lg border border-border bg-surface px-4 py-3 text-center"
          >
            <p className="text-xs text-muted">Date disponible</p>
            <p className="mt-1 text-sm font-semibold text-foreground">{d}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
