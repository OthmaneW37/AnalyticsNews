"use client";

import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { motion } from "framer-motion";
import { PageHeader } from "@/components/PageHeader";
import { ChartWrapper } from "@/components/ChartWrapper";
import { getQualityStats } from "@/lib/api";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  LabelList,
} from "recharts";

export default function QualityPage() {
  const { data: stats, isLoading } = useQuery({
    queryKey: ["quality-stats"],
    queryFn: () => getQualityStats(),
  });

  const pieData = useMemo(() => {
    if (!stats) return [];
    return [
      { name: "OK",   value: stats.quality.ok,   color: "var(--green)" },
      { name: "FAIL", value: stats.quality.fail,  color: "var(--red)" },
    ].filter((d) => d.value > 0);
  }, [stats]);

  const sourceChart = useMemo(() => {
    if (!stats) return [];
    return Object.entries(stats.by_source).map(([name, v]) => ({
      name,
      ok:   v.ok,
      fail: v.fail,
    }));
  }, [stats]);

  const funnel = stats?.funnel ?? [];
  const quality = stats?.quality ?? { ok: 0, fail: 0, total: 0 };
  const pct = quality.total > 0 ? ((quality.ok / quality.total) * 100).toFixed(1) : "0";
  const retentionPct =
    funnel.length >= 3 && funnel[0].value > 0
      ? ((funnel[2].value / funnel[0].value) * 100).toFixed(1)
      : "—";

  if (isLoading) {
    return (
      <div>
        <PageHeader title="Qualité" subtitle="Contrôle et métriques" />
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
          {[1, 2, 3].map((i) => (
            <div key={i} className="h-28 animate-pulse rounded-xl bg-border" />
          ))}
        </div>
      </div>
    );
  }

  return (
    <div>
      <PageHeader title="Qualité" subtitle="Contrôle et métriques" />

      {/* KPIs */}
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-4">
        {[
          { label: "Articles Gold",    value: quality.total, color: "text-foreground" },
          { label: "Qualité OK",        value: quality.ok,    color: "text-success",   sub: `${pct}%` },
          { label: "Rejetés",           value: quality.fail,  color: "text-danger",    sub: quality.total > 0 ? `${(100 - parseFloat(pct)).toFixed(1)}%` : "0%" },
          { label: "Rétention finale",  value: `${retentionPct}%`, color: "text-accent", sub: "Bronze → Gold" },
        ].map((kpi, i) => (
          <motion.div
            key={kpi.label}
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: i * 0.05 }}
            className="rounded-xl border border-border bg-surface p-5"
          >
            <p className="text-xs font-medium uppercase tracking-wider text-muted">{kpi.label}</p>
            <p className={`mt-2 text-3xl font-bold ${kpi.color}`}>{kpi.value}</p>
            {kpi.sub && <p className="mt-1 text-xs text-muted">{kpi.sub}</p>}
          </motion.div>
        ))}
      </div>

      {/* Funnel réel */}
      <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-2">
        <div className="rounded-xl border border-border bg-surface p-5">
          <h3 className="mb-1 text-sm font-semibold text-foreground">
            Funnel Bronze → Silver → Gold
          </h3>
          <p className="mb-4 text-xs text-muted">
            Articles réels à chaque couche du pipeline (données du jour)
          </p>
          <ChartWrapper className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={funnel} layout="vertical">
                <XAxis
                  type="number"
                  tick={{ fill: "var(--text-muted)", fontSize: 12 }}
                  axisLine={{ stroke: "var(--bg-border)" }}
                  tickLine={false}
                />
                <YAxis
                  dataKey="name"
                  type="category"
                  tick={{ fill: "var(--text-primary)", fontSize: 11 }}
                  axisLine={false}
                  tickLine={false}
                  width={130}
                />
                <Tooltip
                  contentStyle={{
                    background: "var(--bg-surface)",
                    border: "1px solid var(--bg-border)",
                    borderRadius: "8px",
                    color: "var(--text-primary)",
                  }}
                  formatter={(v) => [(v as number).toLocaleString("fr-FR"), "Articles"]}
                />
                <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                  {funnel.map((entry, i) => (
                    <Cell key={i} fill={entry.color} />
                  ))}
                  <LabelList
                    dataKey="value"
                    position="right"
                    style={{ fill: "var(--text-muted)", fontSize: 12 }}
                    formatter={(v) => (v as number).toLocaleString("fr-FR")}
                  />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </ChartWrapper>
        </div>

        <div className="rounded-xl border border-border bg-surface p-5">
          <h3 className="mb-1 text-sm font-semibold text-foreground">
            Répartition qualité Gold
          </h3>
          <p className="mb-4 text-xs text-muted">
            Articles OK vs rejetés dans la couche Gold
          </p>
          {pieData.length > 0 ? (
            <>
              <ChartWrapper className="h-52">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={pieData}
                      cx="50%"
                      cy="50%"
                      innerRadius={55}
                      outerRadius={85}
                      paddingAngle={4}
                      dataKey="value"
                    >
                      {pieData.map((entry, i) => (
                        <Cell key={i} fill={entry.color} stroke="none" />
                      ))}
                    </Pie>
                    <Tooltip
                      contentStyle={{
                        background: "var(--bg-surface)",
                        border: "1px solid var(--bg-border)",
                        borderRadius: "8px",
                        color: "var(--text-primary)",
                      }}
                    />
                  </PieChart>
                </ResponsiveContainer>
              </ChartWrapper>
              <div className="mt-2 flex justify-center gap-6">
                {pieData.map((d) => (
                  <div key={d.name} className="flex items-center gap-2">
                    <span className="h-3 w-3 rounded-full" style={{ backgroundColor: d.color }} />
                    <span className="text-xs text-muted">
                      {d.name} ({d.value.toLocaleString("fr-FR")})
                    </span>
                  </div>
                ))}
              </div>
            </>
          ) : (
            <div className="flex h-52 items-center justify-center text-sm text-muted">
              Aucune donnée Gold pour aujourd'hui
            </div>
          )}
        </div>
      </div>

      {/* Taux par source */}
      {sourceChart.length > 0 && (
        <div className="mt-6 rounded-xl border border-border bg-surface p-5">
          <h3 className="mb-1 text-sm font-semibold text-foreground">Taux par source (Gold)</h3>
          <p className="mb-4 text-xs text-muted">Répartition OK / FAIL des articles Gold par source</p>
          <ChartWrapper className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={sourceChart}>
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
                <Bar dataKey="ok"   stackId="a" fill="var(--green)" radius={[0, 0, 4, 4]} name="OK" />
                <Bar dataKey="fail" stackId="a" fill="var(--red)" radius={[4, 4, 0, 0]} name="FAIL" />
              </BarChart>
            </ResponsiveContainer>
          </ChartWrapper>
        </div>
      )}

      {/* Explication du pipeline */}
      <div className="mt-6 rounded-xl border border-border bg-surface p-5">
        <h3 className="mb-3 text-sm font-semibold text-foreground">Architecture du pipeline</h3>
        <div className="flex flex-col gap-3 sm:flex-row">
          {[
            { name: "Bronze",  color: "var(--text-muted)", desc: "Articles bruts scrappés depuis les sources (JSON)" },
            { name: "Silver",  color: "var(--accent)", desc: "Nettoyage, déduplication, détection de langue, qualité" },
            { name: "Gold",    color: "var(--green)", desc: "BERTopic, enrichissement Polymarket, couche analytique" },
          ].map((layer) => (
            <div key={layer.name} className="flex-1 rounded-lg border border-border-subtle bg-background p-4">
              <div className="flex items-center gap-2 mb-2">
                <span className="h-3 w-3 rounded-full" style={{ backgroundColor: layer.color }} />
                <span className="text-sm font-semibold" style={{ color: layer.color }}>{layer.name}</span>
              </div>
              <p className="text-xs text-muted">{layer.desc}</p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
