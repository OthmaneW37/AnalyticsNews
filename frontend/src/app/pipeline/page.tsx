"use client";

import { useState, useRef, useEffect, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { motion, AnimatePresence } from "framer-motion";
import {
  Play, Loader2, Terminal, AlertCircle,
  Search, X, ExternalLink, CheckCircle2,
  ChevronDown, ChevronRight,
} from "lucide-react";
import { PageHeader } from "@/components/PageHeader";
import { StatusBadge } from "@/components/StatusBadge";
import { getPipelineStatus, getPipelineLogs, runPipeline } from "@/lib/api";
import { MEDIA_DATABASE } from "@/lib/types";

// ─── Step definitions ────────────────────────────────────────────────────────
const STEPS = [
  { label: "Scraping",              key: "scraping", desc: "Collecte des articles depuis les sources" },
  { label: "Bronze → Silver",       key: "silver",   desc: "Nettoyage, déduplication, détection langue" },
  { label: "Silver → BERTopic",     key: "bertopic", desc: "Clustering thématique des articles" },
  { label: "Gold + Polymarket",     key: "gold",     desc: "Enrichissement signaux et agrégation" },
  { label: "DuckDB / Warehouse",    key: "duckdb",   desc: "Indexation analytique finale" },
];

function detectStepFromLogs(logs: string[]): number {
  let step = -1;
  for (const line of logs) {
    const l = line.toLowerCase();
    if (l.includes("scraping") || l.includes("phase 1") || l.includes("articles scraped")) step = Math.max(step, 0);
    if (l.includes("silver") || l.includes("bronze") || l.includes("phase 2"))            step = Math.max(step, 1);
    if (l.includes("bertopic") || l.includes("topic"))                                     step = Math.max(step, 2);
    if (l.includes("gold") || l.includes("polymarket") || l.includes("phase 3"))          step = Math.max(step, 3);
    if (l.includes("duckdb") || l.includes("warehouse") || l.includes("phase 4"))         step = Math.max(step, 4);
  }
  return step;
}

function logLineColor(line: string) {
  if (/error/i.test(line))                                   return "text-[var(--terminal-red)]";
  if (/warn/i.test(line))                                    return "text-[var(--terminal-amber)]";
  if (/\[ok\]|success|done|terminé|complet/i.test(line))    return "text-[var(--terminal-green)]";
  if (/phase|>>>|===/i.test(line))                           return "text-accent font-semibold";
  if (/info/i.test(line))                                    return "text-muted";
  return "text-foreground";
}

// ─── Source picker ────────────────────────────────────────────────────────────
function SourcePicker({
  selected,
  onChange,
}: {
  selected: string[];
  onChange: (s: string[]) => void;
}) {
  const [query, setQuery]       = useState("");
  const [catFilter, setCatFilter] = useState("Tous");
  const [open, setOpen]         = useState(true);

  const categories = useMemo(
    () => ["Tous", ...Array.from(new Set(MEDIA_DATABASE.map((m) => m.category))).sort()],
    []
  );

  const filtered = useMemo(
    () =>
      MEDIA_DATABASE.filter((m) => {
        const q = query.toLowerCase();
        return (
          (catFilter === "Tous" || m.category === catFilter) &&
          (!q ||
            m.name.toLowerCase().includes(q) ||
            m.key.toLowerCase().includes(q) ||
            m.country.toLowerCase().includes(q) ||
            m.description.toLowerCase().includes(q))
        );
      }),
    [query, catFilter]
  );

  const toggle = (key: string, implemented: boolean) => {
    if (!implemented) return;
    onChange(selected.includes(key) ? selected.filter((s) => s !== key) : [...selected, key]);
  };

  const FLAG: Record<string, string> = {
    GB: "🇬🇧", MA: "🇲🇦", QA: "🇶🇦", US: "🇺🇸", FR: "🇫🇷",
    DE: "🇩🇪", EG: "🇪🇬", AE: "🇦🇪", TR: "🇹🇷", CN: "🇨🇳",
  };

  return (
    <div>
      {/* Selected chips (always visible) */}
      {selected.length > 0 && (
        <div className="mb-3 flex flex-wrap gap-1.5">
          {selected.map((key) => {
            const m = MEDIA_DATABASE.find((x) => x.key === key);
            return (
              <span
                key={key}
                className="flex items-center gap-1 rounded-full bg-[var(--accent-subtle)] px-2.5 py-0.5 text-[11px] font-medium text-accent"
              >
                {m?.name ?? key}
                <button onClick={() => toggle(key, true)}>
                  <X className="h-3 w-3" />
                </button>
              </span>
            );
          })}
        </div>
      )}

      {/* Collapsible browser */}
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex w-full items-center gap-2 rounded-lg border border-border-subtle bg-background px-3 py-2 text-xs text-muted hover:text-foreground transition-colors"
      >
        <Search className="h-3.5 w-3.5" />
        <span className="flex-1 text-left">
          {query || "Rechercher un média parmi 30+ sources…"}
        </span>
        {open ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
      </button>

      <AnimatePresence>
        {open && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <div className="mt-2 space-y-2">
              {/* Search input */}
              <div className="flex items-center gap-2 rounded-lg border border-border bg-background px-3 py-2">
                <Search className="h-3.5 w-3.5 shrink-0 text-muted" />
                <input
                  type="text"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Rechercher…"
                  className="flex-1 bg-transparent text-xs text-foreground outline-none placeholder:text-muted"
                />
                {query && (
                  <button onClick={() => setQuery("")}>
                    <X className="h-3 w-3 text-muted hover:text-foreground" />
                  </button>
                )}
              </div>

              {/* Category pills */}
              <div className="flex flex-wrap gap-1">
                {categories.map((cat) => (
                  <button
                    key={cat}
                    onClick={() => setCatFilter(cat)}
                    className={`rounded-full px-2 py-0.5 text-[10px] font-medium transition-colors ${
                      catFilter === cat
                        ? "bg-accent text-white"
                        : "bg-surface-hover text-muted hover:text-foreground"
                    }`}
                  >
                    {cat}
                  </button>
                ))}
              </div>

              {/* Media list */}
              <div className="max-h-32 overflow-y-auto space-y-1 pr-0.5">
                {filtered.map((media) => {
                  const isSel = selected.includes(media.key);
                  return (
                    <button
                      key={media.key}
                      onClick={() => toggle(media.key, media.implemented)}
                      disabled={!media.implemented}
                      className={`group w-full flex items-center gap-2.5 rounded-lg border px-2 py-1.5 text-left transition-all ${
                        isSel
                          ? "border-accent/40 bg-[var(--accent-subtle)]"
                          : media.implemented
                          ? "border-border-subtle bg-background hover:border-border hover:bg-surface-hover"
                          : "border-border-subtle/40 bg-background/40 opacity-40 cursor-not-allowed"
                      }`}
                    >
                      <span className="text-base leading-none">
                        {FLAG[media.country] ?? "🌐"}
                      </span>
                      <div className="min-w-0 flex-1">
                        <div className="flex items-center gap-1.5">
                          <span className={`text-xs font-medium ${isSel ? "text-accent" : "text-foreground"}`}>
                            {media.name}
                          </span>
                          {!media.implemented && (
                            <span className="rounded bg-surface px-1 text-[8px] uppercase text-muted">bientôt</span>
                          )}
                        </div>
                        <p className="truncate text-[10px] text-muted">{media.description}</p>
                      </div>
                      <div className="flex items-center gap-1.5 shrink-0">
                        {isSel && <CheckCircle2 className="h-3.5 w-3.5 text-accent" />}
                        <a
                          href={media.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          onClick={(e) => e.stopPropagation()}
                          className="opacity-0 group-hover:opacity-100 text-muted hover:text-accent transition-opacity"
                        >
                          <ExternalLink className="h-3 w-3" />
                        </a>
                      </div>
                    </button>
                  );
                })}
                {filtered.length === 0 && (
                  <p className="py-3 text-center text-xs text-muted">Aucun média trouvé</p>
                )}
              </div>

              <p className="text-[10px] text-muted">
                {selected.length} sélectionnée{selected.length > 1 ? "s" : ""}
                {" · "}{MEDIA_DATABASE.filter((m) => !m.implemented).length} médias à venir
              </p>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ─── Main page ────────────────────────────────────────────────────────────────
export default function PipelinePage() {
  const queryClient = useQueryClient();
  const [selectedSources, setSelectedSources] = useState<string[]>(["bbc", "hespress"]);
  const [maxPerFeed, setMaxPerFeed] = useState(15);
  const [logSince, setLogSince] = useState(0);
  const [allLogs, setAllLogs] = useState<string[]>([]);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const [optimisticRunning, setOptimisticRunning] = useState(false);
  const terminalRef = useRef<HTMLDivElement>(null);

  const { data: status, refetch: refetchStatus } = useQuery({
    queryKey: ["pipeline-status"],
    queryFn: getPipelineStatus,
    refetchInterval: (query) =>
      query.state.data?.running || optimisticRunning ? 1000 : 30000,
  });

  const { data: logsData } = useQuery({
    queryKey: ["pipeline-logs", logSince],
    queryFn: () => getPipelineLogs(logSince),
    refetchInterval: status?.running || optimisticRunning ? 1500 : false,
    enabled: status?.running || optimisticRunning || logSince > 0,
  });

  useEffect(() => {
    if (logsData && logsData.logs.length > 0) {
      setAllLogs((prev) => [...prev, ...logsData.logs]);
      setLogSince((prev) => prev + logsData.logs.length);
      if (optimisticRunning) setOptimisticRunning(false);
    }
  }, [logsData]);

  useEffect(() => {
    if (status?.running) setOptimisticRunning(false);
  }, [status?.running]);

  useEffect(() => {
    if (terminalRef.current) {
      terminalRef.current.scrollTop = terminalRef.current.scrollHeight;
    }
  }, [allLogs]);

  const mutation = useMutation({
    mutationFn: runPipeline,
    onMutate: () => {
      setOptimisticRunning(true);
      setAllLogs([]);
      setLogSince(0);
      setErrorMsg(null);
    },
    onSuccess: () => {
      refetchStatus();
      queryClient.invalidateQueries({ queryKey: ["pipeline-status"] });
      queryClient.invalidateQueries({ queryKey: ["data"] });
      queryClient.invalidateQueries({ queryKey: ["keywords"] });
      queryClient.invalidateQueries({ queryKey: ["available-dates"] });
      queryClient.invalidateQueries({ queryKey: ["quality-stats"] });
    },
    onError: (err: Error) => {
      setOptimisticRunning(false);
      setErrorMsg(err.message.includes("409")
        ? "Un pipeline est déjà en cours d'exécution."
        : err.message
      );
    },
  });

  const running  = status?.running ?? false;
  const finished = !!status?.finished_at && !running;
  const isActive = running || optimisticRunning;

  const currentStep = useMemo(() => {
    if (optimisticRunning) return 0;
    const fromLogs = detectStepFromLogs(allLogs);
    if (running)  return fromLogs;
    if (finished) return STEPS.length - 1;
    return -1;
  }, [allLogs, status, optimisticRunning, running, finished]);

  return (
    <div>
      <PageHeader title="Pipeline" subtitle="Contrôle et supervision">
        <StatusBadge status={isActive ? { ...(status ?? {}), running: true } as typeof status : status} />
      </PageHeader>

      {/* ── MAIN LAYOUT : Progression left-wide | Logs right-narrow ── */}
      <div className="grid grid-cols-1 gap-6 xl:grid-cols-5">

        {/* ── LEFT COLUMN : Configuration + Terminal logs (spans 3/5) ── */}
        <div className="space-y-4 xl:col-span-3">

          {/* ─ Config card (Toujours visible en premier) ─ */}
          <div className="rounded-xl border border-border bg-surface p-4 sm:p-5">
            <h3 className="mb-3 text-sm font-semibold text-foreground">Configuration</h3>

            <div className="mb-3">
              <label className="mb-1.5 block text-xs font-medium text-muted">Sources médias</label>
              <SourcePicker selected={selectedSources} onChange={setSelectedSources} />
            </div>

            <div className="mb-4">
              <label className="mb-1.5 block text-xs font-medium text-muted">
                Max articles par flux
              </label>
              <div className="flex items-center gap-3">
                <input
                  type="range"
                  min={5}
                  max={100}
                  step={5}
                  value={maxPerFeed}
                  onChange={(e) => setMaxPerFeed(Number(e.target.value))}
                  className="flex-1 accent-accent"
                />
                <span className="w-10 text-right text-sm font-medium text-foreground tabular-nums">
                  {maxPerFeed}
                </span>
              </div>
            </div>

            {errorMsg && (
              <div className="mb-3 flex items-start gap-2 rounded-lg border border-danger/20 bg-danger/10 px-3 py-2 text-xs text-danger">
                <AlertCircle className="h-4 w-4 shrink-0 mt-0.5" />
                {errorMsg}
              </div>
            )}

            <button
              id="launch-pipeline-btn"
              onClick={() => mutation.mutate({ sources: selectedSources, max_per_feed: maxPerFeed })}
              disabled={isActive || selectedSources.length === 0}
              className={`flex w-full items-center justify-center gap-2 rounded-lg px-4 py-2.5 text-sm font-semibold transition-all ${
                isActive
                  ? "cursor-not-allowed bg-warning/10 text-warning border border-warning/20"
                  : selectedSources.length === 0
                  ? "cursor-not-allowed bg-surface-hover text-muted"
                  : "bg-accent text-black hover:bg-accent-hover active:scale-[0.98]"
              }`}
            >
              {isActive ? (
               <><Loader2 className="h-4 w-4 animate-spin" />En cours…</>
              ) : (
                <><Play className="h-4 w-4" />Lancer le pipeline</>
              )}
            </button>
          </div>

          {/* ─ Terminal logs (Affiché en dessous de la config, toujours visible, sans scroll horizontal) ─ */}
          <div className="rounded-xl border border-border terminal-frame overflow-hidden">
            {/* Terminal header */}
            <div className="flex items-center gap-2 border-b border-[var(--terminal-line)] terminal-header px-4 py-3">
              {/* Mac-style dots */}
              <div className="flex items-center gap-1.5">
                <span className="h-3 w-3 rounded-full bg-red-500/70" />
                <span className="h-3 w-3 rounded-full bg-yellow-500/70" />
                <span className="h-3 w-3 rounded-full bg-green-500/70" />
              </div>
              <div className="flex-1 text-center">
                <div className="inline-flex items-center gap-2 text-xs text-[var(--terminal-muted)]">
                  <Terminal className="h-3.5 w-3.5" />
                  <span>pipeline.log</span>
                  {isActive && (
                    <motion.span
                      className="h-1.5 w-1.5 rounded-full bg-[var(--terminal-green)]"
                      animate={{ opacity: [1, 0.2, 1] }}
                      transition={{ repeat: Infinity, duration: 1 }}
                    />
                  )}
                </div>
              </div>
              <span className="text-[10px] tabular-nums text-[var(--terminal-line)]">
                {allLogs.length} ln
              </span>
            </div>

            {/* Log content (overflow-x-hidden pour supprimer le scroll horizontal) */}
            <div
              ref={terminalRef}
              className="h-[calc(100vh-20rem)] min-h-80 overflow-y-auto overflow-x-hidden p-4 font-mono text-[11px] leading-relaxed terminal-content"
            >
              <AnimatePresence initial={false}>
                {allLogs.map((line, i) => (
                  <motion.div
                    key={`log-${i}`}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    className={`whitespace-pre-wrap break-all overflow-x-hidden ${logLineColor(line)}`}
                    style={{ wordBreak: "break-all" }}
                  >
                    <span className="mr-2 select-none text-[var(--terminal-line)] tabular-nums">
                      {String(i + 1).padStart(3, "0")}
                    </span>
                    {line}
                  </motion.div>
                ))}
              </AnimatePresence>

              {allLogs.length === 0 && !isActive && (
                <div className="flex h-full min-h-40 flex-col items-center justify-center text-center pr-2">
                  <Terminal className="h-10 w-10 text-[var(--terminal-line)] mb-3" />
                  <p className="text-xs text-[var(--terminal-muted)]">En attente de logs…</p>
                  <p className="mt-1 text-[10px] text-[var(--terminal-line)]">
                    Sélectionnez des sources et lancez le pipeline
                  </p>
                </div>
              )}

              {allLogs.length === 0 && isActive && (
                <motion.p
                  className="text-[var(--terminal-green)] text-xs"
                  animate={{ opacity: [1, 0.4, 1] }}
                  transition={{ repeat: Infinity, duration: 1.5 }}
                >
                  ▶ Démarrage en cours…
                </motion.p>
              )}
            </div>
          </div>
        </div>

        {/* ── RIGHT COLUMN : Progression (spans 2/5) ── */}
        <div className="xl:col-span-2">
          <div className="sticky top-6">
            {/* ─ Step tracker (Affiché quand actif ou terminé) ─ */}
            {(isActive || finished) && (
              <div className="rounded-xl border border-border bg-surface p-4 sm:p-5">
                <div className="mb-4 flex items-center justify-between">
                  <h3 className="text-base font-semibold text-foreground">Progression</h3>
                  {finished && (
                    <span className="flex items-center gap-1.5 rounded-full bg-success/10 px-3 py-1 text-xs font-medium text-success">
                      <CheckCircle2 className="h-3.5 w-3.5" />
                      Terminé
                    </span>
                  )}
                  {isActive && (
                    <span className="flex items-center gap-1.5 rounded-full bg-warning/10 px-3 py-1 text-xs font-medium text-warning">
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      En cours
                    </span>
                  )}
                </div>

                <div className="space-y-1.5">
                  {STEPS.map((step, i) => {
                    const isDone   = finished || (isActive && currentStep > i);
                    const isNow    = isActive && currentStep === i;
                    const isPending = !isDone && !isNow;

                    return (
                      <motion.div
                        key={step.key}
                        initial={false}
                        animate={{ opacity: isPending && !isActive ? 0.5 : 1 }}
                        className={`flex items-center gap-3 rounded-xl border px-3 py-2 transition-all ${
                          isDone  ? "border-success/20 bg-success/5" :
                          isNow   ? "border-warning/30 bg-warning/5" :
                                   "border-border-subtle bg-background/50"
                        }`}
                      >
                        {/* Icon */}
                        <div className={`flex h-7 w-7 shrink-0 items-center justify-center rounded-full text-xs font-bold transition-all ${
                          isDone ? "bg-success/15 text-success"  :
                          isNow  ? "bg-warning/15 text-warning"  :
                                   "bg-surface text-muted border border-border-subtle"
                        }`}>
                          {isDone ? (
                            <svg className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                              <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                            </svg>
                          ) : isNow ? (
                            <Loader2 className="h-3.5 w-3.5 animate-spin" />
                          ) : (
                            <span>{i + 1}</span>
                          )}
                        </div>

                        {/* Text */}
                        <div className="min-w-0 flex-1">
                          <p className={`text-xs font-semibold ${
                            isDone ? "text-success" : isNow ? "text-warning" : "text-muted"
                          }`}>
                            {step.label}
                          </p>
                          <p className="text-[10px] text-muted truncate">{step.desc}</p>
                        </div>

                        {/* Pulse dot for current step */}
                        {isNow && (
                          <motion.div
                            className="h-1.5 w-1.5 shrink-0 rounded-full bg-warning"
                            animate={{ opacity: [1, 0.2, 1], scale: [1, 1.2, 1] }}
                            transition={{ repeat: Infinity, duration: 1.2 }}
                          />
                        )}
                      </motion.div>
                    );
                  })}
                </div>

                {/* Timeline progress bar globale (Hauteur 6px min, vert vif si terminé) */}
                <div className="mt-4">
                  <div className="flex items-center justify-between text-[10px] text-muted mb-1.5">
                    <span>0%</span>
                    <span className="font-medium text-foreground">
                      {finished ? "100%" : isActive
                        ? `${Math.round(((currentStep + 1) / STEPS.length) * 100)}%`
                        : "En attente"}
                    </span>
                    <span>100%</span>
                  </div>
                  <div className="w-full rounded-full bg-border overflow-hidden" style={{ height: "6px" }}>
                    <motion.div
                      className={`rounded-full ${finished ? "bg-success" : "bg-warning"}`}
                      style={{ height: "6px" }}
                      initial={{ width: "0%" }}
                      animate={{
                        width: finished ? "100%" :
                               isActive ? `${Math.max(5, ((currentStep + 1) / STEPS.length) * 100)}%` :
                               "0%"
                      }}
                      transition={{ duration: 0.6, ease: "easeOut" }}
                    />
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
