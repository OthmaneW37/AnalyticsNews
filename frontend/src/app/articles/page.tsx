"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { motion, AnimatePresence } from "framer-motion";
import {
  Search,
  Filter,
  X,
  Globe,
  Calendar,
  Tag,
  Shield,
  ExternalLink,
} from "lucide-react";
import { PageHeader } from "@/components/PageHeader";
import { getData, getAvailableDates } from "@/lib/api";
import { ArticleData } from "@/lib/types";

export default function ArticlesPage() {
  const [date, setDate] = useState<string>("");
  const [sourceFilter, setSourceFilter] = useState<string>("");
  const [langFilter, setLangFilter] = useState<string>("");
  const [topicFilter, setTopicFilter] = useState<string>("");
  const [selectedArticle, setSelectedArticle] = useState<ArticleData | null>(
    null
  );

  const { data: dataResponse, isLoading } = useQuery({
    queryKey: ["data", date],
    queryFn: () => getData(date || undefined),
  });

  const { data: datesResponse } = useQuery({
    queryKey: ["available-dates"],
    queryFn: getAvailableDates,
  });

  const articles = dataResponse?.articles ?? [];
  const dates = datesResponse?.dates ?? [];

  const filtered = articles.filter((a) => {
    const matchSource = !sourceFilter || a.source === sourceFilter;
    const matchLang = !langFilter || a.langue === langFilter;
    const matchTopic = !topicFilter || (a.topic || "").includes(topicFilter);
    return matchSource && matchLang && matchTopic;
  });

  const uniqueSources = Array.from(new Set(articles.map((a) => a.source)));
  const uniqueLangs = Array.from(new Set(articles.map((a) => a.langue)));

  return (
    <div>
      <PageHeader title="Articles" subtitle="Explorateur de données" />

      <div className="mb-4 flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-2 rounded-lg border border-border bg-surface px-3 py-2">
          <Calendar className="h-4 w-4 text-muted" />
          <select
            value={date}
            onChange={(e) => setDate(e.target.value)}
            className="bg-transparent text-sm text-foreground outline-none"
          >
            <option value="">Aujourd'hui</option>
            {dates.map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-2 rounded-lg border border-border bg-surface px-3 py-2">
          <Globe className="h-4 w-4 text-muted" />
          <select
            value={sourceFilter}
            onChange={(e) => setSourceFilter(e.target.value)}
            className="bg-transparent text-sm text-foreground outline-none"
          >
            <option value="">Toutes sources</option>
            {uniqueSources.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-2 rounded-lg border border-border bg-surface px-3 py-2">
          <Tag className="h-4 w-4 text-muted" />
          <select
            value={langFilter}
            onChange={(e) => setLangFilter(e.target.value)}
            className="bg-transparent text-sm text-foreground outline-none"
          >
            <option value="">Toutes langues</option>
            {uniqueLangs.map((l) => (
              <option key={l} value={l}>
                {l}
              </option>
            ))}
          </select>
        </div>

        <div className="flex items-center gap-2 rounded-lg border border-border bg-surface px-3 py-2">
          <Search className="h-4 w-4 text-muted" />
          <input
            type="text"
            placeholder="Filtrer par sujet..."
            value={topicFilter}
            onChange={(e) => setTopicFilter(e.target.value)}
            className="bg-transparent text-sm text-foreground outline-none placeholder:text-muted"
          />
        </div>

        {(sourceFilter || langFilter || topicFilter || date) && (
          <button
            onClick={() => {
              setSourceFilter("");
              setLangFilter("");
              setTopicFilter("");
              setDate("");
            }}
            className="flex items-center gap-1 rounded-lg border border-danger/20 bg-danger/10 px-3 py-2 text-xs font-medium text-danger hover:bg-danger/20"
          >
            <X className="h-3 w-3" />
            Réinitialiser
          </button>
        )}
      </div>

      <div className="rounded-lg border border-border bg-surface shadow-sm">
        <div className="grid grid-cols-12 gap-4 border-b border-border px-5 py-3 text-xs font-medium uppercase tracking-wider text-muted">
          <div className="col-span-4">Titre</div>
          <div className="col-span-2">Source</div>
          <div className="col-span-2">Date</div>
          <div className="col-span-2">Sujet</div>
          <div className="col-span-1">Qualité</div>
          <div className="col-span-1">Signal</div>
        </div>

        <div className="max-h-[32rem] overflow-y-auto">
          {isLoading && (
            <div className="space-y-2 p-4">
              {Array.from({ length: 6 }).map((_, i) => (
                <div
                  key={i}
                  className="h-12 animate-pulse rounded-lg bg-border"
                />
              ))}
            </div>
          )}

          {!isLoading && filtered.length === 0 && (
            <div className="flex flex-col items-center justify-center py-16">
              <Filter className="h-8 w-8 text-muted" />
              <p className="mt-3 text-sm text-muted">Aucun article trouvé</p>
            </div>
          )}

          {filtered.map((article, idx) => (
            <motion.div
              key={article.id ?? `article-${idx}`}
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              onClick={() => setSelectedArticle(article)}
              className="grid cursor-pointer grid-cols-12 gap-4 border-b border-border px-5 py-3 transition-colors hover:bg-surface-hover"
            >
              <div className="col-span-4 truncate text-sm font-medium text-foreground">
                {article.titre || "Sans titre"}
              </div>
              <div className="col-span-2 text-sm text-muted">
                {article.source}
              </div>
              <div className="col-span-2 text-xs text-muted">
                {article.date
                  ? new Date(article.date).toLocaleDateString("fr-FR", { day: "2-digit", month: "short", year: "2-digit" })
                  : "—"}
              </div>
              <div className="col-span-2 truncate text-sm text-muted">
                {article.topic || "—"}
              </div>
              <div className="col-span-1">
                <span
                  className={`rounded-full px-2 py-0.5 text-[10px] font-medium uppercase ${
                    article.quality === "OK"
                      ? "bg-success/10 text-success"
                      : article.quality === "UNKNOWN"
                      ? "bg-muted/10 text-muted"
                      : "bg-danger/10 text-danger"
                  }`}
                >
                  {article.quality}
                </span>
              </div>
              <div className="col-span-1 text-sm tabular-nums text-muted">
                {article.poly_prob != null
                  ? `${(article.poly_prob * 100).toFixed(0)}%`
                  : "—"}
              </div>
            </motion.div>
          ))}
        </div>
      </div>

      <AnimatePresence>
        {selectedArticle && (
          <motion.div
            key="backdrop"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-50 flex justify-end bg-black/50 backdrop-blur-sm"
            onClick={() => setSelectedArticle(null)}
          >
            <motion.div
              key={selectedArticle.id}
              initial={{ x: "100%" }}
              animate={{ x: 0 }}
              exit={{ x: "100%" }}
              transition={{ type: "spring", damping: 25, stiffness: 200 }}
              onClick={(e: React.MouseEvent<HTMLDivElement>) => e.stopPropagation()}
              className="h-full w-full max-w-lg overflow-y-auto border-l border-border bg-surface p-6 shadow-2xl"
            >
              <div className="mb-6 flex items-center justify-between">
                <h3 className="text-lg font-bold text-foreground">
                  Détail article
                </h3>
                <button
                  onClick={() => setSelectedArticle(null)}
                  className="rounded-lg p-1 text-muted hover:bg-surface-hover hover:text-foreground"
                >
                  <X className="h-5 w-5" />
                </button>
              </div>

              <div className="space-y-4">
                <div>
                  <p className="text-xs uppercase tracking-wider text-muted">
                    Titre
                  </p>
                  <p className="mt-1 text-sm font-medium text-foreground">
                    {selectedArticle.titre}
                  </p>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <p className="text-xs uppercase tracking-wider text-muted">
                      Source
                    </p>
                    <p className="mt-1 text-sm text-foreground">
                      {selectedArticle.source}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs uppercase tracking-wider text-muted">
                      Langue
                    </p>
                    <p className="mt-1 text-sm text-foreground">
                      {selectedArticle.langue}
                    </p>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <p className="text-xs uppercase tracking-wider text-muted">
                      Date
                    </p>
                    <p className="mt-1 text-sm text-foreground">
                      {selectedArticle.date
                        ? new Date(selectedArticle.date).toLocaleString("fr-FR", { dateStyle: "medium", timeStyle: "short" })
                        : "—"}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs uppercase tracking-wider text-muted">
                      Qualité
                    </p>
                    <p className="mt-1 text-sm text-foreground">
                      {selectedArticle.quality}
                    </p>
                  </div>
                </div>

                <div>
                  <p className="text-xs uppercase tracking-wider text-muted">
                    Sujet
                  </p>
                  <p className="mt-1 text-sm text-foreground">
                    {selectedArticle.topic || "—"}
                  </p>
                </div>

                {selectedArticle.poly_prob != null && (
                  <div>
                    <p className="text-xs uppercase tracking-wider text-muted">
                      Signal Polymarket
                    </p>
                    <div className="mt-1 flex items-center gap-2">
                      <div className="h-2 flex-1 rounded-full bg-border">
                        <div
                          className="h-2 rounded-full bg-accent"
                          style={{
                            width: `${selectedArticle.poly_prob * 100}%`,
                          }}
                        />
                      </div>
                      <span className="text-sm font-medium text-foreground">
                        {(selectedArticle.poly_prob * 100).toFixed(1)}%
                      </span>
                    </div>
                    {selectedArticle.poly_q && (
                      <p className="mt-1 text-xs text-muted">
                        {selectedArticle.poly_q}
                      </p>
                    )}
                  </div>
                )}

                {selectedArticle.url && (
                  <a
                    href={selectedArticle.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="inline-flex items-center gap-2 rounded-lg bg-[var(--accent-subtle)] px-4 py-2 text-sm font-medium text-accent hover:bg-surface-hover"
                  >
                    <ExternalLink className="h-4 w-4" />
                    Voir l'article
                  </a>
                )}

                <div className="rounded-lg border border-border-subtle bg-background p-3">
                  <div className="flex items-center gap-2 text-xs text-muted">
                    <Shield className="h-3 w-3" />
                    <span>Couche : {selectedArticle.quality === "OK" ? "Gold" : "Silver"}</span>
                  </div>
                </div>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
