"""
dashboard/app.py
----------------
Dashboard Streamlit — News Pipeline Phase 4

Visualise :
1. Métriques clés (articles, sources, topics, signaux Polymarket)
2. Volume de couverture par topic (bar chart)
3. Signal Polymarket par topic (gauge + table)
4. Répartition par langue / pays / source (pie chart)
5. Timeline de publication (line chart)
6. Rapport qualité (donut chart)

Usage :
  cd news-pipeline
  streamlit run dashboard/app.py
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta

# Ajoute la racine du projet au path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from datalake.silver_processor import SilverProcessor
from datalake.gold_aggregator import GoldAggregator

# ============================================================
# CONFIG PAGE
# ============================================================
st.set_page_config(
    page_title="News Intelligence Dashboard",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# CSS PERSONNALISÉ
# ============================================================
st.markdown("""
<style>
    /* Police et fond */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    
    /* Header principal */
    .main-header {
        background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
        padding: 2rem 2rem 1.5rem 2rem;
        border-radius: 16px;
        margin-bottom: 1.5rem;
        text-align: center;
        box-shadow: 0 8px 32px rgba(48, 43, 99, 0.4);
    }
    .main-header h1 {
        color: #fff;
        font-size: 2.2rem;
        font-weight: 700;
        margin: 0;
        letter-spacing: -0.5px;
    }
    .main-header p {
        color: rgba(255,255,255,0.7);
        margin: 0.5rem 0 0 0;
        font-size: 1rem;
    }
    
    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(135deg, #1a1a2e, #16213e);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.3);
    }
    
    /* Metric delta */
    [data-testid="stMetricDelta"] {
        font-size: 0.8rem !important;
    }
    
    /* Section headers */
    .section-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #e2e8f0;
        margin-bottom: 0.8rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #4f46e5;
        display: inline-block;
    }
    
    /* Polymarket signal badge */
    .signal-high   { color: #22c55e; font-weight: 600; }
    .signal-medium { color: #f59e0b; font-weight: 600; }
    .signal-low    { color: #ef4444; font-weight: 600; }
    
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0c29, #1a1a2e);
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# SIDEBAR — Filtres et configuration
# ============================================================
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/news.png", width=64)
    st.title("⚙️ Configuration")
    st.markdown("---")

    selected_date = st.date_input(
        "📅 Date des données",
        value=datetime.utcnow().date(),
        max_value=datetime.utcnow().date(),
    )

    sources_options = ["hespress", "bbc", "gdelt"]
    selected_sources = st.multiselect(
        "📡 Sources",
        options=sources_options,
        default=sources_options,
    )

    min_articles = st.slider(
        "🔢 Nb minimum d'articles par topic",
        min_value=1, max_value=20, value=2
    )

    show_fail = st.checkbox("Afficher les articles FAIL qualité", value=False)

    st.markdown("---")
    st.markdown("""
    **Couches de données**
    - 🟤 **Bronze** : Brut
    - ⚪ **Silver** : Nettoyé
    - 🟡 **Gold** : Enrichi BERTopic + Polymarket
    """)

# ============================================================
# CHARGEMENT DES DONNÉES
# ============================================================
@st.cache_data(ttl=300, show_spinner="Chargement des données Silver...")
def load_data(date_str: str, sources: list[str]) -> pd.DataFrame:
    """Charge et concatène les données Silver pour les sources sélectionnées."""
    processor = SilverProcessor(silver_root="data/silver")
    frames = []

    for source in sources:
        df = processor.load(source=source, date=date_str)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    if "date_publication" in combined.columns:
        combined["date_publication"] = pd.to_datetime(combined["date_publication"], errors="coerce")
    return combined


@st.cache_data(ttl=300, show_spinner="Chargement Gold...")
def load_gold(date_str: str) -> pd.DataFrame:
    """Charge les données Gold (avec topics et signaux Polymarket)."""
    aggregator = GoldAggregator(gold_root="data/gold")
    return aggregator.load(date=date_str)


# ============================================================
# HEADER
# ============================================================
st.markdown("""
<div class="main-header">
    <h1>📰 News Intelligence Dashboard</h1>
    <p>Pipeline multilingue · BERTopic · Signaux Polymarket · GDELT</p>
</div>
""", unsafe_allow_html=True)

date_str = selected_date.strftime("%Y-%m-%d")

# Chargement
silver_df = load_data(date_str, selected_sources)
gold_df = load_gold(date_str)

# Utilise Silver si Gold pas disponible
working_df = gold_df if not gold_df.empty else silver_df

if working_df.empty:
    st.warning(
        f"⚠️ Aucune donnée disponible pour le **{date_str}** avec les sources sélectionnées.\n\n"
        "Lancez d'abord le pipeline :\n```\npython -X utf8 test_phase1.py --source bbc\n```"
    )
    st.stop()

# Filtres sidebar appliqués
if "source" in working_df.columns and selected_sources:
    working_df = working_df[working_df["source"].str.replace(".co.uk", "").str.replace(".com", "").isin(
        [s.replace("hespress", "hespress").replace("bbc", "bbc") for s in selected_sources]
    ) | working_df["raw_source"].str.contains("|".join(selected_sources), case=False, na=False)]

if not show_fail and "quality_status" in working_df.columns:
    working_df = working_df[working_df["quality_status"] == "OK"]

# ============================================================
# KPI ROW 1 — Métriques principales
# ============================================================
st.markdown('<p class="section-title">📊 Métriques Clés</p>', unsafe_allow_html=True)

col1, col2, col3, col4, col5 = st.columns(5)

total_articles = len(working_df)
n_sources = working_df["source"].nunique() if "source" in working_df.columns else 0
n_langs = working_df["langue"].nunique() if "langue" in working_df.columns else 0
n_topics = (
    working_df["topic_id"].nunique() - (1 if -1 in working_df.get("topic_id", pd.Series()).values else 0)
    if "topic_id" in working_df.columns else 0
)
quality_ok_pct = (
    round((working_df["quality_status"] == "OK").mean() * 100, 1)
    if "quality_status" in working_df.columns else 100
)

with col1:
    st.metric("📰 Articles", f"{total_articles:,}", delta=None)

with col2:
    st.metric("📡 Sources", f"{n_sources}", delta=None)

with col3:
    st.metric("🌍 Langues", f"{n_langs}", delta=None)

with col4:
    st.metric("🧠 Topics BERTopic", f"{n_topics}", delta=None)

with col5:
    st.metric("✅ Qualité OK", f"{quality_ok_pct}%",
              delta=f"{quality_ok_pct - 80:.1f}% vs seuil 80%",
              delta_color="normal")

st.markdown("---")

# ============================================================
# ROW 2 — Couverture par topic + Signal Polymarket
# ============================================================
col_left, col_right = st.columns([3, 2])

with col_left:
    st.markdown('<p class="section-title">🧠 Couverture par Topic BERTopic</p>', unsafe_allow_html=True)

    if "topic_label" in working_df.columns:
        topic_counts = (
            working_df.groupby("topic_label")
            .size()
            .reset_index(name="articles")
            .sort_values("articles", ascending=False)
            .head(15)
        )
        topic_counts = topic_counts[topic_counts["articles"] >= min_articles]

        if not topic_counts.empty:
            fig_topics = px.bar(
                topic_counts,
                x="articles",
                y="topic_label",
                orientation="h",
                color="articles",
                color_continuous_scale="Viridis",
                labels={"articles": "Articles", "topic_label": "Topic"},
                template="plotly_dark",
            )
            fig_topics.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
                coloraxis_showscale=False,
                margin=dict(l=0, r=0, t=10, b=0),
                height=380,
            )
            st.plotly_chart(fig_topics, use_container_width=True)
        else:
            st.info("Pas encore de topics BERTopic — lancez le pipeline complet avec BERTopic activé.")
    else:
        # Fallback : couverture par source
        if "source" in working_df.columns:
            src_counts = working_df["source"].value_counts().reset_index()
            src_counts.columns = ["source", "articles"]
            fig = px.bar(src_counts, x="articles", y="source", orientation="h",
                         color="articles", color_continuous_scale="Blues",
                         template="plotly_dark")
            fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                              height=300, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.markdown('<p class="section-title">🎯 Signaux Polymarket</p>', unsafe_allow_html=True)

    if "polymarket_prob" in working_df.columns and working_df["polymarket_prob"].notna().any():
        # Top 5 topics avec signal Polymarket
        poly_data = (
            working_df[working_df["polymarket_prob"].notna()]
            .groupby("topic_label")
            .agg(
                articles=("topic_label", "count"),
                avg_prob=("polymarket_prob", "mean"),
                question=("polymarket_question", "first"),
            )
            .reset_index()
            .sort_values("avg_prob", ascending=False)
            .head(5)
        )

        for _, row in poly_data.iterrows():
            prob = row["avg_prob"]
            color = "#22c55e" if prob >= 0.6 else ("#f59e0b" if prob >= 0.4 else "#ef4444")
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=round(prob * 100, 1),
                number={"suffix": "%", "font": {"color": color, "size": 28}},
                gauge={
                    "axis": {"range": [0, 100], "tickcolor": "gray"},
                    "bar": {"color": color},
                    "bgcolor": "rgba(255,255,255,0.05)",
                    "steps": [
                        {"range": [0, 40], "color": "rgba(239,68,68,0.1)"},
                        {"range": [40, 60], "color": "rgba(245,158,11,0.1)"},
                        {"range": [60, 100], "color": "rgba(34,197,94,0.1)"},
                    ],
                },
                title={"text": row["topic_label"][:30], "font": {"size": 12, "color": "lightgray"}},
            ))
            fig_gauge.update_layout(
                height=160, paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=30, b=0)
            )
            st.plotly_chart(fig_gauge, use_container_width=True)
            if row["question"]:
                st.caption(f"🏦 *{str(row['question'])[:80]}...*")
    else:
        # Métriques de démonstration style Streamlit
        st.metric(
            label="Signal exemple — Gaza ceasefire",
            value="68%",
            delta="+5pts vs hier",
            help="Marché Polymarket : 'Will a ceasefire be announced this month?'"
        )
        st.metric(
            label="Signal exemple — US Economy recession",
            value="42%",
            delta="-3pts vs hier",
            help="Marché Polymarket : 'Will US enter recession in 2025?'"
        )
        st.info("💡 Les signaux réels apparaissent après l'exécution du pipeline Gold avec BERTopic activé.")

st.markdown("---")

# ============================================================
# ROW 3 — Répartition géographique + Timeline
# ============================================================
col3a, col3b = st.columns(2)

with col3a:
    st.markdown('<p class="section-title">🌍 Répartition par Langue & Source</p>', unsafe_allow_html=True)

    if "langue" in working_df.columns:
        lang_counts = working_df["langue"].value_counts().reset_index()
        lang_counts.columns = ["langue", "articles"]
        lang_map = {"fr": "🇫🇷 Français", "en": "🇬🇧 English", "ar": "🇲🇦 Arabe", "unknown": "❓ Inconnu"}
        lang_counts["langue"] = lang_counts["langue"].map(lambda x: lang_map.get(x, x))

        fig_lang = px.pie(
            lang_counts, values="articles", names="langue",
            color_discrete_sequence=px.colors.qualitative.Bold,
            template="plotly_dark",
        )
        fig_lang.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=0, b=0), height=280,
        )
        st.plotly_chart(fig_lang, use_container_width=True)

with col3b:
    st.markdown('<p class="section-title">📈 Volume de publications (timeline)</p>', unsafe_allow_html=True)

    if "date_publication" in working_df.columns and working_df["date_publication"].notna().any():
        timeline_df = (
            working_df.dropna(subset=["date_publication"])
            .set_index("date_publication")
            .resample("1H")
            .size()
            .reset_index(name="articles")
        )
        timeline_df.columns = ["heure", "articles"]

        fig_time = px.area(
            timeline_df, x="heure", y="articles",
            labels={"heure": "Heure (UTC)", "articles": "Articles"},
            color_discrete_sequence=["#6366f1"],
            template="plotly_dark",
        )
        fig_time.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=10, b=0), height=280,
        )
        st.plotly_chart(fig_time, use_container_width=True)
    else:
        st.info("Dates de publication non disponibles dans les données actuelles.")

st.markdown("---")

# ============================================================
# ROW 4 — Rapport Qualité + Table interactive
# ============================================================
col4a, col4b = st.columns([1, 2])

with col4a:
    st.markdown('<p class="section-title">🔍 Rapport Qualité Silver</p>', unsafe_allow_html=True)

    if "quality_status" in working_df.columns:
        qual_counts = working_df["quality_status"].value_counts().reset_index()
        qual_counts.columns = ["statut", "count"]

        fig_qual = px.pie(
            qual_counts, values="count", names="statut",
            color="statut",
            color_discrete_map={"OK": "#22c55e", "FAIL": "#ef4444"},
            template="plotly_dark", hole=0.55,
        )
        fig_qual.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=0, b=0), height=250,
        )
        st.plotly_chart(fig_qual, use_container_width=True)

        fail_df = working_df[working_df["quality_status"] == "FAIL"]
        if not fail_df.empty:
            st.caption(f"**{len(fail_df)} articles FAIL** — raisons :")
            flags_flat = fail_df["quality_flags"].explode().value_counts()
            for flag, count in flags_flat.items():
                st.caption(f"• `{flag}` : {count}×")

with col4b:
    st.markdown('<p class="section-title">📋 Articles récents</p>', unsafe_allow_html=True)

    display_cols = [c for c in ["titre_clean", "source", "langue", "date_publication",
                                "topic_label", "polymarket_prob_pct", "quality_status"]
                   if c in working_df.columns]

    display_df = working_df[display_cols].head(50).copy()
    if "date_publication" in display_df.columns:
        display_df["date_publication"] = display_df["date_publication"].astype(str).str[:16]

    st.dataframe(
        display_df,
        use_container_width=True,
        height=300,
        column_config={
            "polymarket_prob_pct": st.column_config.TextColumn("📊 Marché"),
            "quality_status": st.column_config.TextColumn("✓ Qualité"),
        }
    )

# ============================================================
# FOOTER
# ============================================================
st.markdown("---")
st.markdown(
    f"""
    <div style="text-align:center; color: rgba(255,255,255,0.4); font-size:0.8rem;">
        News Intelligence Pipeline · Phase 1→4 · Données au {date_str} ·
        {total_articles:,} articles · BERTopic + Polymarket + GDELT
    </div>
    """,
    unsafe_allow_html=True,
)
