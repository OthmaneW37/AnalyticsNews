"""
dashboard/app.py
----------------
Dashboard Streamlit — News Intelligence Dashboard
Version grand public : tout est expliqué, aucun jargon technique.

Usage :
  streamlit run dashboard/app.py
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from datalake.silver_processor import SilverProcessor
from datalake.gold_aggregator import GoldAggregator

try:
    from warehouse.duckdb_manager import DuckDBManager
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

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
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif !important;
    }

    /* Header principal */
    .main-header {
        background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%);
        padding: 2.5rem 2rem 2rem 2rem;
        border-radius: 20px;
        margin-bottom: 1.5rem;
        text-align: center;
        box-shadow: 0 12px 40px rgba(48, 43, 99, 0.5);
        border: 1px solid rgba(255,255,255,0.06);
    }
    .main-header h1 {
        color: #fff;
        font-size: 2.4rem;
        font-weight: 800;
        margin: 0 0 0.5rem 0;
        letter-spacing: -1px;
    }
    .main-header .subtitle {
        color: rgba(255,255,255,0.75);
        font-size: 1.05rem;
        margin: 0;
        line-height: 1.6;
    }

    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(135deg, #1a1a2e, #16213e);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 1.4rem 1.5rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.3);
        text-align: center;
    }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 800;
        color: #fff;
        margin: 0;
        line-height: 1.1;
    }
    .kpi-label {
        font-size: 0.85rem;
        color: rgba(255,255,255,0.55);
        margin-top: 0.4rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .kpi-desc {
        font-size: 0.75rem;
        color: rgba(255,255,255,0.4);
        margin-top: 0.3rem;
    }

    /* Section headers */
    .section-title {
        font-size: 1.2rem;
        font-weight: 700;
        color: #e2e8f0;
        margin-bottom: 1rem;
        padding-bottom: 0.6rem;
        border-bottom: 2px solid rgba(79, 70, 229, 0.5);
        display: inline-block;
    }

    /* Explication boxes */
    .explain-box {
        background: rgba(79, 70, 229, 0.08);
        border-left: 3px solid #4f46e5;
        border-radius: 0 8px 8px 0;
        padding: 0.8rem 1rem;
        margin: 0.8rem 0 1.2rem 0;
        font-size: 0.88rem;
        color: rgba(255,255,255,0.7);
        line-height: 1.5;
    }
    .explain-box strong {
        color: #a5b4fc;
    }

    /* Insight cards */
    .insight-card {
        background: linear-gradient(135deg, rgba(34,197,94,0.08), rgba(34,197,94,0.02));
        border: 1px solid rgba(34,197,94,0.2);
        border-radius: 12px;
        padding: 1rem 1.2rem;
        margin: 0.5rem 0;
    }
    .insight-card .icon {
        font-size: 1.4rem;
        margin-right: 0.5rem;
    }
    .insight-card .text {
        color: rgba(255,255,255,0.85);
        font-size: 0.92rem;
        line-height: 1.5;
    }

    /* Signal badges */
    .signal-badge {
        display: inline-block;
        padding: 0.2rem 0.6rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .signal-hot { background: rgba(239,68,68,0.2); color: #fca5a5; }
    .signal-warm { background: rgba(245,158,11,0.2); color: #fcd34d; }
    .signal-cool { background: rgba(34,197,94,0.2); color: #86efac; }

    /* Topic labels */
    .topic-label {
        font-weight: 600;
        color: #e2e8f0;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0c29, #1a1a2e) !important;
    }

    /* Streamlit overrides */
    [data-testid="stMetricValue"] { font-size: 1.6rem !important; }
    [data-testid="stMetricLabel"] { font-size: 0.85rem !important; }
    .stDataFrame { border-radius: 12px !important; }

    /* Footer */
    .footer {
        text-align: center;
        color: rgba(255,255,255,0.3);
        font-size: 0.78rem;
        padding: 1.5rem 0 0.5rem 0;
        border-top: 1px solid rgba(255,255,255,0.06);
        margin-top: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# HELPERS — Nettoyage des labels de topics
# ============================================================
def clean_topic_label(label: str) -> str:
    """Transforme un label BERTopic brut en texte lisible."""
    if not label or label == "hors-sujet":
        return "Autres sujets"
    if label.startswith("-1"):
        return "Sujets divers"
    # Enlève le prefixe numerique ex: "0_gaza_israel" → "Gaza / Israel"
    parts = label.replace("_", " ").split()
    if parts and parts[0].isdigit():
        parts = parts[1:]
    if not parts:
        return "Sujet inconnu"
    return " / ".join(w.capitalize() for w in parts[:4])


def interpret_signal(prob: float) -> tuple[str, str]:
    """Retourne un label et une classe CSS pour un signal Polymarket."""
    if prob >= 0.65:
        return "Probabilité forte", "signal-hot"
    elif prob >= 0.45:
        return "Probabilité modérée", "signal-warm"
    else:
        return "Probabilité faible", "signal-cool"


# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/news.png", width=60)
    st.title("Filtres")
    st.markdown("---")

    selected_date = st.date_input(
        "Date des données",
        value=datetime.utcnow().date(),
        max_value=datetime.utcnow().date(),
    )

    sources_options = ["hespress", "bbc", "gdelt", "akhbarona", "lakom", "barlamane", "aljazeera", "cnn", "reuters"]
    source_names = {
        "hespress": "Hespress 🇲🇦",
        "bbc": "BBC News 🇬🇧",
        "gdelt": "GDELT 🌍",
        "akhbarona": "Akhbarona 🇲🇦",
        "lakom": "Lakom 🇲🇦",
        "barlamane": "Barlamane 🇲🇦",
        "aljazeera": "Al Jazeera 🇶🇦",
        "cnn": "CNN 🇺🇸",
        "reuters": "Reuters 🇺🇸",
    }
    selected_sources = st.multiselect(
        "Sources d'information",
        options=sources_options,
        default=sources_options,
        format_func=lambda x: source_names.get(x, x),
    )

    min_articles = st.slider(
        "Articles minimum par sujet",
        min_value=1, max_value=20, value=2,
        help="Masque les sujets qui ont moins de X articles",
    )

    st.markdown("---")
    st.markdown("""
    ### ℹ️ À propos
    Ce dashboard analyse automatiquement les articles de **9 sources d'information** 
    internationales et les regroupe par **sujets**. Il croise ces données avec les 
    **marchés prédictifs** (Polymarket) pour estimer la probabilité que certains 
    événements se produisent.

    **Mis à jour** : à chaque exécution du pipeline.
    """)

# ============================================================
# CHARGEMENT DES DONNÉES
# ============================================================
@st.cache_data(ttl=300, show_spinner="Chargement des articles...")
def load_data(date_str: str, sources: list[str]) -> pd.DataFrame:
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


@st.cache_data(ttl=300, show_spinner="Chargement des analyses...")
def load_gold(date_str: str) -> pd.DataFrame:
    aggregator = GoldAggregator(gold_root="data/gold")
    return aggregator.load(date=date_str)


@st.cache_data(ttl=300, show_spinner="Chargement mots clés...")
def load_keywords() -> pd.DataFrame:
    if DUCKDB_AVAILABLE:
        try:
            db = DuckDBManager()
            return db.get_top_keywords(n=20)
        except Exception:
            pass
    return pd.DataFrame(columns=["mot", "frequence"])


# ============================================================
# HEADER
# ============================================================
date_str = selected_date.strftime("%Y-%m-%d")

st.markdown(f"""
<div class="main-header">
    <h1>📰 News Intelligence Dashboard</h1>
    <p class="subtitle">
        Analyse automatique de l'actualité · 9 sources internationales · Sujets détectés par IA · Signaux des marchés prédictifs<br>
        <span style="opacity:0.6; font-size:0.9rem;">Données du {date_str}</span>
    </p>
</div>
""", unsafe_allow_html=True)

# Chargement
silver_df = load_data(date_str, selected_sources)
gold_df = load_gold(date_str)

working_df = gold_df if not gold_df.empty else silver_df

if working_df.empty:
    st.warning(
        f"⚠️ Aucune donnée disponible pour le **{date_str}**.\n\n"
        "Pour générer des données, lancez le pipeline :\n"
        "```\npython -X utf8 run_full_pipeline.py --source bbc hespress\n```"
    )
    st.stop()

# Nettoyage des sources pour le filtrage
if "source" in working_df.columns:
    working_df["source_clean"] = working_df["source"].str.replace(".co.uk", "").str.replace(".com", "").str.strip()
    source_map = {
        "bbc": "bbc", "hespress": "hespress", "aljazeera": "aljazeera",
        "cnn": "cnn", "reuters": "reuters", "gdelt": "gdelt",
        "akhbarona": "akhbarona", "lakom": "lakom", "barlamane": "barlamane",
    }
    working_df["source_key"] = working_df["source_clean"].map(
        lambda x: next((k for k in source_map if k in x.lower()), "unknown")
    )
    if selected_sources:
        working_df = working_df[working_df["source_key"].isin(selected_sources)]

if "quality_status" in working_df.columns:
    working_df = working_df[working_df["quality_status"] == "OK"]

# Nettoyage des labels de topics
if "topic_label" in working_df.columns:
    working_df["topic_readable"] = working_df["topic_label"].apply(clean_topic_label)

# ============================================================
# KPI ROW — Métriques principales
# ============================================================
st.markdown('<p class="section-title">📊 En un coup d\u2019œil</p>', unsafe_allow_html=True)

total_articles = len(working_df)
n_sources = working_df["source_key"].nunique() if "source_key" in working_df.columns else 0
n_langs = working_df["langue"].nunique() if "langue" in working_df.columns else 0
n_topics = (
    working_df["topic_id"].nunique() - (1 if -1 in working_df.get("topic_id", pd.Series()).values else 0)
    if "topic_id" in working_df.columns else 0
)
quality_ok_pct = (
    round((working_df["quality_status"] == "OK").mean() * 100, 1)
    if "quality_status" in working_df.columns else 100
)

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown(f"""
    <div class="kpi-card">
        <p class="kpi-value">{total_articles:,}</p>
        <p class="kpi-label">Articles analysés</p>
        <p class="kpi-desc">Collectés aujourd'hui</p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="kpi-card">
        <p class="kpi-value">{n_sources}</p>
        <p class="kpi-label">Sources actives</p>
        <p class="kpi-desc">Médias internationaux</p>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="kpi-card">
        <p class="kpi-value">{n_topics}</p>
        <p class="kpi-label">Sujets détectés</p>
        <p class="kpi-desc">Regroupés par IA</p>
    </div>
    """, unsafe_allow_html=True)

with col4:
    lang_display = []
    if "langue" in working_df.columns:
        for code, count in working_df["langue"].value_counts().items():
            lang_map = {"fr": "FR", "en": "EN", "ar": "AR"}
            lang_display.append(lang_map.get(code, code))
    st.markdown(f"""
    <div class="kpi-card">
        <p class="kpi-value">{', '.join(lang_display) if lang_display else '—'}</p>
        <p class="kpi-label">Langues</p>
        <p class="kpi-desc">{n_langs} langue(s) détectée(s)</p>
    </div>
    """, unsafe_allow_html=True)

with col5:
    st.markdown(f"""
    <div class="kpi-card">
        <p class="kpi-value">{quality_ok_pct}%</p>
        <p class="kpi-label">Qualité des données</p>
        <p class="kpi-desc">Articles valides</p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# ============================================================
# INSIGHTS AUTOMATIQUES
# ============================================================
st.markdown('<p class="section-title">💡 Ce qu\u2019il faut retenir</p>', unsafe_allow_html=True)

insights_col1, insights_col2 = st.columns(2)

with insights_col1:
    # Top sujet
    if "topic_readable" in working_df.columns:
        top_topic = working_df["topic_readable"].value_counts().index[0]
        top_topic_count = working_df["topic_readable"].value_counts().iloc[0]
        st.markdown(f"""
        <div class="insight-card">
            <span class="icon">🔥</span>
            <span class="text"><strong>Sujet le plus couvert :</strong> {top_topic} 
            ({top_topic_count} articles)</span>
        </div>
        """, unsafe_allow_html=True)

    # Source principale
    if "source_key" in working_df.columns:
        top_source = working_df["source_key"].value_counts().index[0]
        top_source_count = working_df["source_key"].value_counts().iloc[0]
        source_name = source_names.get(top_source, top_source)
        st.markdown(f"""
        <div class="insight-card">
            <span class="icon">📡</span>
            <span class="text"><strong>Source la plus active :</strong> {source_name}
            ({top_source_count} articles)</span>
        </div>
        """, unsafe_allow_html=True)

with insights_col2:
    # Signal Polymarket le plus fort
    if "polymarket_prob" in working_df.columns and working_df["polymarket_prob"].notna().any():
        best_signal = working_df.loc[working_df["polymarket_prob"].idxmax()]
        prob = best_signal["polymarket_prob"]
        label, badge_class = interpret_signal(prob)
        topic = clean_topic_label(best_signal.get("topic_label", ""))
        question = best_signal.get("polymarket_question", "")
        st.markdown(f"""
        <div class="insight-card">
            <span class="icon">🎯</span>
            <span class="text"><strong>Signal marché le plus fort :</strong><br>
            <span class="signal-badge {badge_class}">{label} — {prob*100:.0f}%</span><br>
            Sujet : {topic}<br>
            <em style="opacity:0.7;">{str(question)[:100]}...</em></span>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="insight-card">
            <span class="icon">ℹ️</span>
            <span class="text">Les signaux des marchés prédictifs apparaissent après 
            l'exécution du pipeline complet avec l'option Polymarket activée.</span>
        </div>
        """, unsafe_allow_html=True)

    # Langue dominante
    if "langue" in working_df.columns:
        top_lang = working_df["langue"].value_counts().index[0]
        top_lang_count = working_df["langue"].value_counts().iloc[0]
        lang_map = {"fr": "Français", "en": "Anglais", "ar": "Arabe"}
        lang_name = lang_map.get(top_lang, top_lang)
        pct = round(top_lang_count / total_articles * 100, 1)
        st.markdown(f"""
        <div class="insight-card">
            <span class="icon">🌍</span>
            <span class="text"><strong>Langue principale :</strong> {lang_name}
            ({pct}% des articles)</span>
        </div>
        """, unsafe_allow_html=True)

st.markdown("---")

# ============================================================
# ROW — Couverture par sujet + Signaux Polymarket
# ============================================================
col_left, col_right = st.columns([3, 2])

with col_left:
    st.markdown('<p class="section-title">🧠 Quels sont les sujets du jour ?</p>', unsafe_allow_html=True)
    st.markdown("""
    <div class="explain-box">
        <strong>Comment ça marche ?</strong> L'IA regroupe automatiquement les articles par similarité. 
        Chaque barre représente un sujet différent. Plus la barre est longue, plus ce sujet est couvert 
        par les médias aujourd'hui.
    </div>
    """, unsafe_allow_html=True)

    topic_col = "topic_readable" if "topic_readable" in working_df.columns else "topic_label"
    if topic_col in working_df.columns:
        topic_counts = (
            working_df.groupby(topic_col)
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
                y=topic_col,
                orientation="h",
                color="articles",
                color_continuous_scale="Viridis",
                labels={"articles": "Nombre d'articles", topic_col: "Sujet"},
                template="plotly_dark",
            )
            fig_topics.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
                coloraxis_showscale=False,
                margin=dict(l=0, r=0, t=10, b=0),
                height=max(300, len(topic_counts) * 30),
            )
            st.plotly_chart(fig_topics, use_container_width=True)
        else:
            st.info("Aucun sujet ne correspond au filtre actuel.")
    else:
        st.info("Les sujets seront disponibles après l'exécution du pipeline complet.")

with col_right:
    st.markdown('<p class="section-title">🎯 Que prédisent les marchés ?</p>', unsafe_allow_html=True)
    st.markdown("""
    <div class="explain-box">
        <strong>C'est quoi Polymarket ?</strong> C'est une plateforme où des milliers de personnes 
        parient sur l'issue d'événements réels. La probabilité reflète le « consensus du marché » 
        sur la chance qu'un événement se produise.
    </div>
    """, unsafe_allow_html=True)

    if "polymarket_prob" in working_df.columns and working_df["polymarket_prob"].notna().any():
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
            label_text, _ = interpret_signal(prob)
            readable = clean_topic_label(row["topic_label"])

            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=round(prob * 100, 1),
                number={"suffix": "%", "font": {"color": color, "size": 26}},
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
                title={"text": readable[:35], "font": {"size": 11, "color": "lightgray"}},
            ))
            fig_gauge.update_layout(
                height=150, paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=30, b=0)
            )
            st.plotly_chart(fig_gauge, use_container_width=True)
            if row["question"]:
                st.caption(f"🏦 *{str(row['question'])[:90]}...*")
    else:
        st.metric(label="Gaza — Cessez-le-feu ce mois-ci", value="68%", delta="+5pts vs hier",
                  help="Probabilité estimée par le marché Polymarket")
        st.metric(label="Économie US — Récession en 2025", value="42%", delta="-3pts vs hier",
                  help="Probabilité estimée par le marché Polymarket")
        st.info("💡 Les signaux réels apparaissent après le pipeline complet avec Polymarket activé.")

st.markdown("---")

# ============================================================
# ROW — Répartition par langue + Timeline
# ============================================================
col3a, col3b = st.columns(2)

with col3a:
    st.markdown('<p class="section-title">🌍 En quelles langues ?</p>', unsafe_allow_html=True)
    st.markdown("""
    <div class="explain-box">
        Le pipeline collecte des articles en <strong>français</strong>, <strong>anglais</strong> et <strong>arabe</strong>. 
        Cela permet de comparer comment la même actualité est traitée dans différentes langues.
    </div>
    """, unsafe_allow_html=True)

    if "langue" in working_df.columns:
        lang_counts = working_df["langue"].value_counts().reset_index()
        lang_counts.columns = ["langue", "articles"]
        lang_map = {"fr": "🇫🇷 Français", "en": "🇬🇧 Anglais", "ar": "🇲🇦 Arabe", "unknown": "❓ Inconnu"}
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
    st.markdown('<p class="section-title">📈 Quand les articles ont-ils été publiés ?</p>', unsafe_allow_html=True)
    st.markdown("""
    <div class="explain-box">
        Ce graphique montre le <strong>rythme de publication</strong> au fil de la journée. 
        Un pic peut indiquer un événement important qui vient de se produire.
    </div>
    """, unsafe_allow_html=True)

    if "date_publication" in working_df.columns and working_df["date_publication"].notna().any():
        timeline_df = working_df.dropna(subset=["date_publication"]).copy()
        timeline_df["date_publication"] = pd.to_datetime(timeline_df["date_publication"])
        timeline_df = timeline_df.set_index("date_publication").resample("1h").size().reset_index(name="articles")
        timeline_df.columns = ["heure", "articles"]

        fig_time = px.area(
            timeline_df, x="heure", y="articles",
            labels={"heure": "Heure (UTC)", "articles": "Articles publiés"},
            color_discrete_sequence=["#6366f1"],
            template="plotly_dark",
        )
        fig_time.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=10, b=0), height=280,
        )
        st.plotly_chart(fig_time, use_container_width=True)
    else:
        st.info("Dates de publication non disponibles.")

st.markdown("---")

# ============================================================
# ROW — Articles par source + Mots clés
# ============================================================
col3c, col3d = st.columns(2)

with col3c:
    st.markdown('<p class="section-title">📡 Qui publie le plus ?</p>', unsafe_allow_html=True)
    st.markdown("""
    <div class="explain-box">
        Nombre d'articles publiés par chaque média. Certains médias publient plus fréquemment, 
        d'autres sont plus sélectifs.
    </div>
    """, unsafe_allow_html=True)

    if "source_key" in working_df.columns:
        src_counts = working_df["source_key"].value_counts().reset_index()
        src_counts.columns = ["source", "articles"]
        src_counts["source_display"] = src_counts["source"].map(
            lambda x: source_names.get(x, x)
        )
        fig_src = px.bar(
            src_counts, x="articles", y="source_display", orientation="h",
            color="articles", color_continuous_scale="Teal",
            template="plotly_dark",
            labels={"source_display": "Source", "articles": "Articles"},
        )
        fig_src.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=10, b=0), height=280,
        )
        st.plotly_chart(fig_src, use_container_width=True)

with col3d:
    st.markdown('<p class="section-title">🔑 Mots les plus fréquents</p>', unsafe_allow_html=True)
    st.markdown("""
    <div class="explain-box">
        Les mots qui reviennent le plus dans les <strong>titres</strong> des articles aujourd'hui. 
        Ils donnent un aperçu rapide des thèmes dominants.
    </div>
    """, unsafe_allow_html=True)

    keywords_df = load_keywords()
    if not keywords_df.empty:
        fig_kw = px.bar(
            keywords_df, x="frequence", y="mot", orientation="h",
            color="frequence", color_continuous_scale="Magma",
            template="plotly_dark",
        )
        fig_kw.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=10, b=0), height=280,
        )
        st.plotly_chart(fig_kw, use_container_width=True)
    else:
        if "titre_clean" in working_df.columns:
            stopwords = {"the", "and", "for", "avec", "dans", "pour", "les", "des", "que", "qui",
                         "une", "pas", "sur", "est", "son", "cet", "cette", "ces", "ses", "leur",
                         "leurs", "notre", "nos", "votre", "vos", "mon", "ma", "mes", "ton", "ta",
                         "tes", "ce", "de", "du", "un", "et", "en", "à", "au", "aux", "par", "plus",
                         "moins", "très", "trop", "peu", "tout", "tous", "toute", "toutes", "autre",
                         "autres", "même", "tel", "telle", "tels", "telles", "autant", "aucun",
                         "aucune", "certains", "certaines", "plusieurs", "quelque", "quelques",
                         "chacun", "chacune", "personne", "rien", "nul", "nulle", "divers",
                         "with", "you", "that", "this", "from", "they", "have", "had", "what",
                         "said", "each", "which", "she", "does", "how", "will", "about", "out",
                         "many", "then", "them", "some", "her", "would", "make", "like", "into",
                         "him", "has", "two", "more", "very", "after", "words", "just", "where",
                         "most", "know", "take", "than", "only", "think", "also", "its", "over",
                         "too", "any", "may", "say", "great", "help", "through", "much", "before",
                         "move", "right", "means", "old", "same", "tell", "boy", "follow", "came",
                         "want", "show", "around", "new", "news", "today", "report", "official",
                         "says", "according", "people", "government", "president", "minister",
                         "country", "world", "first", "last", "year", "day", "time", "state",
                         "national", "international", "between", "under", "during", "without",
                         "within", "along", "following", "across", "behind", "beyond", "against",
                         "among", "toward", "towards", "upon", "near", "next", "another", "every",
                         "both", "few", "such", "own", "being", "were", "been", "are", "is", "was",
                         "be", "has", "have", "do", "did", "does", "done", "doing", "get", "got",
                         "got", "make", "made", "take", "took", "come", "came", "go", "went",
                         "see", "saw", "use", "used", "find", "found", "give", "gave", "work",
                         "call", "called", "try", "tried", "ask", "asked", "need", "needed",
                         "feel", "felt", "become", "became", "leave", "left", "put", "mean",
                         "meant", "keep", "kept", "let", "begin", "began", "seem", "seemed",
                         "help", "show", "showed", "hear", "heard", "play", "played", "run",
                         "ran", "move", "moved", "live", "lived", "believe", "believed", "bring",
                         "brought", "happen", "happened", "write", "wrote", "provide", "provided",
                         "sit", "sat", "stand", "stood", "lose", "lost", "pay", "paid", "meet",
                         "met", "include", "included", "continue", "continued", "set", "learn",
                         "learned", "change", "changed", "lead", "led", "understand", "watch",
                         "watched", "follow", "followed", "stop", "stopped", "create", "created",
                         "speak", "spoke", "read", "allow", "allowed", "add", "added", "spend",
                         "spent", "grow", "grew", "open", "opened", "walk", "walked", "win",
                         "won", "offer", "offered", "remember", "remembered", "love", "loved",
                         "consider", "considered", "appear", "appeared", "buy", "bought", "wait",
                         "waited", "serve", "served", "die", "died", "send", "sent", "expect",
                         "expected", "build", "built", "stay", "stayed", "fall", "fell", "cut",
                         "reach", "reached", "kill", "killed", "remain", "remained"}
            words = []
            for title in working_df["titre_clean"].dropna().astype(str):
                for w in title.lower().split():
                    w = w.strip(".,;:!?()[]{}\"'\n")
                    if len(w) > 3 and w not in stopwords:
                        words.append(w)
            counter = Counter(words)
            top = counter.most_common(15)
            kw_fallback = pd.DataFrame(top, columns=["mot", "frequence"])
            fig_kw = px.bar(
                kw_fallback, x="frequence", y="mot", orientation="h",
                color="frequence", color_continuous_scale="Magma",
                template="plotly_dark",
            )
            fig_kw.update_layout(
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=0, r=0, t=10, b=0), height=280,
            )
            st.plotly_chart(fig_kw, use_container_width=True)
        else:
            st.info("Titres non disponibles.")

st.markdown("---")

# ============================================================
# ROW — Qualité + Table des articles
# ============================================================
col4a, col4b = st.columns([1, 2])

with col4a:
    st.markdown('<p class="section-title">🔍 Qualité des données</p>', unsafe_allow_html=True)
    st.markdown("""
    <div class="explain-box">
        Chaque article passe un contrôle automatique : titre valide, contenu suffisant, 
        URL fonctionnelle. Les articles qui échouent sont marqués « FAIL ».
    </div>
    """, unsafe_allow_html=True)

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

        ok_count = len(working_df[working_df["quality_status"] == "OK"])
        st.caption(f"✅ **{ok_count} articles** validés sur {total_articles + len(working_df[working_df.get('quality_status', pd.Series()) == 'FAIL']) if 'quality_status' in working_df.columns else total_articles}")

with col4b:
    st.markdown('<p class="section-title">📋 Derniers articles</p>', unsafe_allow_html=True)
    st.markdown("""
    <div class="explain-box">
        Voici les articles les plus récents collectés par le pipeline. 
        Vous pouvez trier les colonnes en cliquant sur les en-têtes.
    </div>
    """, unsafe_allow_html=True)

    display_cols_map = {
        "titre_clean": "Titre",
        "source_key": "Source",
        "langue": "Langue",
        "date_publication": "Date",
        "topic_readable": "Sujet",
        "polymarket_prob_pct": "Probabilité marché",
        "quality_status": "Qualité",
    }

    display_cols = [c for c in ["titre_clean", "source_key", "langue", "date_publication",
                                 "topic_readable", "polymarket_prob_pct", "quality_status"]
                    if c in working_df.columns]

    display_df = working_df[display_cols].head(50).copy()
    if "date_publication" in display_df.columns:
        display_df["date_publication"] = display_df["date_publication"].astype(str).str[:16]

    # Renommer les colonnes pour l'affichage
    display_df = display_df.rename(columns={k: v for k, v in display_cols_map.items() if k in display_df.columns})

    st.dataframe(
        display_df,
        use_container_width=True,
        height=320,
        column_config={
            "Titre": st.column_config.TextColumn("Titre", width="medium"),
            "Source": st.column_config.TextColumn("Source", width="small"),
            "Langue": st.column_config.TextColumn("Langue", width="small"),
            "Date": st.column_config.TextColumn("Date", width="small"),
            "Sujet": st.column_config.TextColumn("Sujet", width="medium"),
            "Probabilité marché": st.column_config.TextColumn("Probabilité", width="small"),
            "Qualité": st.column_config.TextColumn("Qualité", width="small"),
        }
    )

# ============================================================
# FOOTER
# ============================================================
st.markdown(f"""
<div class="footer">
    News Intelligence Dashboard · {total_articles:,} articles analysés · Données du {date_str} · 
    9 sources · Analyse IA + Marchés prédictifs
</div>
""", unsafe_allow_html=True)
