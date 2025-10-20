"""
title: Discovery Agent (Advanced)
author: OpenWebUI Assistant
version: 2.0.0
requirements: requests, aiohttp, openai, langdetect, pydantic
license: MIT
description: Agente de busca e curadoria v2.0 que utiliza SearXNG + Exa.ai (neural, hardcoded 20 resultados) com LLM para planejar buscas e selecionar as melhores fontes de informação.
"""
from __future__ import annotations

from typing import List, Tuple, Optional, Callable, Dict, Literal
import logging
import json

logger = logging.getLogger(__name__)

import asyncio

from datetime import date, datetime, timedelta
import math
import os
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict
from urllib.parse import urlparse

# Pydantic imports
from pydantic import BaseModel, HttpUrl, Field

try:
    from langdetect import detect, LangDetectException

    LANGDETECT_AVAILABLE = True
except ImportError:
    LANGDETECT_AVAILABLE = False

# OpenAI availability
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except Exception:
    OPENAI_AVAILABLE = False

# Set up logger
logger = logging.getLogger(__name__)


# DiscoveryValves moved to Tools class


async def emit_status(__event_emitter__: Optional[Callable], message: str):
    if __event_emitter__:
        await __event_emitter__({"type": "status", "data": {"description": message}})


class DiscoveryMetrics:
    """Métricas por run_id e slice_id para Discovery"""
    
    def __init__(self, run_id: str, enable_metrics: bool = True):
        self.run_id = run_id
        self.enable_metrics = enable_metrics
        self.metrics = {
            "run_id": run_id,
            "slices": {},
            "total_queries": 0,
            "total_results": 0,
            "total_llm_calls": 0,
            "start_time": time.time(),
            "end_time": None
        }
    
    def start_slice(self, slice_id: str, query: str):
        if not self.enable_metrics:
            return
        self.metrics["slices"][slice_id] = {
            "query": query,
            "start_time": time.time(),
            "end_time": None,
            "queries_generated": 0,
            "results_found": 0,
            "llm_calls": 0,
            "engines_used": []
        }
    
    def end_slice(self, slice_id: str):
        if not self.enable_metrics or slice_id not in self.metrics["slices"]:
            return
        self.metrics["slices"][slice_id]["end_time"] = time.time()
    
    def record_query_generation(self, slice_id: str, num_queries: int):
        if not self.enable_metrics or slice_id not in self.metrics["slices"]:
            return
        self.metrics["slices"][slice_id]["queries_generated"] += num_queries
        self.metrics["total_queries"] += num_queries
    
    def record_results(self, slice_id: str, num_results: int, engine: str):
        if not self.enable_metrics or slice_id not in self.metrics["slices"]:
            return
        self.metrics["slices"][slice_id]["results_found"] += num_results
        self.metrics["total_results"] += num_results
        if engine not in self.metrics["slices"][slice_id]["engines_used"]:
            self.metrics["slices"][slice_id]["engines_used"].append(engine)
    
    def record_llm_call(self, slice_id: str):
        if not self.enable_metrics or slice_id not in self.metrics["slices"]:
            return
        self.metrics["slices"][slice_id]["llm_calls"] += 1
        self.metrics["total_llm_calls"] += 1
    
    def finalize(self):
        if not self.enable_metrics:
            return
        self.metrics["end_time"] = time.time()
        self.metrics["duration"] = self.metrics["end_time"] - self.metrics["start_time"]
        logger.info(f"Discovery metrics for run_id {self.run_id}: {self.metrics}")
    
    def get_metrics(self) -> Dict[str, Any]:
        return self.metrics.copy()

# feature flag for network availability
try:
    import aiohttp  # type: ignore

    AIOHTTP_AVAILABLE = True
except Exception:
    AIOHTTP_AVAILABLE = False

# Inline Pydantic DTOs so this tool is standalone (no external schemas.py)

class UrlCandidate(BaseModel):
    url: HttpUrl
    source_engine: str
    serp_position: Optional[int] = None
    query_slice_id: Optional[str] = None
    query_variant: Optional[str] = None
    score: float = 0.0
    hints: Dict[str, Any] = Field(default_factory=dict)

class DiscoveryResult(BaseModel):
    run_id: str
    company_key: Optional[str] = None
    query_text: str
    candidates: List[UrlCandidate] = Field(default_factory=list)
    stats: Dict[str, float] = Field(default_factory=dict)
    execution: List[Dict[str, Any]] = Field(default_factory=list)

class DiscoverRequest(BaseModel):
    run_id: str
    query_text: str
    profile: Literal["news","general"] = "general"
    companies: Optional[List[str]] = None
    aliases: Optional[List[str]] = None
    after: Optional[date] = None
    before: Optional[date] = None
    pages_per_slice: int = 2
    valves: Optional[Dict[str, Any]] = None
    whitelist: Optional[List[str]] = None
    language: str = ""


def _get_valve(valves: Optional[Any], key: str, env_key: str, default: Optional[str] = None) -> Optional[str]:
    """Helper: prefer valves (dict or BaseModel) then env var, then default."""
    # valves may be dict or object
    if valves:
        try:
            if isinstance(valves, dict):
                v = valves.get(key)
                if v is None:
                    v = valves.get(env_key)
                if v is not None:
                    return v
            else:
                v = getattr(valves, key, None)
                if v is not None:
                    return v
                v = getattr(valves, env_key, None)
                if v is not None:
                    return v
        except Exception:
            pass
    # fallback to environment
    try:
        return os.getenv(env_key, default)
    except Exception:
        return default


def discover_for_scraper_json(request_input: Any) -> str:
    """Convenience function for OpenWebUI: recebe um DiscoverRequest-like (dict ou object)
    e retorna um JSON (string) com a lista de objetos {url, title} que o Scraper espera.

    Não chama outros tools — apenas transforma a saída de `discover()` em formato
    legível para humanos e compatível com `get_scraped_content`.
    """
    try:
        if isinstance(request_input, dict):
            req = DiscoverRequest(**request_input)
        elif isinstance(request_input, DiscoverRequest):
            req = request_input
        else:
            # tentamos criar pela forma mais permissiva
            req = DiscoverRequest(**dict(request_input))
    except Exception as e:
        # Falha ao construir request: log detalhado e retornar dict vazio
        logger.error(f"Failed to construct DiscoverRequest from input: {e}", exc_info=True)
        return {"urls": [], "error": str(e)}

    # discover() é síncrono e retorna DiscoveryResult
    try:
        res = discover(req)
    except Exception as e:
        logger.error(f"Discovery execution failed: {e}", exc_info=True)
        return {"urls": [], "error": str(e)}

    out = []
    for c in getattr(res, "candidates", []):
        # Suporta tanto objetos Pydantic quanto dicts
        if hasattr(c, "url"):
            url = str(c.url)
        elif isinstance(c, dict):
            url = c.get("url", "")
        else:
            url = ""

        title = ""
        if hasattr(c, "hints") and isinstance(c.hints, dict):
            title = c.hints.get("title", "")
        elif isinstance(c, dict):
            title = c.get("title", "")

        out.append({"url": url, "title": title})

    # PATCH 1 (FASE 1): Retornar dict ao invés de JSON string
    # Elimina overhead de serialize/deserialize e facilita debug
    return {"urls": out, "candidates": out, "query": req.query_text if hasattr(req, 'query_text') else ""}


# Brazilian boost table (from supersearcher)
BRAZILIAN_BOOST_TABLE = {
    # Premium (3.0)
    "valor.globo.com": 3.0,
    # Alto (2.5)
    "folha.uol.com.br": 2.5,
    "estadao.com.br": 2.5,
    # Médio-Alto (2.0-2.2)
    "g1.globo.com": 2.0,
    "exame.com": 2.2,
    "infomoney.com.br": 2.2,
    "moneytimes.com.br": 2.0,
    "conjur.com.br": 2.1,
    "www.jusbrasil.com.br": 2.0,
    # Médio (1.8-1.9)
    "uol.com.br": 1.8,
    "band.com.br": 1.8,
    "correiobraziliense.com.br": 1.8,
    "cnnbrasil.com.br": 1.8,
    "istoedinheiro.com.br": 1.8,
    "poder360.com.br": 1.9,
    "migalhas.com.br": 1.9,
    "www.brasildefato.com.br": 1.8,
    "ebc.com.br": 1.8,
    # Básico (1.5-1.7)
    "terra.com.br": 1.5,
    "r7.com": 1.5,
    "metropoles.com": 1.7,
    "jb.com.br": 1.7,
}

def _get_brazilian_boost_table():
    """Get expanded Brazilian boost table with www variations"""
    from urllib.parse import urlparse
    
    expanded = {}
    for domain, boost in BRAZILIAN_BOOST_TABLE.items():
        expanded[domain] = boost
        if not domain.startswith("www."):
            expanded[f"www.{domain}"] = boost
        # Casos especiais
        if domain == "folha.uol.com.br":
            expanded["www1.folha.uol.com.br"] = boost
        elif domain == "uol.com.br":
            expanded["noticias.uol.com.br"] = boost
        elif domain == "g1.globo.com":
            expanded["oglobo.globo.com"] = boost
        elif domain == "ebc.com.br":
            expanded["agenciabrasil.ebc.com.br"] = boost
    return expanded

def _fmt_candidate(cand, idx):
    """Format candidate for LLM selector with enriched context"""
    from urllib.parse import urlparse
    
    def trunc(s, n): 
        return (s or "")[:n]
    
    # Extract domain from URL
    domain = ""
    try:
        domain = urlparse(str(cand.url)).netloc
    except:
        domain = "unknown"
    
    return {
        "index": idx,
        "title": trunc(getattr(cand, "title", ""), 120),
        "snippet": trunc(getattr(cand, "snippet", ""), 250),
        "url": str(cand.url),
        "domain": domain,
        "engine": getattr(cand, "source_engine", "unknown"),
        "score": round(float(getattr(cand, "selection_score", getattr(cand, "score", 0))), 2),
        "flags": {
            "is_listing": bool(getattr(cand, "is_listing", False)),
            "authority": bool(getattr(cand, "is_authority", False)),
            "news_host": bool(getattr(cand, "is_news_host", False)),
        },
    }


def _soft_relevance_boost(raw_item: Dict[str, Any], criteria: str) -> Tuple[float, float]:
    """Apply light, interpretable boosts based on simple textual criteria.

    Returns (boosted_score, boost_amount)
    """
    try:
        base = float(raw_item.get("selection_score", raw_item.get("score", 0)) or 0.0)
    except Exception:
        base = 0.0
    boost = 0.0
    crit = (criteria or "").lower()
    dom = (raw_item.get("domain") or "").lower()

    # Priorize internacionais
    if "internacion" in crit or "internacionais" in crit:
        if not (dom.endswith(".br") or dom.endswith(".pt")) and dom:
            boost += 0.15
        else:
            boost -= 0.05

    # Normas (ASTM/ISO)
    if "normas" in crit or "astm" in crit or "iso" in crit:
        if any(x in dom for x in ("astm.org", "iso.org")):
            boost += 0.25

    # Fabricantes específicos (ex.: saint-gobain, placo)
    if "fabricante" in crit or "fabricantes" in crit:
        if any(x in dom for x in ("saint-gobain", "placo")):
            boost += 0.10

    boosted = base * (1.0 + boost)
    return boosted, boost

def _build_selector_payload(query_text, candidates, news_mode, time_hint, domain_cap, crit_text, top_n=10, intent_profile: Dict = None):
    """Build payload for LLM selector with context signals"""
    def trunc(s, n): 
        return (s or "")[:n]
    # Normalize query text to avoid stray bullets/newlines confusing the selector
    try:
        import re as _re
        _qt = query_text or ""
        # remove leading bullet markers at line starts and collapse whitespace
        _qt = _re.sub(r"(^|\n)[\*•\-]\s+", " ", _qt)
        _qt = _re.sub(r"\s+", " ", _qt).strip()
    except Exception:
        _qt = (query_text or "").replace("\n", " ").strip()
    
    # Log prioritization criteria for debugging
    logger.info(f"[Selector] Prioritization criteria received: '{crit_text}'")
    
    payload = {
        "original_query": _qt.replace("@noticias", "").strip(),
        "candidates": [{
            "idx": i,
            "title": trunc(c.get("title", ""), 300),
            "url": trunc(c.get("url", ""), 300),
            "score": c.get("score", 0.0),
            "authority": c.get("is_authority", False),
            "news_host": c.get("is_news_host", False),
            "domain": trunc(c.get("domain", ""), 80),
        } for i, c in enumerate(candidates or [])],
        "context": {
            "news_profile_active": news_mode,
            "time_window_hint": trunc(time_hint, 200),
            "domain_cap": int(domain_cap or 2),
            "top_n": top_n,
            "prioritization_criteria": trunc(crit_text, 500),
            "intent_profile": intent_profile or {}
        }
    }
    return payload

def _selector_prompt(payload: dict) -> str:
    """Build robust selector prompt with explicit rules"""
    import json

    intent = payload.get("context", {}).get("intent_profile", {})
    
    base_prompt = (
        "Você é um seletor de URLs ultra-preciso. Devolva somente JSON válido, sem comentários.\\n\\n"
        "TAREFA:\\nDada a consulta do usuário, selecione as melhores URLs (ordem de prioridade).\\n\\n"
        f'CONSULTA (bruta):\\n"{payload["original_query"]}"\\n\\n'
        "CONTEXTO:\\n"
        f"- news_profile_active: {json.dumps(payload['context']['news_profile_active'])}\\n"
        f"- time_window_hint: {json.dumps(payload['context']['time_window_hint'])}\\n"
        f"- domain_cap: {payload['context']['domain_cap']}\\n"
        f"- top_n: {payload['context'].get('top_n', 'NA')}\\n"
        f"- prioritization_criteria: {json.dumps(payload['context']['prioritization_criteria'])}"
    )

    orientations = []
    
    if intent.get("country_bias"):
        country = intent["country_bias"]
        orientations.append(f"PRIORIZE fontes do país/região: {country.upper()}")
    
    if intent.get("news_mode"):
        orientations.append("PRIORIZE artigos jornalísticos recentes com datas claras")
    
    axis_type = intent.get("axis_type", "geral")
    if axis_type == "temporal":
        orientations.append("DIVERSIDADE TEMPORAL: Distribua seleção entre diferentes períodos/meses")
    elif axis_type == "conceitual":
        orientations.append("PRIORIZE: Fontes governamentais, acadêmicas, marcos regulatórios")
    elif axis_type == "prático":
        orientations.append("PRIORIZE: Cases reais, implementações, experiências documentadas")
    elif axis_type == "comparativo":
        orientations.append("PRIORIZE: Estudos comparativos, benchmarks, múltiplas jurisdições")
    
    if intent.get("prefer_domains"):
        domains = ", ".join(intent["prefer_domains"])
        orientations.append(f"PREFIRA resultados dos domínios: {domains}")
    
    if orientations:
        orientation_text = "\n".join([f"• {o}" for o in orientations])
        base_prompt += f"\n\nORIENTAÇÕES ESPECÍFICAS:\n{orientation_text}\n"

    # Avoid implicit concatenation issues by breaking into steps
    candidates_json = json.dumps(payload["candidates"], ensure_ascii=False)
    base_prompt += ("\nCANDIDATOS (JSON):\n" + candidates_json + "\n\n")
    base_prompt += (
        "REGRAS (aplique na ordem):\n"
        "1) Relevância semântica à CONSULTA > depois use \"score\" como desempate.\n"
        "2) Mantenha diversidade: no máx. domain_cap por domínio.\n"
        "   EXCEÇÃO: se a CONSULTA trouxer restrição explícita de site/domínio (p.ex., 'site:...'),\n"
        "   ignore o domain_cap para esse domínio específico e priorize satisfazer a restrição.\n"
        "3) Se news_profile_active=true: prefira artigos (não \"listings\"), salvo se a consulta pedir coberturas gerais.\n"
        "4) Se time_window_hint existir: prefira títulos/URLs claramente na janela.\n"
        "5) Se prioritization_criteria existir: sobrepõe os demais critérios quando houver conflito.\n"
        "6) Penalize duplicatas/espelhos e títulos genéricos.\n"
        "7) Em empate, prefira authority=true e news_host=true.\n\n"
        "8) **Retorne EXATAMENTE top_n itens, exceto se tiver menos candidatos que top_n, em \"selected_indices\"** (use \"additional_indices\" só como reserva).\n\n"
        "OBS: Se for impossível selecionar exatamente top_n sem violar regras estritas, **relaxe** as regras na ordem a seguir até preencher top_n:\n"
        "  a) Aumente o limite por domínio (domain_cap) incrementalmente para permitir mais itens do mesmo domínio;\n"
        "  b) Permita 'listings' caso estejam entre os melhores por relevância;\n"
        "  c) Aceite itens com menor 'authority' ou score quando necessário.\n\n"
        "Sempre preserve a ordem de relevância semântica e documente no campo 'why' quais relaxamentos foram aplicados.\n\n"
        "SAÍDA (JSON puro):\n"
        "{\n"
        '  "selected_indices": [0,1,2],\n'
        '  "additional_indices": [3,4],\n'
        '  "why": ["curto","curto"]\n'
        "}\n"
    )
    return base_prompt


class QueryClassifier:
    """Classify a query into profiles like 'news' or 'general' using simple
    heuristics. This mirrors supersearcher intent classification at a high level."""
    @staticmethod
    def _is_news_query(q: str) -> bool:
        s = (q or "").lower()
        return any(k in s for k in ("notícia", "noticias", "news", "últimas", "última hora"))

    @staticmethod
    def _is_news_category_query(q: str) -> bool:
        """Detecta se a query menciona notícias como categoria (hint para SearXNG)"""
        import re
        s = (q or "").lower()
        news_patterns = [
            r'\b(not[ií]cia[s]?|lan[çc]amento[s]?|an[úu]ncio[s]?|revelad[ao]|estreia)\b'
        ]
        return any(re.search(pattern, s) for pattern in news_patterns)

    @staticmethod
    def _is_legal_query(q: str) -> bool:
        import re
        s = (q or "").lower()
        # Use word boundaries to avoid false positives (e.g., "lei" in "brasileiras")
        legal_patterns = [
            r'\blgpd\b', r'\bstf\b', r'\bstj\b', r'\bjurisprud\b', 
            r'\blei\b', r'\bdecreto\b', r'\bportaria\b', r'\bregula\b'
        ]
        return any(re.search(pattern, s) for pattern in legal_patterns)

    @staticmethod
    def _is_business_finance_query(q: str) -> bool:
        import re
        s = (q or "").lower()
        # Use word boundaries to avoid false positives
        business_patterns = [
            r'\bmercado\b', r'\bempresas\b', r'\bfinance\b', 
            r'\binvest\b', r'\bb3\b', r'\bcvm\b', r'\bresultado\b'
        ]
        return any(re.search(pattern, s) for pattern in business_patterns)

    @staticmethod
    def is_news_profile_active(q: str, overrides: Optional[Dict] = None) -> bool:
        """Detecta se o perfil de notícias está ativo (@noticias ou override)"""
        explicit_news = ("@noticias" in (q or "").lower()) or (overrides and overrides.get("news_profile", False))
        return bool(explicit_news)

    @staticmethod
    def classify(q: str) -> Literal["news", "general"]:
        """Classify a query into 'news' or 'general' profile."""
        return "news" if QueryClassifier._is_news_query(q) else "general"

    @staticmethod
    def get_searxng_categories(q: str, has_filetypes: bool, force_news: bool = False) -> str:
        cats = ["general"]
        # news como categoria (hint para SearXNG) - igual financeiro/juridico
        if QueryClassifier._is_news_category_query(q) or force_news:
            cats.append("news")
        if QueryClassifier._is_legal_query(q):
            cats.append("juridico")
        if QueryClassifier._is_business_finance_query(q):
            cats.append("financeiro")
        if has_filetypes and "files" not in cats:
            cats.append("files")
        # de-dup
        seen = set()
        uniq = []
        for c in cats:
            if c not in seen and c:
                seen.add(c)
                uniq.append(c)
        return ",".join(uniq)


def classify_query_intent(query: str) -> Dict[str, Any]:
    """Ported light-weight intent classifier from supersearcher.py"""
    q_lower = query.lower()
    temporal_indicators = ["atual", "recente", "último", "hoje", "2024", "2025", "últimas"]
    comparison_indicators = ["vs", "versus", "comparar", "comparativa", "diferença", "melhor que"]
    research_indicators = ["análise", "pesquisa", "estudo", "relatório", "investigação"]
    legal_indicators = [
        "danos",
        "vícios",
        "construção",
        "jurídico",
        "legal",
        "processo",
        "ação",
        "recurso",
        "sentença",
        "decisão",
        "lei",
        "código",
        "artigo",
    ]
    business_indicators = [
        "análise",
        "empresa",
        "resultados",
        "anúncios",
        "balanço",
        "lucro",
        "receita",
        "ações",
        "investimento",
        "mercado",
        "financeiro",
        "relatório",
        "trimestral",
        "anual",
    ]

    words = query.split()
    word_count = len(words)
    if word_count > 8 or any(r in q_lower for r in research_indicators):
        complexity = "high"
    elif word_count > 4:
        complexity = "medium"
    else:
        complexity = "simple"

    return {
        "needs_current_info": any(t in q_lower for t in temporal_indicators),
        "needs_comparison": any(c in q_lower for c in comparison_indicators),
        "needs_deep_research": any(r in q_lower for r in research_indicators)
        or any(l in q_lower for l in legal_indicators)
        or any(b in q_lower for b in business_indicators),
        "complexity_level": complexity,
    }


class AdvancedDateParser:
    """Advanced date parsing for natural language queries with Portuguese support."""
    
    # Portuguese month mappings
    MONTH_NAMES = {
        'jan': 1, 'janeiro': 1, 'fev': 2, 'fevereiro': 2, 'mar': 3, 'março': 3,
        'abr': 4, 'abril': 4, 'mai': 5, 'maio': 5, 'jun': 6, 'junho': 6,
        'jul': 7, 'julho': 7, 'ago': 8, 'agosto': 8, 'set': 9, 'setembro': 9,
        'out': 10, 'outubro': 10, 'nov': 11, 'novembro': 11, 'dez': 12, 'dezembro': 12
    }
    
    @staticmethod
    def parse_relative_date(text: str, base_date: Optional[date] = None) -> Optional[date]:
        """Parse relative dates like 'última semana', 'mês passado', 'há 3 meses'."""
        if not base_date:
            base_date = datetime.utcnow().date()
        
        text = text.lower().strip()
        
        # Relative patterns
        patterns = [
            (r'última semana|semana passada', lambda: base_date - timedelta(weeks=1)),
            (r'último mês|mês passado', lambda: (base_date.replace(day=1) - timedelta(days=1)).replace(day=1)),
            (r'último ano|ano passado', lambda: base_date.replace(year=base_date.year - 1)),
            (r'hoje', lambda: base_date),
            (r'ontem', lambda: base_date - timedelta(days=1)),
            (r'há (\d+) dias?', lambda m: base_date - timedelta(days=int(m.group(1)))),
            (r'há (\d+) semanas?', lambda m: base_date - timedelta(weeks=int(m.group(1)))),
            (r'há (\d+) meses?', lambda m: _subtract_months(base_date, int(m.group(1)))),
            (r'(\d+) dias? atrás', lambda m: base_date - timedelta(days=int(m.group(1)))),
            (r'(\d+) semanas? atrás', lambda m: base_date - timedelta(weeks=int(m.group(1)))),
            (r'(\d+) meses? atrás', lambda m: _subtract_months(base_date, int(m.group(1)))),
        ]
        
        for pattern, func in patterns:
            match = re.search(pattern, text)
            if match:
                try:
                    return func(match)
                except (ValueError, AttributeError):
                    continue
        return None
    
    @staticmethod
    def parse_month_year_range(text: str) -> Tuple[Optional[date], Optional[date]]:
        """Parse month/year ranges like 'mar/24 e jun/25' or 'entre março de 2024 e junho de 2025'."""
        text = text.lower().strip()
        
        # Pattern: mar/24 e jun/25
        short_pattern = r'(\w+)/(\d{2})\s+e\s+(\w+)/(\d{2})'
        match = re.search(short_pattern, text)
        if match:
            try:
                month1, year1, month2, year2 = match.groups()
                start_month = AdvancedDateParser.MONTH_NAMES.get(month1)
                end_month = AdvancedDateParser.MONTH_NAMES.get(month2)
                if start_month and end_month:
                    start_year = 2000 + int(year1)
                    end_year = 2000 + int(year2)
                    start_date = date(start_year, start_month, 1)
                    end_date = date(end_year, end_month, 1)
                    # Get last day of end month
                    if end_month == 12:
                        end_date = date(end_year + 1, 1, 1) - timedelta(days=1)
                    else:
                        end_date = date(end_year, end_month + 1, 1) - timedelta(days=1)
                    return start_date, end_date
            except (ValueError, KeyError):
                pass
        
        # Pattern: entre março de 2024 e junho de 2025
        long_pattern = r'entre\s+(\w+)\s+de\s+(\d{4})\s+e\s+(\w+)\s+de\s+(\d{4})'
        match = re.search(long_pattern, text)
        if match:
            try:
                month1, year1, month2, year2 = match.groups()
                start_month = AdvancedDateParser.MONTH_NAMES.get(month1)
                end_month = AdvancedDateParser.MONTH_NAMES.get(month2)
                if start_month and end_month:
                    start_date = date(int(year1), start_month, 1)
                    end_date = date(int(year2), end_month, 1)
                    # Get last day of end month
                    if end_month == 12:
                        end_date = date(int(year2) + 1, 1, 1) - timedelta(days=1)
                    else:
                        end_date = date(int(year2), end_month + 1, 1) - timedelta(days=1)
                    return start_date, end_date
            except (ValueError, KeyError):
                pass
        
        # Single relative date
        relative_date = AdvancedDateParser.parse_relative_date(text)
        if relative_date:
            return relative_date, None
        
        return None, None
    
    @staticmethod
    def parse_date_range_from_query(query: str) -> Tuple[Optional[date], Optional[date]]:
        """Extract date range from natural language query."""
        # Look for explicit date patterns
        after, before = AdvancedDateParser.parse_month_year_range(query)
        if after or before:
            return after, before
        
        # Look for relative date indicators
        relative_indicators = [
            'última semana', 'semana passada', 'último mês', 'mês passado',
            'último ano', 'ano passado', 'hoje', 'ontem', 'há', 'atrás',
            'recentes', 'atual', 'atualizado', 'novo', 'novos'
        ]
        
        query_lower = query.lower()
        if any(indicator in query_lower for indicator in relative_indicators):
            # Default to last 30 days for "recent" queries
            end_date = datetime.utcnow().date()
            start_date = end_date - timedelta(days=30)
            return start_date, end_date
        
        return None, None


def _subtract_months(base_date: date, months: int) -> date:
    """Subtract months from a date, handling edge cases."""
    year = base_date.year
    month = base_date.month
    
    month -= months
    while month <= 0:
        month += 12
        year -= 1
    
    # Handle day overflow (e.g., Jan 31 - 1 month = Dec 31)
    try:
        return date(year, month, base_date.day)
    except ValueError:
        # Last day of the target month
        next_month = date(year, month + 1, 1) if month < 12 else date(year + 1, 1, 1)
        return next_month - timedelta(days=1)


class TimeSlicer:
    """Produce temporal slices between `after` and `before` (inclusive by month).

    Example: month_slices(date(2025,1,1), date(2025,3,1)) ->
    [(2025-01-01,2025-01-31),(2025-02-01,2025-02-28),(2025-03-01,2025-03-31)]
    
    Example: quarter_slices(date(2022,1,1), date(2022,7,1)) ->
    [(2022-01-01,2022-03-31),(2022-04-01,2022-06-30),(2022-07-01,2022-07-31)]
    """

    @staticmethod
    def month_slices(after: Optional[date], before: Optional[date]) -> List[Tuple[date, date]]:
        # If no bounds provided, return a single open slice (None handled by caller)
        if not after and not before:
            return []

        # Defaults
        if not after:
            after = before
        if not before:
            before = after

        # Normalize to first day of months
        start = date(after.year, after.month, 1)
        end = date(before.year, before.month, 1)

        slices: List[Tuple[date, date]] = []
        cur = start
        while cur <= end:
            next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
            slice_end = next_month - timedelta(days=1)
            slices.append((cur, slice_end))
            cur = next_month
        return slices

    @staticmethod
    def quarter_slices(after: Optional[date], before: Optional[date]) -> List[Tuple[date, date]]:
        """Produce quarterly slices (3 months each) with remaining months grouped together.
        
        Strategy: Create 3-month slices, then group remaining months together.
        Example: quarter_slices(date(2022,1,1), date(2022,7,31)) ->
        [(2022-01-01,2022-03-31),(2022-04-01,2022-06-30),(2022-07-01,2022-07-31)]
        """
        if not after and not before:
            return []

        # Defaults
        if not after:
            after = before
        if not before:
            before = after

        # Normalize to first day of months
        start = date(after.year, after.month, 1)
        end = date(before.year, before.month, 1)

        slices: List[Tuple[date, date]] = []
        cur = start
        
        # Calculate total months
        total_months = (end.year - start.year) * 12 + (end.month - start.month) + 1
        
        # If 3 months or less, return as single slice
        if total_months <= 3:
            # Get the actual end date of the last month
            last_day = (end.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            slices.append((start, last_day))
            return slices
        
        # Create 3-month slices
        while cur <= end:
            # Calculate next quarter (3 months ahead)
            next_quarter = cur
            for _ in range(3):
                next_quarter = (next_quarter.replace(day=28) + timedelta(days=4)).replace(day=1)
            
            # If we would go past the end date, group remaining months together
            if next_quarter > end:
                # Last slice: remaining months
                last_day = (end.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
                slices.append((cur, last_day))
                break
            else:
                # Regular slice: 3 months
                slice_end = next_quarter - timedelta(days=1)
                slices.append((cur, slice_end))
                cur = next_quarter
        
        return slices


class QueryBuilderSearxng:
    """Build a SearxNG-compatible query payload from parameters.

    The real implementation will include qdf/time_range translation and other
    rules. Here we provide a typed skeleton to be expanded.
    """

    @staticmethod
    def build(
        core_query: str,
        after: Optional[date] = None,
        before: Optional[date] = None,
        language: Optional[str] = None,
        categories: Optional[List[str]] = None,
        page: int = 1,
        sites: Optional[List[str]] = None,
        filetypes: Optional[List[str]] = None,
        qdf: Optional[int] = None,
        intext: bool = False,
        restrict_sites: bool = False,
        restrict_filetypes: bool = False,
    ) -> dict:
        # Build a flat query string; SearXNG params must be simple types
        q_parts: List[str] = []
        if intext:
            q_parts.append(f"intext:{core_query}")
        else:
            q_parts.append(core_query)
        if restrict_sites and sites:
            site_clause = _or_clause("site", sites)
            if site_clause:
                q_parts.append(site_clause)
        if restrict_filetypes and filetypes:
            filetype_clause = _or_clause("filetype", filetypes)
            if filetype_clause:
                q_parts.append(filetype_clause)
        # Avoid passing dict/list params like time_range; rely on q and pagination
        q_string = " ".join(q_parts)
        params = {
            "q": q_string,
            "pageno": page,
            "format": "json",
        }
        # Optionally pass language if provided (simple type)
        if language:
            params["language"] = language
        # Optionally pass categories (e.g., news)
        if categories:
            try:
                params["categories"] = ",".join([c for c in categories if c])
            except Exception:
                pass
        # Add simple date hints into query for better temporal focus (align to supersearcher)
        try:
            if after:
                params["q"] = params["q"] + f" after:{after.isoformat()}"
            if before:
                params["q"] = params["q"] + f" before:{before.isoformat()}"
            
            # Set approximate time_range for SearXNG engines that don't support after/before in q
            if after or before:
                try:
                    if after and before:
                        # Calculate approximate time range based on date span
                        delta = abs((before - after).days)
                        if delta <= 7:
                            params["time_range"] = "week"
                        elif delta <= 31:
                            params["time_range"] = "month"
                        elif delta <= 365:
                            params["time_range"] = "year"
                        else:
                            params["time_range"] = "year"  # Default to year for longer spans
                    elif after:
                        # Only after date - assume recent content
                        delta = abs((datetime.utcnow().date() - after).days)
                        if delta <= 7:
                            params["time_range"] = "week"
                        elif delta <= 31:
                            params["time_range"] = "month"
                        else:
                            params["time_range"] = "year"
                    elif before:
                        # Only before date - assume historical content
                        delta = abs((datetime.utcnow().date() - before).days)
                        if delta <= 7:
                            params["time_range"] = "week"
                        elif delta <= 31:
                            params["time_range"] = "month"
                        else:
                            params["time_range"] = "year"
                except Exception:
                    pass
            elif qdf:
                # Use qdf_level to set time_range if no specific dates
                qdf_map = {5: "day", 4: "week", 3: "month", 2: "year", 1: "month", 0: None}
                tr = qdf_map.get(int(qdf))
                if tr:
                    params["time_range"] = tr
        except Exception:
            pass
        # If there are absolute dates, also add an approximate time_range to increase temporal coverage
        if (after is not None or before is not None) and ("time_range" not in params):
            try:
                from datetime import date
                # compute an approximate span
                if after and before:
                    delta = abs((datetime.combine(before, datetime.min.time()) - datetime.combine(after, datetime.min.time())).days)
                elif after and not before:
                    delta = abs((datetime.utcnow().date() - after).days)
                elif before and not after:
                    delta = abs((before - datetime.utcnow().date()).days)
                else:
                    delta = None
                if delta is not None:
                    if delta <= 7:
                        params["time_range"] = "week"
                    elif delta <= 31:
                        params["time_range"] = "month"
                    elif delta <= 365:
                        params["time_range"] = "year"
            except Exception:
                pass
        return params


class Heuristics:
    @staticmethod
    def compute_authority_prior(url_or_domain: str) -> float:
        # Simple domain heuristics: gov/edu/news domains score higher
        # Ensure we have a full URL for is_authority check
        u = url_or_domain if url_or_domain.startswith("http") else f"https://{url_or_domain}"
        score = 1.0
        if is_authority(u):
            score += 2.0  # Big boost for known authorities
        # Extract domain for NEWS_HOSTS check
        try:
            from urllib.parse import urlparse
            d = urlparse(u).netloc.lower()
        except Exception:
            d = url_or_domain.lower()
        if d in NEWS_HOSTS:
            score += 1.0  # Smaller boost for news sources
        return score


class Deduper:
    @staticmethod
    def normalize_url(raw_url: str) -> str:
        # Conservative normalization: only remove obvious tracking params
        from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

        parsed = urlparse(raw_url)
        qs = dict(parse_qsl(parsed.query, keep_blank_values=True))
        
        # Only remove well-known tracking parameters
        tracking_params = {
            "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            "fbclid", "gclid", "msclkid", "ref", "source", "campaign"
        }
        
        for k in list(qs.keys()):
            if k.lower() in tracking_params:
                qs.pop(k, None)
        
        new_query = urlencode(qs, doseq=True)
        # Keep fragment (#) as it may be important for page navigation
        cleaned = parsed._replace(query=new_query)
        return urlunparse(cleaned)

    @staticmethod
    def dedupe_candidates(cands: List[UrlCandidate]) -> List[UrlCandidate]:
        seen = {}  # key -> (candidate, score)
        out: List[UrlCandidate] = []
        for c in cands:
            key = Deduper.normalize_url(str(c.url))
            if key in seen:
                # Keep the candidate with higher score
                existing_candidate, existing_score = seen[key]
                if c.score > existing_score:
                    # Replace the existing candidate
                    out.remove(existing_candidate)
                    out.append(c)
                    seen[key] = (c, c.score)
                # If current score is lower or equal, skip it
            else:
                seen[key] = (c, c.score)
                out.append(c)
        return out


class ResultNormalizer:
    @staticmethod
    def normalize_raw_result(raw: dict, slice_id: Optional[str] = None, variant: Optional[str] = None) -> UrlCandidate:
        url = raw.get("url") or raw.get("link") or ""
        engine = raw.get("engine") or raw.get("search_plan", {}).get("engine", "unknown")
        pos = raw.get("position") or raw.get("pos")
        score = float(raw.get("score", 1.0)) if raw.get("score") is not None else 1.0
        
        # Extract title and snippet
        title = raw.get("title") or raw.get("title_html") or ""
        snippet = raw.get("content") or raw.get("snippet") or ""
        
        # compute authority prior
        try:
            from urllib.parse import urlparse

            domain = urlparse(url).netloc
        except Exception:
            domain = ""
        # Apply authority boost and news adjustment
        pre_adjustment_score = Heuristics.compute_authority_prior(url)  # Pass full URL, not just domain
        score *= pre_adjustment_score
        score = adjust_news(url, score)
        # penalize listing pages using consistent regex
        path = raw.get("url", "")
        if RE_LISTING.search(path):
            score *= 0.5

        return UrlCandidate(
            url=url,
            source_engine=engine,
            serp_position=pos,
            query_slice_id=slice_id,
            query_variant=variant,
            score=score,
            hints={
                "domain": domain, 
                "authority_prior": pre_adjustment_score,
                "title": title[:200],  # Increased for better context
                "snippet": snippet[:400]  # Increased for better context
            },
        )


class AiohttpContextManager:
    """Context Manager para sessões aiohttp com limpeza automática."""
    
    def __init__(self, timeout: int = 30, headers: dict = None):
        self.timeout = timeout
        self.headers = headers or {}
        self.session = None
    
    async def __aenter__(self):
        if not AIOHTTP_AVAILABLE:
            raise Exception("aiohttp not available")
        
        import aiohttp
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            headers=self.headers
        )
        return self.session
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            try:
                if exc_type is not None:
                    logger.error(f"aiohttp session error: {exc_val}")
            except Exception as e:
                logger.error(f"Error during aiohttp session cleanup: {e}")
            finally:
                try:
                    await self.session.close()
                except Exception as e:
                    logger.error(f"Error closing aiohttp session: {e}")
        return False  # Não suprime exceções


class RunnerSearxng:
    """Runner that executes SearxNG queries. If aiohttp is present and
    an endpoint is provided via env var SEARXNG_ENDPOINT it will perform real
    requests; otherwise returns an empty list (dry-run).
    """

    def __init__(self, endpoint: Optional[str] = None, max_pages: int = 1, max_results: int = 100, valves: Optional[Dict[str, Any]] = None):
        # valves-first: prefer explicit valves, fallback to env
        endpoint_val = None
        if valves and isinstance(valves, dict):
            endpoint_val = valves.get("searxng_endpoint") or valves.get("SEARXNG_ENDPOINT")
        self.endpoint = endpoint or endpoint_val or os.getenv("SEARXNG_ENDPOINT")
        self.max_pages = max_pages
        self.max_results = max_results
        # retries/backoff valves
        self.max_retries = 2
        self.base_backoff = 0.8
        try:
            if valves and isinstance(valves, dict):
                self.max_retries = int(valves.get("searxng_max_retries", 2))
                self.base_backoff = float(valves.get("searxng_base_backoff", 0.8))
        except Exception:
            pass

    async def run(self, params: dict) -> List[dict]:
        # Try to perform network search if endpoint and aiohttp available
        if not self.endpoint:
            raise RuntimeError("SearXNG endpoint not configured")
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp not available")

        import aiohttp, asyncio, random

        aggregated = []
        pages_seen = 0
        params = dict(params)
        params["format"] = "json"
        try:
            async with AiohttpContextManager(timeout=15) as session:
                for page in range(1, self.max_pages + 1):
                    params["pageno"] = page
                    # small jitter to avoid bursts
                    try:
                        await asyncio.sleep(random.uniform(0.15, 0.5))
                    except Exception:
                        pass
                    # Log/print da query completa enviada ao SearXNG
                    try:
                        q_preview = (params.get("q", "") or "")[:500]
                        log_params = {
                            "q": q_preview,
                            "pageno": params.get("pageno"),
                            "language": params.get("language", ""),
                            "format": params.get("format"),
                            "categories": params.get("categories", ""),
                            "time_range": params.get("time_range", ""),
                        }
                        logger.info(f"[SearXNG] GET {self.endpoint} params={log_params}")
                        print(f"[SearXNG] → page={params.get('pageno')} cats={log_params['categories']} lang={log_params['language']} tr={log_params['time_range']}\n    q: {q_preview}")
                        try:
                            with open("searxng_queries.log", "a", encoding="utf-8") as fp:
                                fp.write("\n=== TOOL_DISCOVERY SearXNG QUERY ===\n")
                                fp.write(f"pageno={params.get('pageno')} lang={params.get('language','')} cats={params.get('categories','')} time_range={params.get('time_range','')}\n")
                                fp.write(q_preview + "\n")
                        except Exception:
                            pass
                    except Exception:
                        pass
                    # retry with backoff for 429/network errors
                    attempt = 0
                    while True:
                        try:
                            async with session.get(self.endpoint, params=params) as resp:
                                if resp.status == 429:
                                    raise RuntimeError("HTTP 429 Too Many Requests")
                                resp.raise_for_status()
                                data = await resp.json()
                                # Log bruto (tamanho) do payload
                                try:
                                    page_count = len(data.get('results', []))
                                    logger.info(f"[SearXNG] Page {page} results count: {page_count}")
                                    print(f"[SearXNG] ← page={page} items={page_count}")
                                except Exception:
                                    pass
                                raw = data.get("results", [])
                                for it in raw:
                                    aggregated.append(it)
                                    if len(aggregated) >= self.max_results:
                                        break
                                pages_seen += 1
                                break  # success
                        except Exception as e:
                            if attempt >= self.max_retries:
                                raise
                            sleep_s = self.base_backoff * (2 ** attempt) + random.uniform(0.05, 0.35)
                            logger.warning(f"[SearXNG] attempt {attempt} failed: {e} - backing off {sleep_s:.2f}s")
                            try:
                                await asyncio.sleep(sleep_s)
                            except Exception:
                                pass
                            attempt += 1
                    if len(aggregated) >= self.max_results:
                        break
        except Exception as e:
            logger.warning(f"SearXNG request failed: {e}", exc_info=True)
            return []
        try:
            print(f"[SearXNG] ✓ total_items={len(aggregated)} pages_fetched={pages_seen} (cap={self.max_results}, max_pages={self.max_pages})")
        except Exception:
            pass
        return aggregated


class RunnerExa:
    """Runner for Exa.ai API (neural search, always top 20)."""

    def __init__(self, api_key: Optional[str] = None, valves: Optional[Dict[str, Any]] = None):
        api_val = None
        if valves and isinstance(valves, dict):
            api_val = valves.get("exa_api_key") or valves.get("EXA_API_KEY")
        self.api_key = api_key or api_val or os.getenv("EXA_API_KEY")
        self.valves = valves or {}  # Store valves for model access
        self.openai_client = None

    async def _get_exa_api_schema(self) -> Dict[str, Any]:
        """Return complete JSON Schema for Exa API."""
        return {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "A query de busca principal, OTIMIZADA PARA INGLÊS.",
                },
                "type": {
                    "type": "string",
                    "enum": ["keyword", "neural", "auto"],
                    "description": "O motor de busca a ser usado.",
                },
                "category": {
                    "type": "string",
                    "enum": [
                        "company",
                        "research paper", 
                        "news",
                        "pdf",
                        "github",
                        "tweet",
                        "personal site",
                        "linkedin profile",
                        "financial report",
                    ],
                    "description": "O tipo de conteúdo a ser buscado (opcional).",
                },
                "numResults": {"type": "integer"},
                "includeDomains": {"type": "array", "items": {"type": "string"}},
                "excludeDomains": {"type": "array", "items": {"type": "string"}},
                "startPublishedDate": {"type": "string", "format": "date-time"},
                "endPublishedDate": {"type": "string", "format": "date-time"},
                "contents": {
                    "type": "object",
                    "properties": {
                        "text": {
                            "oneOf": [
                                {"type": "boolean"},
                                {
                                    "type": "object",
                                    "properties": {
                                        "maxCharacters": {"type": "integer"}
                                    },
                                },
                            ]
                        },
                        "highlights": {
                            "type": "object",
                            "properties": {
                                "numSentences": {"type": "integer"},
                                "highlightsPerUrl": {"type": "integer"},
                            },
                        },
                        "summary": {"type": "boolean"},
                    },
                },
            },
            "required": ["query"],
        }

    async def _plan_search_with_llm(self, user_prompt: str) -> Dict[str, Any]:
        """Use OpenAI to convert prompt to structured JSON for Exa.ai (hardcoded neural + 20 results)."""
        if not self.openai_client:
            raise RuntimeError("OpenAI client not initialized")
            
        api_schema = await self._get_exa_api_schema()
        from datetime import timezone
        now = datetime.now(timezone.utc)

        # Build example payload via json.dumps to avoid f-string brace parsing issues
        example_payload = {
            "query": "Manus AI capabilities and differentiators",
            "type": "neural",
            "numResults": 20,
            "contents": {
                "highlights": {
                    "numSentences": 3
                }
            }
        }

        system_prompt = f"""
        Você é um agente de IA especialista em traduzir pedidos em linguagem natural para chamadas de API JSON precisas.
        Sua tarefa é analisar o prompt do usuário e gerar um objeto JSON que corresponda perfeitamente ao schema da API Exa /search.

        **Contexto Atual:**
        - Data e hora de hoje: {now.isoformat()}

        **JSON Schema da API:**
        ```json
        {json.dumps(api_schema, indent=2, ensure_ascii=False)}
        ```

        **Instruções RÍGIDAS (HARDCODED):**
        1. **SEMPRE use 'neural' como valor para `type`** - este sistema é hardcoded para busca neural
        2. **SEMPRE use 20 como valor para `numResults`** - este sistema é hardcoded para exatamente 20 resultados
        3. **QUERY SIMPLES:** Crie uma query NATURAL e SIMPLES em inglês, sem sintaxe de busca avançada (sem site:, filetype:, etc.)
        4. **INTERPRETE DATAS:** Use o contexto de data atual para calcular datas relativas (ex: "última semana")
        5. **CATEGORIA (OPCIONAL):** 
           - **SEMPRE use `category: "news"`** quando o prompt contém @noticias
           - **Use `category: "linkedin profile"`** quando o usuário pedir perfis do LinkedIn, informações de pessoas, ou dados profissionais
           - **Use `category: "company"`** quando o usuário pedir informações sobre empresas ou perfis corporativos
           - **Use `category: "research paper"`** para artigos acadêmicos e papers
           - **Use `category: "github"`** para repositórios e código
           - **Use `category: "tweet"`** para posts do Twitter/X
           - **Use `category: "financial report"`** para relatórios financeiros
           - **NÃO infira `category`** apenas pela presença de palavras como "pdf" na consulta
           - **Para outros casos**, omita `category` para busca geral
         6. **DOMAINS:** Se o usuário mencionar sites específicos, use includeDomains (não inclua na query)
         7. **SEJA PRECISO:** Siga o schema à risca. Retorne **APENAS** o objeto JSON válido

        **EXEMPLO DE SAÍDA:**
        {json.dumps(example_payload, indent=2, ensure_ascii=False)}
        """

        response = self.openai_client.chat.completions.create(
            model=self.valves.get("openai_model", "gpt-5-mini") if self.valves else "gpt-5-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": "json_object"},
            temperature=1.0,  # gpt-5-mini only supports default temperature (1.0)
        )
        plan_str = response.choices[0].message.content
        return json.loads(plan_str)

    async def _call_exa_async(self, session, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Call Exa API asynchronously."""
        headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json",
            "User-Agent": "Discovery-Agent-v2.0/2.0.0",
        }
        async with session.post(
            "https://api.exa.ai/search", headers=headers, json=payload, timeout=90
        ) as resp:
            if resp.status != 200:
                error_text = await resp.text()
                raise aiohttp.ClientResponseError(
                    resp.request_info,
                    resp.history,
                    status=resp.status,
                    message=error_text,
                    headers=resp.headers,
                )
            return await resp.json()

    def _format_result_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Format Exa result item."""
        snippet = (
            "\n".join(item.get("highlights", []))
            or item.get("summary", "")
            or item.get("text", "")[:500]
        )
        return {
            "url": item.get("url"), 
            "title": item.get("title"), 
            "snippet": snippet,
            "score": item.get("score", 1.0),
            "engine": "exa",
            "position": 1  # Exa doesn't provide position, use 1
        }

    async def run(self, payload: dict) -> List[dict]:
        """Run Exa search with neural mode, always returning exactly 20 results in a single query.

        For @noticias (or whenever caller provides a time window), we pass the
        time constraints into the planner prompt so it can include
        startPublishedDate/endPublishedDate and infer category=news when relevant.
        """
        if not self.api_key:
            raise RuntimeError("Exa API key not configured")
        if not AIOHTTP_AVAILABLE:
            raise RuntimeError("aiohttp not available")
        if not self.openai_client:
            raise RuntimeError("OpenAI client not initialized for Exa planning")

        try:
            import aiohttp

            # ALWAYS use LLM planner for Exa (like novoExa.py)
            user_query = payload.get("query", "")
            # Optional time hints coming from plan slicing
            after_str = payload.get("after")
            before_str = payload.get("before")
            news_profile = bool(payload.get("news_profile", False))
            # Sanitize engine-specific tokens so Exa planner doesn't infer domains/categories incorrectly
            def _sanitize_for_exa(q: str) -> str:
                s = q or ""
                s = re.sub(r"\bsite:[^\s)]+", " ", s, flags=re.I)
                s = re.sub(r"\bfiletype:[^\s)]+", " ", s, flags=re.I)
                s = re.sub(r"\b(after|before):[^\s)]+", " ", s, flags=re.I)
                s = re.sub(r"\s+", " ", s).strip()
                return s
            user_prompt_sanitized = _sanitize_for_exa(user_query)
            # Append explicit constraints so the planner can generate dated payload
            planner_user_prompt = user_prompt_sanitized
            time_hints = []
            # Convert to RFC3339 when possible; otherwise pass raw
            def _to_rfc3339(d: str, end_of_day: bool) -> str:
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(d)
                    if end_of_day:
                        return dt.strftime('%Y-%m-%dT23:59:59Z')
                    else:
                        return dt.strftime('%Y-%m-%dT00:00:00Z')
                except Exception:
                    return d
            if after_str:
                time_hints.append(f"startPublishedDate={_to_rfc3339(after_str, end_of_day=False)}")
            if before_str:
                time_hints.append(f"endPublishedDate={_to_rfc3339(before_str, end_of_day=True)}")
            if time_hints:
                planner_user_prompt += "\nConstraints: " + ", ".join(time_hints)
            if news_profile:
                planner_user_prompt += "\nProfile: news=true (prefer news content category when appropriate)"
            try:
                print(f"[Exa] Sanitized user prompt: {user_prompt_sanitized}")
            except Exception:
                pass
            planned_params = await self._plan_search_with_llm(planner_user_prompt)
            
            # HARDCODED: Always neural mode and exactly 20 results
            planned_params["type"] = "neural"
            planned_params["numResults"] = 20
            
            # Drop category by default unless explicitly kept via valves
            try:
                keep_cat = bool(self.valves.get("exa_keep_category", False)) if self.valves else False
            except Exception:
                keep_cat = False
            # Detect explicit filetype/pdf intent in user query or valves
            uq = (user_query or "").lower()
            explicit_pdf = ("filetype:pdf" in uq) or ("apenas pdf" in uq) or ("somente pdf" in uq) or ("only pdf" in uq) or ("pdf only" in uq)
            try:
                force_filetypes = self.valves.get("force_filetypes") if self.valves else None
                if isinstance(force_filetypes, str):
                    explicit_pdf = explicit_pdf or ("pdf" in [p.strip().lower() for p in force_filetypes.split(',') if p.strip()])
                elif isinstance(force_filetypes, list):
                    explicit_pdf = explicit_pdf or ("pdf" in [str(x).strip().lower() for x in force_filetypes])
            except Exception:
                pass
            if not (keep_cat or explicit_pdf):
                planned_params.pop("category", None)
            
            # Add contents for highlights if not present
            if "contents" not in planned_params:
                planned_params["contents"] = {"highlights": {"numSentences": 3}}

            logger.info(f"Exa LLM-planned query (hardcoded neural + 20): {planned_params.get('query', 'N/A')}")
            logger.info(f"Exa payload: {json.dumps(planned_params, indent=2, ensure_ascii=False)}")
            try:
                print(f"[Exa] Query: {planned_params.get('query','')}")
                print(f"[Exa] Payload: {json.dumps(planned_params, ensure_ascii=False)}")
            except Exception:
                pass

            async with aiohttp.ClientSession() as session:
                exa_data = await self._call_exa_async(session, planned_params)
            
            # Process results
            exa_results = exa_data.get("results", [])
            formatted_results = [self._format_result_item(r) for r in exa_results]
            
            logger.info(f"Exa returned {len(formatted_results)} results (target: 20)")
            return formatted_results  # Return exactly what Exa gives us (should be 20)
            
        except Exception as e:
            logger.warning(f"Exa request failed: {e}", exc_info=True)
            return []


@dataclass
class SearchPlan:
    core_query: str
    after: Optional[str]
    before: Optional[str]
    slice_id: Optional[str]
    variant: str
    page: int
    sites: List[str]
    filetypes: List[str]
    engine: str = "searxng"
    qdf_level: Optional[int] = None
    lang: Optional[str] = None
    country: Optional[str] = None


def generate_search_plans(req: DiscoverRequest) -> List[SearchPlan]:
    plans: List[SearchPlan] = []
    
    # Normalize valves to avoid AttributeError when None or Pydantic model
    from pydantic import BaseModel
    v = req.valves
    if isinstance(v, BaseModel):
        v = v.model_dump()
    elif v is None:
        v = {}
    req.valves = v
    logger.info(f"[Discovery] Normalized req.valves: {req.valves}")
    
    # Detectar perfil de notícias baseado em @noticias (não categoria)
    news_profile_active = QueryClassifier.is_news_profile_active(req.query_text)
    profile = "news" if news_profile_active else "general"
    
    # Get pages_per_slice from valves
    try:
        pages_per_slice = req.pages_per_slice or int(_get_valve(req.valves, "pages_per_slice", "DISCOVERY_PAGES_PER_SLICE", "2"))
    except (ValueError, TypeError):
        pages_per_slice = 2
    
    # Build from a single core query; any LLM expansion should be handled by caller
    core_queries: List[str] = [req.query_text]

    # decide engines by valves
    enable_searx = req.valves.get('enable_searxng', True) if req.valves else True
    enable_exa = req.valves.get('enable_exa', True) if req.valves else True

    engines = []
    if enable_searx:
        engines.append("searxng")
    if enable_exa:
        engines.append("exa")
    if not engines:
        raise RuntimeError("All search engines are disabled")

    if news_profile_active:
        # Janela padrão para news se usuário não deu datas
        from datetime import timedelta
        DEFAULT_NEWS_DAYS = int(_get_valve(req.valves, "news_default_days", "DISCOVERY_NEWS_DEFAULT_DAYS", "90"))
        
        eff_after = req.after
        eff_before = req.before
        if not eff_after and not eff_before:
            eff_before = datetime.utcnow().date()
            eff_after = eff_before - timedelta(days=DEFAULT_NEWS_DAYS)
        
        # Fatiar por mês quando janela <= 90d, senão por trimestre
        delta_days = (eff_before - eff_after).days if (eff_after and eff_before) else DEFAULT_NEWS_DAYS
        if delta_days <= 90:
            slices = TimeSlicer.month_slices(eff_after, eff_before)
        else:
            slices = TimeSlicer.quarter_slices(eff_after, eff_before)
        
        if not slices:
            slices = [(eff_after, eff_before)]
        for start, end in slices:
            slice_id = start.isoformat()[:7] if start else None
            for core in core_queries:
                for variant in ("Q1",):  # Only Q1 variant for news
                    for eng in engines:
                        # Create single plan per query (pagination handled by runners)
                        plans.append(
                            SearchPlan(
                                core_query=core.replace("@noticias", "").strip(),
                                after=start.isoformat() if start else None,
                                before=end.isoformat() if end else None,
                                slice_id=slice_id,
                                variant=variant,
                                page=1,  # Always page 1, pagination handled by runners
                                sites=req.whitelist or [],
                                filetypes=[],
                                qdf_level=4,  # QDF alto para news
                                engine=eng,
                            )
                            )
    else:
        for core in core_queries:
            for eng in engines:
                # Create plans for each page (1 to pages_per_slice)
                # Create single plan per query (pagination handled by runners)
                plans.append(
                    SearchPlan(
                        core_query=core.replace("@noticias", "").strip(),
                        after=req.after.isoformat() if req.after else None,
                        before=req.before.isoformat() if req.before else None,
                        slice_id=None,
                        variant="Q1",
                        page=1,  # Always page 1, pagination handled by runners
                        sites=req.whitelist or [],
                        filetypes=[],
                        engine=eng,
                    )
                )
    return plans


async def generate_queries_with_llm(query: str, intent_profile: Dict[str, Any], messages: Optional[List[Dict]] = None, language: str = "", __event_emitter__: Optional[Callable] = None, valves: Optional[Tools.DiscoveryValves] = None) -> List[Dict[str, Any]]:
    """Ported LLM planner: retorna planos/queries JSON gerados pelo LLM.

    Saída esperada: List[dict] com campos como core_query, sites, filetypes, date_range, qdf_level, notes
    """
    if __event_emitter__:
        await emit_status(__event_emitter__, "🧠 Gerando planos de busca com LLM...")

    if not OPENAI_AVAILABLE or not valves or not (valves.get('openai_api_key', '') if isinstance(valves, dict) else getattr(valves, 'openai_api_key', '')):
        raise RuntimeError("LLM planner required but OpenAI or API key is not available")

    # Construir prompt inspirado no supersearcher para gerar planos adaptativos (1 a 3)
    
    # Detectar perfil de notícias baseado em @noticias
    news_profile_active = QueryClassifier.is_news_profile_active(query)
    
    # Capturar restrições de valves
    v_lang = (valves.get('search_language') if isinstance(valves, dict) else getattr(valves, 'search_language', '')) or ''
    v_country = (valves.get('search_country') if isinstance(valves, dict) else getattr(valves, 'search_country', '')) or ''
    v_filetypes_raw = (valves.get('force_filetypes') if isinstance(valves, dict) else getattr(valves, 'force_filetypes', []))
    v_filetypes: List[str] = []
    try:
        if isinstance(v_filetypes_raw, list):
            v_filetypes = [str(x).strip() for x in v_filetypes_raw if str(x).strip()]
        elif isinstance(v_filetypes_raw, str):
            # support comma-separated string
            parts = [p.strip() for p in v_filetypes_raw.split(',') if p.strip()]
            v_filetypes = parts
        else:
            v_filetypes = []
    except Exception:
        v_filetypes = []
    # Sinais de classificação simples (espelhando supersearcher)
    try:
        classification = classify_query_intent(query)
    except Exception:
        classification = {"needs_current_info": False, "needs_comparison": False, "needs_deep_research": False, "complexity_level": "medium"}
    classification_context = ""
    if classification.get("needs_current_info"):
        classification_context += "FOCO TEMPORAL: Priorize informações recentes e datas explícitas quando existirem.\n"
    if classification.get("needs_comparison"):
        classification_context += "ANÁLISE COMPARATIVA: Busque fontes que permitam comparação (reports, reviews).\n"
    if classification.get("needs_deep_research"):
        classification_context += "PESQUISA APROFUNDADA: Inclua fontes técnicas/whitepapers/relatórios.\n"

    valves_context = ""
    if v_lang:
        valves_context += f"Restrinja idioma (quando possível) a: {v_lang}.\n"
    if v_country:
        valves_context += f"Preferência de país/região: {v_country}.\n"
    if v_filetypes:
        try:
            valves_context += f"Force filetypes (quando fizer sentido): {', '.join(v_filetypes)}.\n"
        except Exception:
            pass

    # Do NOT inject chat history to avoid cross-contamination between chats
    history = ""

    intent_context_str = json.dumps(intent_profile or {}, ensure_ascii=False, indent=2)

    # Prompt de 2a geração (v2), mais estruturado e com exemplos
    base_prompt_template = (
        "Você é um estrategista de busca especialista e assistente de IA focado em pesquisa aprofundada. "
        "Sua tarefa é decompor uma consulta complexa de usuário em 1 a 3 planos de busca independentes e paralelizáveis. "
        "Cada plano deve ser otimizado para uma faceta diferente da consulta, maximizando a diversidade e cobertura da busca. "
        "Retorne SOMENTE o JSON com a lista de planos.\\n"
        "\n"
        "--- \n"
        "### [CONTEXTO DA CONSULTA]\n"
        "{intent_context}\n"
        "\n"
        "### [ORIENTAÇÕES BASEADAS NO CONTEXTO]\n"
        "**Se intent_profile indica país específico (country_bias):**\n"
        "- Inclua esse país nas queries geradas\n"
        "- Exemplo: 'country_bias': 'br' → inclua 'Brasil' nas queries\n"
        "\n"
        "**Se intent_profile indica mode notícias (news_mode: true):**\n"
        "- Pelo menos 1 plan deve focar em desenvolvimentos recentes\n"
        "- Use janela temporal específica se disponível\n"
        "\n"
        "**Se intent_profile indica priorização (has_prioritization: true):**\n"
        "- Mantenha instruções de priorização nas queries\n"
        "- Exemplo: 'priorize fontes oficiais' deve aparecer nos plans\n"
        "\n"
        "**Se intent_profile indica domínios preferidos (prefer_domains):**\n"
        "- Considere esses domínios na formulação das queries\n"
        "\n"
        "---\n"
        "### [CONSULTA DO USUÁRIO]\n"
        "<<< {query} >>>\n"
        "\n"
        "--- \n"
        "### [REGRAS PARA GERAÇÃO DE `core_query`]\n"
        "1. **Formato de Busca:** O `core_query` DEVE ser uma string de busca pura para um motor de busca. Use operadores como `\"\"` para frases, `site:`, `filetype:`, `OR`.\n"
        "2. **Sem Explicações:** NÃO inclua no `core_query` texto explicativo, numeração (e.g., '1)'), ou anotações (e.g., '(objetivo:...)').\n"
        "3. **Idioma:** Use português para temas brasileiros (country_bias: br), inglês para temas internacionais.\n"
        "4. **Exemplo de `core_query` CORRETO:** `\"world trade as % of GDP\" 1948..1980 site:un.org OR site:worldbank.org`\n"
        "5. **Exemplo de `core_query` INCORRETO:** `1) \"world trade\" \"% of world GDP\" ... (objetivo: séries ...)`\n"
        "\n"
        "--- \n"
        "### [SCHEMA DE SAÍDA – ESTRITO]\n"
        "- NÃO use a chave \"core_queries\".\n"
        "- Cada plano DEVE ter: (a) \"core_query\" (string) OU (b) \"queries\" (lista de objetos), onde cada objeto contém ao menos \"core_query\".\n"
        "- Campos opcionais por item: \"sites\", \"filetypes\", \"qdf_level\", \"date_range\" (como dict {{after,before}} ou string \"after..before\").\n"
        "\n"
        "Exemplos válidos:\n"
        "{{\n  \"plans\": [ {{ \"core_query\": \"phrase\", \"sites\": [], \"filetypes\": [] }} ]\n}}\n"
        "{{\n  \"plans\": [ {{ \"queries\": [ {{\"core_query\": \"q1\"}}, {{\"core_query\": \"q2\"}} ] }} ]\n}}\n"
        "\n"
        "--- \n"
        "### [SAÍDA (JSON PURO)]\n"
        "Lembre-se: o tamanho de \"plans\" deve ser 1, 2 ou 3, conforme sua análise.\n"
        "{{\n  \"plans\": []\n}}"
    )
    base_prompt = base_prompt_template.format(intent_context=intent_context_str, query=query)

    try:
        api_key = valves.get('openai_api_key', '') if isinstance(valves, dict) else getattr(valves, 'openai_api_key', '')
        base_url = valves.get('openai_base_url', 'https://api.openai.com/v1') if isinstance(valves, dict) else getattr(valves, 'openai_base_url', 'https://api.openai.com/v1')
        model = valves.get('openai_model', 'gpt-5-mini') if isinstance(valves, dict) else getattr(valves, 'openai_model', 'gpt-5-mini')
        client = OpenAI(api_key=api_key, base_url=base_url)
        prompt_messages = [{"role": "user", "content": base_prompt}]
        if messages:
            prompt_messages = messages + prompt_messages

        # Logging do prompt do planner
        try:
            prompt_text = base_prompt if isinstance(base_prompt, str) else str(base_prompt)
            logger.info(f"[Planner] Prompt length: {len(prompt_text)} chars")
            logger.info(f"[Planner] Prompt preview: {prompt_text[:800]}{'...' if len(prompt_text) > 800 else ''}")
        except Exception:
            pass

        # Log full prompt and chat kwargs for debugging
        logger.info(f"[Tool_Discovery_Planner] Full Prompt: {json.dumps(prompt_messages, ensure_ascii=False, indent=2)}")
        chat_kwargs = {
            "model": model,
            "messages": prompt_messages,
            "max_completion_tokens": 10000,
            "response_format": {"type": "json_object"},
        }
        logger.info(f"[Tool_Discovery_Planner] Chat_kwargs: {json.dumps(chat_kwargs, ensure_ascii=False, indent=2)}")
        
        # Use synchronous call to support sync client usage
        r = client.chat.completions.create(
            model=model,
            messages=prompt_messages,
            max_completion_tokens=10000,
            response_format={"type": "json_object"},
        )
        
        # Log raw LLM response
        raw_response = r.choices[0].message.content
        logger.info(f"[Tool_Discovery_Planner] Raw LLM Response: {raw_response}")
        content = r.choices[0].message.content
        # Logging da resposta bruta do planner
        try:
            if isinstance(content, dict):
                logger.info(f"[Planner] Raw content dict keys: {list(content.keys())}")
            else:
                raw_preview = content[:1200] if isinstance(content, str) else str(content)[:1200]
                logger.info(f"[Planner] Raw content preview: {raw_preview}{'...' if len(str(content)) > 1200 else ''}")
                logger.info(f"[Planner] Full raw content: {str(content)}")
                # Persist full content for comparison with supersearcher
                try:
                    with open("planner_first_call.log", "a", encoding="utf-8") as fp:
                        fp.write("\n=== TOOL_DISCOVERY LLM FIRST RESPONSE ===\n")
                        fp.write(str(content) + "\n")
                except Exception:
                    pass
        except Exception:
            pass
        # content may be dict if response_format worked
        # Robust JSON parsing (mirroring supersearcher)
        def _try_parse_json(text: str):
            if not text:
                return None
            try:
                return json.loads(text)
            except Exception:
                import re
                t = text.strip()
                t = re.sub(r"^```json\s*\n", "", t, flags=re.IGNORECASE)
                t = re.sub(r"```$", "", t)
                m = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", t)
                if m:
                    try:
                        return json.loads(m.group(0))
                    except Exception:
                        return None
                return None

        parsed = content if isinstance(content, dict) else _try_parse_json(content)
        # Fallback: some providers may deliver JSON via tool_calls even with json_object
        if not parsed:
            try:
                tool_args = (
                    r.choices[0].message.tool_calls[0].function.arguments  # type: ignore[attr-defined]
                )
                parsed = _try_parse_json(tool_args)
                try:
                    logger.info(
                        f"[Planner] Parsed from tool_calls length={len(tool_args) if isinstance(tool_args, str) else 'NA'}"
                    )
                except Exception:
                    pass
            except Exception:
                parsed = None
        plans = (parsed.get("plans") if isinstance(parsed, dict) else []) or []

        # Normalize plans to expected minimal dicts
        out = []
        for p in plans:
            if isinstance(p, str):
                plan = {"core_query": p, "sites": [], "filetypes": [], "date_range": {}, "qdf_level": None}
            elif isinstance(p, dict):
                plan = p
            else:
                continue
                
            # Aplicar QDF alto se news_profile_active
            if news_profile_active:
                plan["qdf_level"] = max(plan.get("qdf_level", 0), 4)
                
            out.append(plan)
            
        logger.info(f"[Planner] Plans parsed (adaptive): {len(out)}")
        if len(out) == 0:
            raise RuntimeError("LLM returned no plans")
        # clamp to at most 3 plans
        final_out = out[:3]
        logger.info(f"[Tool_Discovery_Planner] Final Plans: {json.dumps(final_out, ensure_ascii=False, indent=2)}")
        return final_out
    except Exception as e:
        # No fallback: bubble up
        raise


async def unified_llm_analysis(scored_candidates_pool: List[Dict[str, Any]], original_query: str, prioritization_criteria: str = "", suggested_filetypes: Optional[List[str]] = None, top_n: int = 10, __event_emitter__: Optional[Callable] = None, valves: Optional[Tools.DiscoveryValves] = None, intent_profile: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Enhanced LLM analysis with enriched context and explicit rules.

    Returns ordered list of selected candidates (dicts).
    """
    if __event_emitter__:
        await emit_status(__event_emitter__, "🧠 Analisando candidatos com LLM para seleção final...")

    # Use valves if provided, otherwise fallback to env vars
    api_key = valves.openai_api_key if valves else _get_valve(None, "openai_api_key", "OPENAI_API_KEY", None)
    base_url = valves.openai_base_url if valves else _get_valve(None, "openai_base_url", "OPENAI_BASE_URL", "https://api.openai.com/v1")
    model = valves.openai_model if valves else _get_valve(None, "openai_model", "OPENAI_MODEL", "gpt-5-mini")
    
    if not OPENAI_AVAILABLE or not api_key:
        raise RuntimeError("LLM selector requires OpenAI and API key")

    # Clip pool by valve to avoid oversized prompts
    try:
        max_pool = int(getattr(valves, "max_candidates_for_llm_analysis", 100) or 100)
    except Exception:
        max_pool = 100
    
    # Intent profile is available for the LLM selector to use in its decision making
    # The LLM selector will handle topic relevance and scoring based on the intent profile
    
    # Apply soft boosts based on prioritization_criteria before trimming
    boosted_pool = []
    for raw in (scored_candidates_pool or [])[: max(1, max_pool * 3)]:  # consider more for boosting/backfill
        boosted_score, boost_amt = _soft_relevance_boost(raw, prioritization_criteria)
        item = dict(raw)
        item["selection_score"] = boosted_score
        item["_boost_amt"] = boost_amt
        boosted_pool.append(item)

    trimmed_pool = sorted(boosted_pool, key=lambda x: x.get("selection_score", x.get("score", 0)), reverse=True)[: max(1, max_pool)]

    # Diagnostic: measure boost impact
    try:
        # take top-K before and after boost for comparison
        pre_sorted = sorted(scored_candidates_pool or [], key=lambda x: x.get("selection_score", x.get("score", 0)), reverse=True)[: max(1, max_pool)]
        post_sorted = trimmed_pool
        pre_urls = [p.get("url") for p in pre_sorted]
        post_urls = [p.get("url") for p in post_sorted]
        moved_up = [u for u in post_urls if u not in pre_urls[:5]]  # items that were not top-5 before
        logger.info(f"[Selector-DIAG] pre_top_urls={pre_urls[:10]}")
        logger.info(f"[Selector-DIAG] post_top_urls={post_urls[:10]}")
        logger.info(f"[Selector-DIAG] moved_up_sample(count={len(moved_up)}): {moved_up[:10]}")
    except Exception:
        pass

    # Convert trimmed pool to selector-friendly dicts
    selector_pool: List[Dict[str, Any]] = []
    for raw in trimmed_pool:
        try:
            u = raw.get("url", "")
            try:
                d = urlparse(u).netloc if u else ""
            except Exception:
                d = ""
            selector_pool.append({
                "title": raw.get("title", ""),
                "snippet": raw.get("snippet", ""),
                "url": u,
                "score": raw.get("selection_score", raw.get("score", 0.0)),
                "is_authority": raw.get("is_authority", False),
                "is_news_host": raw.get("is_news_host", False),
                "domain": d,
            })
        except Exception as e:
            logger.warning(f"Failed to convert candidate for selector: {e}")
            continue

    # Determine context signals - usar news_profile_active
    news_profile_active = QueryClassifier.is_news_profile_active(original_query)
    
    # Extract time window hint from query or use default
    time_hint = None
    if news_profile_active:
        time_hint = "last_30d"  # Default for news queries
    
    # Build enriched payload
    # Get domain_cap from valves or use default (ensure int)
    try:
        domain_cap = int(_get_valve(valves, "domain_cap", "DISCOVERY_DOMAIN_CAP", 2) or 2)
    except Exception:
        domain_cap = 2
    logger.info(f"[Selector] Using domain_cap={domain_cap}")
    # Auto-relax domain_cap when there's an explicit single-domain restriction in the query
    try:
        import re
        domains = re.findall(r"site:\s*([a-z0-9\.-]+)", (original_query or ""), flags=re.I)
        unique_domains = sorted(set(domains))
        if len(unique_domains) == 1:
            # Allow many results from that domain to satisfy the restriction
            domain_cap = max(domain_cap, 50)
            logger.info(f"[Selector] domain_cap relaxed due to explicit site restriction: {unique_domains[0]} -> {domain_cap}")
    except Exception:
        pass
    
    payload = _build_selector_payload(
        query_text=original_query,
        candidates=selector_pool,
        news_mode=news_profile_active,
        time_hint=time_hint,
        domain_cap=domain_cap,  # Configurable max URLs per domain
        crit_text=prioritization_criteria,
        top_n=top_n,
        intent_profile=intent_profile
    )
    
    # Generate robust prompt
    prompt = _selector_prompt(payload)
    
    # Logging
    logger.info(f"[Selector] Prompt length: {len(prompt)}")
    logger.info(f"[Selector] Prompt preview: {prompt[:800]}{'...' if len(prompt) > 800 else ''}")

    try:
        client = OpenAI(
            api_key=api_key, 
            base_url=base_url
        )
        
        r = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=10000,
            response_format={"type": "json_object"},
        )
        
        content = r.choices[0].message.content
        logger.info(f"[Selector] Raw content preview: {str(content)[:1200]}{'...' if len(str(content)) > 1200 else ''}")
        
        # Parse response robustly (reuse planner parser approach)
        if isinstance(content, dict):
            analysis_data = content
        else:
            def _try_parse_json(text: str):
                import re as _re
                if not text:
                    return None
                try:
                    return json.loads(text)
                except Exception:
                    t = text.strip()
                    t = _re.sub(r"^```json\s*\n", "", t, flags=_re.I)
                    t = _re.sub(r"```$", "", t)
                    m = _re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", t)
                    if m:
                        try:
                            return json.loads(m.group(0))
                        except Exception:
                            return None
                    return None
            analysis_data = _try_parse_json(content)
            if not analysis_data:
                raise ValueError("LLM selector returned non-JSON content")
        
        # Validate and extract indices
        selected_indices = analysis_data.get("selected_indices", [])
        additional_indices = analysis_data.get("additional_indices", [])
        
        # Validate indices within bounds of the actually-sent pool
        valid_selected = [i for i in selected_indices if isinstance(i, int) and 0 <= i < len(trimmed_pool)]
        valid_additional = [i for i in additional_indices if isinstance(i, int) and 0 <= i < len(trimmed_pool)]
        
        # Build final selection from trimmed_pool
        selected = [trimmed_pool[i] for i in valid_selected] + [trimmed_pool[i] for i in valid_additional]
        # Keep original LLM order for optional soft backfill later
        _selected_all = list(selected)

        # Hard-apply domain_cap post selection (belt-and-suspenders)
        # Use the effective domain_cap (possibly relaxed above), not a fresh valve read
        try:
            cap = int(domain_cap)
        except Exception:
            cap = 2
        _seen: Dict[str, int] = {}
        capped: List[Dict[str, Any]] = []
        for item in selected:
            d = _norm_host(item.get("url", ""))
            _seen[d] = _seen.get(d, 0) + 1
            if _seen[d] <= cap:
                capped.append(item)
        selected = capped

        # Deterministic backfill to reach top_n respecting domain_cap (effective cap)
        desired = int(top_n)
        if len(selected) < desired:
            existing = {it.get("url", "") for it in selected}
            counts: Dict[str, int] = {}
            for it in selected:
                d = _norm_host(it.get("url", ""))
                counts[d] = counts.get(d, 0) + 1
            for it in trimmed_pool:
                if len(selected) >= desired:
                    break
                u = it.get("url", "")
                if not u or u in existing:
                    continue
                d = _norm_host(u)
                if counts.get(d, 0) < cap:
                    selected.append(it)
                    existing.add(u)
                    counts[d] = counts.get(d, 0) + 1

        # Optional: if still short, allow soft_domain_cap backfill to reach top_n
        try:
            soft_cap = _as_bool(_get_valve(valves, "soft_domain_cap", "DISCOVERY_SOFT_DOMAIN_CAP", False))
        except Exception:
            soft_cap = False
        if len(selected) < desired and soft_cap:
            existing = {it.get("url", "") for it in selected}
            for it in trimmed_pool:
                if len(selected) >= desired:
                    break
                u = it.get("url", "")
                if not u or u in existing:
                    continue
                selected.append(it)
                existing.add(u)
        
        # Log final selected URLs
        selected_urls = [c.get("url", "") for c in selected[:top_n]]
        logger.info(f"[Tool_Discovery_Selector] Final Selected URLs: {json.dumps(selected_urls, ensure_ascii=False, indent=2)}")
        
        return selected[:top_n]
        
    except Exception as e:
        logger.error(f"LLM selector failed: {e}")
        raise





import re
import time
from typing import Any, Dict


def qdf_to_tbs(qdf_level: Optional[int]) -> Optional[str]:
    if qdf_level is None:
        return None
    return {
        5: "qdr:d",
        4: "qdr:w",
        3: "qdr:m",
        2: "qdr:y",
        1: "qdr:m3",
        0: None,
    }.get(qdf_level)


# build_serper_payload removed - using Exa.ai instead


def extract_query_components(text: str) -> Dict[str, Any]:
    result = {"sites": set(), "filetypes": set(), "qdf_level": None}
    for m in re.finditer(r"site:([a-z0-9\.\-\_]+)", text, flags=re.I):
        result["sites"].add(m.group(1).lower())
    for m in re.finditer(r"(?:apenas|foque em|somente)\s+([a-z0-9\.\-]+)", text, flags=re.I):
        if "." in m.group(1):
            result["sites"].add(m.group(1).lower())
    # Capture patterns like 'priorize/prefira/dê preferência ... site X' as preferred sites
    for m in re.finditer(
        r"(?:prioriz[ae]|prefir[ae]|d[\u00ea\u00e9]\s+prefer[\u00ea\u00e9]ncia(?:\s+para)?|de\s+prefer[\u00ea\u00e9]ncia(?:\s+para)?)\s+"
        r"(?:(?:resultados?\s+do\s+)?site|dom[\u00ed\u00ed]nio)\s+([a-z0-9\.\-]+)",
        text, flags=re.I
    ):
        if "." in m.group(1):
            result["sites"].add(m.group(1).lower())
    for m in re.finditer(r"filetype:(pdf|html|docx|csv|xlsx|pptx|txt|md)", text, flags=re.I):
        result["filetypes"].add(m.group(1).lower())
    if re.search(r"\bpdfs?\b", text, flags=re.I):
        result["filetypes"].add("pdf")
    if re.search(r"\b(csvs?|excel|planilhas?)\b", text, flags=re.I):
        result["filetypes"].add("csv")
    if re.search(r"\b(powerpoints?|apresentações?)\b", text, flags=re.I):
        result["filetypes"].add("pptx")
    m = re.search(r"--QDF\s*=\s*([0-5])", text, flags=re.I)
    if m:
        result["qdf_level"] = int(m.group(1))
    return {"sites": list(result["sites"]), "filetypes": list(result["filetypes"]), "qdf_level": result["qdf_level"]}


def normalize_language_tag(lang: Optional[str]) -> Optional[str]:
    """Map language tags to 2-letter codes SearXNG understands (e.g., pt-BR -> pt)."""
    if not lang:
        return None
    try:
        s = str(lang).strip().lower()
        if s in {"pt-br", "ptbr", "pt_br", "pt-pt", "pt"}:
            return "pt"
        if s in {"en-us", "en_us", "en-gb", "eng", "en"}:
            return "en"
        if len(s) >= 2:
            return s[:2]
    except Exception:
        return None
    return None


def sanitize_core_query_for_search(text: str) -> str:
    """Convert verbose, enumerated planner text into a single, valid search query string.

    - Keeps the first line that looks like a query (quotes/site:/filetype:/year-range/OR)
    - Strips numbering like "1)" or bullets
    - Removes parenthetical notes like (objetivo: ...)
    - Collapses whitespace and trims length
    """
    import re
    if not text:
        return ""
    t = str(text)
    # Remove parentheticals that look like explanations/objectives
    try:
        t = re.sub(r"\((?:[^)]*?(?:objetivo|intenção|intencao)[^)]*?)\)", " ", t, flags=re.IGNORECASE)
    except Exception:
        pass
    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
    # Prefer lines with quoted phrases or year-ranges over plain site-groups
    query_like: Optional[str] = None
    for ln in lines:
        if re.search(r'(\".+\"|\b\d{4}\.\.\d{4}\b)', ln, flags=re.IGNORECASE):
            ln = re.sub(r'^(?:\d+\)\s*|[\-\*•]\s*)', '', ln).strip()
            query_like = ln
            break
    if not query_like:
        for ln in lines:
            if re.search(r'(site:|filetype:|\bOR\b)', ln, flags=re.IGNORECASE):
                ln = re.sub(r'^(?:\d+\)\s*|[\-\*•]\s*)', '', ln).strip()
                query_like = ln
                break
    # If nothing looked like a query, fall back to the raw trimmed text
    # This keeps @noticias natural-language queries working for SearXNG
    if not query_like:
        return t.strip()[:300]

    # Try to auto-close unmatched parentheses (common when truncation happens)
    try:
        if query_like.count('(') > query_like.count(')'):
            query_like = query_like + ')'
    except Exception:
        pass
    # Hard fail if only a site: OR group remains
    try:
        import re as _re
        only_sites = _re.fullmatch(r"\s*\(?\s*(?:site:[^\s)]+\s*(?:OR\s*site:[^\s)]+\s*)+)\)?\s*", query_like, flags=_re.IGNORECASE)
        if only_sites:
            raise RuntimeError("Sanitized query contains only site: clauses without keywords")
    except RuntimeError:
        raise
    except Exception:
        pass
    return query_like[:300]


def build_intent_profile(user_text: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Constrói um perfil de intenção unificado baseado na mensagem completa do usuário.
    Replica a estratégia do supersearcher: extrai sinais multilíngues e cria um policy object
    que viaja por todo o pipeline sem perder informações.
    """
    if not user_text or not user_text.strip():
        return {}
    
    txt = user_text.lower()
    intent = {}
    
    # 1) Detecção de país/região (multilíngue)
    usa_indicators = ["usa", "eua", "united states", "u.s.", "america", "estados unidos"]
    uk_indicators = ["uk", "reino unido", "britain", "inglaterra"]
    br_indicators = ["brasil", "brazil", "br"]
    
    if any(k in txt for k in usa_indicators):
        intent["country_bias"] = "us"
        intent["lang_bias"] = "en"
    elif any(k in txt for k in uk_indicators):
        intent["country_bias"] = "uk" 
        intent["lang_bias"] = "en"
    elif any(k in txt for k in br_indicators):
        intent["country_bias"] = "br"
        intent["lang_bias"] = "pt-BR"
    
    # 2) Detecção de modo notícias (multilíngue)
    news_indicators = ["até agora", "so far", "últimas", "notícias", "news", "recentes", "atual"]
    if any(k in txt for k in news_indicators):
        intent["news_mode"] = True
        intent["time_window"] = "past_12_months"
    
    # 3) Detecção de priorização (multilíngue)
    priority_indicators = [
        "priorize", "prefira", "privilegie", "favoreça", "prioritize", "prefer",
        "dê preferência", "de preferência", "prioridade para", "foco em", "foque em"
    ]
    if any(k in txt for k in priority_indicators):
        intent["has_prioritization"] = True
    
    # 4) Detecção de janela temporal
    if any(k in txt for k in ["este ano", "últimos 12", "past 12", "último ano"]):
        intent["time_window"] = "past_12_months"
    elif any(k in txt for k in ["últimos 6", "past 6", "último semestre"]):
        intent["time_window"] = "past_6_months"
    elif any(k in txt for k in ["últimos 3", "past 3", "último trimestre"]):
        intent["time_window"] = "past_3_months"
    
    # 5) Detecção de domínios preferidos explícitos (genérico)
    if "site:" in txt or "domínio" in txt:
        # Extrair domínios mencionados
        import re
        domains = re.findall(r'site:([a-z0-9.-]+)', txt)
        if domains:
            intent["prefer_domains"] = domains
            # domain_cap será lido das valves, não hardcoded aqui
    
    # 6) Configurações padrão
    # domain_cap será lido das valves, não hardcoded aqui
    
    return intent




def extract_prioritization_criteria(text: str) -> str:
    """
    Extrai instruções de priorização de forma geral a partir de gatilhos como:
    - priorize, prioridade para
    - prefira, de preferência (para), dê preferência (a/para)
    - (dê) foco em, foque em
    - privilegie, valorize
    - apenas, somente
    Suporta negação ("não priorize ...") e normaliza para frases curtas.
    Retorna uma string com itens separados por '; '.
    """
    if not text or not text.strip():
        return ""

    import re
    import unicodedata

    # Mantém o original para preservar acentos no alvo; usa lower() só para matching
    raw = text
    lower = text.lower()

    # Gatilhos principais (PT + variantes). Repara nos grupos opcionais e acentos.
    TRIGGER_RE = re.compile(
        r"""
        (?P<neg>nao\s+|não\s+)?                                # negação opcional
        (?:
            prioriz(?:e|ar)                                    # priorize/priorizar
            | prioridade(?:\s+para)?                           # prioridade (para)
            | prefir(?:a|ir)                                   # prefira/prefirir
            | d[eê]\s+prefer[eê]ncia(?:\s+(?:a|para))?         # de preferência (a/para)
            | d[eê]\s+foco(?:\s+em)?                           # dê foco (em)
            | foqu(?:e|e\s+em|e-se\s+em)?                      # foque/foque em/foque-se em
            | focar(?:\s+em)?                                  # focar em
            | privilegi(?:e|ar)                                # privilegie/privilegiar
            | valorize                                         # valorize
        )
        \s+
        (?P<target>[^.;:\n,]{1,160})                           # captura o alvo até pontuação forte
        """,
        re.IGNORECASE | re.UNICODE | re.VERBOSE,
    )

    # Padrão para "apenas/somente X ..."
    ONLY_RE = re.compile(
        r"""
        \b(?:apenas|somente)\s+
        (?P<target>[^.;:\n,]{1,160})
        """,
        re.IGNORECASE | re.UNICODE | re.VERBOSE,
    )

    def _clean_target(s: str) -> str:
        # tira pedaços comuns de ruído ao fim
        s = s.strip().strip('""\'()[] ')
        # corta complementos vagos no final
        s = re.sub(r"\b(?:por\s+exemplo|como|etc)\b.*$", "", s, flags=re.IGNORECASE)
        # evita capturar dois comandos unidos por conjunções fortes
        s = re.split(r"\b(?:;| e | ou | mas | porém | contudo | todavia)\b", s, maxsplit=1)[0]
        # normaliza espaços
        s = re.sub(r"\s{2,}", " ", s).strip()
        # limita tamanho
        if len(s) > 100:
            s = s[:100].rstrip()
        return s

    seen = set()
    out = []

    # 1) "apenas/somente …"
    for m in ONLY_RE.finditer(raw):
        tgt = _clean_target(m.group("target"))
        if tgt and tgt.lower() not in seen:
            out.append(f"use apenas {tgt}")
            seen.add(tgt.lower())

    # 2) Gatilhos gerais com (possível) negação
    for m in TRIGGER_RE.finditer(raw):
        tgt = _clean_target(m.group("target"))
        if not tgt:
            continue
        neg = bool(m.group("neg"))
        key = ("neg:" if neg else "pos:") + tgt.lower()
        if key in seen:
            continue
        seen.add(key)
        if neg:
            out.append(f"evite {tgt}")
        else:
            out.append(f"priorize {tgt}")


    return "; ".join(out) if out else ""


BAD_HOSTS = {
    # Known low-quality, piracy, or irrelevant portals for our context
    "btdig.com",
    "annas-archive.org",
    "torrentgalaxy.to",
    "1337x.to",
    "thepiratebay.org",
    "nyaa.si",
    "dailymail.co.uk",
    "mail.google.com",
    "maps.google.com",
}


def is_content_acceptable(
    result: dict, original_query: str, strict: bool = False, valves: Optional[Dict[str, Any]] = None
) -> bool:
    """Filtro inteligente unificado para verificar se o conteúdo é aceitável.

    Args:
        result: Dicionário com title, snippet, url
        original_query: Query original para comparação
        strict: Se True, aplica filtros mais rigorosos
        valves: Configurações para controlar filtros

    Returns:
        bool: True se o conteúdo é aceitável
    """
    title = result.get("title", "")
    snippet = result.get("snippet", "")
    url = result.get("url", "")

    # Combinar texto para análise
    full_text = f"{title} {snippet}".strip()

    if not full_text:
        return True  # Aceita se não há texto para analisar

    # Configurações via valves
    strict_language_filter = _as_bool(_get_valve(valves, "strict_language_filter", "STRICT_LANGUAGE_FILTER", True))
    allow_forums = _as_bool(_get_valve(valves, "allow_forums", "ALLOW_FORUMS", True))
    non_latin_threshold = float(_get_valve(valves, "non_latin_threshold", "NON_LATIN_THRESHOLD", "0.15"))

    # Calcular total_chars para uso em múltiplos filtros
    total_chars = len(full_text)

    # === FILTRO 1: IDIOMAS INACEITÁVEIS (configurável) ===
    if strict_language_filter:
        # Verificar caracteres CJK (Chinês, Japonês, Coreano)
        cjk_chars = sum(
            1
            for c in full_text
            if 0x4E00 <= ord(c) <= 0x9FFF  # CJK Unified Ideographs
            or 0x3400 <= ord(c) <= 0x4DBF  # CJK Extension A
            or 0x3040 <= ord(c) <= 0x309F  # Hiragana
            or 0x30A0 <= ord(c) <= 0x30FF
        )  # Katakana

        # Caracteres árabes/persas
        arabic_chars = sum(1 for c in full_text if 0x0600 <= ord(c) <= 0x06FF)

        # Caracteres cirílicos (russo, etc.)
        cyrillic_chars = sum(1 for c in full_text if 0x0400 <= ord(c) <= 0x04FF)

        # Se mais que o threshold são caracteres não-latinos, muito provável ser idioma indesejado
        non_latin_ratio = (cjk_chars + arabic_chars + cyrillic_chars) / max(total_chars, 1)
        if non_latin_ratio > non_latin_threshold:
            return False

    # === FILTRO 2: SPAM/LIXO ÓBVIO ===
    text_lower = full_text.lower()

    # Indicadores de spam (verificar palavras completas, não substrings)
    spam_indicators = [
        "porn",
        "xxx",
        "sex",
        "casino",
        "poker",
        "gambling",
        "cialis",
        "viagra",
        "pharmacy",
        "drugs",
        "pills",
        "free money",
        "make money fast",
        "get rich quick",
        "click here",
        "buy now",
        "act now",
        "limited time",
        "weight loss",
        "diet pills",
        "lose weight fast",
    ]

    # Verificar se alguma palavra de spam aparece como palavra completa
    text_words = set(text_lower.split())
    found_spam_words = text_words.intersection(spam_indicators)

    if found_spam_words:
        return False

    # === FILTRO 3: DOMÍNIOS SUSPEITOS OU INDESEJADOS ===
    from urllib.parse import urlparse
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        host = ""
    if host in BAD_HOSTS:
        return False
    # paths indesejados (login/portal/mail/maps/forums)
    path_lower = urlparse(url).path.lower() if url else ""
    # Evitar falsos positivos como "/authors" contendo "auth"
    path_segments = [seg for seg in path_lower.split('/') if seg]
    if any(seg in {"login", "signin", "auth", "clientportal", "mail", "maps", "md5"} for seg in path_segments):
        return False
    # Rejeitar apenas portais específicos problemáticos (não /Portals/ que é usado por sites institucionais)
    # Também não rejeitar portais educacionais legítimos como vestibulares.estrategia.com
    problematic_portals = ["/clientportal/", "/userportal/", "/memberportal/", "/adminportal/", "/billingportal/", "/paymentportal/"]
    if any(portal in path_lower for portal in problematic_portals):
        return False
    # TLD/padrões que indicam agregadores ou índices de arquivos
    if any(k in host for k in ("btdig", "annas-archive", "torrent", "magnet:")):
        return False
    # Heurística para fóruns genéricos sem relação (configurável)
    if not allow_forums and any(h in host for h in ("reddit.com", "forums.", ".forum", "board.")) and "empresas" not in original_query.lower():
        return False

    # === FILTRO 4: LANGDETECT (SE DISPONÍVEL) ===
    if LANGDETECT_AVAILABLE:
        try:
            detected_lang = detect(full_text)
            # Aceita idiomas ocidentais principais
            acceptable_languages = {"pt", "en", "es", "fr", "it", "de", "nl"}
            if detected_lang not in acceptable_languages:
                # Mas ainda permite se tem muitos caracteres latinos (caso de textos mistos)
                latin_chars = sum(1 for c in full_text if ord(c) < 256)
                latin_ratio = latin_chars / max(total_chars, 1)
                return latin_ratio > 0.85
        except LangDetectException:
            pass  # Continua para outras verificações

    # === FILTRO 5: RELEVÂNCIA MÍNIMA ===
    # Aplicar filtro mais rigoroso se strict=True
    if strict:
        # Para modo strict, exige mais relevância
        query_words = set(original_query.lower().split())
        text_words = set(text_lower.split())

        # Palavras muito comuns que não contam
        stop_words = {
            "de",
            "da",
            "do",
            "para",
            "com",
            "em",
            "no",
            "na",
            "um",
            "uma",
            "the",
            "of",
            "to",
            "and",
            "a",
            "in",
            "is",
            "it",
            "you",
            "that",
            "he",
            "was",
            "for",
            "on",
            "are",
            "as",
            "with",
            "his",
            "they",
        }

        meaningful_query_words = query_words - stop_words
        meaningful_text_words = text_words - stop_words

        # Se tem palavras relacionadas a IA/tech/brasileiro, sempre aceita
        tech_words = {
            "ai",
            "ia",
            "artificial",
            "intelligence",
            "inteligencia",
            "tecnologia",
            "technology",
            "model",
            "modelo",
            "data",
            "dados",
            "analysis",
            "analise",
            "machine",
            "learning",
            "deep",
            "neural",
            "algoritmo",
            "algorithm",
            "benchmark",
            "benchmarks",
            "dataset",
            "brasil",
            "brazil",
            "panorama",
            "pdf",
            "recomendacoes",
            "recomendações",
            "regulatorio",
            "regulatório",
            "sentimentos",
            "português",
            "portuguesa",
            "brasileiro",
            "brasileira",
            "its",
            "rio",
            "senado",
            "federal",
            "governo",
            "publicas",
            "públicas",
        }

        if meaningful_text_words.intersection(tech_words):
            return True  # Qualquer contexto tech/BR é aceito

        if meaningful_query_words and meaningful_query_words.intersection(
            meaningful_text_words
        ):
            return True  # Tem sobreposição com query

        # Para modo strict, exige mais conteúdo
        if len(full_text) < 50:  # Texto muito curto para modo strict
            return False

        # Se chegou até aqui e não tem nada relacionado, rejeita
        return (
            len(meaningful_text_words) > 2
        )  # Exige pelo menos 3 palavras significativas
    else:
        # Modo permissivo (padrão)
        if len(full_text) < 30:  # Texto muito curto
            return True

        return True  # Aceita por padrão

    return True

def _run_coro_blocking(coro):
    """Run coroutine in a safe way, handling existing event loops."""
    try:
        loop = asyncio.get_running_loop()
        # Loop is running -> use thread-safe execution
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(asyncio.run, coro)
            return future.result()
    except RuntimeError:
        # No loop running -> safe to use asyncio.run
        return asyncio.run(coro)


def _get_url(item):
    """Get URL from search result (handles both 'url' and 'link' keys)."""
    return item.get("url") or item.get("link")


def _as_bool(x):
    """Convert value to boolean, handling string representations correctly."""
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    return str(x).strip().lower() in {"1", "true", "yes", "on"}


def _or_clause(prefix: str, items: List[str]) -> str:
    """Create OR clause for multiple site: or filetype: restrictions."""
    toks = [f"{prefix}:{i.strip()}" for i in items if i and i.strip()]
    return "(" + " OR ".join(toks) + ")" if len(toks) > 1 else (toks[0] if toks else "")


def detect_axis_type(query_text: str, profile: str) -> str:
    """Lightweight detector for axis_type used by the selector prompt.

    Returns one of: "temporal", "conceitual", "prático", "comparativo", "geral".
    """
    try:
        txt = (query_text or "").lower()
        if any(k in txt for k in ["linha do tempo", "timeline", "1950", "1968", "1971", "1973", "1976", "1944", "1980", "últimos", "periodo", "período"]):
            return "temporal"
        if any(k in txt for k in ["definição", "definicao", "conceito", "marco", "regulat", "institucional", "articles of agreement", "final act"]):
            return "conceitual"
        if any(k in txt for k in ["estudo de caso", "case", "implementa", "experiência", "experiencia", "prático", "pratico"]):
            return "prático"
        if any(k in txt for k in ["compar", "vs", "versus", "benchmark", "metadado", "metadata", "metodologia", "methodology"]):
            return "comparativo"
    except Exception:
        pass
    return "geral"

def discover(req: DiscoverRequest) -> DiscoveryResult:
    """High-level discover orchestration.

    Performs planning, hybrid search execution (network calls via runners),
    normalization, filtering and optional LLM-based selection.
    """

    # Normalize valves to avoid AttributeError when None or Pydantic model
    from pydantic import BaseModel
    v = req.valves
    if isinstance(v, BaseModel):
        v = v.model_dump()
    elif v is None:
        v = {}
    req.valves = v

    # Build intent profile from complete user message (replica supersearcher strategy)
    intent_profile = build_intent_profile(req.query_text, req.valves)
    intent_profile["axis_type"] = detect_axis_type(req.query_text, req.profile)
    logger.info(f"[Discovery] Intent profile: {intent_profile}")

    # Track if this is a @noticias query for category forcing
    is_noticias_query = "@noticias" in req.query_text.lower()
    
    # Force profile to "news" only if @noticias is explicitly detected
    if is_noticias_query:
        logger.info("[Discovery] @noticias tag detected - forcing profile to 'news'")
        req.profile = "news"
    
    # Enhanced date parsing: extract dates from natural language query
    parsed_after, parsed_before = AdvancedDateParser.parse_date_range_from_query(req.query_text)
    if parsed_after and not req.after:
        req.after = parsed_after
        logger.info(f"[Discovery] Parsed start date from query: {parsed_after}")
    if parsed_before and not req.before:
        req.before = parsed_before
        logger.info(f"[Discovery] Parsed end date from query: {parsed_before}")
    
    # Build search plans based on profile and query type
    plans: List[SearchPlan]
    
    # For "@noticias" queries, bypass LLM planner and create a single, focused plan
    # This results in a narrower search (e.g., 3 months * 1 query * 1 variant = 3 searches)
    if req.profile == "news" and "@noticias" in req.query_text.lower():
        logger.info("[Planner] @noticias query detected. Bypassing LLM planner for a focused, single-query search.")
        base_query = req.query_text.replace("@noticias", "").strip()
        plan_specs = [{
            "core_query": base_query,
            "sites": req.whitelist or [],
            "filetypes": [],
            "date_range": {"after": None, "before": None}, # Slicer will override this
            "qdf_level": None,
            "lang": req.language or "",
            "country": "br",
        }]
        # Build plans directly for @noticias (no slicing for SearXNG; Exa single run)
        engines: List[str] = []
        if req.valves.get('enable_searxng', True):
            engines.append('searxng')
        if req.valves.get('enable_exa', True):
            engines.append('exa')
        if not engines:
            engines = ['searxng']

        plans = []
        # pages_per_slice
        try:
            pages_per_slice = req.pages_per_slice or int(_get_valve(req.valves, "pages_per_slice", "DISCOVERY_PAGES_PER_SLICE", "2"))
        except (ValueError, TypeError):
            pages_per_slice = 2
        # Compute default news window when user did not provide dates
        from datetime import timedelta
        try:
            DEFAULT_NEWS_DAYS = int(_get_valve(req.valves, "news_default_days", "DISCOVERY_NEWS_DEFAULT_DAYS", "90"))
        except Exception:
            DEFAULT_NEWS_DAYS = 90
        eff_after = req.after
        eff_before = req.before
        if not eff_after and not eff_before:
            eff_before = datetime.utcnow().date()
            eff_after = eff_before - timedelta(days=DEFAULT_NEWS_DAYS)
        # Slice by month when small window; otherwise by quarter (apply to SearXNG only)
        delta_days = (eff_before - eff_after).days if (eff_after and eff_before) else DEFAULT_NEWS_DAYS
        if delta_days <= 90:
            slices = TimeSlicer.month_slices(eff_after, eff_before)
        else:
            slices = TimeSlicer.quarter_slices(eff_after, eff_before)
        if not slices:
            slices = [(eff_after, eff_before)]

        # Ensure Exa is planned only once overall (single query; global window). SearXNG gets per-slice plans.
        exa_planned = False
        for start, end in slices:
            slice_id = start.isoformat()[:7] if start else None
            for spec in plan_specs:
                cq = str(spec.get("core_query", req.query_text)).strip()
                for eng in engines:
                    if eng == 'exa':
                        if exa_planned:
                            continue
                        plans.append(SearchPlan(
                            core_query=cq,
                            after=eff_after.isoformat() if eff_after else None,
                            before=eff_before.isoformat() if eff_before else None,
                            slice_id=None,
                            variant='Q1',
                            page=1,
                            sites=(spec.get("sites") or (req.whitelist or [])),
                            filetypes=(spec.get("filetypes") or []),
                            qdf_level=spec.get("qdf_level"),
                            engine=eng,
                            lang=spec.get("lang"),
                            country=spec.get("country"),
                        ))
                        exa_planned = True
                    else:  # searxng
                        for page_num in list(range(1, pages_per_slice + 1)):
                            plans.append(SearchPlan(
                                core_query=cq,
                                after=start.isoformat() if start else None,
                                before=end.isoformat() if end else None,
                                slice_id=slice_id,
                                variant='Q1',
                                page=page_num,
                                sites=(spec.get("sites") or (req.whitelist or [])),
                                filetypes=(spec.get("filetypes") or []),
                                qdf_level=spec.get("qdf_level"),
                                engine=eng,
                                lang=spec.get("lang"),
                                country=spec.get("country"),
                            ))
        
    # Use LLM planner whenever there is NO explicit @noticias tag
    # (even if profile=="news"), to keep @noticias as the only trigger for special flow
    if not is_noticias_query:
        use_llm = req.valves.get('enable_multi_query_generation', True) if req.valves else True
        try:
            has_key = bool(req.valves and req.valves.get('openai_api_key', ''))
            logger.info(f"[Planner] flags use_llm={use_llm} openai_available={OPENAI_AVAILABLE} has_api_key={has_key}")
        except Exception:
            pass
        if use_llm and OPENAI_AVAILABLE and (req.valves.get('openai_api_key', '') if req.valves else False):
            llm_plans = _run_coro_blocking(generate_queries_with_llm(
                req.query_text,
                intent_profile=intent_profile,
                language=req.language,
                __event_emitter__=None,
                valves=req.valves,
            ))
            # Convert LLM plans to structured specs
            plan_specs: List[Dict[str, Any]] = []
            for p in llm_plans:
                if isinstance(p, dict):
                    # Prefer explicit queries list within plan
                    queries_list = p.get("queries")
                    if isinstance(queries_list, list) and queries_list:
                        # Optional plan-level date_range
                        after_val = None
                        before_val = None
                        dr = p.get("date_range")
                        if isinstance(dr, dict):
                            after_val = dr.get("after")
                            before_val = dr.get("before")
                        elif isinstance(dr, str) and ".." in dr:
                            parts = dr.split("..", 1)
                            after_val = parts[0] or None
                            before_val = parts[1] or None
                        for qi in queries_list:
                            if not isinstance(qi, dict):
                                raise RuntimeError("Planner query item is not a dict")
                            core = qi.get("core_query") or qi.get("query")
                            if not core or not str(core).strip():
                                raise RuntimeError("Planner query item missing core_query")
                            plan_specs.append({
                                "core_query": core,
                                "sites": qi.get("sites") or [],
                                "filetypes": qi.get("filetypes") or [],
                                "after": after_val,
                                "before": before_val,
                                "qdf_level": qi.get("qdf_level") if qi.get("qdf_level") is not None else None,
                            })
                    else:
                        # Support legacy variant: core_queries (list of strings)
                        core_queries_legacy = p.get("core_queries")
                        if isinstance(core_queries_legacy, list) and core_queries_legacy:
                            after_val = None
                            before_val = None
                            dr = p.get("date_range")
                            if isinstance(dr, dict):
                                after_val = dr.get("after")
                                before_val = dr.get("before")
                            elif isinstance(dr, str) and ".." in dr:
                                parts = dr.split("..", 1)
                                after_val = parts[0] or None
                                before_val = parts[1] or None
                            for core in core_queries_legacy:
                                if not core or not str(core).strip():
                                    continue
                                plan_specs.append({
                                    "core_query": str(core).strip(),
                                    "sites": p.get("sites") or [],
                                    "filetypes": p.get("filetypes") or [],
                                    "after": after_val,
                                    "before": before_val,
                                    "qdf_level": p.get("qdf_level") if p.get("qdf_level") is not None else None,
                                })
                        else:
                            # Single-core plan
                            after_val = None
                            before_val = None
                            dr = p.get("date_range")
                            if isinstance(dr, dict):
                                after_val = dr.get("after")
                                before_val = dr.get("before")
                            elif isinstance(dr, str) and ".." in dr:
                                parts = dr.split("..", 1)
                                after_val = parts[0] or None
                                before_val = parts[1] or None
                            core = p.get("core_query") or p.get("query")
                            if not core or not str(core).strip():
                                raise RuntimeError("Planner returned plan without core_query")
                            plan_specs.append({
                                "core_query": core,
                                "sites": p.get("sites") or [],
                                "filetypes": p.get("filetypes") or [],
                                "after": after_val,
                                "before": before_val,
                                "qdf_level": p.get("qdf_level") if p.get("qdf_level") is not None else None,
                            })
                else:
                    raise RuntimeError("Planner returned non-dict plan item")
            # No fallback: if the LLM fails to generate any plan, the tool must fail loudly
            if len(plan_specs) == 0:
                raise RuntimeError("LLM returned no plans")
            try:
                logger.info(f"[Planner] plan_specs_count={len(plan_specs)}")
                with open("planner_specs.log", "a", encoding="utf-8") as fp:
                    fp.write("\n=== TOOL_DISCOVERY PLAN SPECS ===\n")
                    import json as _json
                    fp.write(_json.dumps(plan_specs, ensure_ascii=False, indent=2) + "\n")
            except Exception:
                pass
            # Build plans directly from specs
            engines: List[str] = []
            if req.valves.get('enable_searxng', True):
                engines.append('searxng')
            if req.valves.get('enable_exa', True):
                engines.append('exa')
            if not engines:
                engines = ['searxng']

            plans = []
            profile = req.profile or QueryClassifier.classify(req.query_text)
            
            # Get pages_per_slice from valves
            try:
                pages_per_slice = req.pages_per_slice or int(_get_valve(req.valves, "pages_per_slice", "DISCOVERY_PAGES_PER_SLICE", "2"))
            except (ValueError, TypeError):
                pages_per_slice = 2
            
            # Ensure Exa is planned only once overall (single query)
            exa_planned: bool = False

            if profile == 'news' and ("@noticias" in req.query_text.lower()):
                # Only slice/compute window for explicit @noticias
                from datetime import timedelta
                try:
                    DEFAULT_NEWS_DAYS = int(_get_valve(req.valves, "news_default_days", "DISCOVERY_NEWS_DEFAULT_DAYS", "90"))
                except Exception:
                    DEFAULT_NEWS_DAYS = 90
                eff_after = req.after
                eff_before = req.before
                if not eff_after and not eff_before:
                    eff_before = datetime.utcnow().date()
                    eff_after = eff_before - timedelta(days=DEFAULT_NEWS_DAYS)
                # Slice by month when small window; otherwise by quarter (apply to SearXNG only)
                delta_days = (eff_before - eff_after).days if (eff_after and eff_before) else DEFAULT_NEWS_DAYS
                if delta_days <= 90:
                    slices = TimeSlicer.month_slices(eff_after, eff_before)
                else:
                    slices = TimeSlicer.quarter_slices(eff_after, eff_before)
                if not slices:
                    slices = [(eff_after, eff_before)]

                # Build per-slice SearXNG plans; Exa only once with global window
                for start, end in slices:
                    slice_id = start.isoformat()[:7] if start else None
                    for spec in plan_specs:
                        cq = str(spec.get("core_query", req.query_text)).strip()
                        for eng in engines:
                            if eng == 'exa':
                                if exa_planned:
                                    continue
                                exa_planned = True
                                plans.append(SearchPlan(
                                    core_query=cq,
                                    after=eff_after.isoformat() if eff_after else None,
                                    before=eff_before.isoformat() if eff_before else None,
                                    slice_id=None,
                                    variant='Q1',
                                    page=1,
                                    sites=(spec.get("sites") or (req.whitelist or [])),
                                    filetypes=(spec.get("filetypes") or []),
                                    qdf_level=spec.get("qdf_level"),
                                    engine=eng,
                                    lang=spec.get("lang"),
                                    country=spec.get("country"),
                                ))
                            else:  # searxng
                                plans.append(SearchPlan(
                                    core_query=cq,
                                    after=start.isoformat() if start else None,
                                    before=end.isoformat() if end else None,
                                    slice_id=slice_id,
                                    variant='Q1',
                                    page=1,
                                    sites=(spec.get("sites") or (req.whitelist or [])),
                                    filetypes=(spec.get("filetypes") or []),
                                    qdf_level=spec.get("qdf_level"),
                                    engine=eng,
                                    lang=spec.get("lang"),
                                    country=spec.get("country"),
                                ))
            else:
                for spec in plan_specs:
                    cq = str(spec.get("core_query", req.query_text)).strip()
                    for eng in engines:
                        # Create single plan per spec; pagination handled by runner. Exa only once overall.
                        if eng == 'exa':
                            if exa_planned:
                                continue
                            exa_planned = True
                        plans.append(SearchPlan(
                            core_query=cq,
                            after=spec.get("after"),
                            before=spec.get("before"),
                            slice_id=None,
                            variant='Q1',
                            page=1,
                            sites=(spec.get("sites") or (req.whitelist or [])),
                            filetypes=(spec.get("filetypes") or []),
                            qdf_level=spec.get("qdf_level"),
                            engine=eng,
                            lang=spec.get("lang"),
                            country=spec.get("country"),
                        ))
            try:
                logger.info(f"[Planner] built_plans={len(plans)} (engines={len(engines)}) profile={profile}")
                with open("planner_built_plans.log", "a", encoding="utf-8") as fp:
                    fp.write("\n=== TOOL_DISCOVERY BUILT PLANS ===\n")
                    for pl in plans:
                        fp.write(str(pl) + "\n")
            except Exception:
                pass
        else:
            # Se LLM estiver habilitado mas não disponível, não seguir com fallback silencioso
            if use_llm:
                raise RuntimeError("LLM planner was enabled but did not execute")
            plans = generate_search_plans(req)
            logger.info(f"[Planner] Using deterministic plan generation: {len(plans)} plans")
    # No implicit fallback: if no plans were produced, fail loudly to surface config issues
    if 'plans' not in locals():
        raise RuntimeError("Planner produced no plans (no @noticias and planner disabled/unavailable)")

    stats: Dict[str, float] = {"plans_generated": float(len(plans)), "candidates": 0.0}
    try:
        logger.info(f"[Planner] plans_generated={len(plans)}")
    except Exception:
        pass

    async def _execute_plans_async(pls: List[SearchPlan]) -> List[dict]:
        try:
            concurrency = int(_get_valve(req.valves, "discovery_concurrency", "DISCOVERY_CONCURRENCY", str(2)))
        except (ValueError, TypeError):
            concurrency = 2
            logger.warning(f"Invalid concurrency value, using default: {concurrency}")
        sem = asyncio.Semaphore(concurrency)
        results: List[dict] = []

        async def _run_plan(p: SearchPlan, intent_profile: Dict[str, Any] = None):
            async with sem:
                try:
                    max_results = int(_get_valve(req.valves, "max_search_results", "DISCOVERY_MAX_RESULTS", "15"))
                except (ValueError, TypeError):
                    max_results = 15
                try:
                    # Use pages_per_slice from request first, then valves, then default
                    max_pages = req.pages_per_slice or int(_get_valve(req.valves, "pages_per_slice", "DISCOVERY_PAGES_PER_SLICE", "2"))
                except (ValueError, TypeError):
                    max_pages = 2
                built_params: Dict[str, Any] = {}
                if p.engine == "searxng":
                    runner = RunnerSearxng(
                        endpoint=_get_valve(req.valves, "searxng_endpoint", "SEARXNG_ENDPOINT", None),
                        max_pages=max_pages,
                        max_results=max_results,
                        valves=req.valves,
                    )
                else:
                    runner = RunnerExa(
                        api_key=_get_valve(req.valves, "exa_api_key", "EXA_API_KEY", None),
                        valves=req.valves,
                    )
                    # Initialize OpenAI client for Exa planning
                    if OPENAI_AVAILABLE:
                        runner.openai_client = OpenAI(
                            api_key=_get_valve(req.valves, "openai_api_key", "OPENAI_API_KEY", ""),
                            base_url=_get_valve(req.valves, "openai_base_url", "OPENAI_BASE_URL", "https://api.openai.com/v1")
                        )
                # Prefer explicit planner language, then intent, then request/valves
                eff_lang = getattr(p, "lang", None) or intent_profile.get("lang_bias") or req.language or _get_valve(req.valves, "search_language", "DISCOVERY_SEARCH_LANGUAGE", "") or None
                eff_country = getattr(p, "country", None) or intent_profile.get("country_bias") or _get_valve(req.valves, "search_country", "DISCOVERY_SEARCH_COUNTRY", "")
                eff_filetypes = p.filetypes
                forced = _get_valve(req.valves, "force_filetypes", "DISCOVERY_FORCE_FILETYPES", None)
                try:
                    if isinstance(forced, str) and forced.strip():
                        eff_filetypes = [ft.strip() for ft in forced.split(',') if ft.strip()]
                    elif isinstance(forced, list) and forced:
                        eff_filetypes = forced
                except Exception:
                    pass

                if p.engine == "searxng":
                    # Safe date parsing
                    after_date = None
                    before_date = None
                    try:
                        if p.after and p.after.strip():
                            after_date = datetime.fromisoformat(p.after).date()
                    except (ValueError, AttributeError):
                        logger.warning(f"Invalid after date format: {p.after}")
                    try:
                        if p.before and p.before.strip():
                            before_date = datetime.fromisoformat(p.before).date()
                    except (ValueError, AttributeError):
                        logger.warning(f"Invalid before date format: {p.before}")
                    
                    qc = extract_query_components(p.core_query)
                    user_sites = qc.get("sites", [])
                    # Also parse explicit site: from the original user query
                    qc_full = extract_query_components(req.query_text or "")
                    user_sites_full = qc_full.get("sites", [])
                    user_filetypes = qc.get("filetypes", [])

                    # sites
                    # Treat any explicit domain provided by the user (in the raw query or core_query)
                    # or by the planner (p.sites) as a hard restriction. Prefer planner sites over raw tokens.
                    restrict_sites = bool(p.sites or user_sites or user_sites_full)
                    # opcional: também restringir se tiver whitelist via valve
                    if not restrict_sites and req.valves and req.valves.get("restrict_sites_when_whitelist", False):
                        restrict_sites = bool(p.sites)
                    eff_sites = p.sites or user_sites or user_sites_full

                    # filetypes
                    forced_filetypes = _get_valve(req.valves, "force_filetypes", "DISCOVERY_FORCE_FILETYPES", None)
                    user_explicitly_asked_filetype = bool(re.search(r"filetype:\s*([a-z0-9]+)", req.query_text or "", flags=re.I))
                    restrict_filetypes = user_explicitly_asked_filetype or bool(forced_filetypes) or bool(user_filetypes)

                    eff_filetypes = []
                    if restrict_filetypes:
                        # merge usuário + valve, dedup
                        if isinstance(forced_filetypes, str) and forced_filetypes.strip():
                            forced_list = [ft.strip() for ft in forced_filetypes.split(',') if ft.strip()]
                        elif isinstance(forced_filetypes, list):
                            forced_list = [str(x).strip() for x in forced_filetypes if str(x).strip()]
                        else:
                            forced_list = []
                        eff_filetypes = list(dict.fromkeys([*user_filetypes, *forced_list]))

                    # Only include 'files' category when filetypes are actually restricted
                    has_restricted_filetypes = restrict_filetypes and bool(eff_filetypes)
                    # Detectar categoria news baseada no conteúdo da query OU perfil @noticias
                    force_news = QueryClassifier._is_news_category_query(p.core_query) or (req.profile == 'news' and "@noticias" in req.query_text.lower())
                    cats = QueryClassifier.get_searxng_categories(p.core_query, has_restricted_filetypes, force_news)
                    # Validate SearXNG categories - only use valid ones
                    valid_categories = {"general", "news", "files", "it", "science", "juridico", "financeiro"}
                    cats_list = [c for c in cats.split(",") if c.strip() in valid_categories]
                    cats = ",".join(cats_list) if cats_list else "general"

                    # Sanitize core query and normalize language tag for SearXNG
                    cq_sanitized = sanitize_core_query_for_search(p.core_query)
                    eff_lang_norm = normalize_language_tag(eff_lang)
                    # If planner/intent country implies English, or sites are non-pt/br, prefer 'en'
                    try:
                        # Heuristic: if user explicitly asks for international press, do NOT force language
                        prior_hint = ((req.query_text or '') + ' ' + (p.core_query or '')).lower()
                        international_tokens = ("international", "internacional", "imprensa internacional", "press coverage")
                        if any(tok in prior_hint for tok in international_tokens):
                            eff_lang_norm = None
                        else:
                            if eff_lang_norm and eff_lang_norm != 'en':
                                english_countries = {'us', 'uk', 'ca', 'au', 'nz', 'ie'}
                                if (str(eff_country or '').lower() in english_countries):
                                    eff_lang_norm = 'en'
                                else:
                                    # If all explicit sites are non-pt/br domains, prefer 'en'
                                    site_tlds = ''.join(eff_sites).lower() if eff_sites else ''
                                    if site_tlds and not any(s.endswith(('.br', '.pt')) for s in eff_sites):
                                        eff_lang_norm = 'en'
                    except Exception:
                        pass

                    params = QueryBuilderSearxng.build(
                        cq_sanitized,
                        after_date,
                        before_date,
                        language=eff_lang_norm or None,
                        categories=[c for c in cats.split(',') if c],
                        page=p.page,
                        sites=eff_sites,
                        filetypes=eff_filetypes,
                        intext=(p.variant == "Q2"),
                        restrict_sites=restrict_sites,
                        restrict_filetypes=restrict_filetypes,
                        qdf=p.qdf_level,
                    )
                    
                    # Log exact query sent to SearXNG
                    logger.info(f"[Tool_Discovery_SearXNG] Exact Query: {json.dumps(params, ensure_ascii=False, indent=2)}")
                    built_params = {"engine": "searxng", "params": params}
                    
                    # Propagar qdf_level se existir (build já trata time_range, mantemos chave para diagnóstico)
                    if p.qdf_level is not None:
                        params["qdf_level"] = p.qdf_level
                else:
                    # Build minimal Serper payload with language/country and optional site/filetype hints
                    eff_country = _get_valve(req.valves, "search_country", "DISCOVERY_SEARCH_COUNTRY", "") or ""
                    # Determine restriction in Serper payload similarly
                    qc = extract_query_components(p.core_query)
                    user_sites = qc.get("sites", [])
                    # Also parse explicit site: from the original user query
                    qc_full = extract_query_components(req.query_text or "")
                    user_sites_full = qc_full.get("sites", [])
                    user_filetypes = qc.get("filetypes", [])

                    # sites
                    # Treat any explicit domain provided by the user (in the raw query or core_query)
                    # or by the planner (p.sites) as a hard restriction. Prefer planner sites over raw tokens.
                    restrict_sites = bool(p.sites or user_sites or user_sites_full)
                    # opcional: também restringir se tiver whitelist via valve
                    if not restrict_sites and req.valves and req.valves.get("restrict_sites_when_whitelist", False):
                        restrict_sites = bool(p.sites)
                    eff_sites = p.sites or user_sites or user_sites_full

                    # filetypes
                    forced_filetypes = _get_valve(req.valves, "force_filetypes", "DISCOVERY_FORCE_FILETYPES", None)
                    user_explicitly_asked_filetype = bool(re.search(r"filetype:\s*([a-z0-9]+)", req.query_text or "", flags=re.I))
                    restrict_filetypes = user_explicitly_asked_filetype or bool(forced_filetypes) or bool(user_filetypes)

                    eff_filetypes = []
                    if restrict_filetypes:
                        # merge usuário + valve, dedup
                        if isinstance(forced_filetypes, str) and forced_filetypes.strip():
                            forced_list = [ft.strip() for ft in forced_filetypes.split(',') if ft.strip()]
                        elif isinstance(forced_filetypes, list):
                            forced_list = [str(x).strip() for x in forced_filetypes if str(x).strip()]
                        else:
                            forced_list = []
                        eff_filetypes = list(dict.fromkeys([*user_filetypes, *forced_list]))

                    # Exa: run a single, free-form neural search planned by Exa's own LLM
                    # Use the original user query text to keep Exa broad and complementary
                    params = {"query": (req.query_text or p.core_query), "after": p.after, "before": p.before, "news_profile": (req.profile == 'news' or ("@noticias" in (req.query_text or "").lower()))}
                    built_params = {"engine": "exa", "params": params}
                # Special handling for Exa - only run once, not in loop
                if p.engine == "exa":
                    # For Exa, we only want to run once per overall request (handled during plan building)
                    # Only run if this is page 1, skip other pages
                    if p.page == 1:
                        raw = await runner.run(params)
                    else:
                        # Skip page 2+ for Exa.ai
                        raw = []
                else:
                    raw = await runner.run(params)
                # Attach execution trace per plan (for citations/debug)
                results.append({
                    "__trace__": True,
                    "slice_id": p.slice_id,
                    "variant": p.variant,
                    "engine": p.engine,
                    "core_query": p.core_query,
                    "after": p.after,
                    "before": p.before,
                    "built": built_params,
                    "count": len(raw) if isinstance(raw, list) else 0,
                })
                out = []
                for r in raw:
                    if isinstance(r, dict):
                        r["search_plan"] = {
                            "core_query": p.core_query,
                            "slice_id": p.slice_id,
                            "variant": p.variant,
                            "page": p.page,
                            "engine": p.engine,
                        }
                        out.append(r)
                return out

        tasks = [asyncio.create_task(_run_plan(p, intent_profile)) for p in pls]
        gathered = await asyncio.gather(*tasks)  # exceções propagam
        for g in gathered:
            # split out traces and normal rows
            if isinstance(g, list):
                for item in g:
                    results.extend([item])
            else:
                results.append(g)
        return results

    # Execute plans and implement real zipper (interleaving)
    raw_pool: List[dict] = []
    try:
        raw_pool = _run_coro_blocking(_execute_plans_async(plans))
    except Exception as e:
        logger.error(f"Error executing search plans: {e}", exc_info=True)
        raw_pool = []

    # Group results by engine for zipper interleaving
    searxng_results = []
    exa_results = []
    
    # Extract execution traces and keep them for final payload
    execution_traces: List[Dict[str, Any]] = []
    filtered_pool: List[dict] = []
    for raw in raw_pool:
        if isinstance(raw, dict) and raw.get("__trace__"):
            execution_traces.append(raw)
            continue
        filtered_pool.append(raw)
    for raw in filtered_pool:
        engine = raw.get("search_plan", {}).get("engine", "searxng")
        if engine == "searxng":
            searxng_results.append(raw)
        else:
            exa_results.append(raw)
    
    # Sort each group by SERP position before interleaving
    searxng_results.sort(key=lambda r: r.get("position") or r.get("pos") or 10**9)
    exa_results.sort(key=lambda r: r.get("position") or r.get("pos") or 10**9)
    
    logger.info(f"Zipper input: {len(searxng_results)} SearXNG, {len(exa_results)} Exa results")
    
    # Real zipper: interleave results from different engines, prioritizing by score
    interleaved_results = []
    seen_urls = {}
    
    iter_searxng = iter(searxng_results)
    iter_exa = iter(exa_results)
    
    while True:
        searxng_res = next(iter_searxng, None)
        exa_res = next(iter_exa, None)
        
        if searxng_res is None and exa_res is None:
            break
        
        # Process SearXNG result
        if searxng_res:
            url = _get_url(searxng_res)
            if url:
                score = searxng_res.get("score", 0)
                if url not in seen_urls or score > seen_urls[url]["score"]:
                    if url in seen_urls:
                        # Remove previous result with lower score
                        interleaved_results.remove(seen_urls[url]["result"])
                    interleaved_results.append(searxng_res)
                    seen_urls[url] = {"result": searxng_res, "score": score}
        
        # Process Exa result
        if exa_res:
            url = _get_url(exa_res)
            if url:
                score = exa_res.get("score", 0)
                if url not in seen_urls or score > seen_urls[url]["score"]:
                    if url in seen_urls:
                        # Remove previous result with lower score
                        interleaved_results.remove(seen_urls[url]["result"])
                    interleaved_results.append(exa_res)
                    seen_urls[url] = {"result": exa_res, "score": score}
    
    logger.info(f"Zipper output: {len(interleaved_results)} interleaved results")
    
    # Apply Brazilian boost if enabled
    if req.valves.get("boost_brazilian_sources", True):
        brazilian_boost_table = _get_brazilian_boost_table()
        for result in interleaved_results:
            from urllib.parse import urlparse
            domain = urlparse(_get_url(result)).netloc
            if domain in brazilian_boost_table:
                result["brazilian_boost"] = brazilian_boost_table[domain]
                result["brazilian_source"] = True
                # Apply boost to score
                current_score = result.get("score", 1.0)
                result["score"] = current_score * brazilian_boost_table[domain]
            else:
                result["brazilian_source"] = False

    # Normalize interleaved results
    normalizer = ResultNormalizer()
    candidates: List[UrlCandidate] = []
    for raw in interleaved_results:
        try:
            cand = normalizer.normalize_raw_result(raw, slice_id=raw.get("search_plan", {}).get("slice_id"), variant=raw.get("search_plan", {}).get("variant"))
            candidates.append(cand)
        except (ValueError, TypeError, AttributeError) as e:
            logger.warning(f"Failed to normalize result: {e}")
            continue
        except Exception as e:
            logger.error(f"Unexpected error normalizing result: {e}")
            continue

    # Deduplicate and rank (zipper already handled deduplication)
    candidates = Deduper.dedupe_candidates(candidates)
    candidates.sort(key=lambda c: c.score, reverse=True)
    stats["candidates"] = len(candidates)

    # LLM selection / final ranking
    analysis_pool = [
        {
            "title": c.hints.get("title", ""),
            "snippet": c.hints.get("snippet", ""),
            "url": str(c.url), 
            "score": c.score, 
            "engine": c.source_engine, 
            "domain": c.hints.get("domain", ""), 
            "authority_prior": c.hints.get("authority_prior", 1.0)
        }
        for c in candidates
    ]
    # Hard site filter (user-explicit only): if the user specified site:example.com,
    # filter the pool to only those domains (including subdomains). Planner suggestions
    # are not used here to avoid over-constraining.
    try:
        explicit_sites = extract_query_components(req.query_text or "").get("sites", [])
        allowed_sites = [s.lower() for s in explicit_sites if "." in s]
        if allowed_sites:
            def _matches_allowed(domain: str) -> bool:
                d = (domain or "").lower()
                return any(d == s or d.endswith("." + s) for s in allowed_sites)
            before_len = len(analysis_pool)
            analysis_pool = [it for it in analysis_pool if _matches_allowed(it.get("domain", ""))]
            logger.info(f"[Filter] Hard site filter applied {allowed_sites} - {before_len} -> {len(analysis_pool)}")
    except Exception as e:
        logger.warning(f"[Filter] Hard site filter skipped due to error: {e}")
    try:
        # Ensure is_content_acceptable is called before LLM analysis
        acceptable_candidates = []
        for c in analysis_pool:
            if is_content_acceptable(c, req.query_text, valves=req.valves):
                acceptable_candidates.append(c)
            else:
                try:
                    logger.warning(f"🗑️ Removendo conteúdo inaceitável: {c.get('url','')[:120]}")
                except Exception:
                    pass

        # Apply suggested-sites boost before LLM selection (prioritization, not restriction)
        try:
            suggested_sites = set()
            for sp in plans:
                if isinstance(sp, SearchPlan) and sp.sites:
                    for s in sp.sites:
                        if s:
                            suggested_sites.add(s)
            for item in acceptable_candidates:
                d = _norm_host(item.get("url", ""))
                if d in suggested_sites:
                    base_score = item.get("score", 1.0)
                    item["selection_score"] = base_score * 1.5
                else:
                    item["selection_score"] = item.get("score", 1.0)
                # Apply authority boost
                item["selection_score"] = boost_authority(item["selection_score"], item)
        except Exception:
            for item in acceptable_candidates:
                item["selection_score"] = item.get("score", 1.0)
                # Apply authority boost even in fallback
                item["selection_score"] = boost_authority(item["selection_score"], item)

        # Create valves for LLM analysis (valves-first)
        try:
            top_curated = int(_get_valve(req.valves, "top_curated_results", "DISCOVERY_TOP_CANDIDATES", "20"))
        except (ValueError, TypeError):
            top_curated = 20
        try:
            max_additional = int(_get_valve(req.valves, "max_additional_snippets", "DISCOVERY_MAX_ADDITIONAL", "10"))
        except (ValueError, TypeError):
            max_additional = 10
        try:
            max_candidates = int(_get_valve(req.valves, "max_candidates_for_llm_analysis", "DISCOVERY_MAX_LLM_CANDIDATES", "100"))
        except (ValueError, TypeError):
            max_candidates = 100
        try:
            domcap = int(_get_valve(req.valves, "domain_cap", "DISCOVERY_DOMAIN_CAP", "2") or 2)
        except Exception:
            domcap = 2

        llm_valves = Tools.DiscoveryValves(
            openai_api_key=_get_valve(req.valves, "openai_api_key", "OPENAI_API_KEY", ""),
            openai_model=_get_valve(req.valves, "openai_model", "OPENAI_MODEL", "gpt-5-mini"),
            openai_base_url=_get_valve(req.valves, "openai_base_url", "OPENAI_BASE_URL", "https://api.openai.com/v1"),
            enable_unified_llm_analysis=True,
            top_curated_results=top_curated,
            max_additional_snippets=max_additional,
            max_candidates_for_llm_analysis=max_candidates,
            domain_cap=domcap,
        )

        # Extract prioritization criteria and log for debugging
        prioritization_criteria = extract_prioritization_criteria(req.query_text)
        logger.info(f"[Discovery] Prioritization criteria extracted: '{prioritization_criteria}'")
        
        selected_dicts = _run_coro_blocking(unified_llm_analysis(
            scored_candidates_pool=acceptable_candidates,
            original_query=req.query_text,
            prioritization_criteria=prioritization_criteria,
            suggested_filetypes=None,
            top_n=top_curated,
            __event_emitter__=None,
            valves=llm_valves,
            intent_profile=intent_profile
        ))
    except Exception as e:
        logger.error("Error during LLM analysis or content acceptability check", exc_info=True)
        raise

    if selected_dicts:
        # preserve order from LLM selection
        selected_urls = [d.get("url") for d in selected_dicts if d.get("url")]
        final_candidates: List[UrlCandidate] = []
        url_to_cand = {}
        for c in candidates:
            try:
                if c.url:
                    url_to_cand[str(c.url)] = c
            except (AttributeError, TypeError):
                logger.warning(f"Invalid URL in candidate: {c}")
                continue
        
        for u in selected_urls:
            if u and u in url_to_cand:
                final_candidates.append(url_to_cand[u])
        candidates = final_candidates

    # Trim strictly to top_curated_results (align with supersearcher)
    try:
        top_curated = int(_get_valve(req.valves, "top_curated_results", "DISCOVERY_TOP_CANDIDATES", "20"))
    except (ValueError, TypeError):
        top_curated = 20
    candidates = candidates[:top_curated]

    return DiscoveryResult(run_id=req.run_id, company_key=(req.companies[0] if req.companies and len(req.companies) > 0 else None), query_text=req.query_text, candidates=candidates, stats=stats, execution=execution_traces)

# =========================================================================
# AUXILIARY FUNCTIONS & CONSTANTS (PORTED FROM supersearcher.py)
# =========================================================================
TODAY = datetime.now().date()

AUTHORITY_BASE = {
    # técnico
    "arxiv.org",
    "openreview.net",
    "aclanthology.org",
    "neurips.cc",
    "iclr.cc",
    "mlsys.org",
    "developer.nvidia.com",
    "nvidia.github.io",
    "pytorch.org",
    "microsoft.github.io",
    "vllm.ai",
    "megablocks.ai",
    "tensorrt-llm.github.io",
    "mistral.ai",
    "qwenlm.ai",
    "deepseek.com",
    "huggingface.co",
    # gov/órgãos BR
    "gov.br",
    "inpi.gov.br",
    "inpe.br",
    "inmetro.gov.br",
    "cnpq.br",
    "finep.gov.br",
    "anatel.gov.br",
    "mdic.gov.br",
    "fazenda.gov.br",
    # imprensa BR
    "g1.globo.com",
    "globo.com",
    "folha.uol.com.br",
    "estadao.com.br",
    "valor.globo.com",
    "exame.com",
    "neofeed.com.br",
    "infomoney.com.br",
    "cnnbrasil.com.br",
}

NEWS_HOSTS = {
    "g1.globo.com",
    "globo.com",
    "folha.uol.com.br",
    "estadao.com.br",
    "valor.globo.com",
    "exame.com",
    "neofeed.com.br",
    "infomoney.com.br",
    "cnnbrasil.com.br",
}

RE_LISTING = re.compile(r"/(tag|tags|editoria|categoria|author|autores|busca|search|page/\d+)/", re.I)
RE_ARTICLE_HINT = re.compile(r"/20\d{2}/\d{2}/")  # padrão /YYYY/MM/


def _norm_host(u: str) -> str:
    """Normaliza o hostname da URL removendo www."""
    from urllib.parse import urlparse
    h = (urlparse(u).hostname or "").lower()
    return h[4:] if h.startswith("www.") else h


def is_authority(url: str, extra_csv: str = "") -> bool:
    """Verifica se a URL é de um domínio de autoridade."""
    host = _norm_host(url)
    extra = {d.strip().lower() for d in extra_csv.split(",") if d.strip()}
    allow = AUTHORITY_BASE | extra
    return any(host == d or host.endswith("." + d) for d in allow)


def boost_authority(selection_score: float, item: dict) -> float:
    """Aplica bônus proporcional para URLs de autoridade baseado no authority_prior."""
    k = float(item.get("authority_prior", 1.0))
    return max(0.0, min(100.0, selection_score * (1.0 + 0.05 * (k - 1.0))))


def adjust_news(url: str, score: float) -> float:
    """Ajusta score para URLs de notícia, rebaixando listagens."""
    host = _norm_host(url)
    if host in NEWS_HOSTS:
        if RE_LISTING.search(url) and not RE_ARTICLE_HINT.search(url):
            return max(0.0, score - 5.0)
    return score


    # Legacy ULTRA COMPACT selector block removed.


class Tools:

    class DiscoveryValves(BaseModel):
        """Configurações para o sistema de descoberta de URLs."""
        
        # =============================================================================
        # CONFIGURAÇÕES DE API
        # =============================================================================
        openai_api_key: str = Field(default="", description="🔑 Chave da API OpenAI")
        openai_model: str = Field(default="gpt-5-mini", description="🤖 Modelo OpenAI")
        openai_base_url: str = Field(default="https://api.openai.com/v1", description="🌐 URL base da OpenAI")
        searxng_endpoint: str = Field(default="", description="🔍 Endpoint do SearXNG")
        exa_api_key: str = Field(default="", description="🔑 Chave da API Exa.ai")
        
        # =============================================================================
        # CONFIGURAÇÕES DE BUSCA
        # =============================================================================
        enable_searxng: bool = Field(default=True, description="🔍 Habilitar SearXNG")
        enable_exa: bool = Field(default=True, description="🔍 Habilitar Exa.ai")
        max_search_results: int = Field(default=15, description="📊 Máximo de resultados por busca")
        pages_per_slice: int = Field(default=2, description="📄 Páginas por slice")
        discovery_concurrency: int = Field(default=4, description="⚡ Concorrência de descoberta")
        
        # =============================================================================
        # CONFIGURAÇÕES DE FILTROS
        # =============================================================================
        strict_language_filter: bool = Field(default=True, description="🌍 Filtro rigoroso de idioma")
        allow_forums: bool = Field(default=True, description="💬 Permitir fóruns (Reddit, etc.)")
        non_latin_threshold: float = Field(default=0.15, description="📊 Threshold de caracteres não-latinos")
        
        # =============================================================================
        # CONFIGURAÇÕES DE ARQUIVOS
        # =============================================================================
        force_filetypes: List[str] = Field(default_factory=list, description="📁 Tipos de arquivo forçados")
        
        # =============================================================================
        # CONFIGURAÇÕES DE ANÁLISE LLM
        # =============================================================================
        enable_unified_llm_analysis: bool = Field(default=True, description="🧠 Habilitar análise LLM unificada")
        top_curated_results: int = Field(default=25, description="⭐ Top resultados curados")
        max_additional_snippets: int = Field(default=10, description="📝 Máximo de snippets adicionais")
        max_candidates_for_llm_analysis: int = Field(default=100, description="🎯 Máximo de candidatos para análise LLM")
        domain_cap: int = Field(default=4, description="🏢 Limite máximo de URLs por domínio")
        soft_domain_cap: bool = Field(default=False, description="🔄 Cap de domínio suave: preenche com excedentes se faltar para top_n")
        
        # =============================================================================
        # CONFIGURAÇÕES DE BOOST
        # =============================================================================
        boost_brazilian_sources: bool = Field(default=True, description="🇧🇷 Boost para fontes brasileiras")
        
        # =============================================================================
        # CONFIGURAÇÕES DE IDIOMA E PAÍS
        # =============================================================================
        search_language: str = Field(default="", description="🌍 Idioma da busca")
        search_country: str = Field(default="", description="🇧🇷 País da busca")
        
        # =============================================================================
        # CONFIGURAÇÕES DE NOTÍCIAS
        # =============================================================================
        news_default_days: int = Field(default=90, description="📅 Janela padrão de dias para @noticias sem datas")
        exa_keep_category: bool = Field(default=False, description="🔍 Keep category in Exa.ai payload")

    # Alias esperado pelo OpenWebUI para renderizar Valves na UI
    class Valves(DiscoveryValves):
        pass

    def __init__(self):
        """Inicializa a instância da classe Tools."""
        self.valves = self.Valves()

    def discover(
        self,
        query_text: str,
        profile: Literal["news", "general"] = "general",
        after: Optional[str] = None,  # YYYY-MM-DD
        before: Optional[str] = None, # YYYY-MM-DD
        whitelist: Optional[str] = "",
        pages_per_slice: int = 2,
        __event_emitter__: Optional[Callable] = None,
    ) -> str:
        """Input: query_text:str; profile:str (news|general); after:YYYY-MM-DD|null; before:YYYY-MM-DD|null; whitelist:csv|null; pages_per_slice:int. Output: json{operation,query,candidates_count,candidates,citations,sources}. Purpose: hybrid websearch (SearXNG+Exa) with LLM selection. IMPORTANT: pass the USER PROMPT verbatim, do not rewrite or truncate; preserve '@noticias' token if present. Do not call internals directly."""
        try:
            # Emit status: start
            try:
                _run_coro_blocking(emit_status(__event_emitter__, f"Iniciando discovery: '{query_text[:140]}'"))
            except Exception:
                pass
            # Parse simples
            a = date.fromisoformat(after) if after else None
            b = date.fromisoformat(before) if before else None
            wl: List[str] = [s.strip() for s in (whitelist or "").split(",") if s.strip()]

            # Monta request usando valves do tool
            req = DiscoverRequest(
                run_id=f"discover_{int(time.time())}",
                query_text=query_text,
                profile=profile,
                after=a,
                before=b,
                whitelist=wl,
                pages_per_slice=pages_per_slice,
                valves=self.valves.model_dump(),
                language=self.valves.search_language or "",
            )

            # Emit status: executando buscas
            try:
                _run_coro_blocking(emit_status(__event_emitter__, "Executando buscas e normalizando resultados"))
            except Exception:
                pass

            res = discover(req)

            # Emit status: preparando citações
            try:
                _run_coro_blocking(emit_status(__event_emitter__, "Preparando citações"))
            except Exception:
                pass

            # Montar citações (compatível com chat)
            citations = [
                {
                    "url": str(c.url),
                    "title": c.hints.get("title", ""),
                    "snippet": c.hints.get("snippet", ""),
                    "engine": c.source_engine,
                    "domain": c.hints.get("domain", ""),
                }
                for c in res.candidates
            ]

            # Também expor per-plan traces como "sources" para facilitar leitura na UI
            sources = []
            try:
                if isinstance(res.execution, list):
                    for t in res.execution:
                        if t.get("built"):
                            sources.append({
                                "engine": t.get("engine"),
                                "core_query": t.get("core_query"),
                                "slice_id": t.get("slice_id"),
                                "after": t.get("after"),
                                "before": t.get("before"),
                                "params": t.get("built", {}).get("params", {}),
                                "count": t.get("count", 0),
                            })
            except Exception:
                pass

            payload = {
                "operation": "discover",
                "query": query_text,
                "profile": profile,
                "candidates_count": len(res.candidates),
                "candidates": [
                    {
                        "url": str(c.url),
                        "title": c.hints.get("title", ""),
                        "snippet": c.hints.get("snippet", ""),
                        "score": c.score,
                        "engine": c.source_engine,
                        "domain": c.hints.get("domain", ""),
                    }
                    for c in res.candidates
                ],
                "citations": citations,
                "sources": sources,
                "success": True,
            }

            # Emit status: concluído
            try:
                _run_coro_blocking(emit_status(__event_emitter__, f"Concluído: {len(res.candidates)} candidatos"))
            except Exception:
                pass

            return json.dumps(payload, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Discovery failed: {e}", exc_info=True)
            return json.dumps({"success": False, "error": str(e)}, ensure_ascii=False, indent=2)


def get_tools():
    """
    title: Discovery Agent
    author: Native Tool
    version: 0.1.0
    requirements: requests, httpx, pydantic
    license: MIT
    description: Ferramenta de descoberta híbrida v2.0 (SearXNG/Exa.ai neural) com planejamento LLM e seleção LLM. Exa sempre retorna 20 resultados em modo neural. Requer passar o query_text completo e não modificado; válvulas ajustam comportamento.
    """
    _tools = []
    _instance = Tools()

    def discovery_tool(
        query_text: str,
        profile: Literal["news", "general"] = "general",
        after: Optional[str] = None,
        before: Optional[str] = None,
        whitelist: Optional[str] = "",
        pages_per_slice: int = 2,
        __event_emitter__ = None,
    ) -> str:
        """Input: query_text:str; profile:str (news|general); after:YYYY-MM-DD|null; before:YYYY-MM-DD|null; whitelist:csv|null; pages_per_slice:int. Output: json{operation,query,candidates_count,candidates,citations,sources}. Purpose: hybrid websearch wrapper (use this via get_tools)."""
        return _instance.discover(
            query_text=query_text,
            profile=profile,
            after=after,
            before=before,
            whitelist=whitelist,
            pages_per_slice=pages_per_slice,
            __event_emitter__=__event_emitter__,
        )

    _tools.append(discovery_tool)
    return _tools
