"""
title: Search & Scrape Coordinator v2.1
author: Killstone AI
author_url: https://github.com/killstone
git_url: https://github.com/killstone/search-scrape-coordinator
description: Coordenador que integra Discovery (SearXNG + Exa) e Scraping (httpx + Jina + Browserless + PDF) em uma única tool standalone para OpenWebUI
required_open_webui_version: 0.4.0
requirements: aiohttp, beautifulsoup4, pydantic, openai, pdfplumber
version: 2.1.0
license: MIT

FEATURES:
- ✅ Discovery integrado: SearXNG + Exa.ai com LLM Planner
- ✅ Scraping multi-estratégia: document (PDF) → httpx → jina → browserless
- ✅ Content cleaning com sistema de candidatos (readability/jsonld/dom)
- ✅ LLM Selector: curadoria semântica dos melhores resultados
- ✅ Event emitters: status updates em tempo real
- ✅ Document Adapter: extração de PDFs com pdfplumber
- ✅ Retry inteligente: backoff exponencial por adapter
- ✅ Sem fallbacks: falha rapidamente se APIs não estão configuradas

CHANGELOG v2.1:
- Fixed: Removidos todos os fallbacks conforme solicitado
- Fixed: SearXNG endpoint padrão funcional (https://searx.be)
- Fixed: Timeout aumentado para 30s
- Fixed: Validação obrigatória de APIs
- Improved: Erro claro quando APIs não estão configuradas

USAGE:
1. Configure valves (API keys para OpenAI, Exa, endpoints)
2. Use search_and_scrape(query="sua query aqui")
3. Receba URLs descobertas + conteúdo scraped + metadados

ARCHITECTURE:
User Query → LLM Planner → [SearXNG + Exa] → Zipper → LLM Selector
→ Top K URLs → Scraper Chain (PDF→httpx→jina→browserless) → Cleaner → Result
"""

# ============================================================================
# SEÇÃO 1: IMPORTS E CONFIGURAÇÃO
# ============================================================================
import asyncio
import io
import json
import logging
import re
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

import aiohttp
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

# OpenAI (opcional)
try:
    from openai import AsyncOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    AsyncOpenAI = None

# Readability (opcional)
try:
    from readability import Document
    READABILITY_AVAILABLE = True
except ImportError:
    READABILITY_AVAILABLE = False
    Document = None

# PDF processing (opcional)
try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False
    pdfplumber = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# SEÇÃO 2: CONSTANTES E CONFIGURAÇÕES
# ============================================================================
class Constants:
    """Constantes centralizadas"""
    DEFAULT_TIMEOUT = 30
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
    MIN_WORDS = 30
    MIN_PARAGRAPHS = 1
    
    # Bot detection patterns
    CLOUDFLARE_SIGNS = [
        "checking your browser", "cloudflare", "please enable cookies",
        "ray id:", "cf-ray", "attention required", "just a moment",
        "authwall", "linkedin.com/authwall", "crunchbase.com", "marketscreener.com"
    ]
    
    # Paywall detection patterns
    PAYWALL_SIGNS = [
        "subscribe to continue", "register to read", "premium content",
        "members only", "subscriber exclusive", "sign in to read"
    ]
    
    # Retry configuration
    RETRY_DELAYS = [2, 5, 10]  # backoff exponencial (segundos)
    MAX_RETRIES = 3
    
    # Content limits
    MAX_CONTENT_LENGTH = 100000  # caracteres
    MAX_PDF_PAGES = 50
    
    # Scraper priorities
    ADAPTER_PRIORITIES = ["document", "httpx", "jina", "browserless"]

# ============================================================================
# SEÇÃO 3: MODELOS PYDANTIC
# ============================================================================
class Valves(BaseModel):
    """Configuração da tool"""
    # APIs
    openai_api_key: str = Field(
        default="",
        description="OpenAI API key para Planner e Selector (OBRIGATÓRIO)"
    )
    openai_model: str = Field(
        default="gpt-4o-mini",
        description="Modelo OpenAI (gpt-4o-mini recomendado)"
    )
    searxng_endpoint: str = Field(
        default="https://searx.be",
        description="SearXNG endpoint URL (ex: https://searx.be ou http://localhost:8080)"
    )
    exa_api_key: str = Field(
        default="",
        description="Exa.ai API key para busca neural (opcional)"
    )
    
    # Limits
    top_n_selector: int = Field(
        default=10,
        description="URLs para LLM Selector analisar (max 20)"
    )
    top_k_scraper: int = Field(
        default=3,
        description="URLs para scraper processar (max 5)"
    )
    request_timeout: int = Field(
        default=30,
        description="HTTP timeout em segundos"
    )
    searxng_timeout: int = Field(
        default=20,
        description="SearXNG timeout em segundos (otimizado para rapidez)"
    )
    exa_timeout: int = Field(
        default=25,
        description="Exa.ai timeout em segundos (busca neural precisa mais tempo)"
    )
    selector_timeout: int = Field(
        default=20,
        description="LLM Selector timeout em segundos"
    )
    max_retries: int = Field(
        default=3,
        description="Tentativas de retry por adapter"
    )
    
    # Scraper endpoints
    jina_endpoint: str = Field(
        default="https://r.jina.ai",
        description="Jina Reader endpoint"
    )
    browserless_endpoint: str = Field(
        default="",
        description="Browserless endpoint (opcional)"
    )
    browserless_token: str = Field(
        default="",
        description="Browserless API token (opcional)"
    )
    
    # Feature flags
    enable_pdf_extraction: bool = Field(
        default=True,
        description="Habilitar extração de PDFs"
    )
    enable_content_cleaning: bool = Field(
        default=True,
        description="Habilitar limpeza avançada de conteúdo"
    )
    max_content_length: int = Field(
        default=100000,
        description="Tamanho máximo de conteúdo em caracteres"
    )

class PlannerResponse(BaseModel):
    """Resposta do LLM Planner"""
    searxng_queries: List[str] = Field(
        description="Queries para SearXNG"
    )
    searxng_categories: List[str] = Field(
        default_factory=lambda: ["general"],
        description="Categorias SearXNG"
    )
    exa_query: str = Field(
        description="Query para Exa.ai"
    )
    exa_use_news: bool = Field(
        default=False,
        description="Usar categoria news no Exa"
    )
    # Campos temporais (opcionais)
    after: Optional[str] = Field(
        default=None,
        description="Data início ISO (YYYY-MM-DD) para SearXNG"
    )
    before: Optional[str] = Field(
        default=None,
        description="Data fim ISO (YYYY-MM-DD) para SearXNG"
    )

class SelectorResponse(BaseModel):
    """Resposta do LLM Selector"""
    selected_indices: List[int] = Field(
        description="Índices das URLs selecionadas"
    )
    reasoning: Optional[str] = Field(
        default=None,
        description="Justificativa da seleção"
    )

class ScrapedResult(BaseModel):
    """Resultado de scraping individual"""
    url: str
    content: str = ""
    word_count: int = 0
    adapter: str = ""
    success: bool = False
    error: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

# ============================================================================
# SEÇÃO 4: UTILITÁRIOS GERAIS
# ============================================================================
import unicodedata
from datetime import datetime, date, timedelta
def extract_domain(url: str) -> str:
    """Extrai domínio da URL
    
    Args:
        url: URL completa para extrair domínio
        
    Returns:
        Domínio em lowercase, ou string vazia se inválida
        
    Example:
        >>> extract_domain("https://example.com/path")
        "example.com"
    """
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower()
    except Exception:
        return ""

def normalize_url(url: str, base_url: str = "") -> str:
    """Normaliza URL (remove fragmentos, resolve relativas)
    
    Args:
        url: URL para normalizar
        base_url: URL base para resolver URLs relativas
        
    Returns:
        URL normalizada ou original se inválida
    """
    try:
        if base_url:
            url = urljoin(base_url, url)
        parsed = urlparse(url)
        # Remove fragment
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}{'?' + parsed.query if parsed.query else ''}"
    except Exception:
        return url

def clean_text(text: str) -> str:
    """Limpeza básica de texto
    
    Remove caracteres especiais, normaliza espaços e line breaks
    
    Args:
        text: Texto para limpar
        
    Returns:
        Texto limpo e normalizado
    """
    if not text:
        return ""
    
    # Normalizar line breaks
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    
    # Remover múltiplas linhas vazias
    text = re.sub(r'\n{3,}', '\n\n', text)
    
    # Normalizar espaços
    text = re.sub(r'[ \t]+', ' ', text)
    
    # Substituir unicode especial
    replacements = {
        '\u201c': '"', '\u201d': '"',
        '\u2018': "'", '\u2019': "'",
        '\u2013': '-', '\u2014': '--',
        '\u2026': '...', '\u00a0': ' ',
        '\u00ad': '',  # soft hyphen
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    
    return text.strip()

def truncate_content(content: str, max_length: int) -> str:
    """Trunca conteúdo mantendo palavras completas"""
    if len(content) <= max_length:
        return content
    
    truncated = content[:max_length]
    last_space = truncated.rfind(' ')
    if last_space > 0:
        truncated = truncated[:last_space]
    
    return truncated + "..."

async def safe_emit(
    emitter: Optional[Callable],
    message: str,
    done: bool = False,
    hidden: bool = False
):
    """Emite evento de forma segura (OpenWebUI é sempre async)"""
    if not emitter:
        return
    
    try:
        await emitter({
            "type": "status",
            "data": {
                "description": message,
                "done": done,
                "hidden": hidden
            }
        })
    except Exception as e:
        logger.debug(f"Emit error: {e}")

def build_real_headers(attempt: int = 0, url: str = "") -> dict:
    """Constrói headers realistas com rotação progressiva - alinhado com Scraper V5"""
    # User agents mais realistas e atualizados
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ]
    
    # Detectar locale por domínio (igual ao V5)
    if url:
        domain = urlparse(url).netloc.lower()
        if domain.endswith('.br'):
            locale = "pt-BR,pt;q=0.9,en-US;q=0.8"
        elif domain.endswith('.es'):
            locale = "es-ES,es;q=0.9,en;q=0.8"
        elif domain.endswith('.fr'):
            locale = "fr-FR,fr;q=0.9,en;q=0.8"
        else:
            locales = ["en-US,en;q=0.9", "pt-BR,pt;q=0.9,en-US;q=0.8", "es-ES,es;q=0.9,en;q=0.8", "fr-FR,fr;q=0.9,en;q=0.8"]
            locale = locales[attempt % len(locales)]
    else:
        locales = ["en-US,en;q=0.9", "pt-BR,pt;q=0.9,en-US;q=0.8", "es-ES,es;q=0.9,en;q=0.8", "fr-FR,fr;q=0.9,en;q=0.8"]
        locale = locales[attempt % len(locales)]
    
    # Headers base com melhor bypass do Cloudflare (igual ao V5)
    headers = {
        "User-Agent": user_agents[attempt % len(user_agents)],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": locale,
        "Accept-Encoding": "gzip, deflate, br",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate", 
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Cache-Control": "max-age=0",
        "DNT": "1",
        "Connection": "keep-alive"
    }
    
    # Adicionar headers Sec-Ch-Ua para bypass do Cloudflare (igual ao V5)
    if attempt == 0:  # Desktop headers
        headers.update({
            "Sec-Ch-Ua": '"Chromium";v="124", "Not(A:Brand";v="24", "Google Chrome";v="124"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"'
        })
    elif attempt == 1:  # Mobile headers
        headers.update({
            "Sec-Ch-Ua": '"Not A(Brand";v="99", "Safari";v="604"',
            "Sec-Ch-Ua-Mobile": "?1",
            "Sec-Ch-Ua-Platform": '"iOS"'
        })
    else:  # Linux headers
        headers.update({
            "Sec-Ch-Ua": '"Chromium";v="124", "Not(A:Brand";v="24", "Google Chrome";v="124"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Linux"'
        })
    
    return headers

def parse_date_range_from_query(query: str) -> Tuple[Optional[date], Optional[date]]:
    """Extract date range from natural language query (alinhado com tool_discovery)"""
    # Look for explicit date patterns first
    after, before = parse_month_year_range(query)
    if after or before:
        return after, before
    
    # Normalize query: lowercase and remove accents
    query_lower = query.lower()
    # Remove accents using unicodedata
    query_lower = ''.join(
        c for c in unicodedata.normalize('NFD', query_lower)
        if unicodedata.category(c) != 'Mn'
    )
    
    end_date = datetime.utcnow().date()
    
    # Pattern: "ultimos X dias/semanas/meses" (acentos já removidos acima)
    patterns = [
        (r'ultimos?\s+(\d+)\s+dias?', lambda m: timedelta(days=int(m.group(1)))),
        (r'ultimos?\s+(\d+)\s+semanas?', lambda m: timedelta(weeks=int(m.group(1)))),
        (r'ultimos?\s+(\d+)\s+meses?', lambda m: timedelta(days=int(m.group(1)) * 30)),
        (r'ultimos?\s+(\d+)\s+anos?', lambda m: timedelta(days=int(m.group(1)) * 365)),
        (r'ultimo\s+mes', lambda m: timedelta(days=30)),
        (r'ultima\s+semana', lambda m: timedelta(weeks=1)),
        (r'ultimo\s+ano', lambda m: timedelta(days=365)),
        (r'last\s+(\d+)\s+days?', lambda m: timedelta(days=int(m.group(1)))),
        (r'last\s+(\d+)\s+weeks?', lambda m: timedelta(weeks=int(m.group(1)))),
        (r'last\s+(\d+)\s+months?', lambda m: timedelta(days=int(m.group(1)) * 30)),
        (r'last\s+(\d+)\s+years?', lambda m: timedelta(days=int(m.group(1)) * 365)),
        (r'past\s+(\d+)\s+days?', lambda m: timedelta(days=int(m.group(1)))),
        (r'past\s+(\d+)\s+months?', lambda m: timedelta(days=int(m.group(1)) * 30)),
        (r'past\s+(\d+)\s+years?', lambda m: timedelta(days=int(m.group(1)) * 365)),
    ]
    
    for pattern, func in patterns:
        match = re.search(pattern, query_lower)
        if match:
            try:
                delta = func(match)
                start_date = end_date - delta
                return start_date, end_date
            except (ValueError, AttributeError):
                continue
    
    # Look for relative date indicators (fallback)
    relative_indicators = [
        'última semana', 'semana passada', 'último mês', 'mês passado',
        'último ano', 'ano passado', 'hoje', 'ontem', 'há', 'atrás',
        'recentes', 'atual', 'atualizado', 'novo', 'novos'
    ]
    
    if any(indicator in query_lower for indicator in relative_indicators):
        # Default to last 30 days for "recent" queries
        start_date = end_date - timedelta(days=30)
        return start_date, end_date
    
    return None, None

def parse_month_year_range(text: str) -> Tuple[Optional[date], Optional[date]]:
    """Parse month-year ranges like 'janeiro 2024' or 'entre janeiro 2024 e fevereiro 2025'"""
    # Portuguese month mappings
    month_names = {
        'janeiro': 1, 'fevereiro': 2, 'março': 3, 'abril': 4,
        'maio': 5, 'junho': 6, 'julho': 7, 'agosto': 8,
        'setembro': 9, 'outubro': 10, 'novembro': 11, 'dezembro': 12
    }
    
    text_lower = text.lower()
    
    # Pattern: "entre janeiro 2024 e fevereiro 2025"
    range_pattern = r'entre\s+(\w+)\s+(\d{4})\s+e\s+(\w+)\s+(\d{4})'
    match = re.search(range_pattern, text_lower)
    if match:
        try:
            month1_name, year1, month2_name, year2 = match.groups()
            month1 = month_names.get(month1_name)
            month2 = month_names.get(month2_name)
            
            if month1 and month2:
                after = date(int(year1), month1, 1)
                before = date(int(year2), month2, 28)  # End of month
                return after, before
        except (ValueError, KeyError):
            pass
    
    # Pattern: "janeiro 2024" (single month)
    single_pattern = r'(\w+)\s+(\d{4})'
    match = re.search(single_pattern, text_lower)
    if match:
        try:
            month_name, year = match.groups()
            month = month_names.get(month_name)
            
            if month:
                after = date(int(year), month, 1)
                before = date(int(year), month, 28)  # End of month
                return after, before
        except (ValueError, KeyError):
            pass
    
    return None, None

# ============================================================================
# SEÇÃO 5: DETECÇÃO DE PROBLEMAS
# ============================================================================
def is_bot_wall(status_code: int, body_text: str = "") -> bool:
    """Detecta bot-wall (Cloudflare, LinkedIn authwall, etc)"""
    # Status codes que indicam bot-wall
    if status_code in [403, 401, 429, 503, 999]:
        return True
    
    if not body_text:
        return False
    
    body_lower = body_text.lower()
    
    # Detectar padrões específicos de bot-wall
    bot_wall_patterns = [
        "checking your browser", "cloudflare", "please enable cookies",
        "ray id:", "cf-ray", "attention required", "just a moment",
        "authwall", "linkedin.com/authwall", "crunchbase.com", 
        "marketscreener.com", "please wait", "verifying you are human"
    ]
    
    return any(pattern in body_lower for pattern in bot_wall_patterns)

def detect_paywall(html: str) -> dict:
    """Detecta paywall no conteúdo"""
    if not html:
        return {"type": "none", "confidence": 0, "hits": []}
    
    html_lower = html.lower()
    hits = []
    
    for sign in Constants.PAYWALL_SIGNS:
        if sign in html_lower:
            hits.append(sign)
    
    confidence = min(len(hits) * 25, 100)
    
    if confidence >= 60:
        paywall_type = "hard"
    elif confidence >= 30:
        paywall_type = "soft"
    else:
        paywall_type = "none"
    
    return {
        "type": paywall_type,
        "confidence": confidence,
        "hits": hits
    }

def is_pdf_url(url: str, content_type: Optional[str] = None) -> bool:
    """Detecta PDF por extensão ou content-type"""
    u = (url or "").lower()
    
    # Check por extensão
    if u.endswith(".pdf"):
        return True
    
    # Check por content-type
    if content_type:
        ct = (content_type or "").lower()
        if "application/pdf" in ct:
            return True
    
    return False

def extract_metadata_from_html(soup: BeautifulSoup) -> dict:
    """Extrai metadados básicos do HTML"""
    metadata = {}
    
    # Title
    title_tag = soup.find("title")
    if title_tag:
        metadata["title"] = title_tag.get_text().strip()
    
    # Meta tags
    for meta in soup.find_all("meta"):
        try:
            # Usar getattr para acessar atributos de forma segura
            name = getattr(meta, 'get', lambda x, default='': default)("name", "") or getattr(meta, 'get', lambda x, default='': default)("property", "")
            content = getattr(meta, 'get', lambda x, default='': default)("content", "")
            
            if name and content:
                metadata[name] = content
        except (AttributeError, TypeError):
            # Skip se não tem atributos
            continue
    
    return metadata

# ============================================================================
# SEÇÃO 6: LLM PLANNER
# ============================================================================
async def plan_search(query: str, valves: Valves) -> PlannerResponse:
    """LLM Planner: gera queries otimizadas para SearXNG e Exa"""
    if not OPENAI_AVAILABLE or not valves.openai_api_key:
        raise ValueError("OpenAI API key é obrigatória para o Planner")
    
    system_prompt = """Você é um planejador de buscas especializado. Analise a query do usuário e gere:

1. **SearXNG Queries** (1-3 queries): 
   - Queries em português/inglês otimizadas para descoberta
   - Use variações semânticas e sinônimos
   - Mantenha concisão
   - **CRÍTICO**: Se detectar termos como "notícias", "news", "últimas 24h", "hoje", "atualizações", "recentes" → SEMPRE inclua "notícias" nas queries
   - **PRESERVAÇÃO**: Mantenha termos como "priorize", "de prioridade", "foco em", "dê ênfase" integralmente nas queries

2. **Categorias SearXNG**: 
   - Sempre incluir "general"
   - **OBRIGATÓRIO**: Adicionar "news" se detectar qualquer termo temporal (últimas 24h, hoje, recente, notícias, etc.)
   - Adicionar "science" se for acadêmico

3. **Exa Query**: 
   - Query única em inglês para busca neural
   - Otimizada para relevância semântica
   - **CRÍTICO**: Se for notícias recentes, inclua "news" ou "recent" na query

4. **exa_use_news**: 
   - **true** se detectar: "notícias", "news", "últimas 24h", "hoje", "atualizações", "recentes", "últimas horas"
   - **false** para conteúdo atemporal

5. **Campos temporais (opcionais)**:
   - **after**: Data início ISO (YYYY-MM-DD) se detectar período específico
   - **before**: Data fim ISO (YYYY-MM-DD) se detectar período específico
   - **CRÍTICO**: Se detectar "entre janeiro 2024 e fevereiro 2025" → after: "2024-01-01", before: "2025-02-28"
   - **CRÍTICO**: Se detectar "últimas 24h" → after: data de ontem, before: hoje
   - **CRÍTICO**: Se detectar "últimos 30 dias" → after: 30 dias atrás, before: hoje

DETECÇÃO DE NOTÍCIAS:
- Palavras-chave: "notícias", "news", "últimas 24h", "hoje", "atualizações", "recentes", "últimas horas"
- Se detectar qualquer uma → exa_use_news = true + incluir "news" nas categorias

DETECÇÃO DE PERÍODOS:
- "entre janeiro 2024 e fevereiro 2025" → after: "2024-01-01", before: "2025-02-28"
- "últimas 24h" → after: data de ontem, before: hoje
- "últimos 30 dias" → after: 30 dias atrás, before: hoje
- "janeiro 2024" → after: "2024-01-01", before: "2024-01-31"

Responda APENAS JSON válido seguindo o schema.

Exemplo 1 (NOTÍCIAS COM PERÍODO):
{
  "searxng_queries": ["Lula congresso notícias últimas 24h", "Lula relações congresso hoje"],
  "searxng_categories": ["general", "news"],
  "exa_query": "Lula congress relations news today recent",
  "exa_use_news": true,
  "after": "2025-10-15",
  "before": "2025-10-16"
}

Exemplo 2 (PERÍODO ESPECÍFICO):
{
  "searxng_queries": ["relações família Bolsonaro Trump janeiro 2024 fevereiro 2025", "Bolsonaro Trump família 2024 2025"],
  "searxng_categories": ["general", "news"],
  "exa_query": "Bolsonaro Trump family relations January 2024 February 2025",
  "exa_use_news": true,
  "after": "2024-01-01",
  "before": "2025-02-28"
}

Exemplo 3 (CONTEÚDO GERAL):
{
  "searxng_queries": ["machine learning tutorial", "ML beginner guide"],
  "searxng_categories": ["general"],
  "exa_query": "machine learning fundamentals tutorial for beginners",
  "exa_use_news": false
}"""

    try:
        if not AsyncOpenAI:
            raise ValueError("OpenAI não está disponível")
        client = AsyncOpenAI(api_key=valves.openai_api_key)
        
        # Incluir data atual no contexto
        from datetime import datetime
        current_date = datetime.now().strftime("%Y-%m-%d")
        current_time = datetime.now().strftime("%H:%M")
        
        user_message = f"""Query: {query}

CONTEXTO TEMPORAL:
- Data atual: {current_date}
- Hora atual: {current_time}
- Para notícias "últimas 24h", considere desde {current_date} 00:00 até agora"""

        response = await asyncio.wait_for(
            client.chat.completions.create(
                model=valves.openai_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                response_format={"type": "json_object"},
                temperature=0.3,
                max_tokens=500
            ),
            timeout=35.0
        )
        
        content = response.choices[0].message.content
        if not content:
            raise ValueError("Resposta vazia do OpenAI")
        parsed = json.loads(content)
        
        result = PlannerResponse(**parsed)
        
        # Auto-detect date range if not provided by LLM
        if not result.after and not result.before:
            after, before = parse_date_range_from_query(query)
            if after:
                result.after = after.isoformat()
            if before:
                result.before = before.isoformat()
        
        logger.info(f"[Planner] Generated {len(result.searxng_queries)} SearXNG queries")
        if result.after or result.before:
            logger.info(f"[Planner] Date range: {result.after} to {result.before}")
        return result
        
    except asyncio.TimeoutError:
        raise TimeoutError("Planner timeout - OpenAI não respondeu a tempo")
    except json.JSONDecodeError as e:
        raise ValueError(f"Planner JSON parse error: {e}")
    except Exception as e:
        raise RuntimeError(f"Planner error: {e}")

# ============================================================================
# SEÇÃO 7: SEARCH EXECUTORS
# ============================================================================
async def search_searxng(
    queries: List[str],
    categories: List[str],
    endpoint: str,
    timeout: int,
    after: Optional[str] = None,
    before: Optional[str] = None
) -> List[Dict]:
    """Executor SearXNG com deduplicação - alinhado com tool discovery"""
    if not endpoint:
        raise ValueError("SearXNG endpoint é obrigatório")
    
    # Normalize endpoint to ensure it ends with /search
    base = endpoint.rstrip('/')
    if not base.endswith('/search'):
        base = f"{base}/search"
    
    all_results = []
    seen_urls = set()
    
    # Headers to request JSON (igual ao discovery)
    headers = {"Accept": "application/json"}
    
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=timeout)
    ) as session:
        for query in queries:
            # Build params igual ao discovery
            params = {
                "q": query,
                "format": "json",
                "pageno": 1
            }
            
            # Add categories if provided (igual ao discovery)
            if categories:
                params["categories"] = ",".join(categories)
            
            # Add temporal filters if provided (igual ao discovery)
            if after:
                params["after"] = after
            if before:
                params["before"] = before
            
            try:
                async with session.get(base, params=params, headers=headers) as resp:
                    if resp.status == 200:
                        # Check content type before attempting to parse JSON (igual ao discovery)
                        content_type = resp.headers.get('content-type', '').lower()
                        if 'application/json' in content_type:
                            data = await resp.json()
                            results = data.get("results", [])
                            
                            # DEBUG: Log detalhado se 0 resultados (igual ao discovery)
                            if not results:
                                logger.warning(f"[SearXNG DEBUG] 0 results in JSON response")
                                logger.warning(f"[SearXNG DEBUG] Full response keys: {list(data.keys())}")
                                logger.warning(f"[SearXNG DEBUG] Query params: {params}")
                            
                            for r in results:
                                url = r.get("url", "")
                                if url and url not in seen_urls:
                                    seen_urls.add(url)
                                    all_results.append({
                                        "url": url,
                                        "title": r.get("title", ""),
                                        "snippet": r.get("content", "")[:500],
                                        "score": r.get("score", 0.8),
                                        "source": "searxng",
                                        "engine": r.get("engine", "unknown")
                                    })
                            
                            logger.info(f"[SearXNG] Query '{query}': {len(results)} resultados")
                        else:
                            logger.warning(f"[SearXNG] Content-Type não é JSON: {content_type}")
                    else:
                        logger.warning(f"[SearXNG] Status {resp.status} para query: {query}")
                        # Stop on 400 (bad query) like discovery
                        if resp.status == 400:
                            break
                        
            except asyncio.TimeoutError:
                logger.warning(f"[SearXNG] Timeout para query: {query}")
            except Exception as e:
                logger.error(f"[SearXNG] Error: {e}")
    
    logger.info(f"[SearXNG] Total: {len(all_results)} URLs únicas")
    return all_results

async def search_exa(
    query: str,
    use_news: bool,
    api_key: str,
    timeout: int
) -> List[Dict]:
    """Executor Exa.ai com busca neural"""
    if not api_key:
        logger.info("[Exa] API key não configurada - pulando")
        return []
    
    payload = {
        "query": query,
        "type": "neural",
        "numResults": 20,
        "contents": {
            "highlights": {
                "numSentences": 3,
                "highlightsPerUrl": 1
            }
        }
    }
    
    if use_news:
        payload["category"] = "news"
        payload["startPublishedDate"] = "2024-01-01"
    
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as session:
            async with session.post(
                "https://api.exa.ai/search",
                json=payload,
                headers=headers
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results = data.get("results", [])
                    
                    formatted = []
                    for r in results:
                        highlights = r.get("highlights", [])
                        snippet = highlights[0] if highlights else r.get("text", "")
                        
                        formatted.append({
                            "url": r.get("url", ""),
                            "title": r.get("title", ""),
                            "snippet": snippet[:500],
                            "score": r.get("score", 0.9),
                            "source": "exa",
                            "published_date": r.get("publishedDate")
                        })
                    
                    logger.info(f"[Exa] {len(formatted)} resultados")
                    return formatted
                else:
                    logger.warning(f"[Exa] Status {resp.status}")
                    
    except asyncio.TimeoutError:
        logger.warning("[Exa] Timeout")
    except Exception as e:
        logger.error(f"[Exa] Error: {e}")
    
    return []

def zipper_results(searxng: List[Dict], exa: List[Dict]) -> List[Dict]:
    """Intercala e deduplica resultados de ambos engines"""
    seen_urls = set()
    seen_domains = defaultdict(int)
    interleaved = []
    
    # Ordenar por score
    searxng.sort(key=lambda x: x["score"], reverse=True)
    exa.sort(key=lambda x: x["score"], reverse=True)
    
    max_len = max(len(searxng), len(exa))
    max_per_domain = 3
    
    for i in range(max_len):
        # Intercalar SearXNG
        if i < len(searxng):
            result = searxng[i]
            url = result["url"]
            domain = extract_domain(url)
            
            if url and url not in seen_urls and seen_domains[domain] < max_per_domain:
                seen_urls.add(url)
                seen_domains[domain] += 1
                interleaved.append(result)
        
        # Intercalar Exa
        if i < len(exa):
            result = exa[i]
            url = result["url"]
            domain = extract_domain(url)
            
            if url and url not in seen_urls and seen_domains[domain] < max_per_domain:
                seen_urls.add(url)
                seen_domains[domain] += 1
                interleaved.append(result)
    
    logger.info(f"[Zipper] {len(interleaved)} URLs após deduplicação")
    return interleaved

# ============================================================================
# SEÇÃO 8: LLM SELECTOR
# ============================================================================
async def select_urls(
    candidates: List[Dict],
    query: str,
    top_n: int,
    valves: Valves
) -> List[int]:
    """LLM Selector: curadoria semântica dos melhores URLs"""
    if not OPENAI_AVAILABLE or not valves.openai_api_key:
        raise ValueError("OpenAI API key é obrigatória para o Selector")
    
    # Limitar candidatos para análise
    candidates_subset = candidates[:50]
    
    # Formatar candidatos para o LLM
    candidates_text = []
    for i, c in enumerate(candidates_subset):
        domain = extract_domain(c["url"])
        candidates_text.append(
            f"{i}. [{c['source']}] {c['title']}\n"
            f"   URL: {c['url'][:100]}\n"
            f"   Domain: {domain}\n"
            f"   Snippet: {c['snippet'][:150]}..."
        )
    
    candidates_str = "\n\n".join(candidates_text)
    
    system_prompt = f"""Você é um seletor especializado de URLs. Analise os candidatos e selecione EXATAMENTE {top_n} URLs mais relevantes para a query do usuário.

**OBRIGATÓRIO**: Você DEVE selecionar exatamente {top_n} URLs, nem mais nem menos.

Critérios de seleção (em ordem de prioridade):
1. **Relevância semântica**: conteúdo diretamente relacionado à query
2. **Qualidade da fonte**: sites confiáveis e autoritativos
3. **Diversidade**: max 2 URLs do mesmo domínio
4. **Atualidade**: preferir fontes recentes quando relevante
5. **Score original**: considerar o score dos motores de busca

**CRÍTICO PARA NOTÍCIAS RECENTES:**
- Se a query menciona "últimas 24h", "hoje", "notícias", "recentes" → PRIORIZE URLs com timestamps recentes
- URLs de sites de notícias (G1, Estadão, Globo, UOL, etc.) têm prioridade para conteúdo temporal
- Ignore URLs antigas (mais de 1 dia) se a query pede notícias recentes

**TERMOS DE PRIORIDADE:**
- Se a query menciona "priorize", "de prioridade", "foco em", "dê ênfase" → PRIORIZE URLs que atendem especificamente ao que foi priorizado
- Considere o contexto da prioridade ao selecionar URLs mais relevantes

**IMPORTANTE**: Se não houver {top_n} URLs recentes, selecione as melhores disponíveis para completar {top_n}.

Retorne APENAS JSON com o schema:
{{
  "selected_indices": [int],
  "reasoning": "breve justificativa da seleção"
}}

Os índices devem estar entre 0 e {len(candidates_subset)-1}."""

    try:
        if not AsyncOpenAI:
            raise ValueError("OpenAI não está disponível")
        client = AsyncOpenAI(api_key=valves.openai_api_key)
        
        # Incluir data atual no contexto do Selector também
        from datetime import datetime
        current_date = datetime.now().strftime("%Y-%m-%d")
        current_time = datetime.now().strftime("%H:%M")
        
        user_message = f"""Query: {query}

CONTEXTO TEMPORAL:
- Data atual: {current_date}
- Hora atual: {current_time}
- Para "últimas 24h", considere desde {current_date} 00:00 até agora

Candidatos:
{candidates_str}"""

        response = await asyncio.wait_for(
            client.chat.completions.create(
                model=valves.openai_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_message}
                ],
                response_format={"type": "json_object"},
                temperature=0.2,
                max_tokens=1000
            ),
            timeout=valves.selector_timeout
        )
        
        content = response.choices[0].message.content
        if not content:
            raise ValueError("Resposta vazia do OpenAI")
        parsed = json.loads(content)
        
        selector_resp = SelectorResponse(**parsed)
        
        # Validar índices
        valid_indices = [
            i for i in selector_resp.selected_indices
            if 0 <= i < len(candidates_subset)
        ]
        
        logger.info(f"[Selector] Selecionados {len(valid_indices)} URLs")
        if selector_resp.reasoning:
            logger.debug(f"[Selector] Reasoning: {selector_resp.reasoning}")
        
        return valid_indices[:top_n]
        
    except asyncio.TimeoutError:
        raise TimeoutError("Selector timeout - OpenAI não respondeu a tempo")
    except json.JSONDecodeError as e:
        raise ValueError(f"Selector JSON parse error: {e}")
    except Exception as e:
        raise RuntimeError(f"Selector error: {e}")

# ============================================================================
# SEÇÃO 9: DOCUMENT ADAPTER (PDF)
# ============================================================================
async def scrape_pdf(url: str, timeout: int, max_pages: int = 50) -> Optional[str]:
    """Baixa PDF e extrai texto com pdfplumber"""
    if not PDFPLUMBER_AVAILABLE:
        logger.debug("[PDF] pdfplumber não disponível")
        return None
    
    headers = {"User-Agent": build_real_headers(0)["User-Agent"]}
    
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    logger.debug(f"[PDF] Status {resp.status} para {url}")
                    return None
                
                content_type = resp.headers.get("content-type", "")
                data = await resp.read()
                
                # Detectar por magic bytes ou content-type
                is_pdf = (
                    (len(data) >= 4 and data[:4] == b"%PDF") or
                    "application/pdf" in content_type.lower()
                )
                
                if not is_pdf:
                    return None
                
                logger.info(f"[PDF] Extraindo texto de {url}")
                
                try:
                    if not pdfplumber:
                        return None
                    with pdfplumber.open(io.BytesIO(data)) as pdf:
                        pages = []
                        num_pages = min(len(pdf.pages), max_pages)
                        
                        for i, page in enumerate(pdf.pages[:num_pages]):
                            try:
                                text = page.extract_text() or ""
                                if text:
                                    pages.append(f"=== Página {i+1} ===\n{text}")
                            except Exception as e:
                                logger.debug(f"[PDF] Erro na página {i+1}: {e}")
                        
                        if not pages:
                            logger.warning(f"[PDF] Nenhum texto extraído de {url}")
                            return None
                        
                        full_text = "\n\n".join(pages).strip()
                        word_count = len(full_text.split())
                        
                        if word_count < Constants.MIN_WORDS:
                            logger.debug(f"[PDF] Texto muito curto ({word_count} palavras)")
                            return None
                        
                        logger.info(f"[PDF] Extraídas {num_pages} páginas, {word_count} palavras")
                        return full_text
                        
                except Exception as e:
                    logger.warning(f"[PDF] Falha na extração para {url}: {e}")
                    return None
                    
    except asyncio.TimeoutError:
        logger.warning(f"[PDF] Timeout para {url}")
    except Exception as e:
        logger.debug(f"[PDF] Download falhou para {url}: {e}")
    
    return None

# ============================================================================
# SEÇÃO 10: WEB SCRAPERS
# ============================================================================
async def scrape_httpx(url: str, timeout: int, attempt: int = 0) -> Optional[str]:
    """Scraper HTTPX com retry - alinhado com Scraper V5
    
    Usa headers realistas e rotação para bypass de bot-walls
    
    Args:
        url: URL para fazer scraping
        timeout: Timeout em segundos
        attempt: Número da tentativa (para rotação de headers)
        
    Returns:
        Conteúdo extraído ou None se falhar
    """
    headers = build_real_headers(attempt, url)
    
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as session:
            async with session.get(url, headers=headers, allow_redirects=True) as response:
                if response.status != 200:
                    logger.debug(f"[HTTPX] Status {response.status} para {url}")
                    return None
                
                html = await response.text()
                
                # Detectar bot-wall
                if is_bot_wall(response.status, html):
                    logger.warning(f"[HTTPX] Bot-wall detectado para {url}")
                    return None
                
                # Parse com BeautifulSoup
                soup = BeautifulSoup(html, "html.parser")
                
                # Remover elementos indesejados
                for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
                    tag.decompose()
                
                # Extrair texto
                text = soup.get_text(separator="\n", strip=True)
                
                # Validar conteúdo
                word_count = len(text.split())
                if word_count < Constants.MIN_WORDS:
                    logger.debug(f"[HTTPX] Conteúdo muito curto ({word_count} palavras)")
                    return None
                
                logger.info(f"[HTTPX] Extraídas {word_count} palavras de {url}")
                return text
                
    except asyncio.TimeoutError:
        logger.warning(f"[HTTPX] Timeout para {url}")
    except Exception as e:
        logger.debug(f"[HTTPX] Falhou: {e}")
    
    return None

async def scrape_jina(url: str, endpoint: str, timeout: int) -> Optional[str]:
    """Scraper Jina Reader
    
    Usa API do Jina Reader para extrair conteúdo limpo
    
    Args:
        url: URL para fazer scraping
        endpoint: Endpoint do Jina Reader
        timeout: Timeout em segundos
        
    Returns:
        Conteúdo extraído ou None se falhar
    """
    jina_url = f"{endpoint}/{url}"
    headers = {
        "Accept": "text/plain",
        "X-Return-Format": "text"
    }
    
    # Timeout mais agressivo para Jina (15s em vez de 30s)
    jina_timeout = min(timeout, 15)
    
    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=jina_timeout)
        ) as session:
            async with session.get(jina_url, headers=headers) as response:
                if response.status != 200:
                    logger.debug(f"[Jina] Status {response.status} para {url}")
                    return None
                
                text = await response.text()
                
                # Validar conteúdo
                word_count = len(text.split())
                if word_count < Constants.MIN_WORDS:
                    logger.debug(f"[Jina] Conteúdo muito curto ({word_count} palavras)")
                    return None
                
                logger.info(f"[Jina] Extraídas {word_count} palavras de {url}")
                return text
                
    except asyncio.TimeoutError:
        logger.warning(f"[Jina] Timeout para {url}")
    except Exception as e:
        logger.debug(f"[Jina] Falhou: {e}")
    
    return None

async def scrape_browserless_function(
    url: str,
    endpoint: str,
    token: str,
    timeout: int
) -> Optional[str]:
    """Scraper Browserless Function - estratégia mais robusta e versátil
    
    Executa script JavaScript customizável para lidar com:
    - Remoção de overlays (cookies, GDPR, paywalls)
    - Espera inteligente (networkidle2)
    - Sinais anti-bot/headless
    - Seletores de conteúdo múltiplos
    
    Args:
        url: URL para fazer scraping
        endpoint: Endpoint do Browserless
        token: Token de autenticação (opcional)
        timeout: Timeout em segundos
        
    Returns:
        Conteúdo extraído ou None se falhar
    """
    if not endpoint:
        return None
    
    # Script JavaScript robusto para extração de conteúdo
    js_script = """
    // Remover overlays e elementos indesejados
    const removeElements = (selectors) => {
        selectors.forEach(selector => {
            document.querySelectorAll(selector).forEach(el => el.remove());
        });
    };
    
    // Remover overlays comuns
    removeElements([
        '[class*="cookie"]', '[class*="gdpr"]', '[class*="paywall"]',
        '[class*="overlay"]', '[class*="modal"]', '[class*="popup"]',
        '[class*="banner"]', '[class*="advertisement"]', '[class*="ad-"]',
        'script', 'style', 'nav', 'footer', 'header', 'aside', 'noscript'
    ]);
    
    // Função para extrair conteúdo principal
    const extractContent = () => {
        const selectors = [
            'article', 'main', '[role="main"]', '.content', 
            '.article-body', '.post-content', '.entry-content',
            '.story-content', '.article-content', '.post-body'
        ];
        
        for (const selector of selectors) {
            const element = document.querySelector(selector);
            if (element) {
                const text = element.innerText || element.textContent || '';
                if (text.trim().split('\\s').length >= 30) {
                    return text.trim();
                }
            }
        }
        
        // Fallback para body
        return document.body.innerText || document.body.textContent || '';
    };
    
    // Aguardar carregamento completo
    await new Promise(resolve => {
        if (document.readyState === 'complete') {
            resolve();
        } else {
            window.addEventListener('load', resolve);
        }
    });
    
    // Aguardar rede ficar ociosa
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    return extractContent();
    """
    
    # Payload para estratégia function
    payload = {
        "url": url,
        "gotoOptions": {
            "waitUntil": ["networkidle2"],
            "timeout": min(timeout * 1000, 30000)  # Max 30s
        },
        "function": js_script
    }
    
    # Construir endpoint
    endpoint_url = endpoint.rstrip('/') + '/function'
    
    # Params com timeout e token
    params = {"timeout": str(min(timeout * 1000, 15000))}  # Max 15s
    if token:
        params["token"] = token
    
    try:
        logger.debug(f"[Browserless-Function] Fazendo request para {endpoint_url}")
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as session:
            async with session.post(
                endpoint_url,
                json=payload,
                params=params
            ) as response:
                if response.status != 200:
                    logger.warning(f"[Browserless-Function] Status {response.status} para {url}")
                    return None
                
                data = await response.json()
                content = data.get("result", "")
                
                if not content:
                    logger.debug(f"[Browserless-Function] Conteúdo vazio para {url}")
                    return None
                
                # Validar conteúdo
                word_count = len(content.split())
                if word_count < Constants.MIN_WORDS:
                    logger.debug(f"[Browserless-Function] Conteúdo muito curto ({word_count} palavras)")
                    return None
                
                logger.info(f"[Browserless-Function] Extraídas {word_count} palavras de {url}")
                return content
                
    except asyncio.TimeoutError:
        logger.warning(f"[Browserless-Function] Timeout para {url}")
    except Exception as e:
        logger.debug(f"[Browserless-Function] Falhou: {e}")
    
    return None

async def scrape_browserless_amp(
    url: str,
    endpoint: str,
    token: str,
    timeout: int
) -> Optional[str]:
    """Scraper Browserless AMP - estratégia otimizada para notícias
    
    Acessa versão AMP da página para contornar scripts complexos e paywalls
    
    Args:
        url: URL para fazer scraping
        endpoint: Endpoint do Browserless
        token: Token de autenticação (opcional)
        timeout: Timeout em segundos
        
    Returns:
        Conteúdo extraído ou None se falhar
    """
    if not endpoint:
        return None
    
    # Payload para estratégia AMP
    payload = {
        "url": url,
        "gotoOptions": {
            "waitUntil": ["domcontentloaded"],
            "timeout": min(timeout * 1000, 20000)  # Max 20s
        },
        "amp": True  # Forçar modo AMP
    }
    
    # Construir endpoint
    endpoint_url = endpoint.rstrip('/') + '/content'
    
    # Params com timeout e token
    params = {"timeout": str(min(timeout * 1000, 10000))}  # Max 10s
    if token:
        params["token"] = token
    
    try:
        logger.debug(f"[Browserless-AMP] Fazendo request para {endpoint_url}")
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout)
        ) as session:
            async with session.post(
                endpoint_url,
                json=payload,
                params=params
            ) as response:
                if response.status != 200:
                    logger.warning(f"[Browserless-AMP] Status {response.status} para {url}")
                    return None
                
                data = await response.json()
                html = data.get("html", "")
                
                if not html:
                    logger.debug(f"[Browserless-AMP] HTML vazio para {url}")
                    return None
                
                # Parse HTML
                soup = BeautifulSoup(html, "html.parser")
                
                # Remover elementos indesejados
                for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
                    tag.decompose()
                
                # Tentar seletores AMP específicos
                content_selectors = [
                    "article", "main", "[role='main']", ".content",
                    ".article-body", ".post-content", ".entry-content"
                ]
                
                content_text = ""
                for selector in content_selectors:
                    element = soup.select_one(selector)
                    if element:
                        content_text = element.get_text(separator="\n", strip=True)
                        if len(content_text.split()) >= Constants.MIN_WORDS:
                            break
                
                # Fallback para body
                if not content_text or len(content_text.split()) < Constants.MIN_WORDS:
                    content_text = soup.get_text(separator="\n", strip=True)
                
                # Validar conteúdo
                word_count = len(content_text.split())
                if word_count < Constants.MIN_WORDS:
                    logger.debug(f"[Browserless-AMP] Conteúdo muito curto ({word_count} palavras)")
                    return None
                
                logger.info(f"[Browserless-AMP] Extraídas {word_count} palavras de {url}")
                return content_text
                
    except asyncio.TimeoutError:
        logger.warning(f"[Browserless-AMP] Timeout para {url}")
    except Exception as e:
        logger.debug(f"[Browserless-AMP] Falhou: {e}")
    
    return None

# ============================================================================
# SEÇÃO 11: SCRAPER ORCHESTRATOR
# ============================================================================
async def scrape_url_with_retry(
    url: str,
    valves: Valves
) -> ScrapedResult:
    """Scraper com 4 adapters e retry otimizado: PDF → httpx → jina → browserless"""
    
    # 0. Try Document (PDF) first
    if valves.enable_pdf_extraction:
        try:
            pdf_text = await scrape_pdf(
                url,
                valves.request_timeout,
                max_pages=Constants.MAX_PDF_PAGES
            )
            if pdf_text:
                cleaned = clean_text(pdf_text)
                truncated = truncate_content(cleaned, valves.max_content_length)
                
                return ScrapedResult(
                    url=url,
                    content=truncated,
                    word_count=len(truncated.split()),
                    adapter="document",
                    success=True,
                    metadata={"format": "pdf"}
                )
        except Exception as e:
            logger.debug(f"[Document] Falhou para {url}: {e}")
    
    # 1. Try HTTPX with smart retry (early exit on bot-wall)
    bot_wall_detected = False
    for attempt in range(valves.max_retries):
        try:
            result = await scrape_httpx(url, valves.request_timeout, attempt)
            if result:
                cleaned = clean_text(result)
                truncated = truncate_content(cleaned, valves.max_content_length)
                
                return ScrapedResult(
                    url=url,
                    content=truncated,
                    word_count=len(truncated.split()),
                    adapter="httpx",
                    success=True,
                    metadata={"attempts": attempt + 1}
                )
        except Exception as e:
            # Early exit se for bot-wall ou domínio problemático
            error_str = str(e).lower()
            if any(pattern in error_str for pattern in ["bot-wall", "403", "429", "999", "authwall"]):
                bot_wall_detected = True
                logger.debug(f"[HTTPX] Bot-wall detectado, pulando retry para {url}")
                break
                
            if attempt < valves.max_retries - 1:
                delay = Constants.RETRY_DELAYS[attempt]
                logger.debug(f"[HTTPX] Retry {attempt+1} em {delay}s para {url}")
                await asyncio.sleep(delay)
            else:
                logger.warning(f"[HTTPX] Todas tentativas falharam para {url}")
    
    # 2. Try Jina (skip se bot-wall foi detectado)
    if not bot_wall_detected:
        try:
            result = await scrape_jina(url, valves.jina_endpoint, valves.request_timeout)
            if result:
                cleaned = clean_text(result)
                truncated = truncate_content(cleaned, valves.max_content_length)
                
                return ScrapedResult(
                    url=url,
                    content=truncated,
                    word_count=len(truncated.split()),
                    adapter="jina",
                    success=True
                )
        except Exception as e:
            logger.warning(f"[Jina] Falhou para {url}: {e}")
    
    # 3. Try Browserless Function (if configured) - estratégia mais robusta
    if valves.browserless_endpoint:
        # Pular LinkedIn e sites conhecidos por terem authwall
        problematic_domains = ["linkedin.com", "crunchbase.com", "marketscreener.com"]
        domain = extract_domain(url)
        
        if any(prob_domain in domain for prob_domain in problematic_domains):
            logger.info(f"[Browserless] Pulando {url} - domínio problemático conhecido")
        else:
            logger.info(f"[Browserless-Function] Tentando {url} com endpoint {valves.browserless_endpoint}")
            try:
                result = await scrape_browserless_function(
                    url,
                    valves.browserless_endpoint,
                    valves.browserless_token,
                    valves.request_timeout
                )
                if result:
                    cleaned = clean_text(result)
                    truncated = truncate_content(cleaned, valves.max_content_length)
                    
                    return ScrapedResult(
                        url=url,
                        content=truncated,
                        word_count=len(truncated.split()),
                        adapter="browserless_function",
                        success=True
                    )
            except Exception as e:
                logger.warning(f"[Browserless-Function] Falhou para {url}: {e}")
    
    # 4. Try Browserless AMP (if configured) - estratégia otimizada para notícias
    if valves.browserless_endpoint:
        # Pular LinkedIn e sites conhecidos por terem authwall
        problematic_domains = ["linkedin.com", "crunchbase.com", "marketscreener.com"]
        domain = extract_domain(url)
        
        if any(prob_domain in domain for prob_domain in problematic_domains):
            logger.info(f"[Browserless] Pulando {url} - domínio problemático conhecido")
        else:
            logger.info(f"[Browserless-AMP] Tentando {url} com endpoint {valves.browserless_endpoint}")
            try:
                result = await scrape_browserless_amp(
                    url,
                    valves.browserless_endpoint,
                    valves.browserless_token,
                    valves.request_timeout
                )
                if result:
                    cleaned = clean_text(result)
                    truncated = truncate_content(cleaned, valves.max_content_length)
                    
                    return ScrapedResult(
                        url=url,
                        content=truncated,
                        word_count=len(truncated.split()),
                        adapter="browserless_amp",
                        success=True
                    )
            except Exception as e:
                logger.warning(f"[Browserless-AMP] Falhou para {url}: {e}")
    
    # Todos adapters falharam
    return ScrapedResult(
        url=url,
        success=False,
        error="Todos adapters falharam"
    )

async def scrape_batch(urls: List[str], valves: Valves) -> List[ScrapedResult]:
    """Scrape múltiplas URLs em paralelo com semáforo otimizado"""
    # Semáforo mais agressivo para melhor performance
    sem = asyncio.Semaphore(5)
    
    async def scrape_with_sem(url):
        async with sem:
            return await scrape_url_with_retry(url, valves)
    
    tasks = [scrape_with_sem(url) for url in urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Processar exceções
    processed = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"[Batch] Exceção para {urls[i]}: {result}")
            processed.append(ScrapedResult(
                url=urls[i],
                success=False,
                error=str(result)
            ))
        else:
            processed.append(result)
    
    return processed

# ============================================================================
# SEÇÃO 12: CONTENT CLEANER
# ============================================================================
class ContentCleaner:
    """Limpeza de conteúdo com sistema de candidatos"""
    
    def clean(self, html: str) -> str:
        """Aplica limpeza inteligente com fallback"""
        if not html or len(html) < 100:
            return ""
        
        # Se não parece HTML, apenas limpar texto
        if not self._looks_like_html(html):
            return clean_text(html)
        
        candidates = []
        
        # Candidato 1: Readability (early exit se funcionar)
        if READABILITY_AVAILABLE and Document:
            try:
                doc = Document(html)
                summary_html = doc.summary()
                soup = BeautifulSoup(summary_html, "html.parser")
                text = soup.get_text(separator='\n\n')
                
                if self._is_substantial(text):
                    logger.info(f"[Cleaner] Selecionado método: readability (score: {len(text.split())})")
                    return clean_text(text)  # Early exit
            except Exception as e:
                logger.debug(f"[Cleaner] Readability falhou: {e}")
        
        # Candidato 2: JSON-LD structured data
        try:
            soup = BeautifulSoup(html, "html.parser")
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    script_content = getattr(script, 'string', None)
                    if not script_content:
                        continue
                    data = json.loads(script_content)
                    article_body = data.get("articleBody") or data.get("text")
                    
                    if article_body and len(article_body.split()) >= 100:
                        candidates.append({
                            'text': article_body,
                            'score': len(article_body.split()) * 1.2,  # bonus
                            'method': 'jsonld'
                        })
                except json.JSONDecodeError:
                    pass
        except Exception as e:
            logger.debug(f"[Cleaner] JSON-LD falhou: {e}")
        
        # Candidato 3: BeautifulSoup seletores
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            # Remover elementos indesejados
            for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
                tag.decompose()
            
            # Tentar seletores comuns
            selectors = [
                'article',
                'main',
                '[role="main"]',
                '.content',
                '.article-content',
                '#content'
            ]
            
            for selector in selectors:
                element = soup.select_one(selector)
                if element:
                    text = element.get_text(separator='\n\n', strip=True)
                    if self._is_substantial(text):
                        candidates.append({
                            'text': text,
                            'score': len(text.split()) * 0.6,
                            'method': f'selector_{selector}'
                        })
                        break
            
            # Fallback: body inteiro
            if not any(c['method'].startswith('selector_') for c in candidates):
                text = soup.get_text(separator='\n\n', strip=True)
                if self._is_substantial(text):
                    candidates.append({
                        'text': text,
                        'score': len(text.split()) * 0.3,
                        'method': 'body_fallback'
                    })
                    
        except Exception as e:
            logger.debug(f"[Cleaner] BeautifulSoup falhou: {e}")
        
        # Selecionar melhor candidato
        if candidates:
            best = max(candidates, key=lambda x: x['score'])
            logger.info(f"[Cleaner] Selecionado método: {best['method']} (score: {int(best['score'])})")
            return clean_text(best['text'])
        
        # Fallback final: retornar HTML limpo como texto
        logger.warning("[Cleaner] Nenhum candidato válido, usando fallback")
        return clean_text(html)
    
    def _looks_like_html(self, content: str) -> bool:
        """Detecta se conteúdo é HTML"""
        if not content or len(content.strip()) < 10:
            return False
        
        content_lower = content.lower().strip()
        
        # DOCTYPE
        if content_lower.startswith('<!doctype'):
            return True
        
        # Tags HTML comuns
        html_indicators = ['<html', '<head', '<body', '<div', '<p>', '<span', '<article']
        return any(indicator in content_lower for indicator in html_indicators)
    
    def _is_substantial(self, text: str) -> bool:
        """Valida se texto é substancial"""
        if not text:
            return False
        
        words = len(text.split())
        paragraphs = len([p for p in text.split('\n\n') if p.strip()])
        
        return words >= 80 and paragraphs >= 1

# ============================================================================
# SEÇÃO 13: TOOL PRINCIPAL
# ============================================================================
class Tools:
    """Search & Scrape Coordinator Tool"""
    
    class Valves(Valves):
        pass
    
    def __init__(self):
        """Inicializa a tool"""
        self.valves = self.Valves()
        self.cleaner = ContentCleaner()
        self.citation = False  # não usar citações automáticas
        
        logger.info("[Tools] Search & Scrape Coordinator v2.1 inicializado")
    
    async def search_and_scrape(
        self,
        query: str,
        __event_emitter__: Optional[Callable] = None
    ) -> str:
        """
        Tool coordenada: busca + scrape em uma única chamada.
        
        Fluxo completo:
        1. LLM Planner gera queries otimizadas (SearXNG + Exa)
        2. Executa buscas em paralelo
        3. Zipper intercala e deduplica resultados
        4. LLM Selector escolhe top N URLs (curadoria semântica)
        5. Scraper processa top K URLs (PDF → httpx → jina → browserless)
        6. Content Cleaner limpa e estrutura conteúdo
        
        CRÍTICO PARA NOTÍCIAS:
        - Se o usuário pedir "notícias", "news", "atualizações", "últimas 24h", "hoje", etc.
        - SEMPRE inclua esses termos na query final para garantir busca de conteúdo recente
        - Exemplo: se usuário pede "Lula congresso últimas 24h" → query deve ser "Lula congresso notícias últimas 24h"
        - O sistema detecta automaticamente termos de temporalidade e adiciona "notícias" quando necessário
        
        PRESERVAÇÃO DE INSTRUÇÕES:
        - Se o usuário incluir termos como "priorize", "de prioridade", "foco em", "dê ênfase", etc.
        - PRESERVE esses termos integralmente na query final
        - Exemplo: "priorize notícias sobre Lula" → manter "priorize" na query
        - Exemplo: "dê prioridade a fontes oficiais" → manter "dê prioridade" na query
        - Esses termos são importantes para o contexto da busca e devem ser mantidos
        
        Args:
            query: Query de busca do usuário (termos de notícias serão preservados/amplificados)
            __event_emitter__: Event emitter para status updates em tempo real
        
        Returns:
            JSON string com estrutura:
            {
                "success": bool,
                "query": str,
                "plan": {
                    "searxng_queries": [str],
                    "exa_query": str,
                    "categories": [str]
                },
                "candidates_found": int,
                "urls_selected": int,
                "urls_scraped": int,
                "results": [
                    {
                        "url": str,
                        "content": str,
                        "word_count": int,
                        "adapter": str,
                        "success": bool,
                        "metadata": dict
                    }
                ],
                "stats": {
                    "total_time": float,
                    "successful_scrapes": int,
                    "failed_scrapes": int
                }
            }
        
        Examples:
            >>> # Para notícias recentes
            >>> result = await search_and_scrape(query="Lula congresso últimas 24h")
            >>> # Sistema automaticamente amplifica para: "Lula congresso notícias últimas 24h"
            
            >>> # Para conteúdo geral
            >>> result = await search_and_scrape(query="Petrobras M&A strategy 2024")
            >>> data = json.loads(result)
            >>> print(f"Encontrados {data['candidates_found']} URLs, scraped {data['urls_scraped']}")
        """
        start_time = time.time()
        
        try:
            # Fase 1: Planejamento
            await safe_emit(__event_emitter__, "🔍 Planejando busca...")
            plan = await plan_search(query, self.valves)
            
            logger.info(f"[Main] Plano: {len(plan.searxng_queries)} SearXNG queries, Exa: {plan.exa_query}")
            
            # Fase 2: Busca
            await safe_emit(__event_emitter__, "🌐 Executando buscas...")
            
            searxng_task = search_searxng(
                plan.searxng_queries,
                plan.searxng_categories,
                self.valves.searxng_endpoint,
                self.valves.searxng_timeout,
                plan.after,
                plan.before
            )
            
            exa_task = search_exa(
                plan.exa_query,
                plan.exa_use_news,
                self.valves.exa_api_key,
                self.valves.exa_timeout
            )
            
            searxng_results, exa_results = await asyncio.gather(
                searxng_task,
                exa_task,
                return_exceptions=True
            )
            
            # Tratar exceções com fallback
            if isinstance(searxng_results, Exception):
                logger.warning(f"[Main] SearXNG falhou: {searxng_results}")
                searxng_results = []  # Continue with Exa results
            
            if isinstance(exa_results, Exception):
                logger.warning(f"[Main] Exa falhou: {exa_results}")
                exa_results = []  # Continue with SearXNG results
            
            # Garantir que são listas
            if not isinstance(searxng_results, list):
                searxng_results = []
            if not isinstance(exa_results, list):
                exa_results = []
            
            # Early exit if no results from both engines
            if not searxng_results and not exa_results:
                raise ValueError("Ambos engines falharam - nenhum resultado encontrado")
            
            # Fase 3: Zipper
            candidates = zipper_results(searxng_results, exa_results)
            
            if not candidates:
                await safe_emit(__event_emitter__, "❌ Nenhum resultado encontrado", done=True)
                return json.dumps({
                    "success": False,
                    "error": "Nenhum candidato encontrado",
                    "query": query,
                    "plan": plan.dict()
                }, ensure_ascii=False, indent=2)
            
            logger.info(f"[Main] {len(candidates)} candidatos após zipper")
            
            # Early exit if we have <= top_n candidates (skip Selector)
            if len(candidates) <= self.valves.top_n_selector:
                logger.info(f"[Main] Early exit: {len(candidates)} candidatos <= {self.valves.top_n_selector}, pulando Selector")
                selected_indices = list(range(len(candidates)))
            else:
                # Fase 4: Seleção (only if needed)
                await safe_emit(
                    __event_emitter__,
                    f"🎯 Selecionando top {self.valves.top_n_selector} URLs..."
                )
                
                selected_indices = await select_urls(
                    candidates,
                    query,
                    self.valves.top_n_selector,
                    self.valves
                )
            
            logger.info(f"[Main] {len(selected_indices)} URLs selecionadas")
            
            # Fase 5: Scraping
            await safe_emit(
                __event_emitter__,
                f"📄 Scraping top {self.valves.top_k_scraper} URLs..."
            )
            
            urls_to_scrape = [
                candidates[i]["url"]
                for i in selected_indices[:self.valves.top_k_scraper]
                if i < len(candidates)
            ]
            
            if not urls_to_scrape:
                await safe_emit(__event_emitter__, "❌ Nenhuma URL selecionada", done=True)
                return json.dumps({
                    "success": False,
                    "error": "Nenhuma URL foi selecionada para scraping",
                    "query": query,
                    "candidates_found": len(candidates)
                }, ensure_ascii=False, indent=2)
            
            logger.info(f"[Main] Scraping {len(urls_to_scrape)} URLs")
            logger.info(f"[Main] Browserless configurado: {bool(self.valves.browserless_endpoint)}")
            if self.valves.browserless_endpoint:
                logger.info(f"[Main] Browserless endpoint: {self.valves.browserless_endpoint}")
            scraped = await scrape_batch(urls_to_scrape, self.valves)
            
            # Fase 6: Limpeza de conteúdo
            if self.valves.enable_content_cleaning:
                await safe_emit(__event_emitter__, "🧹 Limpando conteúdo...")
                
                for result in scraped:
                    if result.success and result.content:
                        try:
                            cleaned = self.cleaner.clean(result.content)
                            result.content = cleaned
                            result.word_count = len(cleaned.split())
                        except Exception as e:
                            logger.error(f"[Main] Erro ao limpar {result.url}: {e}")
            
            # Estatísticas
            successful = [r for r in scraped if r.success]
            failed = [r for r in scraped if not r.success]
            
            total_time = time.time() - start_time
            
            logger.info(
                f"[Main] Concluído: {len(successful)} sucessos, "
                f"{len(failed)} falhas em {total_time:.1f}s"
            )
            
            # Fase 7: Resultado final
            await safe_emit(__event_emitter__, "✅ Concluído!", done=True)
            
            return json.dumps({
                "success": True,
                "query": query,
                "plan": {
                    "searxng_queries": plan.searxng_queries,
                    "exa_query": plan.exa_query,
                    "categories": plan.searxng_categories,
                    "use_news": plan.exa_use_news,
                    "after": plan.after,
                    "before": plan.before
                },
                "candidates_found": len(candidates),
                "urls_selected": len(selected_indices),
                "urls_scraped": len(urls_to_scrape),
                "results": [r.dict() for r in scraped],
                "stats": {
                    "total_time": round(total_time, 2),
                    "successful_scrapes": len(successful),
                    "failed_scrapes": len(failed),
                    "adapters_used": list(set(r.adapter for r in successful))
                }
            }, ensure_ascii=False, indent=2)
            
        except Exception as e:
            logger.error(f"[Main] Erro crítico: {e}", exc_info=True)
            await safe_emit(__event_emitter__, f"❌ Erro: {str(e)}", done=True)
            
            return json.dumps({
                "success": False,
                "error": str(e),
                "query": query
            }, ensure_ascii=False, indent=2)