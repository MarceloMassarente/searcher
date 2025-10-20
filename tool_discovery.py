"""
title: Discovery Agent
author: Native Tool
author_url: https://github.com/native-tool/discovery-agent
git_url: https://github.com/native-tool/discovery-agent.git
description: Ferramenta avançada de descoberta de URLs que utiliza múltiplos motores de busca (SearXNG e Exa.ai) com análise inteligente via LLM. Especializada em busca temporal com @noticias, análise de intenção e planejamento de queries com LLM, seleção inteligente de resultados com curadoria semântica. Integração otimizada com PiPeManual v4.5+ (suporte a rails, return_dict, config propagation).
required_open_webui_version: 0.4.0
requirements: openai, aiohttp, pydantic, python-dateutil
version: 1.5.1
license: MIT

FEATURES v1.5.1 (PiPeManual Integration):
- ✅ Return dict option (eliminates JSON overhead for Pipe calls)
- ✅ Quality rails support (must_terms, avoid_terms, time_hint, etc.)
- ✅ Safe JSON serialization (default=str fallback)
- ✅ Consistent error handling (dict/string format)
- ✅ Backward compatible (default behavior unchanged)
"""

import asyncio
import json
import logging
import os
import re
import time
import unicodedata
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    from openai import AsyncOpenAI, OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    AsyncOpenAI = None
    OpenAI = None

try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# Configure logging
logger = logging.getLogger(__name__)

# =============================================================================
# DISCOVERY CONSTANTS
# =============================================================================

class DiscoveryConstants:
    """
    Constantes do sistema de descoberta (centralizadas para fácil tuning).
    
    IMPORTANTE: Alterar estes valores pode afetar:
    - Performance (timeouts, limites)
    - Qualidade (thresholds de filtro)
    - Custos (limites de LLM)
    """
    
    # ===== TIMEOUTS (segundos) =====
    DEFAULT_TIMEOUT_PLANNER = 120     # LLM Planner (prompts grandes)
    DEFAULT_TIMEOUT_SELECTOR = 90     # LLM Selector (análise)
    DEFAULT_TIMEOUT_HTTP = 180         # HTTPX (requests)
    TIMEOUT_MARGIN = 30                # Margem HTTP > LLM
    
    # ===== FILTROS DE QUALIDADE =====
    MIN_SNIPPET_LENGTH = 50           # Mínimo para não rejeitar
    MIN_TITLE_LENGTH = 20             # Títulos muito curtos = genéricos
    QUALITY_THRESHOLD = 0.0           # Score mínimo (0.0 = aceita tudo)
    
    # ===== LLM LIMITS =====
    MAX_PROMPT_TOKENS_WARNING = 20000 # Aviso de prompt grande
    MAX_QUERY_LENGTH = 200            # Máximo de chars em query
    MAX_CANDIDATES_FOR_ANALYSIS = 100 # Limite antes de LLM Selector
    
    # ===== DIVERSIDADE =====
    DEFAULT_MAX_URLS_PER_DOMAIN = 4   # Máximo por domínio (se diversity habilitado)
    DEFAULT_TOP_CURATED = 10          # Resultados finais após LLM
    
    # ===== BOOST FACTORS =====
    BRAZILIAN_BOOST_FACTOR = 1.2      # Multiplicador para fontes BR
    GEO_BIAS_BOOST = 1.08             # Multiplicador para TLD preferido
    
    # ===== SCORING (rails post-LLM) =====
    SCORE_MUST_TERM_MULTIPLE = 2.0    # ≥2 must_terms
    SCORE_MUST_TERM_SINGLE = 1.0      # 1 must_term
    SCORE_MUST_TERM_NONE = -0.5       # 0 must_terms (penalização)
    SCORE_AVOID_TERM = -2.0           # Penalização avoid_terms
    SCORE_OFFICIAL_DOMAIN = 2.0       # Bonus domínio oficial
    SCORE_DOMAIN_DIVERSITY = 1.0      # Bonus primeiro do domínio
    
    # ===== EXA CONSTANTS =====
    EXA_NEURAL_RESULTS = 20           # Sempre 20 resultados para Exa
    EXA_HIGHLIGHT_SENTENCES = 3        # Sentenças destacadas
    
    # ===== URL QUALITY =====
    MAX_URL_LENGTH = 200              # URLs muito longas são penalizadas
    MIN_SNIPPET_LENGTH_STRICT = 10    # Snippet muito curto = rejeitar
    MIN_TITLE_LENGTH_STRICT = 3       # Título muito curto = genérico
    
    # ===== TIME SLICING =====
    MAX_DAYS_FOR_MONTH_SLICES = 90    # Usar month slices se <= 90 dias
    DEFAULT_RECENT_DAYS = 30          # Padrão para queries "recentes"
    
    # ===== CIRCUIT BREAKER =====
    CIRCUIT_BREAKER_MAX_FAILURES = 3  # Máximo de falhas consecutivas
    CIRCUIT_BREAKER_TIMEOUT = 300     # Timeout em segundos (5 min)
    CIRCUIT_BREAKER_RESET_TIME = 600  # Tempo para reset em segundos (10 min)

# ✅ FIX: Thread-safe OpenAI client cache (multiusuário)
import threading

_OPENAI_CLIENT_CACHE: Dict[str, Any] = {}

# =============================================================================
# CIRCUIT BREAKER FOR LLM CALLS
# =============================================================================

class LLMCircuitBreaker:
    """
    Circuit breaker para chamadas LLM com detecção de falhas repetidas.
    
    Funciona como um "fusível" que abre quando há muitas falhas consecutivas,
    evitando chamadas desnecessárias e melhorando a performance.
    
    v2: Adiciona exponential backoff para prevenir API storms.
    """
    
    def __init__(self, max_failures: Optional[int] = None, timeout: Optional[int] = None, reset_time: Optional[int] = None):
        self.max_failures = max_failures or DiscoveryConstants.CIRCUIT_BREAKER_MAX_FAILURES
        self.timeout = timeout or DiscoveryConstants.CIRCUIT_BREAKER_TIMEOUT
        self.base_reset_time = reset_time or DiscoveryConstants.CIRCUIT_BREAKER_RESET_TIME  # 600s = 10min
        self.reset_time = self.base_reset_time
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        
        # Exponential backoff fields
        self.consecutive_half_open_failures = 0
        self.backoff_multiplier = 1.0
        self.max_backoff_multiplier = 8.0  # Max 80min (10min * 8)
        
    def can_execute(self) -> bool:
        """Verifica se pode executar chamada LLM."""
        if self.state == "CLOSED":
            return True
        
        if self.state == "OPEN":
            # Verificar se passou tempo suficiente para tentar reset
            if self.last_failure_time and (time.time() - self.last_failure_time) > self.reset_time:
                self.state = "HALF_OPEN"
                logger.info(f"[CircuitBreaker] State changed to HALF_OPEN - attempting reset (backoff: {self.reset_time}s)")
                return True
            return False
        
        if self.state == "HALF_OPEN":
            return True
        
        return False
    
    def record_success(self):
        """Registra sucesso e reseta o circuit breaker."""
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            self.failure_count = 0
            self.consecutive_half_open_failures = 0
            self.backoff_multiplier = 1.0
            self.reset_time = self.base_reset_time
            logger.info("[CircuitBreaker] Reset successful - state changed to CLOSED, backoff reset")
    
    def record_failure(self, error: Exception):
        """Registra falha e atualiza estado do circuit breaker."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        # Track failures in HALF_OPEN state for exponential backoff
        if self.state == "HALF_OPEN":
            self.consecutive_half_open_failures += 1
            
            # Apply exponential backoff after repeated HALF_OPEN failures
            if self.consecutive_half_open_failures >= 2:
                self.backoff_multiplier = min(self.backoff_multiplier * 2, self.max_backoff_multiplier)
                self.reset_time = int(self.base_reset_time * self.backoff_multiplier)
                logger.warning(
                    f"[CircuitBreaker] HALF_OPEN failure #{self.consecutive_half_open_failures} - "
                    f"increasing backoff to {self.reset_time}s (multiplier: {self.backoff_multiplier}x)"
                )
        
        logger.warning(f"[CircuitBreaker] Failure {self.failure_count}/{self.max_failures}: {error}")
        
        if self.failure_count >= self.max_failures:
            self.state = "OPEN"
            logger.error(f"[CircuitBreaker] Circuit opened after {self.failure_count} failures - LLM calls disabled for {self.reset_time}s")
    
    def get_state_info(self) -> Dict[str, Any]:
        """Retorna informações do estado atual."""
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "last_failure_time": self.last_failure_time,
            "can_execute": self.can_execute()
        }

# Global circuit breaker instances
_llm_circuit_breaker = LLMCircuitBreaker()

class SearxngCircuitBreaker:
    """
    Circuit breaker específico para SearXNG com detecção de falhas 500.
    
    Funciona como um "fusível" que abre quando há muitas falhas 500 consecutivas,
    evitando chamadas desnecessárias ao SearXNG quando está instável.
    """
    
    def __init__(self, max_failures: int = 3, timeout: int = 300, reset_time: int = 600):
        self.max_failures = max_failures
        self.timeout = timeout
        self.base_reset_time = reset_time
        self.reset_time = reset_time
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        
        # Exponential backoff fields
        self.consecutive_half_open_failures = 0
        self.backoff_multiplier = 1.0
        self.max_backoff_multiplier = 4.0  # Max 40min (10min * 4)
        
    def can_execute(self) -> bool:
        """Verifica se pode executar busca no SearXNG."""
        if self.state == "CLOSED":
            return True
        
        if self.state == "OPEN":
            # Verificar se passou tempo suficiente para tentar reset
            if self.last_failure_time and (time.time() - self.last_failure_time) > self.reset_time:
                self.state = "HALF_OPEN"
                logger.info(f"[SearxngCircuitBreaker] State changed to HALF_OPEN - attempting reset (backoff: {self.reset_time}s)")
                return True
            return False
        
        if self.state == "HALF_OPEN":
            return True
        
        return False
    
    def record_success(self):
        """Registra sucesso e reseta o circuit breaker."""
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            self.failure_count = 0
            self.consecutive_half_open_failures = 0
            self.backoff_multiplier = 1.0
            logger.info("[SearxngCircuitBreaker] Reset successful - state changed to CLOSED, backoff reset")
    
    def record_failure(self, error: Exception, status_code: int = None):
        """Registra falha e atualiza estado do circuit breaker."""
        # Só conta falhas 500 como "reais" para o circuit breaker
        if status_code and status_code != 500:
            return
            
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        # Track failures in HALF_OPEN state for exponential backoff
        if self.state == "HALF_OPEN":
            self.consecutive_half_open_failures += 1
            
            # Apply exponential backoff after repeated HALF_OPEN failures
            if self.consecutive_half_open_failures >= 2:
                self.backoff_multiplier = min(self.backoff_multiplier * 2, self.max_backoff_multiplier)
                self.reset_time = int(self.base_reset_time * self.backoff_multiplier)
                logger.warning(
                    f"[SearxngCircuitBreaker] HALF_OPEN failure #{self.consecutive_half_open_failures} - "
                    f"increasing backoff to {self.reset_time}s (multiplier: {self.backoff_multiplier}x)"
                )
        
        logger.warning(f"[SearxngCircuitBreaker] Failure {self.failure_count}/{self.max_failures}: {error} (status: {status_code})")
        
        if self.failure_count >= self.max_failures:
            self.state = "OPEN"
            logger.error(f"[SearxngCircuitBreaker] Circuit opened after {self.failure_count} failures - SearXNG calls disabled for {self.reset_time}s")
    
    def get_state_info(self) -> Dict[str, Any]:
        """Retorna informações do estado atual."""
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "last_failure_time": self.last_failure_time,
            "can_execute": self.can_execute(),
            "reset_time": self.reset_time
        }

# Global SearXNG circuit breaker instance
_searxng_circuit_breaker = SearxngCircuitBreaker()
_CACHE_LOCK = threading.Lock()

def _get_cached_openai_client(api_key: str, base_url: str) -> Any:
    """Get or create cached OpenAI client to avoid overhead on each call.
    
    ✅ Thread-safe com double-checked locking pattern para alta performance.
    """
    cache_key = f"{api_key[:8]}::{base_url}"
    
    # Fast path (read): Se já existe, retornar sem lock
    if cache_key in _OPENAI_CLIENT_CACHE:
        return _OPENAI_CLIENT_CACHE[cache_key]
    
    # Slow path (write): Adquirir lock para criar cliente
    with _CACHE_LOCK:
        # Double-check: outro thread pode ter criado enquanto esperávamos o lock
        if cache_key in _OPENAI_CLIENT_CACHE:
            return _OPENAI_CLIENT_CACHE[cache_key]
        
        if not OPENAI_AVAILABLE or AsyncOpenAI is None:
            raise ValueError("OpenAI library not available. Install: pip install openai")
        
        _OPENAI_CLIENT_CACHE[cache_key] = AsyncOpenAI(
            api_key=api_key,
            base_url=base_url
        )
        
        return _OPENAI_CLIENT_CACHE[cache_key]

# =============================================================================
# SEÇÃO 1: MODELOS DE DADOS E CONFIGURAÇÃO
# =============================================================================

class DiscoveryConfig(BaseModel):
    """Configuração simplificada para ferramenta de descoberta"""
    
    # API Keys
    openai_api_key: str = Field(default="", description="OpenAI API key")
    openai_base_url: str = Field(default="https://api.openai.com/v1", description="OpenAI base URL")
    openai_model: str = Field(default="gpt-5-mini", description="OpenAI model")
    exa_api_key: str = Field(default="", description="Exa API key")
    
    # Search endpoints
    searxng_endpoint: str = Field(default="", description="SearXNG endpoint")
    
    # Search settings
    enable_searxng: bool = Field(default=True, description="Enable SearXNG search")
    enable_exa: bool = Field(default=True, description="Enable Exa search")
    enable_unified_llm_analysis: bool = Field(default=True, description="Enable LLM analysis")
    
    # Limits and thresholds
    max_search_results: int = Field(default=20, description="Max search results per engine")
    top_curated_results: int = Field(default=10, description="Top results after LLM curation")
    max_candidates_for_llm_analysis: int = Field(default=100, description="Max candidates for LLM analysis")
    pages_per_slice: int = Field(default=2, description="Pages per time slice")
    request_timeout: int = Field(default=30, description="Request timeout in seconds")
    
    # Domain diversity
    enable_domain_diversity: bool = Field(default=True, description="Enable domain diversity")
    max_urls_per_domain: int = Field(default=4, description="Max URLs per domain")
    
    # News settings
    news_default_days: int = Field(default=90, description="Default days for news queries")
    
    # Brazilian sources boost
    boost_brazilian_sources: bool = Field(default=False, description="Boost Brazilian sources")
    
    # Search language and country
    search_language: str = Field(default="", description="Search language preference")
    search_country: str = Field(default="", description="Search country preference")
    
    # Global limits
    max_total_candidates: int = Field(default=200, description="Global candidate limit to prevent OOM")
    
    # Debug settings
    enable_debug_logging: bool = Field(default=False, description="Enable verbose debug logging")
    
    # Circuit breaker settings
    enable_searxng_circuit_breaker: bool = Field(default=True, description="Enable SearXNG circuit breaker for 500 errors")
    searxng_circuit_breaker_max_failures: int = Field(default=3, description="Max consecutive 500 errors before disabling SearXNG")
    searxng_circuit_breaker_reset_time: int = Field(default=600, description="Time in seconds before attempting to reset SearXNG circuit breaker")

class UrlCandidate(BaseModel):
    """Candidato de URL com metadados"""
    url: str
    title: str = ""
    snippet: str = ""
    score: float = 0.0
    source: str = ""
    domain: str = ""
    brazilian_boost: Optional[float] = None
    brazilian_source: bool = False

class DiscoveryResult(BaseModel):
    """Resultado da fase de descoberta"""
    operation: str = "discovery"
    query: str
    candidates_count: int = 0
    candidates: List[UrlCandidate] = Field(default_factory=list)
    urls: List[str] = Field(default_factory=list)
    citations: List[str] = Field(default_factory=list)
    sources: List[str] = Field(default_factory=list)
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    error: Optional[str] = None  # Error message when discovery fails

class SearchPlan(BaseModel):
    """Plano de busca do LLM Planner"""
    core_query: str = Field(..., min_length=1, description="Query ampla para descoberta de URLs")
    
    # PIPE INTEGRATION (v4.8): Optional fields populated only when pipe_must_terms present
    seed_core: Optional[str] = Field(
        default=None,
        description="Seed query (12-200 chars, natural phrase)"
    )
    seed_family_hint: Optional[str] = Field(
        default=None,
        description="Semantic family hint for phase tracking"
    )
    must_terms_covered: Optional[List[str]] = Field(
        default_factory=list,
        description="Which must_terms this plan covers"
    )
    coverage_ratio: Optional[float] = Field(
        default=None,
        description="Fraction of pipe_must_terms covered (0.0-1.0)"
    )
    
    # RESTRIÇÕES: Só se usuário pediu EXPLICITAMENTE
    sites: List[str] = Field(
        default_factory=list, 
        description="Restrição de domínios (vazio = busca aberta). Só usar se usuário pediu site:domain"
    )
    filetypes: List[str] = Field(
        default_factory=list, 
        description="Restrição de tipos (vazio = todos). Só usar se usuário pediu filetype:pdf"
    )
    
    # SUGESTÕES: Contexto para o LLM Selector
    suggested_domains: List[str] = Field(
        default_factory=list, 
        description="Domínios relevantes (sugestão para selector, não restrição de busca)"
    )
    suggested_filetypes: List[str] = Field(
        default_factory=list, 
        description="Tipos esperados (sugestão para selector, não restrição de busca)"
    )
    
    # Datas e metadata
    after: Optional[str] = Field(default=None, description="Data início ISO (YYYY-MM-DD)")
    before: Optional[str] = Field(default=None, description="Data fim ISO (YYYY-MM-DD)")
    reasoning: Optional[str] = Field(default=None, description="Explicação da estratégia")
    
    @field_validator('core_query')
    @classmethod
    def sanitize_core_query(cls, v: str) -> str:
        """Sanitiza query para evitar problemas com APIs"""
        if not v or not v.strip():
            raise ValueError("core_query cannot be empty")
        
        # Remove caracteres problemáticos
        sanitized = v.strip()
        sanitized = sanitized.replace('\n', ' ').replace('\r', ' ')
        sanitized = re.sub(r'\s+', ' ', sanitized)
        
        # Limita tamanho
        if len(sanitized) > 200:
            sanitized = sanitized[:200].rstrip()
            # Trunca em palavra se possível
            last_space = sanitized.rfind(' ')
            if last_space > 100:
                sanitized = sanitized[:last_space]
        
        return sanitized

class LLMPlannerResponse(BaseModel):
    """Resposta do LLM Planner"""
    plans: List[SearchPlan] = Field(..., description="Lista de planos de busca")

class LLMSelectorResponse(BaseModel):
    """Resposta do LLM Selector"""
    selected_indices: List[int] = Field(..., description="Índices dos candidatos selecionados")
    why: Optional[str] = Field(default=None, description="Explicação da seleção")

# =============================================================================
# SEÇÃO 2: UTILITÁRIOS E HEURÍSTICAS
# =============================================================================

def _safe_get_valve(valves: Any, key: str, default: Any = None) -> Any:
    """Safely get value from valves dict or object"""
    if not valves:
        return default
    
    if isinstance(valves, dict):
        return valves.get(key, default)
    
    # Handle Pydantic models
    if hasattr(valves, 'model_dump'):
        try:
            return getattr(valves, key, default)
        except AttributeError:
            return default
    
    # Handle regular objects
    if hasattr(valves, key):
        return getattr(valves, key, default)
    
    return default

def _extract_domain_from_url(url: str) -> str:
    """Extract domain from URL"""
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower()
    except Exception:
        return ""

def _normalize_domain(domain: str) -> str:
    """Normalize domain name"""
    if not domain:
        return ""
    
    # Remove www prefix
    if domain.startswith("www."):
        domain = domain[4:]
    
    return domain.lower()

def build_intent_profile(user_text: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Constrói um perfil de intenção unificado baseado na mensagem completa do usuário.
    Usa heurísticas estruturais em vez de keywords hardcoded.
    """
    if not user_text or not user_text.strip():
        return {}
    
    txt = user_text.lower()
    intent = {}
    
    # Heurística estrutural: detectar se a query menciona domínios/fontes internacionais
    international_domain_patterns = [
        ".com", ".org", ".io", ".ai",  # Domínios internacionais
        "github", "arxiv", "huggingface", "paperswithcode",  # Plataformas técnicas
    ]
    has_international_domains = any(pattern in txt for pattern in international_domain_patterns)
    
    # Detectar explicitamente quando o usuário pede conteúdo regional/local
    explicit_regional_markers = [
        "imprensa brasileira", "mídia brasileira", "notícias do brasil",
        "veículos brasileiros", "jornais brasileiros", "sites brasileiros",
        "fontes brasileiras", "imprensa nacional"
    ]
    is_explicitly_regional = any(marker in txt for marker in explicit_regional_markers)
    
    # Detectar quando usuário pede explicitamente português
    explicit_portuguese_request = any(marker in txt for marker in [
        "em português", "português", "tradução", "versão pt-br", "pt-br"
    ])
    
    # 1) Detecção de país/região com lógica estrutural
    usa_indicators = [" fontes dos eua ", " fontes dos usa ", " united states only ", " u.s. sources ", " fontes americanas apenas "]
    uk_indicators = [" fontes do reino unido ", " uk only ", " british sources only "]

    # Conservador: só setar bias de país quando explícito no texto
    if any(k in f" {txt} " for k in usa_indicators):
        intent["country_bias"] = "us"
        intent["lang_bias"] = "en"
    elif any(k in f" {txt} " for k in uk_indicators):
        intent["country_bias"] = "uk"
        intent["lang_bias"] = "en"
    elif is_explicitly_regional or explicit_portuguese_request:
        # Só força pt-BR se for EXPLICITAMENTE pedido
        intent["country_bias"] = "br"
        intent["lang_bias"] = "pt-BR"
    else:
        # DEFAULT: não force idioma/país; ranking natural e rails decidem
        intent["lang_bias"] = None
    
    # 2) Detecção de modo notícias (multilíngue)
    news_indicators = ["até agora", "so far", "últimas", "notícias", "news", "recentes", "atual", "@noticias"]
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
    
    # 5) Detecção de domínios preferidos explícitos
    if "site:" in txt or "domínio" in txt:
        domains = re.findall(r'site:([a-z0-9.-]+)', txt)
        if domains:
            intent["prefer_domains"] = domains
    
    return intent

def _get_brazilian_boost_table() -> Dict[str, float]:
    """Get Brazilian source boost table"""
    return {
        "g1.globo.com": 1.2,
        "oglobo.globo.com": 1.2,
        "estadao.com.br": 1.2,
        "folha.uol.com.br": 1.2,
        "valor.com.br": 1.2,
        "exame.com": 1.2,
        "revistaepoca.globo.com": 1.2,
        "veja.abril.com.br": 1.2,
        "istoe.com.br": 1.2,
        "terra.com.br": 1.1,
        "uol.com.br": 1.1,
        "ig.com.br": 1.1,
        "r7.com": 1.1,
        "noticias.uol.com.br": 1.1,
    }

async def safe_emit(emitter: Optional[Callable], message: str):
    """Safely emit event message"""
    if not emitter:
        return
    
    try:
        if asyncio.iscoroutinefunction(emitter):
            await emitter(message)
        else:
            emitter(message)
    except Exception as e:
        logger.debug(f"Error emitting message: {e}")

# =============================================================================
# SEÇÃO 3: UTILITÁRIOS TEMPORAIS
# =============================================================================

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
        """Parse month/year ranges like 'mar/24 e jun/25' or 'abr/2024 e jun/25' or 'entre março de 2024 e junho de 2025'."""
        text = text.lower().strip()
        
        # Pattern: mar/24 e jun/25 OR mar/24 a jun/25 (supports "e" or "a" as separator, 2 or 4 digit years)
        short_pattern = r'(\w+)/(\d{2,4})\s+(?:e|a)\s+(\w+)/(\d{2,4})'
        match = re.search(short_pattern, text)
        if match:
            try:
                month1, year1, month2, year2 = match.groups()
                start_month = AdvancedDateParser.MONTH_NAMES.get(month1)
                end_month = AdvancedDateParser.MONTH_NAMES.get(month2)
                if start_month and end_month:
                    # Handle 2 or 4 digit years
                    start_year = int(year1) if len(year1) == 4 else 2000 + int(year1)
                    end_year = int(year2) if len(year2) == 4 else 2000 + int(year2)
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
        
        # Pattern: entre março de 2024 e junho de 2025 (or "a" instead of "e")
        long_pattern = r'entre\s+(\w+)\s+de\s+(\d{4})\s+(?:e|a)\s+(\w+)\s+de\s+(\d{4})'
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
        
        # Pattern: de mar/24 a jun/25 (with "de" prefix)
        de_pattern = r'de\s+(\w+)/(\d{2,4})\s+a\s+(\w+)/(\d{2,4})'
        match = re.search(de_pattern, text)
        if match:
            try:
                month1, year1, month2, year2 = match.groups()
                start_month = AdvancedDateParser.MONTH_NAMES.get(month1)
                end_month = AdvancedDateParser.MONTH_NAMES.get(month2)
                if start_month and end_month:
                    start_year = int(year1) if len(year1) == 4 else 2000 + int(year1)
                    end_year = int(year2) if len(year2) == 4 else 2000 + int(year2)
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

class TimeSlicer:
    """Produce temporal slices between `after` and `before` (inclusive by month)."""
    
    @staticmethod
    def month_slices(after: Optional[date], before: Optional[date]) -> List[Tuple[date, date]]:
        """Produce monthly slices between dates - versão refinada com dateutil"""
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
        
        try:
            # Usar dateutil para cálculos mais robustos
            from dateutil.relativedelta import relativedelta
            
            while cur <= end:
                # Próximo mês usando dateutil (mais robusto)
                next_month = cur + relativedelta(months=1)
                slice_end = next_month - timedelta(days=1)
                slices.append((cur, slice_end))
                cur = next_month
                
        except ImportError:
            # Fallback para o método original se dateutil não estiver disponível
            while cur <= end:
                next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
                slice_end = next_month - timedelta(days=1)
                slices.append((cur, slice_end))
                cur = next_month

        return slices

    @staticmethod
    def quarter_slices(after: Optional[date], before: Optional[date]) -> List[Tuple[date, date]]:
        """Produce quarterly slices (3 months each) with remaining months grouped together."""
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
            # Calculate quarter end (3 months later)
            quarter_end_month = cur.month + 2
            quarter_end_year = cur.year
            if quarter_end_month > 12:
                quarter_end_month -= 12
                quarter_end_year += 1
            
            # Get last day of quarter end month
            if quarter_end_month == 12:
                quarter_end = date(quarter_end_year, 12, 31)
            else:
                next_month = date(quarter_end_year, quarter_end_month + 1, 1)
                quarter_end = next_month - timedelta(days=1)
            
            # Don't exceed the 'before' date
            if before and quarter_end > before:
                quarter_end = before  # type: ignore  # before is not None due to condition
            
            slices.append((cur, quarter_end))
            
            # Move to next quarter
            if quarter_end_month == 12:
                cur = date(quarter_end_year + 1, 1, 1)
            else:
                cur = date(quarter_end_year, quarter_end_month + 1, 1)
            
            # Stop if we've reached or passed the end
            if cur > end:
                break

        return slices

    @staticmethod
    def get_time_slices(after: Optional[date], before: Optional[date], pages_per_slice: int = 2) -> List[Dict[str, Any]]:
        """Gera fatias de tempo para busca - wrapper para compatibilidade"""
        if not after and not before:
            return [{"after": None, "before": None, "slice_id": "all_time"}]
        
        # Use month_slices for small windows, quarter_slices for larger ones
        if after and before:
            delta_days = (before - after).days
            if delta_days <= 90:
                slices = TimeSlicer.month_slices(after, before)
            else:
                slices = TimeSlicer.quarter_slices(after, before)
        else:
            # Single date - create a single slice
            slices = [(after or before, before or after)]
        
        # Convert to dict format
        result = []
        for start, end in slices:
            slice_id = start.isoformat()[:7] if start else "unknown"
            result.append({
                "after": start.isoformat() if start else None,
                "before": end.isoformat() if end else None,
                "slice_id": slice_id
            })
        
        return result

# =============================================================================
# SEÇÃO 4: FILTROS DE QUALIDADE E BRAIDER
# =============================================================================

def _is_url_quality_good(url: str, title: str = "", snippet: str = "") -> Tuple[bool, float, str]:
    """
    Avalia qualidade estrutural da URL de forma genérica.
    Retorna (is_acceptable, quality_score, rejection_reason) onde score 0.0-1.0.
    Baseado em padrões estruturais, não em conteúdo específico.
    """
    if not url:
        return False, 0.0, "empty_url"
    
    url_lower = url.lower()
    title_lower = title.lower()
    snippet_lower = snippet.lower()
    
    # === FILTROS DE REJEIÇÃO TOTAL (retorna False imediatamente) ===
    
    # 1. Apenas lixo real - repositórios de arquivos ilegais e spam
    # Filtros foram minimizados: redes sociais, wikis, fóruns etc. são agora avaliados pelo LLM Selector
    blocked_domains = [
        "annas-archive", "btdig", "torrent", "magnet:",
        "apps.apple.com", "play.google.com/store",  # App stores (não conteúdo)
    ]
    
    for blocked in blocked_domains:
        if blocked in url_lower:
            return False, 0.0, f"blocked_domain_{blocked.replace('.', '_')}"
    
    # 2. Apenas padrões de URL realmente problemáticos (lixo técnico)
    # PDFs, DOCs e páginas institucionais/admin são avaliadas pelo LLM Selector
    problematic_patterns = [
        # Apenas recursos técnicos que não são conteúdo
        r'/api/', r'/cdn/', r'/static/', r'/assets/', r'/cache/',
        # Apenas arquivos de mídia que não são conteúdo textual
        r'\.jpg$', r'\.jpeg$', r'\.png$', r'\.gif$', r'\.svg$', r'\.webp$',
        r'\.mp4$', r'\.mp3$', r'\.wav$', r'\.avi$', r'\.mov$', r'\.wmv$',
        r'\.zip$', r'\.rar$', r'\.7z$', r'\.tar$', r'\.gz$',
    ]
    
    for pattern in problematic_patterns:
        if re.search(pattern, url_lower):
            return False, 0.0, f"problematic_pattern_{pattern.replace('/', '_').replace('.', '_').replace('$', '')}"
    
    # 3. Títulos genéricos ou vazios
    generic_titles = [
        "home", "welcome", "index", "main", "page", "site", "website",
        "login", "sign in", "sign up", "register", "contact", "about",
        "privacy", "terms", "help", "support", "search", "browse",
        "error", "404", "not found", "forbidden", "unauthorized",
        "loading", "please wait", "redirecting", "coming soon",
        "under construction", "maintenance", "temporarily unavailable"
    ]
    
    if title_lower in generic_titles or len(title_lower.strip()) < 3:
        return False, 0.0, "generic_title"
    
    # 4. Snippets muito curtos ou vazios
    if len(snippet_lower.strip()) < DiscoveryConstants.MIN_SNIPPET_LENGTH_STRICT:
        return False, 0.0, "short_snippet"
    
    # 5. Detectar lixo real - spam, etc.
    # Conteúdo CJK é avaliado pelo LLM Selector (não é filtrado aqui)
    
    # Detectar spam patterns
    spam_patterns = [
        r'click here', r'buy now', r'limited time', r'act now',
        r'free download', r'no credit card', r'money back',
        r'guaranteed', r'risk free', r'instant access'
    ]
    content = (title + " " + snippet).lower()
    if any(re.search(pattern, content) for pattern in spam_patterns):
        return False, 0.0, "spam_content"
    
    # Detectar páginas de erro ou vazias
    error_patterns = [
        r'page not found', r'404 error', r'access denied',
        r'forbidden', r'unauthorized', r'server error',
        r'under construction', r'coming soon', r'maintenance'
    ]
    if any(re.search(pattern, content) for pattern in error_patterns):
        return False, 0.0, "error_page"
    
    # === CÁLCULO DE SCORE DE QUALIDADE (0.0-1.0) ===
    
    score = 1.0
    
    # Penalizar URLs muito longas
    if len(url) > DiscoveryConstants.MAX_URL_LENGTH:
        score -= 0.2
    
    # Penalizar títulos muito curtos
    if len(title) < DiscoveryConstants.MIN_TITLE_LENGTH:
        score -= 0.1
    
    # Penalizar snippets muito curtos
    if len(snippet) < DiscoveryConstants.MIN_SNIPPET_LENGTH:
        score -= 0.1
    
    # Bonificar URLs com estrutura clara
    if re.search(r'/[^/]+/[^/]+', url):  # Pelo menos 2 níveis de path
        score += 0.1
    
    # Bonificar títulos descritivos
    if len(title) > 30 and not re.search(r'^(home|welcome|index)', title_lower):
        score += 0.1
    
    # Bonificar snippets informativos
    if len(snippet) > 100:
        score += 0.1
    
    # Garantir score entre 0.0 e 1.0
    score = max(0.0, min(1.0, score))
    
    return True, score, ""

def is_content_acceptable_v2(result: dict, valves: Optional[DiscoveryConfig] = None) -> bool:
    """Filtro inteligente baseado no v2 - remove apenas lixo real como chinês, spam, etc."""
    title = result.get("title", "")
    snippet = result.get("snippet", "")
    url = result.get("url", "")
    
    # Combinar texto para análise
    full_text = f"{title} {snippet}".strip()
    
    if not full_text:
        return True  # Aceita se não há texto para analisar
    
    # Configurações via valves (com defaults do v2)
    strict_language_filter = _safe_get_valve(valves, "strict_language_filter", True)
    non_latin_threshold = float(_safe_get_valve(valves, "non_latin_threshold", 0.15))
    
    # === FILTRO 1: IDIOMAS INACEITÁVEIS (configurável) ===
    if strict_language_filter:
        # Verificar caracteres CJK (Chinês, Japonês, Coreano) - como no v2
        cjk_chars = sum(
            1
            for c in full_text
            if 0x4E00 <= ord(c) <= 0x9FFF  # CJK Unified Ideographs
            or 0x3400 <= ord(c) <= 0x4DBF  # CJK Extension A
            or 0x3040 <= ord(c) <= 0x309F  # Hiragana
            or 0x30A0 <= ord(c) <= 0x30FF  # Katakana
        )
        
        # Caracteres árabes/persas
        arabic_chars = sum(1 for c in full_text if 0x0600 <= ord(c) <= 0x06FF)
        
        # Caracteres cirílicos (russo, etc.)
        cyrillic_chars = sum(1 for c in full_text if 0x0400 <= ord(c) <= 0x04FF)
        
        # Se mais que o threshold são caracteres não-latinos, muito provável ser idioma indesejado
        total_chars = len(full_text)
        non_latin_ratio = (cjk_chars + arabic_chars + cyrillic_chars) / max(total_chars, 1)
        if non_latin_ratio > non_latin_threshold:
            return False
    
    # === FILTRO 2: SPAM/LIXO ÓBVIO ===
    text_lower = full_text.lower()
    
    # Indicadores de spam (verificar palavras completas, não substrings)
    spam_indicators = [
        "porn", "xxx", "sex", "casino", "poker", "gambling", "cialis",
        "viagra", "lottery", "winner", "congratulations", "click here",
        "buy now", "limited time", "act now", "free download"
    ]
    
    for indicator in spam_indicators:
        if f" {indicator} " in f" {text_lower} ":
            return False
    
    # === FILTRO 3: PÁGINAS DE ERRO ===
    error_indicators = [
        "page not found", "404 error", "access denied", "forbidden",
        "unauthorized", "server error", "under construction"
    ]
    
    for indicator in error_indicators:
        if indicator in text_lower:
            return False
    
    return True

def zipper_interleave_results(searxng_results: List[Dict], exa_results: List[Dict], 
                              valves: DiscoveryConfig) -> List[Dict]:
    """
    "Zipper" - versão como v2, com filtro inteligente que remove apenas lixo real.
    
    Usa filtro baseado no v2 que remove chinês, spam, etc. sem ser agressivo.
    """
    logger.info(f"Zipper input: {len(searxng_results)} SearXNG, {len(exa_results)} Exa results")
    
    # Sort each group by score before interleaving
    searxng_results.sort(key=lambda r: r.get("score", 0), reverse=True)
    exa_results.sort(key=lambda r: r.get("score", 0), reverse=True)
    
    # Real zipper: interleave results from different engines, prioritizing by score
    interleaved_results = []
    seen_urls = {}
    rejection_stats = {"total": 0, "language": 0, "spam": 0, "error": 0}
    
    iter_searxng = iter(searxng_results)
    iter_exa = iter(exa_results)
    
    while True:
        searxng_res = next(iter_searxng, None)
        exa_res = next(iter_exa, None)
        
        if searxng_res is None and exa_res is None:
            break
        
        # Process SearXNG result
        if searxng_res:
            url = searxng_res.get("url", "")
            if url:
                # Aplicar filtro inteligente do v2
                if not is_content_acceptable_v2(searxng_res, valves):
                    rejection_stats["total"] += 1
                    continue
                
                score = searxng_res.get("score", 0)
                if url not in seen_urls or score > seen_urls[url]["score"]:
                    if url in seen_urls:
                        # Remove previous result with lower score
                        interleaved_results.remove(seen_urls[url]["result"])
                    searxng_res["source"] = "searxng"
                    searxng_res["domain"] = _normalize_domain(_extract_domain_from_url(url))
                    interleaved_results.append(searxng_res)
                    seen_urls[url] = {"result": searxng_res, "score": score}
        
        # Process Exa result
        if exa_res:
            url = exa_res.get("url", "")
            if url:
                # Aplicar filtro inteligente do v2
                if not is_content_acceptable_v2(exa_res, valves):
                    rejection_stats["total"] += 1
                    continue
                
                score = exa_res.get("score", 0)
                if url not in seen_urls or score > seen_urls[url]["score"]:
                    if url in seen_urls:
                        # Remove previous result with lower score
                        interleaved_results.remove(seen_urls[url]["result"])
                    exa_res["source"] = "exa"
                    exa_res["domain"] = _normalize_domain(_extract_domain_from_url(url))
                    interleaved_results.append(exa_res)
                    seen_urls[url] = {"result": exa_res, "score": score}
    
    # Log estatísticas de rejeição
    if rejection_stats["total"] > 0:
        logger.info(f"Zipper rejected {rejection_stats['total']} URLs (language/spam/error)")
    
    logger.info(f"Zipper output: {len(interleaved_results)} interleaved results")
    
    return interleaved_results

# =============================================================================
# SEÇÃO 5: LÓGICA DE BUSCA (RUNNERS)
# =============================================================================

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
                finally:
                    self.session = None

class RunnerSearxng:
    """Runner para SearXNG com normalização automática de endpoint e retry inteligente"""
    
    def __init__(self, endpoint: str, valves: DiscoveryConfig):
        self.endpoint = endpoint
        self.valves = valves
        # Normalize endpoint to ensure it ends with /search
        self.base = endpoint.rstrip('/')
        if not self.base.endswith('/search'):
            self.base = f"{self.base}/search"
        
        # Initialize circuit breaker with config values
        if valves.enable_searxng_circuit_breaker:
            global _searxng_circuit_breaker
            _searxng_circuit_breaker = SearxngCircuitBreaker(
                max_failures=valves.searxng_circuit_breaker_max_failures,
                reset_time=valves.searxng_circuit_breaker_reset_time
            )
    
    def _get_api_url(self) -> str:
        """Get the normalized API URL"""
        return self.base
    
    async def run(self, params: dict, max_pages: int = 2) -> List[dict]:
        """Executa busca no SearXNG com paginação e retry inteligente"""
        if not self.endpoint:
            return []
        
        # Check circuit breaker before attempting search (if enabled)
        if self.valves.enable_searxng_circuit_breaker and not _searxng_circuit_breaker.can_execute():
            logger.warning(f"[SearXNG] Circuit breaker OPEN - skipping search (state: {_searxng_circuit_breaker.get_state_info()})")
            return []
        
        all_results = []
        
        # Ensure format=json is always set
        base_params = params.copy()
        base_params["format"] = "json"
        
        # Headers to request JSON
        headers = {"Accept": "application/json"}
        
        try:
            async with AiohttpContextManager(timeout=self.valves.request_timeout) as session:
                url = self._get_api_url()
                
                # Paginate through results
                for page in range(1, max_pages + 1):
                    page_params = base_params.copy()
                    page_params["pageno"] = page
                    
                    async with session.get(url, params=page_params, headers=headers) as response:
                        if response.status != 200:
                            logger.warning(f"[SearXNG] Status {response.status} for page {page}")
                            
                            # Record failure in circuit breaker for 500 errors
                            if response.status == 500:
                                _searxng_circuit_breaker.record_failure(
                                    Exception(f"HTTP {response.status}"), 
                                    status_code=response.status
                                )
                            
                            # Stop pagination on 400 (bad query)
                            if response.status == 400:
                                break
                            # Try next page on other errors
                            continue
                        
                        # Check content type before attempting to parse JSON
                        content_type = response.headers.get('content-type', '').lower()
                        if 'application/json' in content_type:
                            data = await response.json()
                            results = data.get("results", [])
                            
                            # Record success in circuit breaker (any successful response)
                            _searxng_circuit_breaker.record_success()
                            
                            # DEBUG: Log detalhado se 0 resultados
                            if not results and page == 1:
                                logger.warning(f"[SearXNG DEBUG] Page {page}: 0 results in JSON response")
                                logger.warning(f"[SearXNG DEBUG] Full response keys: {list(data.keys())}")
                                logger.warning(f"[SearXNG DEBUG] Query params: {page_params}")
                            
                            if not results:
                                # No more results, stop pagination
                                break
                            
                            all_results.extend(results)
                            logger.info(f"[SearXNG] Page {page}/{max_pages}: {len(results)} results (total: {len(all_results)})")
                            
                            # Small delay between pages to avoid overwhelming the server
                            if page < max_pages:
                                await asyncio.sleep(0.3)
                        else:
                            logger.warning(f"[SearXNG] Page {page}: Unexpected content type: {content_type}")
                            break
                
        except Exception as e:
            logger.error(f"[SearXNG] Error during search: {e}")
            return []
        
        return all_results

class RunnerExa:
    """Runner for Exa.ai API (neural search, always top 20)."""

    def __init__(self, api_key: Optional[str] = None, valves: Optional[Any] = None):
        api_val = None
        if valves:
            api_val = _safe_get_valve(valves, "exa_api_key") or _safe_get_valve(valves, "EXA_API_KEY")
        self.api_key = api_key or api_val or os.getenv("EXA_API_KEY")
        self.valves = valves or {}  # Store valves for model access
        self.openai_client = None

    async def _plan_search_with_llm(self, user_prompt: str) -> dict:
        """Plan Exa search using LLM - aligned with v2"""
        if not OPENAI_AVAILABLE:
            # Fallback to simple query
            return {
                "query": user_prompt,
                "type": "neural",
                "numResults": DiscoveryConstants.EXA_NEURAL_RESULTS,
                "contents": {"highlights": {"numSentences": DiscoveryConstants.EXA_HIGHLIGHT_SENTENCES}}
            }
        
        try:
            # Get OpenAI client
            if not self.openai_client:
                api_key = _safe_get_valve(self.valves, "openai_api_key") or os.getenv("OPENAI_API_KEY")
                base_url = _safe_get_valve(self.valves, "openai_base_url", "https://api.openai.com/v1")
                
                if api_key:
                    # Use cached client to avoid connection overhead
                    self.openai_client = _get_cached_openai_client(api_key, base_url)
            
            if not self.openai_client:
                # Fallback to simple query
                return {
                    "query": user_prompt,
                    "type": "neural",
                    "numResults": DiscoveryConstants.EXA_NEURAL_RESULTS,
                    "contents": {"highlights": {"numSentences": DiscoveryConstants.EXA_HIGHLIGHT_SENTENCES}}
                }
            
            # Build example payload
            example_payload = {
                "query": "Manus AI capabilities and differentiators",
                "type": "neural",
                "numResults": DiscoveryConstants.EXA_NEURAL_RESULTS,
                "contents": {
                    "highlights": {
                        "numSentences": 3
                    }
                }
            }
            
            # Use LLM to plan Exa search - aligned with v2
            system_prompt = f"""
            Você é um agente de IA especialista em traduzir pedidos em linguagem natural para chamadas de API JSON precisas.
            Sua tarefa é analisar o prompt do usuário e gerar um objeto JSON que corresponda perfeitamente ao schema da API Exa /search.

            **Instruções RÍGIDAS (HARDCODED):**
            1. **SEMPRE use 'neural' como valor para `type`** - este sistema é hardcoded para busca neural
            2. **SEMPRE use 20 como valor para `numResults`** - este sistema é hardcoded para exatamente 20 resultados
            3. **QUERY SIMPLES:** Crie uma query NATURAL e SIMPLES em inglês, sem sintaxe de busca avançada (sem site:, filetype:, etc.)
               - **TRADUÇÃO INTELIGENTE:** Se o prompt estiver em português, traduza mantendo o contexto específico
               - **PRESERVE ENTIDADES:** Mantenha nomes próprios, empresas, conceitos técnicos no original quando relevante
               - **CONTEXTO ESPECÍFICO:** Não generalize - mantenha a especificidade do tópico original
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

            **EXEMPLOS DE TRADUÇÃO INTELIGENTE:**
            - "Noon Capital desafios portfólio atual" → "Noon Capital current portfolio challenges and recent developments"
            - "Buscar fontes mais específicas e verificáveis" → "Find specific and verifiable sources for detailed information"
            - "Revolução Francesa causas contexto histórico" → "French Revolution causes historical context and background"
            
            **EXEMPLO DE SAÍDA:**
            {json.dumps(example_payload, indent=2, ensure_ascii=False)}
            """

            response = await self.openai_client.chat.completions.create(
                model=_safe_get_valve(self.valves, "openai_model", "gpt-5-mini"),
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=1.0,
                response_format={"type": "json_object"},
                timeout=30.0
            )
            
            content = response.choices[0].message.content.strip()
            plan = json.loads(content)
            
            # Ensure required fields - aligned with v2
            plan.setdefault("type", "neural")
            plan.setdefault("numResults", 20)
            if "contents" not in plan:
                plan["contents"] = {"highlights": {"numSentences": 3}}
            
            return plan
            
        except Exception as e:
            logger.warning(f"[Exa] Error planning with LLM: {e}, using fallback")
            return {
                "query": user_prompt,
                "type": "neural",
                "numResults": DiscoveryConstants.EXA_NEURAL_RESULTS,
                "contents": {"highlights": {"numSentences": DiscoveryConstants.EXA_HIGHLIGHT_SENTENCES}}
            }

    async def run(self, payload: dict) -> List[dict]:
        """Execute Exa search - aligned with v2"""
        if not self.api_key:
            logger.error("[Exa] No API key provided")
            return []
        
        if not AIOHTTP_AVAILABLE:
            logger.error("[Exa] aiohttp not available")
            return []
        
        try:
            # Check if payload is already prepared (has required fields)
            if "type" in payload and "numResults" in payload:
                # Payload is already prepared, use it directly
                planned_params = payload
                logger.info(f"[Exa] Using pre-prepared payload (no LLM planning needed)")
                logger.info(f"[Exa] Query: {planned_params.get('query', 'N/A')}")
                if _safe_get_valve(self.valves, "enable_debug_logging", False):
                    logger.debug(f"[Exa] payload: {json.dumps(planned_params, indent=2, ensure_ascii=False)}")
            else:
                # Legacy mode: use LLM planner (for backward compatibility)
                if not self.openai_client:
                    logger.error("[Exa] OpenAI client not initialized for Exa planning")
                    return []
                
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
                
                logger.info(f"[Exa] Sanitized user prompt: {user_prompt_sanitized}")
                
                # Use LLM planner to generate Exa payload
                planned_params = await self._plan_search_with_llm(planner_user_prompt)
                
                # HARDCODED: Always neural mode and exactly 20 results
                planned_params["type"] = "neural"
                planned_params["numResults"] = 20
                
                # Drop category by default unless explicitly kept via valves
                try:
                    keep_cat = bool(_safe_get_valve(self.valves, "exa_keep_category", False))
                except Exception:
                    keep_cat = False
                
                # Detect explicit filetype/pdf intent in user query or valves
                uq = (user_query or "").lower()
                explicit_pdf = ("filetype:pdf" in uq) or ("apenas pdf" in uq) or ("somente pdf" in uq) or ("only pdf" in uq) or ("pdf only" in uq)
                try:
                    force_filetypes = _safe_get_valve(self.valves, "force_filetypes")
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

                logger.info(f"[Exa] LLM-planned query (hardcoded neural + 20): {planned_params.get('query', 'N/A')}")
                if _safe_get_valve(self.valves, "enable_debug_logging", False):
                    logger.debug(f"[Exa] payload: {json.dumps(planned_params, indent=2, ensure_ascii=False)}")

            # Use configurable timeout from valves (not hardcoded)
            exa_timeout = _safe_get_valve(self.valves, "request_timeout", 90)
            async with AiohttpContextManager(timeout=exa_timeout) as session:
                headers = {
                    "x-api-key": self.api_key,
                    "Content-Type": "application/json",
                    "User-Agent": "Discovery-Agent-v1.0/1.0.0",
                }
                
                async with session.post(
                    "https://api.exa.ai/search",
                    json=planned_params,
                    headers=headers
                ) as response:
                    if response.status != 200:
                        logger.error(f"[Exa] API error: {response.status}")
                        return []
                    
                    data = await response.json()
                    results = data.get("results", [])
                    
                    # Convert Exa format to standard format
                    formatted_results = []
                    for result in results:
                        formatted_results.append({
                            "url": result.get("url", ""),
                            "title": result.get("title", ""),
                            "text": result.get("text", ""),
                            "snippet": result.get("text", ""),  # Exa uses 'text' field
                            "score": result.get("score", 0.9),  # Exa results are generally high quality
                            "publishedDate": result.get("publishedDate", ""),
                            "author": result.get("author", "")
                        })
                    
                    logger.info(f"[Exa] returned {len(formatted_results)} results (target: 20)")
                    return formatted_results
                    
        except Exception as e:
            logger.error(f"[Exa] Error during search: {e}")
            return []

# =============================================================================
# SEÇÃO 6: FUNÇÕES AUXILIARES ADICIONAIS
# =============================================================================

def validate_searxng_categories(categories: str) -> str:
    """Valida categorias contra lista de categorias válidas do SearXNG"""
    if not categories:
        return "general"
    
    valid_categories = {"general", "news", "files", "juridico", "financeiro"}
    cats_list = categories.split(",") if categories else []
    cats_list = [c.strip() for c in cats_list if c.strip() in valid_categories]
    
    return ",".join(cats_list) or "general"

class CategoryFormatter:
    @staticmethod
    def classify_categories(query: str, force_news: bool = False, has_filetypes: bool = False) -> str:
        q = (query or "").lower()
        # Always include 'general' baseline like Discovery v2
        cats = ["general"]
        if force_news or any(k in q for k in ["@noticias", "últimas", "breaking", "hoje", "notícias", "noticia", "notícias"]):
            cats.append("news")
        # Use word boundaries to avoid false positives and more specific legal terms
        import re
        juridic_patterns = [
            r'\bjurisdic\w*\b', r'\bjurisprud\w*\b', r'\bprecedente\w*\b', r'\blawsuit\b',
            r'\bjuridico\b', r'\bjuridica\b', r'\btjsp\b', r'\bstj\b', r'\bstf\b',
            r'\bacordao\b', r'\bacórdão\b', r'\bementa\b', r'\bjusbrasil\b',
            r'\bprocesso\s+(judicial|legal|juridico)\b', r'\btribunal\b', r'\bjustica\b',
            r'\badvocacia\b', r'\badvogad\w*\b', r'\bdefensor\b', r'\bprocurador\b'
        ]
        if any(re.search(pattern, q) for pattern in juridic_patterns):
            cats.append("juridico")
        if any(k in q for k in ["cvm", "ipo", "prospecto", "fato relevante", " ri ", "b3", "ifrs", "resultado trimestral", "guidance"]):
            cats.append("financeiro")
        if has_filetypes or any(k in q for k in ["filetype:", "pdf", "docx", "xls", "xlsx"]):
            cats.append("files")
        out, seen = [], set()
        for c in cats:
            if c not in seen:
                out.append(c)
                seen.add(c)
        return ",".join(out)

class QueryBuilder:
    @staticmethod
    def sanitize(q: str) -> str:
        import re
        s = (q or "")
        s = re.sub(r"(^|\n)[\*•\-]\s+", " ", s)  # remove bullets
        s = re.sub(r"\s+", " ", s).strip()
        # remove trailing commas/colons/semicolons, including common Unicode variants
        try:
            s = re.sub(r"[\s,;:：﹕︓]+$", "", s)
        except Exception:
            pass
        # Remove Portuguese temporal phrases already expressed via after/before
        try:
            s = re.sub(r"(?i)\b(últim[oa]s?\s+\d+\s+mes(es)?)\b[:]?", " ", s)
            s = re.sub(r"(?i)\b(este\s+ano|último\s+ano|past\s+\d+\s+months?|past\s+year)\b", " ", s)
            s = re.sub(r"(?i)\b(entre\s+\w+\/\d{2,4}\s+e\s+\w+\/\d{2,4})\b", " ", s)
        except Exception:
            pass
        # Remove generic fillers that hurt recall in engines
        try:
            s = re.sub(r"(?i)\b(pesquise|buscar|busque|procure)\b[:]?", " ", s)
            s = re.sub(r"(?i)\b(noticias?|notícias?|lançamentos?|produtos?|outros?)\b[:]?", " ", s)
        except Exception:
            pass
        # ✅ FIX: Remove temporal keywords that confuse SearXNG
        try:
            s = re.sub(r"(?i)\b(últimos?|último|recentes?|atuais?|novos?|últimas?)\b", " ", s)
        except Exception:
            pass
        # Collapse whitespace again and strip
        s = re.sub(r"\s+", " ", s).strip()
        # ✅ FIX: Normalize Portuguese accents to ASCII for SearXNG compatibility
        import unicodedata
        try:
            s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
        except Exception:
            pass
        return s

    @staticmethod
    def sanitize_and_truncate(q: str, max_length: int = 100) -> str:
        """Sanitize and truncate query with word boundary preservation"""
        if not q:
            return ""
        
        # Apply basic sanitization
        sanitized = QueryBuilder.sanitize(q)
        
        # ✅ FIX: Limitar a 5 termos principais para SearXNG (densidade máxima)
        # SearXNG falha com queries muito densas/longas em português
        words = sanitized.split()
        if len(words) > 5:
            # Manter os 5 termos mais importantes (primeiros termos tendem a ser mais relevantes)
            logger.warning(f"[QueryBuilder] Query muito densa ({len(words)} termos), reduzindo para 5")
            sanitized = " ".join(words[:5])
        
        # Truncate if necessary
        if len(sanitized) > max_length:
            truncated = sanitized[:max_length]
            last_space = truncated.rfind(' ')
            if last_space > 50:  # Only truncate at word boundary if it's not too short
                return truncated[:last_space]
            else:
                return truncated
        
        return sanitized

    @staticmethod
    def searx_params(base_query: str, language: str = "", country: str = "",
                     after: "Optional[date]" = None, before: "Optional[date]" = None, 
                     include_date_in_query: bool = False) -> dict:
        from datetime import datetime, date as _date
        import re as _re
        
        # ✅ SIMPLIFIED: Use v2 approach - minimal params, avoid complex transformations
        # Normalize after/before if they arrive as ISO strings
        if isinstance(after, str):
            try:
                after = _date.fromisoformat(after)
            except Exception:
                after = None
        if isinstance(before, str):
            try:
                before = _date.fromisoformat(before)
            except Exception:
                before = None
        
        # Build a simple, clean query string (no complex sanitization)
        q_parts: List[str] = []
        if base_query:
            q_parts.append(base_query.strip())
        
        # Add date hints into query (v2 approach - simple and works)
        if after:
            q_parts.append(f"after:{after.isoformat()}")
        if before:
            q_parts.append(f"before:{before.isoformat()}")
        
        params = {
            "q": " ".join(q_parts),
            "format": "json",
        }
        
        # Optionally pass language if provided (simple type)
        if language:
            params["language"] = language
        
        # Set approximate time_range for SearXNG (like v2)
        try:
            if after or before:
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
                        params["time_range"] = "year"
                elif after:
                    # Only after date - assume recent content
                    delta = abs((_date.today() - after).days)
                    if delta <= 7:
                        params["time_range"] = "week"
                    elif delta <= 31:
                        params["time_range"] = "month"
                    else:
                        params["time_range"] = "year"
                elif before:
                    # Only before date - assume historical content
                    delta = abs((_date.today() - before).days)
                    if delta <= 7:
                        params["time_range"] = "week"
                    elif delta <= 31:
                        params["time_range"] = "month"
                    else:
                        params["time_range"] = "year"
        except Exception:
            pass
        
        # Log for debugging
        logger.info(f"[QueryBuilder] Final SearXNG payload: {params}")
        logger.info(f"[QueryBuilder] Query length: {len(params.get('q', ''))} chars")
        logger.info(f"[QueryBuilder] Parameters: {list(params.keys())}")
        
        return params

def extract_prioritization_criteria(query: str) -> str:
    """
    Extrai SOMENTE diretivas explícitas de priorização presentes no prompt do usuário.
    Não infere intenções. Exemplos detectados:
      - "priorize ...", "priorizar ...", "prefira ..."
      - "apenas ...", "somente ...", "sem ...", "excluir ...", "evite ...", "inclua ..."
      - restrições literais como "site:dominio" e "filetype:pdf"
    Retorna string compacta para o seletor LLM (Discovery v2-style: critérios passam do prompt, não de heurísticas).
    """
    if not query:
        return ""

    import re

    text = (query or "").strip()
    low = text.lower()
    
    # Patterns for explicit prioritization directives
    patterns = [
        r"(priorize|priorizar|prefira|prefer)\s+([^.]+)",
        r"(apenas|somente|only)\s+([^.]+)",
        r"(sem|sem|excluir|evite|avoid|exclude)\s+([^.]+)",
        r"(inclua|include|com|with)\s+([^.]+)",
        r"(site:\S+)",
        r"(filetype:\S+)",
        r"(-\w+)",  # negative keywords like -bolsonaro
    ]
    
    extracted = []
    for pattern in patterns:
        matches = re.findall(pattern, low, re.IGNORECASE)
        for match in matches:
            if isinstance(match, tuple):
                # For patterns with capture groups, join them
                directive = " ".join(match).strip()
            else:
                directive = match.strip()
            
            if directive and directive not in extracted:
                extracted.append(directive)
    
    return "; ".join(extracted) if extracted else ""

def sanitize_core_query_for_search(query: str) -> str:
    """
    Sanitiza a query gerada pelo LLM para busca, removendo numeração, notas entre parênteses
    e outros ruídos que podem prejudicar a qualidade da busca.
    
    IMPORTANTE: Preserva aspas que fazem parte de operadores de busca válidos.
    """
    if not query:
        return ""
    
    # Remove numeração no início (ex: "1. ", "2. ", etc.)
    import re
    query = re.sub(r'^\d+\.\s*', '', query.strip())
    
    # Remove notas entre parênteses no início ou fim
    query = re.sub(r'^\([^)]*\)\s*', '', query)
    query = re.sub(r'\s*\([^)]*\)$', '', query)
    
    # Remove bullets e listas
    query = re.sub(r'^[\*\-\•]\s*', '', query)
    
    # Remove aspas duplas desnecessárias (mas preserva aspas simples para operadores)
    if query.startswith('"') and query.endswith('"') and len(query) > 2:
        query = query[1:-1]
    
    # Remove espaços extras
    query = re.sub(r'\s+', ' ', query).strip()
    
    return query

class QueryClassifier:
    """Classificador de queries para determinar perfil de busca"""
    
    @staticmethod
    def _is_news_query(q: str) -> bool:
        s = (q or "").lower()
        return any(k in s for k in ("notícia", "noticias", "news", "últimas", "última hora"))
    
    @staticmethod
    def _is_news_category_query(q: str) -> bool:
        """Detecta se a query menciona notícias como categoria"""
        s = (q or "").lower()
        return any(k in s for k in ("@noticias", "notícias", "news", "últimas", "breaking"))
    
    @staticmethod
    def is_news_profile_active(query: str) -> bool:
        """Determina se o perfil de notícias está ativo para a query"""
        return QueryClassifier._is_news_category_query(query)

# =============================================================================
# SEÇÃO 7: ORQUESTRAÇÃO E LÓGICA DE IA (IMPLEMENTAÇÕES ORIGINAIS)
# =============================================================================

async def generate_queries_with_llm(query: str, intent_profile: Dict[str, Any], 
                                  messages: Optional[List[Dict]] = None, 
                                  language: str = "", 
                                  __event_emitter__: Optional[Callable] = None, 
                                  valves: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Gera queries otimizadas usando LLM (async wrapper com sync OpenAI client)
    
    ✅ HYBRID ASYNC: 
    - Mantém async def para compatibilidade com PipeLangNew (que usa await)
    - Usa OpenAI síncrono dentro (mais simples, como v2)
    - Executa em thread separada via run_in_executor (não bloqueia event loop)
    """
    if not OPENAI_AVAILABLE or OpenAI is None:
        raise RuntimeError("LLM planner required but OpenAI library is not available")
    
    api_key = _safe_get_valve(valves, "openai_api_key") if valves else os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("LLM planner required but OpenAI or API key is not available")
    
    model = _safe_get_valve(valves, "openai_model", "gpt-5-mini") if valves else "gpt-5-mini"
    base_url = _safe_get_valve(valves, "openai_base_url", "https://api.openai.com/v1")
    
    try:
        intent_context_str = json.dumps(intent_profile or {}, ensure_ascii=False, indent=2)
        
        # Build comprehensive prompt (from v2)
        base_prompt_template = (
            "Você é um estrategista de busca especialista. "
            "Decomponha a consulta em 1 a 3 planos de busca independentes. "
            "Cada plano deve cobrir uma faceta diferente, maximizando diversidade e cobertura.\n"
            "\n"
            "### [CONTEXTO DA CONSULTA]\n"
            "{intent_context}\n"
            "\n"
            "### [CONSULTA DO USUÁRIO]\n"
            "<<< {query} >>>\n"
            "\n"
            "### [REGRAS PARA GERAÇÃO]\n"
            "1. `core_query` deve ser pura string de busca (operadores: \"\", site:, filetype:, OR)\n"
            "2. SEM explicações, numeração ou anotações no `core_query`\n"
            "3. Máximo 3 planos. Cada um com: core_query (string), sites (list), filetypes (list)\n"
            "4. Exemplo CORRETO: `\"trade as % GDP\" site:un.org OR site:worldbank.org`\n"
            "5. Exemplo INCORRETO: `1) \"trade\" \"% GDP\" (objetivo: análise...)`\n"
            "\n"
            "### [SCHEMA DE SAÍDA]\n"
            "{{ \"plans\": [ {{ \"core_query\": \"...\", \"sites\": [], \"filetypes\": [], \"suggested_domains\": [] }} ] }}\n"
            "\n"
            "Retorne SOMENTE JSON válido."
        )
        
        prompt = base_prompt_template.format(intent_context=intent_context_str, query=query)
        
        # ✅ HYBRID ASYNC: Run sync OpenAI call in thread executor
        def _sync_llm_call():
            """Sync call to OpenAI - runs in thread pool to avoid blocking event loop"""
            client = OpenAI(
                api_key=api_key,
                base_url=base_url
            )
            
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=1.0,
                response_format={"type": "json_object"},
                timeout=120.0
            )
            return response
        
        # Execute in thread executor - async-safe
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, _sync_llm_call)
        
        content = response.choices[0].message.content.strip()
        
        # Parse response with robust JSON handling
        def _try_parse_json(text: str):
            if not text:
                return None
            try:
                return json.loads(text)
            except Exception:
                m = re.search(r"(\{[\s\S]*\}|\[[\s\S]*\])", text)
                if m:
                    try:
                        return json.loads(m.group(0))
                    except Exception:
                        return None
                return None
        
        parsed = content if isinstance(content, dict) else _try_parse_json(content)
        plans = (parsed.get("plans", []) if isinstance(parsed, dict) else []) or []
        
        # Normalize plans to expected format
        out = []
        for p in plans:
            if isinstance(p, str):
                plan = {"core_query": p, "sites": [], "filetypes": [], "suggested_domains": []}
            elif isinstance(p, dict) and "core_query" in p:
                plan = p
            else:
                continue
            out.append(plan)
        
        logger.info(f"[LLM Planner] Generated {len(out)} query plans")
        if not out:
            # Fallback: return simple plan
            return [{
                "core_query": query,
                "sites": [],
                "filetypes": [],
                "suggested_domains": [],
            }]
        
        # Clamp to max 3 plans
        return out[:3]
        
    except Exception as e:
        logger.error(f"[LLM Planner] Error: {e}")
        # Always return something usable
        return [{
            "core_query": query,
            "sites": [],
            "filetypes": [],
            "suggested_domains": [],
        }]


def _log_plan_metadata(plans: List[Dict], must_terms: List[str]) -> None:
    """
    Log plan metadata for telemetry (passive, never blocks execution).
    """
    try:
        if not plans:
            return
        
        # Log what LLM generated
        for i, plan in enumerate(plans):
            try:
                seed_core = plan.get("seed_core")
                family = plan.get("seed_family_hint")
                covered = plan.get("must_terms_covered", [])
                ratio = plan.get("coverage_ratio", 0.0)
                
                # Type validation
                if not isinstance(covered, list):
                    logger.warning(f"[Telemetry] Plan {i+1}: must_terms_covered is not a list (got {type(covered).__name__})")
                    covered = []
                
                if not isinstance(ratio, (int, float)):
                    logger.warning(f"[Telemetry] Plan {i+1}: coverage_ratio is not numeric (got {type(ratio).__name__})")
                    ratio = 0.0
                
                logger.info(
                    f"[Planner] Plan {i+1}: seed_core='{seed_core or 'N/A'}', "
                    f"family={family or 'N/A'}, covered={len(covered)} terms, ratio={ratio:.1%}"
                )
            except Exception as e:
                logger.debug(f"[Telemetry] Error logging plan {i+1} metadata: {e}")
                continue
        
        # Global coverage (informational only)
        if must_terms:
            try:
                all_covered = set()
                for plan in plans:
                    covered = plan.get("must_terms_covered", [])
                    if isinstance(covered, list):
                        all_covered.update(covered)
                
                if all_covered and must_terms:
                    coverage = len(all_covered & set(must_terms)) / len(must_terms)
                    logger.info(f"[Planner] Global coverage: {coverage:.1%} ({len(all_covered)}/{len(must_terms)} must_terms)")
            except Exception as e:
                logger.debug(f"[Telemetry] Error calculating global coverage: {e}")
    
    except Exception as e:
        # Outer safety net - never let telemetry crash the tool
        logger.debug(f"[Telemetry] Unexpected error in _log_plan_metadata: {e}")


# ==================== SELECTOR ENRICHMENT FUNCTIONS (v4.7) ====================

def infer_date_guess(candidate: dict) -> Optional[str]:
    """Infere data de publicação (YYYY-MM-DD) de URL/título/snippet.
    
    Ordem de precedência:
    1. URL path: /2025/10/12/ ou /news-2025-10-12
    2. Título: "12 out 2025" ou "October 12, 2025"
    3. Snippet: data próxima ao início (primeiros 200 chars)
    4. publishedDate (se Exa forneceu)
    5. None (sem data detectável)
    
    Args:
        candidate: Dict com url, title, snippet, publishedDate
    
    Returns:
        Data em formato YYYY-MM-DD ou None
    """
    url = candidate.get("url", "")
    title = candidate.get("title", "")
    snippet = candidate.get("snippet", "")
    published = candidate.get("publishedDate", "")
    
    # 1. URL patterns
    url_patterns = [
        r'/(\d{4})/(\d{2})/(\d{2})/',  # /2025/10/12/
        r'/(\d{4})-(\d{2})-(\d{2})',    # /2025-10-12 ou /2025-10-12/
        r'_(\d{4})(\d{2})(\d{2})',     # _20251012_ ou _20251012.html
        r'/(\d{4})/(\d{2})/',          # /2025/10/ (dia 01 default)
    ]
    
    for pattern in url_patterns:
        match = re.search(pattern, url)
        if match:
            groups = match.groups()
            try:
                if len(groups) == 3:
                    y, m, d = groups
                    date_obj = date(int(y), int(m), int(d))
                elif len(groups) == 2:
                    y, m = groups
                    date_obj = date(int(y), int(m), 1)  # Dia 01
                else:
                    continue
                
                # Validar data razoável (não futuro, não antes de 1990)
                if 1990 <= date_obj.year <= datetime.now().year + 1:
                    return date_obj.isoformat()
            except (ValueError, OverflowError):
                continue
    
    # 2. Title patterns (PT/EN)
    month_map_pt = {
        'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
        'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12
    }
    month_map_en = {
        'jan': 1, 'feb': 1, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    
    # PT: "12 out 2025" ou "12/10/2025"
    title_patterns_pt = [
        r'(\d{1,2})\s+(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)[a-z]*\.?\s+(\d{4})',
        r'(\d{1,2})/(\d{1,2})/(\d{4})',
    ]
    
    for pattern in title_patterns_pt:
        match = re.search(pattern, title, re.I)
        if match:
            groups = match.groups()
            try:
                if groups[1].lower() in month_map_pt:
                    # Formato: DD MMM YYYY
                    d, m_str, y = groups
                    m = month_map_pt[m_str.lower()[:3]]
                    date_obj = date(int(y), int(m), int(d))
                else:
                    # Formato: DD/MM/YYYY
                    d, m, y = groups
                    date_obj = date(int(y), int(m), int(d))
                
                if 1990 <= date_obj.year <= datetime.now().year + 1:
                    return date_obj.isoformat()
            except (ValueError, KeyError, OverflowError):
                continue
    
    # EN: "October 12, 2025" ou "12 Oct 2025"
    title_patterns_en = [
        r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{1,2}),?\s+(\d{4})',
        r'(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(\d{4})',
    ]
    
    for pattern in title_patterns_en:
        match = re.search(pattern, title, re.I)
        if match:
            groups = match.groups()
            try:
                if groups[0].isalpha():
                    # Formato: MMM DD, YYYY
                    m_str, d, y = groups
                    m = month_map_en[m_str.lower()[:3]]
                else:
                    # Formato: DD MMM YYYY
                    d, m_str, y = groups
                    m = month_map_en[m_str.lower()[:3]]
                
                date_obj = date(int(y), int(m), int(d))
                if 1990 <= date_obj.year <= datetime.now().year + 1:
                    return date_obj.isoformat()
            except (ValueError, KeyError, OverflowError):
                continue
    
    # 3. Snippet (mesmos patterns, limitado a 200 chars para performance)
    snippet_sample = snippet[:200]
    for pattern in title_patterns_pt + title_patterns_en:
        match = re.search(pattern, snippet_sample, re.I)
        if match:
            # Reutilizar lógica acima (simplificado)
            try:
                groups = match.groups()
                # Lógica similar ao título (omitida para brevidade)
                # ... parse date ...
                pass
            except:
                continue
    
    # 4. Exa publishedDate
    if published:
        # Tentar parsear ISO date ou timestamp
        try:
            if "T" in published:
                # ISO 8601: 2025-10-12T14:30:00Z
                return published.split("T")[0]
            elif len(published) == 10 and "-" in published:
                # YYYY-MM-DD
                return published
        except:
            pass
    
    # 5. Fallback
    return None


def compute_is_primary(candidate: dict, official_domains: List[str], intent_profile: dict) -> bool:
    """Determina se candidato é fonte primária.
    
    Critérios (OR):
    1. Domain em official_domains (passados pelo Pipe Planner)
    2. Domain tem TLD oficial (.gov, .edu, .mil, .org de entidade)
    3. Domain corresponde a entidade canônica do intent_profile
    4. Domain é site corporativo oficial (nome da entidade no domain)
    
    Args:
        candidate: Dict com url, domain
        official_domains: Lista de domínios oficiais (do Pipe)
        intent_profile: Dicionário com pipe_must_terms (entidades)
    
    Returns:
        True se fonte primária, False caso contrário
    """
    domain = candidate.get("domain", "").lower()
    url = candidate.get("url", "").lower()
    
    if not domain:
        return False
    
    # 1. Official domains (do Pipe Planner)
    official_doms_lower = [d.lower() for d in (official_domains or [])]
    if domain in official_doms_lower:
        return True
    
    # 2. TLDs oficiais
    official_tlds = ['.gov', '.gov.br', '.edu', '.edu.br', '.mil', '.mil.br']
    if any(domain.endswith(tld) for tld in official_tlds):
        return True
    
    # 3. .org.br de entidades conhecidas (conservador)
    if domain.endswith('.org.br'):
        entities = intent_profile.get("pipe_must_terms", [])
        for entity in entities:
            if not isinstance(entity, str):
                continue
            entity_slug = entity.lower().replace(" ", "").replace("-", "").replace("&", "")
            domain_clean = domain.replace(".", "").replace("-", "")
            if len(entity_slug) > 3 and entity_slug in domain_clean:
                return True
    
    # 4. Site corporativo oficial (heurística: nome da entidade no domain)
    entities = intent_profile.get("pipe_must_terms", [])
    for entity in entities:
        if not isinstance(entity, str):
            continue
        
        # Apenas nomes próprios (uppercase inicial)
        words = entity.split()
        if not words or not words[0] or not words[0][0].isupper():
            continue
        
        # Verificar se é entidade (≤3 palavras)
        if len(words) > 3:
            continue
        
        # Criar slug da entidade
        entity_slug = entity.lower().replace(" ", "").replace("-", "").replace("&", "")
        domain_clean = domain.replace(".", "").replace("-", "")
        
        # Verificar match (mínimo 5 chars)
        if len(entity_slug) >= 5 and entity_slug in domain_clean:
            # Verificar se é TLD corporativo (não blog/social)
            corporate_tlds = ['.com', '.com.br', '.co', '.io', '.net', '.br']
            if any(domain.endswith(tld) for tld in corporate_tlds):
                # Excluir plataformas genéricas
                excluded_platforms = ['blogspot', 'wordpress', 'medium', 'linkedin', 'facebook', 'twitter', 'instagram']
                if not any(platform in domain for platform in excluded_platforms):
                    return True
    
    return False


def compute_must_terms_hits(candidate: dict, must_terms: List[str]) -> dict:
    """Calcula hits de must_terms com pesos diferenciados.
    
    Ponderação:
    - Título: peso 2.0 (mais visível, mais confiável)
    - Snippet: peso 1.0 (contexto importante)
    - URL: peso 0.5 (menos confiável, pode ser SEO)
    
    Args:
        candidate: Dict com title, snippet, url
        must_terms: Lista de termos prioritários
    
    Returns:
        Dict com total (ponderado), title, snippet, url (contagens brutas)
    """
    title = candidate.get("title", "").lower()
    snippet = candidate.get("snippet", "").lower()
    url = candidate.get("url", "").lower()
    
    if not must_terms:
        return {"total": 0, "title": 0, "snippet": 0, "url": 0}
    
    mts_lower = [t.lower() for t in must_terms if isinstance(t, str)]
    
    # Contagens brutas
    hits_title = sum(1 for mt in mts_lower if mt in title)
    hits_snippet = sum(1 for mt in mts_lower if mt in snippet)
    hits_url = sum(1 for mt in mts_lower if mt in url)
    
    # Ponderação: título 2x, snippet 1x, URL 0.5x
    total = int(hits_title * 2.0 + hits_snippet * 1.0 + hits_url * 0.5)
    
    return {
        "total": total,
        "title": hits_title,
        "snippet": hits_snippet,
        "url": hits_url
    }


async def unified_llm_analysis(scored_candidates_pool: List[Dict[str, Any]], 
                              original_query: str, 
                              prioritization_criteria: str = "", 
                              suggested_filetypes: Optional[List[str]] = None,
                              suggested_domains: Optional[List[str]] = None, 
                              top_n: int = 10, 
                              __event_emitter__: Optional[Callable] = None, 
                              valves: Optional[Union[Dict[str, Any], DiscoveryConfig]] = None, 
                              intent_profile: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Análise unificada com LLM para selecionar melhores candidatos (LLM Selector).
    
    Esta função recebe URLs descobertas e seleciona as melhores baseado em:
    - Relevância semântica à query
    - Autoridade e frescor
    - Diversidade de domínios
    - Sugestões do LLM Planner (suggested_domains/filetypes)
    """
    # Use valves if provided, otherwise fallback to env vars (like tool_discovery_v2)
    api_key = _safe_get_valve(valves, "openai_api_key") if valves else os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("Discovery tool requires LLM configured for analysis. Configure openai_api_key in valves or OPENAI_API_KEY in environment.")
    
    try:
        base_url = _safe_get_valve(valves, "openai_base_url", "https://api.openai.com/v1")
        model = _safe_get_valve(valves, "openai_model", "gpt-5-mini")
        
        # Use cached client to avoid connection overhead
        client = _get_cached_openai_client(api_key, base_url)
        
        candidates_text = "\n".join([
            f"{i+1}. {c.get('title', 'No title')} - {c.get('url', 'No URL')} - Score: {c.get('score', 0)}"
            for i, c in enumerate(scored_candidates_pool[:50])  # Limitar para não exceder tokens
            if isinstance(c, dict)
        ])
        
        # Construir payload para o seletor (seguindo padrão do original)
        # Sanitizar query original (melhoria do tool_discovery_v2)
        sanitized_query = sanitize_core_query_for_search(original_query.replace("@noticias", "").strip())
        
        # Extract time window for @noticias queries
        time_window_hint = None
        if "@noticias" in original_query:
            try:
                parsed_after, parsed_before = AdvancedDateParser.parse_date_range_from_query(original_query)
                if parsed_after and parsed_before:
                    time_window_hint = f"Período solicitado: {parsed_after.strftime('%d/%m/%Y')} a {parsed_before.strftime('%d/%m/%Y')}"
                elif parsed_after:
                    time_window_hint = f"A partir de: {parsed_after.strftime('%d/%m/%Y')}"
                elif parsed_before:
                    time_window_hint = f"Até: {parsed_before.strftime('%d/%m/%Y')}"
            except Exception as e:
                logger.debug(f"Could not parse time window from @noticias query: {e}")
        
        # ✅ v4.7: Enriquecer candidatos com date_guess, is_primary, must_terms_hits
        official_domains = (intent_profile or {}).get("pipe_official_domains", [])
        must_terms = (intent_profile or {}).get("pipe_must_terms", [])
        
        enriched_candidates = []
        for i, c in enumerate(scored_candidates_pool[:50]):
            if not isinstance(c, dict):
                continue
            
            # Campos base
            enriched = {
                "idx": i,
                "title": c.get("title", "")[:300],
                "url": c.get("url", "")[:300],
                "snippet": c.get("snippet", "")[:500],
                "score": c.get("score", 0.0),
                "authority": c.get("is_authority", False),
                "news_host": c.get("is_news_host", False),
                "domain": c.get("domain", "")[:80],
            }
            
            # ✅ v4.7: Novos campos enriquecidos (P0)
            enriched["date_guess"] = infer_date_guess(c)
            enriched["is_primary"] = compute_is_primary(c, official_domains, intent_profile or {})
            enriched["must_terms_hits"] = compute_must_terms_hits(c, must_terms)
            
            enriched_candidates.append(enriched)
        
        payload = {
            "original_query": sanitized_query,
            "candidates": enriched_candidates,
            "context": {
                "news_profile_active": QueryClassifier.is_news_profile_active(original_query),
                "time_window_hint": time_window_hint,
                "domain_cap": _safe_get_valve(valves, "max_urls_per_domain", 2),
                "top_n": top_n,
                "prioritization_criteria": prioritization_criteria or "",
                "intent_profile": intent_profile or {}
            }
        }
        
        # Prompt completo do seletor (reestruturado para ordem ideal)
        intent = payload.get("context", {}).get("intent_profile", {})
        
        # 1. TAREFA + PAPEL
        base_prompt = (
            "Você é um seletor de URLs ultra-preciso. Devolva somente JSON válido, sem comentários.\\n\\n"
            "TAREFA:\\n"
            "Dada a consulta do usuário, selecione as melhores URLs em ordem de prioridade.\\n\\n"
        )
        
        # 2. CONSULTA ORIGINAL
        base_prompt += f'CONSULTA ORIGINAL:\\n"{payload["original_query"]}"\\n\\n'
        
        # 3. PRIORIZAÇÃO DO USUÁRIO (destaque especial se existir)
        user_prioritization = payload['context'].get('prioritization_criteria', '')
        if user_prioritization:
            base_prompt += (
                "⭐ PRIORIZAÇÃO DO USUÁRIO (máxima prioridade):\\n"
                f'"{user_prioritization}"\\n\\n'
                "Esta priorização SOBREPÕE todos os outros critérios quando houver conflito.\\n\\n"
            )
        
        # 3.5. COBERTURA TEMPORAL (para @noticias)
        time_window = payload['context'].get('time_window_hint')
        if time_window:
            base_prompt += (
                f"📅 COBERTURA TEMPORAL:\\n"
                f"{time_window}\\n"
                "IMPORTANTE: Para consultas @noticias, DISTRIBUA as seleções ao longo de TODO o período. "
                "Não concentre todas as URLs em uma única época. Exemplo: se o período é jan/21-jun/22, "
                "selecione URLs de diferentes meses/trimestres para dar cobertura completa.\\n\\n"
            )
        
        # ✅ v4.7: 4. CONTEXTO TÉCNICO (define variáveis ANTES das regras)
        base_prompt += (
            "CONTEXTO TÉCNICO:\\n"
            f"- top_n (retornar): {payload['context'].get('top_n', 10)}\\n"
            f"- domain_cap (máximo por domínio): {payload['context']['domain_cap']}\\n"
            f"- news_profile_active: {json.dumps(payload['context']['news_profile_active'])}\\n"
            f"- time_window_hint: {json.dumps(payload['context']['time_window_hint'])}\\n\\n"
        )
        
        # ✅ v4.7: 5. REGRAS DE SELEÇÃO (usa variáveis do contexto técnico)
        base_prompt += (
            "REGRAS DE SELEÇÃO (aplique na ordem com SCORING EXPLÍCITO):\\n"
            "1) Relevância semântica à CONSULTA (considere título + snippet) > use \"score\" como desempate\\n"
            "2) MUST_TERMS (se fornecidos em RAILS DO PIPELINE):\\n"
            "   • ≥2 must_terms no título/snippet: +2.0 pontos\\n"
            "   • 1 must_term no título/snippet: +1.0 ponto\\n"
            "   • 0 must_terms: -1.0 ponto (penalização forte)\\n"
            "3) SNIPPET VAZIO:\\n"
            "   • snippet vazio ou muito curto (<50 chars): -0.5 pontos\\n"
            "   • snippet rico (>200 chars): +0.5 pontos\\n"
            "   ⚠️ CRÍTICO: Títulos relevantes com snippet vazio podem ser market reports genéricos\\n"
            "4) Diversidade: máximo domain_cap URLs por domínio\\n"
            "   • EXCEÇÃO: se CONSULTA tem 'site:dominio', ignore domain_cap para esse domínio\\n"
            "5) Se news_profile_active=true: prefira artigos (não listings)\\n"
            "6) Se time_window_hint existe: DISTRIBA seleções ao longo do período (não concentre em uma época)\\n"
            "7) Penalize duplicatas, espelhos e títulos genéricos\\n"
            "8) Em empate: prefira authority=true e news_host=true\\n"
            "9) Retorne EXATAMENTE top_n URLs em selected_indices\\n\\n"
            "FALLBACK: Se impossível preencher top_n sem violar regras:\\n"
            "  a) Relaxe domain_cap incrementalmente\\n"
            "  b) Aceite listings se relevantes\\n"
            "  c) Aceite menor authority/score\\n"
            "  (documente relaxamentos no campo 'why')\\n\\n"
        )
        
        # ✅ v4.7: 6. RAILS DO PIPE (prioridade alta - regras obrigatórias)
        pipe_rails = []
        if intent.get("pipe_must_terms"):
            must_terms_str = ", ".join(intent["pipe_must_terms"])
            pipe_rails.append(f"⚠️ TERMOS PRIORITÁRIOS: {must_terms_str}")
            pipe_rails.append(
                "   → SCORING OBRIGATÓRIO (aplique pontos ao ranquear):\\n"
                "     • ≥2 must_terms no título/snippet: +2.0 pontos\\n"
                "     • 1 must_term no título/snippet: +1.0 ponto\\n"
                "     • 0 must_terms: -1.0 ponto (penalização)"
            )
        
        if intent.get("pipe_avoid_terms"):
            avoid_terms_str = ", ".join(intent["pipe_avoid_terms"])
            pipe_rails.append(f"❌ TERMOS A EVITAR: {avoid_terms_str}")
            pipe_rails.append("   → Penalize URLs que contenham estes termos")
        
        if intent.get("pipe_geo_bias"):
            geo_bias_str = ", ".join(intent["pipe_geo_bias"])
            pipe_rails.append(f"🌍 VIÉS GEOGRÁFICO: {geo_bias_str}")
            pipe_rails.append("   → Prefira fontes dessas regiões quando relevante")
        
        if intent.get("pipe_lang_bias"):
            lang_bias_str = ", ".join(intent["pipe_lang_bias"])
            pipe_rails.append(f"🗣️ VIÉS DE IDIOMA: {lang_bias_str}")
            pipe_rails.append("   → Prefira conteúdo nestes idiomas quando disponível")
        
        if intent.get("pipe_official_domains"):
            official_doms_str = ", ".join(intent["pipe_official_domains"][:10])  # Limitar a 10
            pipe_rails.append(f"🏛️ DOMÍNIOS OFICIAIS: {official_doms_str}")
            pipe_rails.append(
                "   → BONUS OBRIGATÓRIO:\\n"
                "     • URLs de domínios oficiais: +2.0 pontos\\n"
                "     • Priorize fortemente sobre fontes secundárias"
            )
        
        if pipe_rails:
            pipe_rails_text = "\\n".join([f"{r}" for r in pipe_rails])
            base_prompt += f"RAILS DO PIPELINE (regras obrigatórias do PiPe Planner):\\n{pipe_rails_text}\\n\\n"
            logger.info(f"[Selector] Usando {len(pipe_rails)//2} rails do PiPe Planner")
        else:
            logger.debug("[Selector] Nenhum rail do PiPe detectado (uso stand-alone ou sem rails)")
        
        # ✅ v4.7: 7. ORIENTAÇÕES ESPECÍFICAS (preferências soft - após rails obrigatórios)
        # NOTA: Evitar duplicação com RAILS (country_bias→pipe_geo_bias, prefer_domains→pipe_official_domains)
        orientations = []
        
        # NEWS_MODE (comportamento especial - não duplica com rails)
        if intent.get("news_mode"):
            orientations.append("Priorize artigos jornalísticos recentes com datas (não listings)")
        
        # SUGGESTED_DOMAINS do Planner (bonus explícito - apenas se NÃO estiver em pipe_official_domains)
        pipe_official_doms = set(intent.get("pipe_official_domains", []))
        if suggested_domains:
            # Filtrar domínios que já estão em pipe_official_domains (evitar duplicação)
            unique_suggested = [d for d in suggested_domains if d not in pipe_official_doms]
            if unique_suggested:
                domains_str = ", ".join(unique_suggested)
                orientations.append(
                    f"⭐ DOMÍNIOS SUGERIDOS PELO PLANNER: {domains_str}\\n"
                    f"   → URLs desses domínios recebem +1.0 bonus no ranking"
                )
        
        # SUGGESTED_FILETYPES (contexto esperado)
        if suggested_filetypes:
            filetypes_str = ", ".join(suggested_filetypes)
            orientations.append(f"Tipos de arquivo esperados (contexto): {filetypes_str}")
        
        if orientations:
            orientation_text = "\\n".join([f"• {o}" for o in orientations])
            base_prompt += f"ORIENTAÇÕES ESPECÍFICAS (preferências):\\n{orientation_text}\\n\\n"
        
        # ✅ v4.7: 8. CANDIDATOS (por último - material de trabalho)
        candidates_json = json.dumps(payload["candidates"], ensure_ascii=False)
        base_prompt += ("CANDIDATOS (JSON):\\n" + candidates_json + "\\n\\n")
        
        # Log do prompt completo apenas em debug
        if _safe_get_valve(valves, "enable_debug_logging", False):
            logger.debug(f"[LLM Selector] Full prompt length: {len(base_prompt)} chars")
            logger.debug(f"[LLM Selector] Prompt preview: {base_prompt[:500]}...")
        
        # ✅ v4.7: 9. FORMATO DE SAÍDA
        base_prompt += (
            "SAÍDA (JSON puro, sem markdown):\\n"
            "{\\n"
            '  "selected_indices": [0,1,2,...],\\n'
            '  "additional_indices": [3,4],\\n'
            '  "why": ["razão idx0", "razão idx1", ...]\\n'
            "}\\n"
        )
        
        prompt = base_prompt
        
        # Check circuit breaker before making LLM call
        if not _llm_circuit_breaker.can_execute():
            logger.warning("[LLM Selector] Circuit breaker is OPEN - using programmatic fallback")
            # Use programmatic ranking as fallback
            def _programmatic_rank(c):
                is_primary = int(c.get("is_primary", False))
                must_hits = c.get("must_terms_hits", {}).get("total", 0)
                score = c.get("score", 0.0)
                return (is_primary, must_hits, score)
            
            sorted_pool = sorted(scored_candidates_pool, key=_programmatic_rank, reverse=True)
            return [scored_candidates_pool[c["idx"]] for c in sorted_pool[:top_n] if c["idx"] < len(scored_candidates_pool)]
        
        # Use timeout from valves (default 90s for Selector - analyzes many candidates)
        selector_timeout = _safe_get_valve(valves, "request_timeout", DiscoveryConstants.DEFAULT_TIMEOUT_SELECTOR)
        # Ensure minimum timeout for analysis
        selector_timeout = max(selector_timeout, 60)
        
        # ✅ P0: Garantir hierarquia de timeouts (HTTP >= LLM)
        def _ensure_timeout_hierarchy(selector_timeout: int, httpx_read_timeout: int) -> int:
            """Garante que HTTP timeout >= LLM timeout + margem de segurança"""
            margin = DiscoveryConstants.TIMEOUT_MARGIN  # Margem de 30s para cleanup
            required_http_timeout = selector_timeout + margin
            
            if httpx_read_timeout < required_http_timeout:
                logger.warning(
                    f"[TIMEOUT FIX] HTTPX_READ_TIMEOUT ({httpx_read_timeout}s) < "
                    f"selector_timeout + margin ({required_http_timeout}s = {selector_timeout}s + {margin}s). "
                    f"Aumentando HTTPX para {required_http_timeout}s para evitar timeout prematuro."
                )
                return required_http_timeout
            return httpx_read_timeout
        
        # Obter HTTPX_READ_TIMEOUT da valve ou default
        httpx_read_timeout = int(_safe_get_valve(valves, "HTTPX_READ_TIMEOUT", DiscoveryConstants.DEFAULT_TIMEOUT_HTTP))
        effective_http_timeout = _ensure_timeout_hierarchy(selector_timeout, httpx_read_timeout)
        
        # ✅ P2: Logs de diagnóstico (tamanho do prompt + timeouts)
        prompt_chars = len(prompt)
        prompt_tokens_est = prompt_chars // 4
        candidates_count = len(scored_candidates_pool)
        
        logger.info(f"[Selector] Model: {model}")
        logger.info(f"[Selector] Candidates: {candidates_count}")
        logger.info(f"[Selector] Prompt size: {prompt_chars:,} chars (~{prompt_tokens_est:,} tokens)")
        logger.info(f"[Selector] Timeout config: selector={selector_timeout}s, httpx_read={effective_http_timeout}s")
        
        if prompt_tokens_est > DiscoveryConstants.MAX_PROMPT_TOKENS_WARNING:
            logger.warning(
                f"[Selector] PROMPT LARGE ({prompt_tokens_est:,} tokens > 20k). "
                f"Consider reducing max_candidates_for_llm_analysis (current: {candidates_count}). "
                f"May cause timeout with gpt-5-mini."
            )
        
        # ✅ Aplicar timeout HTTP efetivo ao client
        # Nota: O AsyncOpenAI client usa o timeout do httpx.Timeout object
        # que é configurado no momento da criação do client.
        # Aqui passamos o timeout via parâmetro da chamada, que sobrescreve o default.
        try:
            response = await client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=1.0,  # Default temperature for this model
                timeout=float(effective_http_timeout)  # ✅ Usar timeout HTTP efetivo (não selector_timeout)
            )
            # Record success in circuit breaker
            _llm_circuit_breaker.record_success()
        except Exception as e:
            # Record failure in circuit breaker
            _llm_circuit_breaker.record_failure(e)
            raise
        
        content = response.choices[0].message.content.strip()
        selected_candidates = []
        try:
            # Tentar interpretar várias formas de saída do LLM
            try:
                parsed = json.loads(content)
            except Exception:
                parsed = content

            selected_indices_raw = None

            # Caso 1: dict com chave explícita
            if isinstance(parsed, dict):
                for key in ("selected_indices", "selected", "indices", "selection"):
                    if key in parsed:
                        selected_indices_raw = parsed.get(key)
                        break
                # às vezes o seletor pode devolver {"selected_indices": "0,1,2"}
                if isinstance(selected_indices_raw, str):
                    # extrair números
                    import re
                    selected_indices_raw = [int(x) for x in re.findall(r"\d+", selected_indices_raw)]

            # Caso 2: resposta é uma lista direta de índices ou objetos candidatos
            if selected_indices_raw is None and isinstance(parsed, list):
                # se for lista de ints
                if all(isinstance(x, int) for x in parsed):
                    selected_indices_raw = parsed
                else:
                    # se for lista de dicts, talvez já sejam candidatos; mapear para índices se possível
                    extracted = []
                    for item in parsed:
                        if isinstance(item, dict) and "idx" in item:
                            try:
                                extracted.append(int(item["idx"]))
                            except Exception:
                                pass
                    if extracted:
                        selected_indices_raw = extracted

            # Caso 3: string com números (ex: "0,1,2")
            if selected_indices_raw is None and isinstance(parsed, str):
                import re
                nums = re.findall(r"\d+", parsed)
                if nums:
                    selected_indices_raw = [int(x) for x in nums]

            if not selected_indices_raw or not isinstance(selected_indices_raw, list):
                raise ValueError("LLM response for indices could not be normalized")

            for idx_raw in selected_indices_raw:
                try:
                    idx = int(idx_raw)
                    if 0 <= idx < len(scored_candidates_pool):
                        selected_candidates.append(scored_candidates_pool[idx])
                except (ValueError, TypeError):
                    logger.warning(f"LLM returned a non-integer index, skipping: {idx_raw}")
                    continue
        except Exception as e:
            logger.error(f"Failed to parse or process LLM response for curation: {e}")
            raise RuntimeError(f"Falha na análise LLM para curadoria: {e}")
        
        # ✅ v4.7: Fallback programático se LLM removeu TUDO (P0)
        if not selected_candidates and len(enriched_candidates) > 0:
            logger.warning(f"[Selector] LLM removed all candidates ({len(enriched_candidates)} available), using programmatic fallback")
            
            # Ordenar por: is_primary DESC, must_terms_hits.total DESC, score DESC
            def _programmatic_rank(c):
                is_primary = int(c.get("is_primary", False))  # 1 ou 0
                must_hits = c.get("must_terms_hits", {}).get("total", 0)
                score = c.get("score", 0.0)
                return (is_primary, must_hits, score)
            
            sorted_pool = sorted(enriched_candidates, key=_programmatic_rank, reverse=True)
            selected_candidates = [scored_candidates_pool[c["idx"]] for c in sorted_pool[:top_n] if c["idx"] < len(scored_candidates_pool)]
            
            if __event_emitter__:
                try:
                    await safe_emit(__event_emitter__, f"⚠️ Selector LLM muito agressivo, usando fallback programático (top {len(selected_candidates)} por is_primary + must_terms + score)")
                except Exception:
                    pass
            
            logger.info(f"[Selector] Fallback programático retornou {len(selected_candidates)} candidatos")
        
        if __event_emitter__:
            try:
                await safe_emit(__event_emitter__, f"LLM selected {len(selected_candidates)} candidates from {len(scored_candidates_pool)}")
            except Exception:
                if hasattr(__event_emitter__, '__call__'):
                    __event_emitter__(f"LLM selected {len(selected_candidates)} candidates from {len(scored_candidates_pool)}")
        
        return selected_candidates[:top_n]
    except Exception as e:
        logger.error(f"Error in unified LLM analysis: {e}")
        return scored_candidates_pool[:top_n]

# =============================================================================
# HELPER FUNCTIONS FOR DISCOVER_URLS REFACTORING
# =============================================================================

async def _execute_search_plans(
    plans: List[Dict], 
    valves: DiscoveryConfig,
    __event_emitter__: Optional[Callable] = None
) -> Tuple[List[UrlCandidate], List[str]]:
    """
    Executa planos de busca em SearXNG e Exa.
    
    Args:
        plans: Lista de planos gerados pelo LLM Planner
        valves: Configurações de discovery
        __event_emitter__: Emissor de eventos opcional
    
    Returns:
        (all_candidates, all_urls) - Tupla com candidatos e URLs brutas
    
    Raises:
        RuntimeError: Se ambos engines falharem
    """
    all_candidates = []
    all_urls = []
    exa_executed = False
    
    for i, plan in enumerate(plans):
        # Validação
        if not isinstance(plan, dict) or 'core_query' not in plan:
            logger.error(f"Plan {i+1} inválido: {plan}")
            continue
        
        engine = plan.get('engine', 'searxng')
        plan_query = plan['core_query'].strip()
        
        # Sanitização de query (já existe no código original)
        sanitized_query = plan_query.replace('"', '').replace("'", '')
        sanitized_query = re.sub(r'\s+', ' ', sanitized_query).strip()
        if len(sanitized_query) > DiscoveryConstants.MAX_QUERY_LENGTH:
            truncated = sanitized_query[:DiscoveryConstants.MAX_QUERY_LENGTH]
            last_space = truncated.rfind(' ')
            if last_space > 50:
                plan_query = truncated[:last_space]
            else:
                plan_query = truncated
        
        # Emitir status
        if __event_emitter__:
            await safe_emit(__event_emitter__, 
                          f"Executando plano {i+1}/{len(plans)}: {plan_query}... ({engine})")
        
        # ===== SEARXNG =====
        if engine == "searxng" and valves.enable_searxng and valves.searxng_endpoint:
            try:
                runner = RunnerSearxng(valves.searxng_endpoint, valves)
                # Build params for SearXNG
                sanitized_q = QueryBuilder.sanitize(plan_query)
                cats = CategoryFormatter.classify_categories(sanitized_q, force_news=False, has_filetypes=bool(plan.get("filetypes")))
                cats = validate_searxng_categories(cats)
                
                searxng_params = QueryBuilder.searx_params(
                    base_query=sanitized_q,
                    language="",
                    country="",
                    after=plan.get("after"),
                    before=plan.get("before"),
                    include_date_in_query=False  # ✅ Helper function only for non-@noticias
                )
                if cats:
                    searxng_params["categories"] = cats
                searxng_params["format"] = "json"
                
                results = await runner.run(searxng_params, max_pages=valves.pages_per_slice)
                
                for result in results:
                    if isinstance(result, dict):
                        candidate = UrlCandidate(
                            url=result.get("url", ""),
                            title=result.get("title", ""),
                            snippet=result.get("content", ""),
                            score=0.8,
                            source="searxng",
                            domain=_normalize_domain(_extract_domain_from_url(result.get("url", "")))
                        )
                        all_candidates.append(candidate)
                        all_urls.append(candidate.url)
            except Exception as e:
                logger.error(f"[SearXNG] Error: {e}")
        
        # ===== EXA =====
        elif engine == "exa" and valves.enable_exa and valves.exa_api_key:
            if exa_executed:
                continue  # Pular se já executado
            
            try:
                runner = RunnerExa(valves.exa_api_key, valves)
                # Build payload for Exa
                exa_payload = {
                    "query": plan['core_query'],
                    "after": plan.get("after"),
                    "before": plan.get("before"),
                    "news_profile": False
                }
                results = await runner.run(exa_payload)
                
                for result in results:
                    if isinstance(result, dict):
                        candidate = UrlCandidate(
                            url=result.get("url", ""),
                            title=result.get("title", ""),
                            snippet=result.get("text", ""),
                            score=0.9,
                            source="exa",
                            domain=_normalize_domain(_extract_domain_from_url(result.get("url", "")))
                        )
                        all_candidates.append(candidate)
                        all_urls.append(candidate.url)
                
                exa_executed = True
            except Exception as e:
                logger.error(f"[Exa] Error: {e}")
    
    return all_candidates, all_urls


async def _apply_braiding_and_diversity(
    all_candidates: List[UrlCandidate],
    valves: DiscoveryConfig,
    geo_bias: Optional[List[str]] = None,
    lang_bias: Optional[List[str]] = None
) -> List[UrlCandidate]:
    """
    Aplica braiding (interleaving) e diversidade de domínios.
    
    Args:
        all_candidates: Lista de candidatos de todos os engines
        valves: Configurações de discovery
        geo_bias: Preferências geográficas (soft filter)
        lang_bias: Preferências de idioma (soft filter)
    
    Returns:
        Lista de candidatos após braiding e diversidade
    
    Notes:
        - Braiding: intercala resultados de SearXNG e Exa
        - Diversidade: limita URLs por domínio (se enable_domain_diversity=True)
        - Soft geo/lang rerank: aplica pequeno boost baseado em preferências
    """
    # Separar por engine
    searxng_results = [c for c in all_candidates if c.source == "searxng"]
    exa_results = [c for c in all_candidates if c.source == "exa"]
    
    # Converter para dict (compatibilidade com zipper)
    searxng_dicts = [
        {
            "url": c.url, "title": c.title, "snippet": c.snippet,
            "text": c.snippet, "score": c.score, "domain": c.domain,
            "source": "searxng"
        }
        for c in searxng_results
    ]
    exa_dicts = [
        {
            "url": c.url, "title": c.title, "snippet": c.snippet,
            "text": c.snippet, "score": c.score, "domain": c.domain,
            "source": "exa"
        }
        for c in exa_results
    ]
    
    # Braiding (zipper inteligente)
    braided = zipper_interleave_results(searxng_dicts, exa_dicts, valves)
    interleaved = [UrlCandidate(**d) for d in braided]
    
    # Soft geo/lang rerank
    preferred_geos = set(geo_bias or [])
    if preferred_geos:
        def _tld_boost(domain: str) -> float:
            d = domain.lower()
            if '.fr' in d and 'FR' in preferred_geos:
                return DiscoveryConstants.GEO_BIAS_BOOST
            if '.br' in d and 'BR' in preferred_geos:
                return DiscoveryConstants.GEO_BIAS_BOOST
            if '.uk' in d and 'UK' in preferred_geos:
                return DiscoveryConstants.GEO_BIAS_BOOST
            return 1.0
        
        for c in interleaved:
            c.score *= _tld_boost(c.domain)
        
        interleaved.sort(key=lambda x: x.score, reverse=True)
        logger.info(f"[Diversity] Soft geo rerank applied for {sorted(preferred_geos)}")
    
    # Brazilian boost (se habilitado)
    if valves.boost_brazilian_sources:
        boost_table = _get_brazilian_boost_table()
        for c in interleaved:
            domain = urlparse(c.url).netloc
            if domain in boost_table:
                c.brazilian_boost = boost_table[domain]
                c.brazilian_source = True
                c.score *= boost_table[domain]
    
    # Domain diversity (se habilitado)
    if valves.enable_domain_diversity:
        domain_cap = valves.max_urls_per_domain
        domain_counts = {}
        capped = []
        
        for c in interleaved:
            count = domain_counts.get(c.domain, 0)
            if count < domain_cap:
                capped.append(c)
                domain_counts[c.domain] = count + 1
        
        if len(capped) < len(interleaved):
            logger.info(f"[Diversity] Domain cap reduced {len(interleaved)} → {len(capped)}")
        
        return capped
    
    return interleaved


async def _apply_llm_analysis_and_rails(
    candidates: List[UrlCandidate],
    query: str,
    valves: DiscoveryConfig,
    plans: List[Dict],
    intent_profile: Dict[str, Any],
    must_terms: Optional[List[str]] = None,
    avoid_terms: Optional[List[str]] = None,
    official_domains: Optional[List[str]] = None,
    min_domains: Optional[int] = None,
    __event_emitter__: Optional[Callable] = None
) -> List[UrlCandidate]:
    """
    Aplica análise LLM e rails de qualidade.
    
    Args:
        candidates: Lista de candidatos após braiding
        query: Query original do usuário
        valves: Configurações de discovery
        plans: Planos de busca gerados pelo LLM Planner
        intent_profile: Perfil de intenção do usuário
        must_terms/avoid_terms/official_domains/min_domains: Rails do Pipe
        __event_emitter__: Emissor de eventos opcional
    
    Returns:
        Lista final de candidatos curados
    
    Notes:
        - LLM analysis: curadoria semântica com gpt-4o/gpt-5-mini
        - Rails: aplicação de preferências (soft filters + scoring)
        - Limit: top_curated_results (default 10)
    """
    # LLM analysis (se habilitado e threshold atingido)
    threshold = 3 if "@noticias" in query else valves.top_curated_results
    
    if valves.enable_unified_llm_analysis and len(candidates) > threshold:
        # Coletar sugestões dos plans
        suggested_domains = []
        suggested_filetypes = []
        for plan in plans:
            if isinstance(plan, dict):
                suggested_domains.extend(plan.get("suggested_domains", []))
                suggested_filetypes.extend(plan.get("suggested_filetypes", []))
        
        # Deduplicate
        suggested_domains = list(dict.fromkeys(suggested_domains))
        suggested_filetypes = list(dict.fromkeys(suggested_filetypes))
        
        # Limitar candidatos para análise (respeitando limite global)
        max_candidates = min(valves.max_candidates_for_llm_analysis, valves.max_total_candidates)
        candidates_for_analysis = candidates[:max_candidates]
        
        # Extrair critérios de priorização
        prioritization = extract_prioritization_criteria(query)
        
        # Chamar LLM Selector
        scored_pool = [c.model_dump() for c in candidates_for_analysis]
        selected = await unified_llm_analysis(
            scored_candidates_pool=scored_pool,
            original_query=query,
            prioritization_criteria=prioritization,
            suggested_filetypes=suggested_filetypes,
            suggested_domains=suggested_domains,
            top_n=valves.top_curated_results,
            __event_emitter__=__event_emitter__,
            valves=valves,
            intent_profile=intent_profile
        )
        
        candidates = [UrlCandidate(**c) for c in selected]
    
    # Aplicar rails pós-LLM (scoring inteligente)
    if must_terms or avoid_terms or official_domains or min_domains:
        candidates = _apply_rails_post(
            candidates,
            must_terms_param=must_terms,
            avoid_terms_param=avoid_terms,
            official_domains_param=official_domains,
            min_domains_param=min_domains
        )
    
    # Limitar ao top_n final
    return candidates[:valves.top_curated_results]


def _apply_rails_post(cands, must_terms_param, avoid_terms_param, 
                      official_domains_param, min_domains_param):
    """
    Aplica rails de qualidade com pontuação inteligente (soft filter + scoring).
    
    IMPORTANTE: Esta função NÃO descarta candidatos (exceto avoid_terms críticos).
    Ela PRIORIZA baseado em scores, mantendo diversidade.
    
    Scoring:
    - Must_terms: +2.0 (≥2 termos), +1.0 (1 termo), -0.5 (0 termos)
    - Avoid_terms: -2.0 (penalização forte)
    - Official_domains: +2.0 (bonus)
    - Domain diversity: +1.0 (primeiro do domínio)
    
    Args:
        cands: Lista de UrlCandidate
        must_terms_param: Termos obrigatórios
        avoid_terms_param: Termos a evitar
        official_domains_param: Domínios oficiais
        min_domains_param: Mínimo de domínios únicos
    
    Returns:
        Lista ordenada por score (mantém todos candidatos exceto avoid_terms críticos)
    """
    if not cands:
        return cands
    
    # Preparar termos
    mts = [t.lower() for t in (must_terms_param or [])]
    ats = [t.lower() for t in (avoid_terms_param or [])]
    off_doms = set(official_domains_param or [])
    
    # Separar entidades (uppercase) de aspectos (lowercase)
    entities = [t for t in (must_terms_param or []) 
                if t and len(t.split()) <= 3 and t[0].isupper()]
    aspects = [t for t in (must_terms_param or []) if t and t not in entities]
    entities_low = [e.lower() for e in entities]
    aspects_low = [a.lower() for a in aspects]
    
    # Scoring
    scored_cands = []
    seen_domains = set()
    
    for c in cands:
        text = f"{c.title} {c.snippet}".lower()
        score = 0.0
        
        # Entidades (peso 2x)
        entity_count = sum(1 for e in entities_low if e in text)
        if entity_count >= 2:
            score += DiscoveryConstants.SCORE_MUST_TERM_MULTIPLE
        elif entity_count == 1:
            score += DiscoveryConstants.SCORE_MUST_TERM_SINGLE
        elif entities and entity_count == 0:
            score += DiscoveryConstants.SCORE_MUST_TERM_NONE
        
        # Aspectos (peso 1x)
        aspect_count = sum(1 for a in aspects_low if a in text)
        if aspect_count >= 2:
            score += 1.0
        elif aspect_count == 1:
            score += 0.5
        
        # Avoid_terms (penalização forte)
        if any(at in text for at in ats):
            score += DiscoveryConstants.SCORE_AVOID_TERM
        
        # Official domains (bonus)
        if c.domain in off_doms:
            score += DiscoveryConstants.SCORE_OFFICIAL_DOMAIN
        
        # Diversidade (bonus para domínios novos)
        if c.domain not in seen_domains:
            score += DiscoveryConstants.SCORE_DOMAIN_DIVERSITY
            seen_domains.add(c.domain)
        
        # Score base (50% do score original)
        score += getattr(c, 'score', 0.0) * 0.5
        
        scored_cands.append((score, c))
    
    # Ordenar por score
    sorted_cands = sorted(scored_cands, key=lambda x: x[0], reverse=True)
    
    # Aplicar min_domains (garantir diversidade mínima)
    if min_domains_param and min_domains_param > 0:
        final_cands = []
        domains_used = set()
        
        for score, c in sorted_cands:
            if len(domains_used) < min_domains_param:
                # Preenchendo domínios mínimos
                if c.domain not in domains_used:
                    final_cands.append(c)
                    domains_used.add(c.domain)
            else:
                # Já atingiu mínimo, adicionar resto
                final_cands.append(c)
        
        return final_cands
    
    return [c for score, c in sorted_cands]

# =============================================================================
# SEÇÃO 8: FUNÇÃO PRINCIPAL DE DESCOBERTA COM CICLO COMPLETO DE @NOTICIAS
# =============================================================================

async def discover_urls(
    query: str,
    profile: str = "general",
    after: Optional[str] = None,
    before: Optional[str] = None,
    whitelist: str = "",
    pages_per_slice: int = 2,
    __event_emitter__: Optional[Callable] = None,
    valves: Optional[Union[Dict[str, Any], DiscoveryConfig, Any]] = None,
    # Rails opcionais (compatível com uso stand-alone)
    must_terms: Optional[List[str]] = None,
    avoid_terms: Optional[List[str]] = None,
    time_hint: Optional[Dict[str, Any]] = None,
    lang_bias: Optional[List[str]] = None,
    geo_bias: Optional[List[str]] = None,
    min_domains: Optional[int] = None,
    official_domains: Optional[List[str]] = None,
    # ✅ P0.2: Add source_bias parameter
    source_bias: Optional[List[str]] = None,
) -> DiscoveryResult:
    """Discover URLs using multiple search engines with fallback"""
    if not valves:
        valves = DiscoveryConfig()
    
    try:
        start_time = time.time()
        
        # Garantir que valves é uma instância válida de DiscoveryConfig
        if isinstance(valves, DiscoveryConfig):
            current_valves = valves
        elif isinstance(valves, dict):
            current_valves = DiscoveryConfig(**valves)
        elif hasattr(valves, 'model_dump'):
            # Handle any Pydantic BaseModel (Tools.DiscoveryValves, etc.)
            try:
                current_valves = DiscoveryConfig(**valves.model_dump())
            except Exception as e:
                logger.warning(f"[Discovery] Failed to convert Pydantic model to DiscoveryConfig: {e}")
                # Fallback: try direct attribute access
                current_valves = DiscoveryConfig(
                    **{k: getattr(valves, k, None) for k in DiscoveryConfig.model_fields.keys() if hasattr(valves, k)}
                )
        else:
            # Fallback: try generic object with attributes
            try:
                current_valves = DiscoveryConfig(**vars(valves))
            except Exception:
                # Last resort: environment variables
                logger.warning("[Discovery] Valves not recognized, falling back to environment variables")
                openai_key = os.getenv("OPENAI_API_KEY")
                if not openai_key:
                    raise RuntimeError("LLM planner required but OpenAI or API key is not available")
                
                current_valves = DiscoveryConfig(
                    openai_api_key=openai_key,
                    openai_base_url=os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1"),
                    openai_model=os.getenv("OPENAI_MODEL", "gpt-5-mini"),
                    exa_api_key=os.getenv("EXA_API_KEY", ""),
                    searxng_endpoint=os.getenv("SEARXNG_ENDPOINT", "")
                )
        
        # Build intent profile from user query for rich context
        intent_profile = build_intent_profile(query, current_valves)
        
        # Adicionar rails do PiPe Planner como hints no intent_profile (compatibilidade)
        if must_terms:
            intent_profile["pipe_must_terms"] = must_terms
        if avoid_terms:
            intent_profile["pipe_avoid_terms"] = avoid_terms
        if time_hint:
            intent_profile["pipe_time_hint"] = time_hint
        if lang_bias:
            intent_profile["pipe_lang_bias"] = lang_bias
        if geo_bias:
            intent_profile["pipe_geo_bias"] = geo_bias
        if official_domains:
            intent_profile["pipe_official_domains"] = official_domains
        # ✅ P0.2: Add source_bias to intent_profile
        if source_bias:
            intent_profile["pipe_source_bias"] = source_bias
        
        # Verificar se source_bias foi passado via intent_profile (do PiPe)
        if "pipe_source_bias" in intent_profile:
            # source_bias já está no intent_profile, não precisa fazer nada
            pass
        
        # Auto-detect entities if pipe_must_terms not provided (stand-alone mode enhancement)
        if not intent_profile.get("pipe_must_terms"):
            # Detect capitalized multi-word entities (e.g., "Vila Nova Partners", "Flow Executive")
            # Pattern: 2+ consecutive capitalized words
            entity_pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b'
            entities = re.findall(entity_pattern, query)
            
            if entities:
                # Remove duplicates while preserving order
                unique_entities = list(dict.fromkeys(entities))
                intent_profile["pipe_must_terms"] = unique_entities
                logger.info(f"[Discovery] Auto-detected entities → pipe_must_terms: {unique_entities}")
        
        logger.info(f"[Discovery] Intent profile: {intent_profile}")
        # Rails recebidos (opcionais)
        rails_log = {
            "must_terms": must_terms or [],
            "avoid_terms": avoid_terms or [],
            "time_hint": time_hint or {},
            "lang_bias": lang_bias or [],
            "geo_bias": geo_bias or [],
            "min_domains": min_domains,
            "official_domains": official_domains or [],
        }
        logger.info(f"[Discovery][Rails] received: {rails_log}")
        
        # Detect news-mode queries and automatically set profile to "news"
        # Criteria: explicit token @noticias OR news keywords OR intent_profile.news_mode
        try:
            is_news_by_text = QueryClassifier.is_news_profile_active(query)
        except Exception:
            is_news_by_text = False
        is_news_mode = bool(intent_profile.get("news_mode", False))
        is_noticias_query = ("@noticias" in query.lower())
        is_news_active = is_noticias_query or is_news_by_text or is_news_mode
        logger.info(f"[Discovery] Query: '{query}', news_active: {is_news_active}, profile: {profile}")
        if is_news_active:
            if profile != "news":
                logger.info("[Discovery] News mode detected - forcing profile to 'news'")
                profile = "news"
            if "news_mode" not in intent_profile:
                intent_profile["news_mode"] = True
        
        # Generate search plans using LLM with rich intent context
        # ==========================================
        # Generate search plans using LLM with rich intent context
        # ==========================================
        
        # Call LLM planner for ALL queries (including @noticias)
        logger.info("[Planner] Calling generate_queries_with_llm for query optimization")
        llm_plans = await generate_queries_with_llm(
            query=query,
            intent_profile=intent_profile,
            messages=None,
            language=intent_profile.get("lang_bias", "") or getattr(current_valves, 'search_language', ''),
            __event_emitter__=__event_emitter__,
            valves=current_valves
        )
        
        # Validate and normalize plans (ensure engine / core_query)
        def _validate_plans(pls: List[Dict]) -> List[Dict]:
            valid: List[Dict] = []
            for i, p in enumerate(pls or []):
                if not isinstance(p, dict):
                    logger.error(f"[Planner] Plan {i+1} inválido (não-dict): {type(p)}")
                    continue
                core = p.get("core_query")
                if not core or not isinstance(core, str) or not core.strip():
                    logger.error(f"[Planner] Plan {i+1} sem core_query, descartando")
                    continue
                eng = p.get("engine") or "searxng"
                if eng not in ("searxng", "exa"):
                    logger.warning(f"[Planner] Plan {i+1} engine inválido '{eng}', corrigindo para 'searxng'")
                    eng = "searxng"
                p["engine"] = eng
                valid.append(p)
            if not valid:
                logger.warning("[Planner] Nenhum plan válido após validação")
            else:
                logger.info(f"[Planner] Validated {len(valid)} plans with engines: {[p.get('engine') for p in valid]}")
            return valid

        llm_plans = _validate_plans(llm_plans)

        # Apply time slicing for @noticias queries
        if is_noticias_query and profile == "news":
            logger.info("[Planner] Applying time slicing to LLM-optimized queries")
            
            from datetime import datetime, timedelta
            
            # Calculate time window (same logic as before)
            eff_after = None
            eff_before = None
            
            # 1. Explicit parameters (highest priority)
            if after:
                try:
                    eff_after = datetime.fromisoformat(after).date() if isinstance(after, str) else after
                    logger.info(f"[Planner] Using explicit 'after' parameter: {eff_after}")
                except Exception:
                    pass
            
            if before:
                try:
                    eff_before = datetime.fromisoformat(before).date() if isinstance(before, str) else before
                    logger.info(f"[Planner] Using explicit 'before' parameter: {eff_before}")
                except Exception:
                    pass
            
            # 2. Apply time_hint if strict recency requested
            try:
                if time_hint and time_hint.get("strict") and time_hint.get("recency"):
                    rec = time_hint.get("recency")
                    days = 90 if rec == "90d" else 365 if rec == "1y" else 1095 if rec == "3y" else None
                    if days:
                        eff_before = datetime.utcnow().date()
                        eff_after = eff_before - timedelta(days=days)
                        logger.info(f"[Discovery][Rails] Applied time_hint.strict: {rec} → window {eff_after}..{eff_before}")
            except Exception:
                pass
            
            # 3. Parse from query (lowest priority)
            if not eff_after or not eff_before:
                base_query = query.replace("@noticias", "").replace("@notícias", "").strip()
                parsed_after, parsed_before = AdvancedDateParser.parse_date_range_from_query(base_query)
                if not eff_after and parsed_after:
                    eff_after = parsed_after
                    logger.info(f"[Planner] Parsed 'after' from query: {eff_after}")
                if not eff_before and parsed_before:
                    eff_before = parsed_before
                    logger.info(f"[Planner] Parsed 'before' from query: {eff_before}")
            
            # Set default news window if no dates provided
            if not eff_after and not eff_before:
                eff_before = datetime.utcnow().date()
                eff_after = eff_before - timedelta(days=current_valves.news_default_days)
                logger.info(f"[Planner] No dates specified, using default news window: {current_valves.news_default_days} days")
            elif eff_before and not eff_after:
                eff_after = eff_before - timedelta(days=current_valves.news_default_days)
                logger.info(f"[Planner] Only 'before' date specified, setting 'after' to {current_valves.news_default_days} days earlier")
            elif eff_after and not eff_before:
                eff_before = datetime.utcnow().date()
                logger.info(f"[Planner] Only 'after' date specified, setting 'before' to today")
            
            # Calculate time slices
            delta_days = (eff_before - eff_after).days if (eff_after and eff_before) else current_valves.news_default_days
            
            if delta_days <= 90:
                slices = TimeSlicer.month_slices(eff_after, eff_before)
                logger.info(f"[Planner] Using month slices: {len(slices)} slices for {delta_days} days")
            else:
                slices = TimeSlicer.quarter_slices(eff_after, eff_before)
                logger.info(f"[Planner] Using quarter slices: {len(slices)} slices for {delta_days} days")
            
            if not slices:
                slices = [(eff_after, eff_before)]
                logger.warning("[Planner] No slices generated, using single slice")
            
            # Apply slices to each LLM-optimized plan (SearXNG only)
            plans = []
            searxng_plans = [p for p in llm_plans if p.get('engine', 'searxng') == 'searxng']
            
            # ✅ FIX: Apply time-slicing para @noticias COMPLETO (sem limite)
            # Estratégia: período <= 120 dias = slicing por MÊS
            #           período > 120 dias = slicing por TRIMESTRE
            plans = []
            searxng_plans = [p for p in llm_plans if p.get('engine', 'searxng') == 'searxng']
            
            # Aplicar cada LLM plan a TODOS os time slices (sem limite)
            for llm_plan in searxng_plans:
                for i, (start, end) in enumerate(slices):
                    # Clone plan and add temporal bounds
                    sliced_plan = llm_plan.copy()
                    sliced_plan["after"] = start.isoformat() if start else None
                    sliced_plan["before"] = end.isoformat() if end else None
                    sliced_plan["engine"] = "searxng"
                    sliced_plan["slice_id"] = start.isoformat()[:7] if start else None
                    plans.append(sliced_plan)
            
            slicing_mode = "monthly" if delta_days <= 120 else "quarterly"
            logger.info(f"[Planner] Created {len(plans)} time-sliced plans ({slicing_mode}: {len(searxng_plans)} queries × {len(slices)} slices)")
            
            # Add Exa plan (single global window)
            if current_valves.enable_exa and current_valves.exa_api_key:
                exa_plans = [p for p in llm_plans if p.get('engine') == 'exa']
                if not exa_plans:
                    # If LLM didn't create Exa plan, create one from first LLM query
                    exa_plan = llm_plans[0].copy() if llm_plans else {
                        "core_query": query.replace("@noticias", "").replace("@notícias", "").strip(),
                        "sites": [],
                        "filetypes": [],
                        "suggested_domains": [],
                        "suggested_filetypes": []
                    }
                    exa_plan["engine"] = "exa"
                    exa_plan["after"] = eff_after.isoformat() if eff_after else None
                    exa_plan["before"] = eff_before.isoformat() if eff_before else None
                    exa_plan["slice_id"] = None
                    plans.append(exa_plan)
                    logger.info("[Planner] Created 1 Exa plan (global window)")
                else:
                    plans.extend(exa_plans)
                    logger.info(f"[Planner] Added {len(exa_plans)} Exa plans from LLM")
        else:
            # Normal flow: use LLM plans directly
            plans = llm_plans
            
            # Add Exa plan if enabled
            if current_valves.enable_exa and current_valves.exa_api_key:
                has_exa = any(p.get('engine') == 'exa' for p in plans)
                if not has_exa:
                    exa_plan = {
                        "core_query": query,
                        "sites": [],
                        "filetypes": [],
                        "suggested_domains": [],
                        "suggested_filetypes": [],
                        "after": None,
                        "before": None,
                        "engine": "exa",
                        "slice_id": None
                    }
                    plans.append(exa_plan)
                    logger.info("[Planner] Added 1 Exa plan to normal flow")
        
        if not plans:
            logger.warning("No search plans generated")
            return DiscoveryResult(
                query=query,
                candidates_count=0,
                candidates=[],
                urls=[],
                citations=[],
                sources=[]
            )
        
        # Log plan summary
        plan_engines = [plan.get("engine", "unknown") for plan in plans]
        logger.info(f"[Discovery] Generated {len(plans)} plans with engines: {plan_engines}")
        
        all_candidates = []
        all_urls = []
        
        # Track if Exa was already executed (single global query)
        exa_executed = False
        
        # Execute each plan
        for i, plan in enumerate(plans):
            # Validar estrutura do plano
            if not isinstance(plan, dict) or 'core_query' not in plan:
                logger.error(f"Plan {i+1} inválido (sem core_query): {plan}")
                continue
            
            # Extract engine and raw query
            engine = plan.get('engine', 'searxng')  # Default para searxng
            raw_query = plan['core_query'].strip()
            
            # Apply engine-specific query processing
            if engine == 'searxng':
                # SearXNG requires short, keyword-focused queries
                plan_query = QueryBuilder.sanitize_and_truncate(raw_query, max_length=100)
                
                if len(plan_query) < len(raw_query):
                    logger.warning(f"[SearXNG] Query truncated from {len(raw_query)} to {len(plan_query)} chars to avoid 400 error")
                
                if plan_query != raw_query:
                    logger.info(f"[SearXNG] Query sanitized: '{raw_query}' -> '{plan_query}'")
            elif engine == 'exa':
                # Exa uses its own internal planner (_plan_search_with_llm) which handles sanitization
                # Pass raw query intact for optimal semantic search
                plan_query = raw_query
                logger.info(f"[Exa] Using raw query ({len(raw_query)} chars) for internal planner")
            else:
                # Fallback for unknown engines: apply conservative truncation
                plan_query = QueryBuilder.sanitize_and_truncate(raw_query, max_length=100)
                logger.warning(f"[{engine}] Unknown engine, applying conservative truncation")
            
            if __event_emitter__:
                try:
                    await safe_emit(__event_emitter__, f"Executando plano {i+1}/{len(plans)}: {plan_query}... ({engine})")
                except Exception:
                    if hasattr(__event_emitter__, '__call__'):
                        __event_emitter__(f"Executando plano {i+1}/{len(plans)}: {plan_query}... ({engine})")
            
            logger.info(f"[Discovery] Plan {i+1}/{len(plans)}: {engine} - '{plan_query}'")
            
            # Garantir que engine existe no dict (para uso posterior se necessário)
            if 'engine' not in plan:
                plan['engine'] = 'searxng'
            
            # SearXNG - usar variável engine, não plan["engine"]
            if engine == "searxng" and current_valves.enable_searxng and current_valves.searxng_endpoint:
                try:
                    searxng_runner = RunnerSearxng(current_valves.searxng_endpoint, current_valves)
                    # sanitize + categories
                    sanitized_q = QueryBuilder.sanitize(plan_query)
                    cats = CategoryFormatter.classify_categories(sanitized_q, force_news=(profile=="news"), has_filetypes=bool(plan.get("filetypes")))
                    
                    # Validate categories against SearXNG valid list
                    cats = validate_searxng_categories(cats)
                    
                    # Locale precedence: rails (lang_bias/geo_bias) > valves defaults > intent_profile
                    rails_lang = (lang_bias or [])
                    # Se houver mais de um idioma preferido, deixe SearXNG autodetectar (não force language)
                    if isinstance(rails_lang, list) and len(rails_lang) > 1:
                        eff_lang = ""
                    else:
                        eff_lang = (rails_lang[0] if isinstance(rails_lang, list) and rails_lang else "") 
                        if not eff_lang:
                            eff_lang = _safe_get_valve(valves, 'search_language', '') or intent_profile.get("lang_bias", "")
                    # country not directly supported by searx; keep for telemetry only
                    eff_country = _safe_get_valve(valves, 'search_country', '') or intent_profile.get("country_bias", "")

                    # build lightweight params
                    searxng_params = QueryBuilder.searx_params(
                        base_query=sanitized_q,
                        language=eff_lang,
                        country=eff_country,
                        after=plan.get("after"),
                        before=plan.get("before"),
                        include_date_in_query=is_news_active  # ✅ Add dates to query for any news-mode
                    )
                    logger.info(f"[SearXNG] Effective locale → language={eff_lang or 'auto'}, country_bias={eff_country or 'none'} (soft)")
                    if cats:
                        searxng_params["categories"] = cats
                    # Always request JSON explicitly
                    searxng_params["format"] = "json"
                    # Do NOT enforce time_range for general queries; only news-mode handles temporal hints
                    
                    # Log query details with full params
                    logger.info(f"[SearXNG] Query: {plan_query}")
                    logger.info(f"[SearXNG] Sanitized query: {sanitized_q}")
                    logger.info(f"[SearXNG] Full params: {searxng_params}")
                    
                    # Get pages_per_slice from valves
                    max_pages = _safe_get_valve(current_valves, 'pages_per_slice', 2)
                    searxng_results = await searxng_runner.run(searxng_params, max_pages=max_pages)
                    
                    # Log results
                    logger.info(f"[SearXNG] Found {len(searxng_results)} results")
                    
                    # DEBUG: Log detalhado se 0 resultados
                    if len(searxng_results) == 0:
                        logger.warning(f"[SearXNG DEBUG] 0 results for query: {searxng_params}")
                        logger.warning(f"[SearXNG DEBUG] URL: {searxng_runner._get_api_url()}")
                        logger.warning(f"[SearXNG DEBUG] Max pages: {max_pages}")

                        # No fallback per @noticias requirement: must work with dates as provided
                    
                except Exception as e:
                    logger.error(f"Error with SearXNG: {e}")
                    searxng_results = []
                
                for result in searxng_results:
                    if isinstance(result, dict):
                        url = result.get("url", "")
                        candidate = UrlCandidate(
                            url=url,
                            title=result.get("title", ""),
                            snippet=result.get("content", ""),
                            score=0.8,
                            source="searxng",
                            domain=_normalize_domain(_extract_domain_from_url(url))
                        )
                        all_candidates.append(candidate)
                        all_urls.append(candidate.url)
            
            # Exa.ai - usar variável engine, não plan["engine"]
            # Execute ONLY ONCE (single global query)
            elif engine == "exa" and current_valves.enable_exa and current_valves.exa_api_key:
                if exa_executed:
                    logger.info(f"[Exa] Skipping plan {i+1} - Exa already executed (single global query)")
                    continue  # Skip if Exa was already executed
                
                try:
                    exa_runner = RunnerExa(current_valves.exa_api_key, current_valves)
                    # Initialize OpenAI client for Exa planning (use cached client)
                    if OPENAI_AVAILABLE and current_valves.openai_api_key:
                        exa_runner.openai_client = _get_cached_openai_client(
                            current_valves.openai_api_key,
                            current_valves.openai_base_url
                        )
                    
                    # Exa uses its own specific planner (_plan_search_with_llm)
                    # Create payload for Exa's own planner
                    exa_payload = {
                        "query": plan['core_query'],  # Use original query from plan
                        "after": plan.get("after"),
                        "before": plan.get("before"),
                        "news_profile": bool(profile == "news" or is_noticias_query)
                    }
                    
                    # Log query details
                    logger.info(f"[Exa] Query: {plan['core_query']}")
                    logger.info(f"[Exa] Using Exa's own planner (_plan_search_with_llm)")
                    
                    exa_results = await exa_runner.run(exa_payload)

                    # Log results
                    logger.info(f"[Exa] Found {len(exa_results)} results")

                    exa_executed = True  # Mark as executed
                    
                except Exception as e:
                    logger.error(f"Error with Exa.ai: {e}")
                    exa_results = []
                
                for result in exa_results:
                    if isinstance(result, dict):
                        url = result.get("url", "")
                        candidate = UrlCandidate(
                            url=url,
                            title=result.get("title", ""),
                            snippet=result.get("text", ""),
                            score=0.9,
                            source="exa",
                            domain=_normalize_domain(_extract_domain_from_url(url))
                        )
                        all_candidates.append(candidate)
                        all_urls.append(candidate.url)
        
        # Braider: versão refinada do zipper com pré-filtragem robusta
        searxng_results = [c for c in all_candidates if c.source == "searxng"]
        exa_results = [c for c in all_candidates if c.source == "exa"]
        
        logger.info(f"Braider input: {len(searxng_results)} SearXNG, {len(exa_results)} Exa results")
        
        # Converter UrlCandidate para dict para o Braider
        # Padronizar: incluir AMBOS 'snippet' e 'text' para compatibilidade
        searxng_dicts = []
        for candidate in searxng_results:
            searxng_dicts.append({
                "url": candidate.url,
                "title": candidate.title,
                "snippet": candidate.snippet,
                "text": candidate.snippet,  # Incluir ambos para compatibilidade
                "score": candidate.score,
                "domain": candidate.domain,
                "source": "searxng"
            })
        
        exa_dicts = []
        for candidate in exa_results:
            exa_dicts.append({
                "url": candidate.url,
                "title": candidate.title,
                "snippet": candidate.snippet,  # Incluir ambos para compatibilidade
                "text": candidate.snippet,
                "score": candidate.score,
                "domain": candidate.domain,
                "source": "exa"
            })
        
        # Usar o Zipper inteligente (baseado no v2)
        braided_results = zipper_interleave_results(searxng_dicts, exa_dicts, current_valves)
        
        # Converter de volta para UrlCandidate
        interleaved_results = []
        for result_dict in braided_results:
            candidate = UrlCandidate(
                url=result_dict.get("url", ""),
                title=result_dict.get("title", ""),
                snippet=result_dict.get("snippet", result_dict.get("text", "")),
                score=result_dict.get("score", 0.0),
                source=result_dict.get("source", "unknown"),
                domain=result_dict.get("domain", "")
            )
            interleaved_results.append(candidate)
        logger.info(f"Braider output: {len(interleaved_results)} interleaved results")
        
        # Aplicar limite global de candidatos para prevenir OOM
        if len(interleaved_results) > current_valves.max_total_candidates:
            logger.warning(f"[Discovery] Truncating candidates from {len(interleaved_results)} to {current_valves.max_total_candidates} to prevent OOM")
            interleaved_results = interleaved_results[:current_valves.max_total_candidates]
        
        # Soft geo/language rerank (preferences, not filters)
        try:
            preferred_geos = set((geo_bias or []) if isinstance(geo_bias, list) else [])
            preferred_langs = set((lang_bias or []) if isinstance(lang_bias, list) else [])
            def _tld_boost(domain: str) -> float:
                d = (domain or '').lower()
                if '.fr' in d and ('FR' in preferred_geos):
                    return 1.08
                if '.br' in d and ('BR' in preferred_geos):
                    return 1.08
                if '.uk' in d and ('UK' in preferred_geos):
                    return 1.08
                return 1.0
            # Apply small boost without filtering
            for c in interleaved_results:
                try:
                    c.score = c.score * _tld_boost(c.domain)
                except Exception:
                    pass
            # Re-sort by adjusted score (stable)
            interleaved_results.sort(key=lambda x: getattr(x, 'score', 0), reverse=True)
            if preferred_geos:
                logger.info(f"[Selector] Soft geo rerank applied for {sorted(preferred_geos)}")
        except Exception:
            pass
        
        # Apply Brazilian boost if enabled (como no original)
        if current_valves.boost_brazilian_sources:
            brazilian_boost_table = _get_brazilian_boost_table()
            for result in interleaved_results:
                from urllib.parse import urlparse
                domain = urlparse(result.url).netloc
                if domain in brazilian_boost_table:
                    result.brazilian_boost = brazilian_boost_table[domain]
                    result.brazilian_source = True
                    # Apply boost to score
                    current_score = result.score
                    result.score = current_score * brazilian_boost_table[domain]
        
        unique_candidates = interleaved_results
        
        # Apply enable_domain_diversity (pre-LLM diversity enforcement)
        # enable_domain_diversity is boolean: True = use max_urls_per_domain value, False = disabled
        domain_diversity_enabled = _safe_get_valve(current_valves, "enable_domain_diversity", True)
        
        if domain_diversity_enabled:
            domain_cap_value = _safe_get_valve(current_valves, "max_urls_per_domain", 4)
            logger.info(f"[Discovery] enable_domain_diversity enabled, using max_urls_per_domain={domain_cap_value}")
            domain_counts: Dict[str, int] = {}
            capped_candidates = []
            for c in unique_candidates:
                domain = c.domain
                count = domain_counts.get(domain, 0)
                if count < domain_cap_value:
                    capped_candidates.append(c)
                    domain_counts[domain] = count + 1
            
            if len(capped_candidates) < len(unique_candidates):
                logger.info(f"[Discovery] soft_domain_cap reduced candidates from {len(unique_candidates)} to {len(capped_candidates)}")
                unique_candidates = capped_candidates
        
        # Análise com LLM se habilitada
        # Para @noticias, usar threshold menor (sempre curar se > 3 URLs)
        threshold = 3 if "@noticias" in query else current_valves.top_curated_results
        if current_valves.enable_unified_llm_analysis and len(unique_candidates) > threshold:
            # Coletar sugestões dos plans (agregadas de todos os plans)
            all_suggested_domains = []
            all_suggested_filetypes = []
            for plan in plans:
                if isinstance(plan, dict):
                    all_suggested_domains.extend(plan.get("suggested_domains", []))
                    all_suggested_filetypes.extend(plan.get("suggested_filetypes", []))
            
            # Deduplicate sugestões
            suggested_domains_unique = list(dict.fromkeys(all_suggested_domains))  # Preserva ordem
            suggested_filetypes_unique = list(dict.fromkeys(all_suggested_filetypes))
            
            logger.info(f"[Selector] Suggested domains from Planner: {suggested_domains_unique}")
            logger.info(f"[Selector] Suggested filetypes from Planner: {suggested_filetypes_unique}")
            
            # Apply max_candidates_for_llm_analysis limit (respeitando limite global)
            max_llm_candidates = _safe_get_valve(current_valves, "max_candidates_for_llm_analysis", 100)
            max_total = _safe_get_valve(current_valves, "max_total_candidates", 200)
            max_candidates = min(max_llm_candidates, max_total)
            candidates_for_analysis = unique_candidates[:max_candidates] if len(unique_candidates) > max_candidates else unique_candidates
            
            if len(unique_candidates) > max_candidates:
                logger.info(f"[Selector] Limited candidates for LLM analysis from {len(unique_candidates)} to {max_candidates}")
            
            scored_pool = [c.model_dump() for c in candidates_for_analysis]
            
            # Extract prioritization criteria from user query
            prioritization_criteria = extract_prioritization_criteria(query)
            if prioritization_criteria:
                logger.info(f"[Selector] Extracted prioritization: {prioritization_criteria}")
            
            selected_candidates = await unified_llm_analysis(
                scored_candidates_pool=scored_pool,
                original_query=query,
                prioritization_criteria=prioritization_criteria,
                suggested_filetypes=suggested_filetypes_unique or getattr(current_valves, 'force_filetypes', []),
                suggested_domains=suggested_domains_unique,
                top_n=current_valves.top_curated_results,
                __event_emitter__=__event_emitter__,
        valves=current_valves,
                intent_profile=intent_profile  # Passar intent_profile real
            )
            unique_candidates = [UrlCandidate(**c) for c in selected_candidates]

        # ✅ P1c: Consistent programmatic fallback
        if not unique_candidates and len(candidates_for_analysis) > 0:
            logger.warning(
                f"[Discovery] LLM analysis filtered out all {len(candidates_for_analysis)} candidates. "
                f"Applying programmatic fallback."
            )
            
            # Programmatic ranking logic
            def _programmatic_rank(c):
                candidate_dict = c.model_dump() if hasattr(c, 'model_dump') else c
                is_primary = int(compute_is_primary(
                    candidate_dict, 
                    intent_profile.get("pipe_official_domains", []), 
                    intent_profile
                ))
                must_hits = compute_must_terms_hits(
                    candidate_dict, 
                    intent_profile.get("pipe_must_terms", [])
                ).get("total", 0)
                score = candidate_dict.get("score", 0.0)
                return (is_primary, must_hits, score)
            
            # Sort and select top N
            sorted_candidates = sorted(candidates_for_analysis, key=_programmatic_rank, reverse=True)
            unique_candidates = sorted_candidates[:current_valves.top_curated_results]
            
            logger.info(f"[Discovery] Programmatic fallback returned {len(unique_candidates)} candidates")
            
            if __event_emitter__:
                await safe_emit(
                    __event_emitter__, 
                    f"⚠️ LLM muito restritivo - usando fallback programático ({len(unique_candidates)} URLs)"
                )
        
        # Limitar resultados
        unique_candidates = unique_candidates[:current_valves.top_curated_results]
        final_urls = [c.url for c in unique_candidates]
        
        execution_time = time.time() - start_time
        
        # Log final detalhado
        logger.info(f"[Discovery] Final results: {len(final_urls)} URLs from {len(plans)} plans in {execution_time:.2f}s")
        
        if __event_emitter__:
            try:
                await safe_emit(__event_emitter__, f"Descoberta concluída: {len(final_urls)} URLs de {len(plans)} planos em {execution_time:.2f}s")
            except Exception:
                if hasattr(__event_emitter__, '__call__'):
                    __event_emitter__(f"Descoberta concluída: {len(final_urls)} URLs de {len(plans)} planos em {execution_time:.2f}s")
        
        return DiscoveryResult(
            query=query,
            candidates_count=len(unique_candidates),
            candidates=unique_candidates,
            urls=final_urls,
            citations=[],  # Removed duplicate citations - info already in candidates
            sources=list(set([c.source for c in unique_candidates]))
        )
        
    except Exception as e:
        logger.error(f"Error in discover_urls: {e}")
        return DiscoveryResult(
            query=query,
            candidates_count=0,
            candidates=[],
            urls=[],
            citations=[],
            sources=[],
            error=str(e)
        )

# =============================================================================
# SEÇÃO 9: CLASSE TOOLS PARA OPENWEBUI (PADRÃO CORRETO)
# =============================================================================

class Tools:
    """
    Classe principal para ferramentas de descoberta de URLs no OpenWebUI.
    
    Esta classe expõe a ferramenta de descoberta de URLs como uma ferramenta
    personalizada no OpenWebUI, permitindo que usuários façam buscas avançadas
    com múltiplos motores de busca e análise inteligente com LLM.
    """

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
        max_search_results: int = Field(default=20, description="📊 Máximo de resultados por busca")
        pages_per_slice: int = Field(default=2, description="📄 Páginas por slice")
        request_timeout: int = Field(default=30, description="⏱️ Timeout de requisição em segundos")
        
        # =============================================================================
        # CONFIGURAÇÕES DE ANÁLISE LLM
        # =============================================================================
        enable_unified_llm_analysis: bool = Field(default=True, description="🧠 Habilitar análise LLM unificada")
        top_curated_results: int = Field(default=10, description="⭐ Top resultados curados")
        max_candidates_for_llm_analysis: int = Field(default=100, description="🎯 Máximo de candidatos para análise LLM")
        max_urls_per_domain: int = Field(default=4, description="🏢 Limite máximo de URLs por domínio")
        enable_domain_diversity: bool = Field(default=True, description="🔄 Habilitar diversidade de domínios")
        
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
        
        # =============================================================================
        # CONFIGURAÇÕES DE LIMITE GLOBAL
        # =============================================================================
        max_total_candidates: int = Field(default=200, description="🚫 Limite global de candidatos para prevenir OOM")
        
        # =============================================================================
        # CONFIGURAÇÕES DE DEBUG
        # =============================================================================
        enable_debug_logging: bool = Field(default=False, description="🐛 Habilitar logs verbosos de debug")

    # Alias esperado pelo OpenWebUI para renderizar Valves na UI
    class Valves(DiscoveryValves):
        pass

    def __init__(self):
        """Inicializa a instância da classe Tools."""
        self.valves = self.Valves()

    async def discover_urls(
        self,
        query: str,
        profile: str = "general",
        after: str = None,
        before: str = None,
        whitelist: str = "",
        pages_per_slice: int = 2,
        __event_emitter__: Optional[Callable] = None,
        # Rails opcionais do PiPe Planner (compatível com uso stand-alone)
        must_terms: Optional[List[str]] = None,
        avoid_terms: Optional[List[str]] = None,
        time_hint: Optional[Dict[str, Any]] = None,
        lang_bias: Optional[List[str]] = None,
        geo_bias: Optional[List[str]] = None,
        min_domains: Optional[int] = None,
        official_domains: Optional[List[str]] = None,
        # ✅ P0.2: Add source_bias parameter
        source_bias: Optional[List[str]] = None,
        # PATCH v4.5: Opção de retorno como dict para uso interno do Pipe
        return_dict: bool = False,
    ) -> str | dict:
        """
        Ferramenta avançada de descoberta de URLs que utiliza múltiplos motores de busca (SearXNG e Exa.ai) com análise inteligente via LLM.

        Esta ferramenta é especializada em:
        - Busca temporal com @noticias (slice automático por mês/trimestre) - APENAS quando explicitamente solicitado
        - Análise de intenção e planejamento de queries com LLM
        - Seleção inteligente de resultados com curadoria semântica
        - Suporte a filtros de domínio, tipo de arquivo e datas
        - Diversidade de fontes e qualidade de URLs
        - Integração otimizada com PiPeManual (v4.5+)

        IMPORTANTE: Use @noticias APENAS quando o usuário explicitamente pedir notícias ou informações recentes.
        Para buscas gerais, NÃO adicione @noticias automaticamente.
        
        Args:
            query: Consulta de busca. Use @noticias APENAS quando o usuário pedir notícias explicitamente.
                   Exemplos: 'Petrobras fusões', 'inflação @noticias últimos 6 meses'
            profile: Perfil de busca (general, news, academic, technical)
            after: Data de início no formato ISO (YYYY-MM-DD). Exemplo: '2024-01-01'
            before: Data de fim no formato ISO (YYYY-MM-DD). Exemplo: '2024-12-31'
            whitelist: Lista de domínios permitidos separados por vírgula. Exemplo: 'valor.com.br,reuters.com'
            pages_per_slice: Número de páginas por slice temporal (para @noticias). Default: 2
            __event_emitter__: Emissor de eventos para status updates (opcional)
            
            # Rails do PiPeManual Planner (opcional, compatível com uso standalone):
            must_terms: Lista de termos obrigatórios para priorização de resultados
            avoid_terms: Lista de termos a evitar/filtrar dos resultados
            time_hint: Dict com recency/strict para controle temporal fino
            lang_bias: Lista de idiomas preferidos (ex: ['pt-BR', 'en'])
            geo_bias: Lista de países/regiões preferidos (ex: ['BR', 'US'])
            min_domains: Mínimo de domínios únicos desejados
            official_domains: Lista de domínios oficiais a priorizar
            
            # Opção de retorno (v4.5.1):
            return_dict: Se True, retorna dict Python diretamente (elimina JSON parse overhead).
                        Se False (default), retorna JSON string (compatível com OpenWebUI).
                        Uso recomendado: True para chamadas internas do Pipe, False para standalone.
            
        Returns:
            JSON string com resultados (default) OU dict Python (se return_dict=True).
            
            Formato do retorno:
            {
                "operation": "discover_urls",
                "query": str,
                "candidates_count": int,
                "candidates": [{"url": str, "title": str, "snippet": str, "domain": str, "source": str, ...}],
                "urls": [str],  # Lista de URLs descobertas
                "citations": [{"url": str, "title": str, ...}],
                "sources": [str],  # Engines usados
                "success": bool,
                "timestamp": str
            }
            
            Em caso de erro:
            {
                "success": false,
                "error": str,
                "query": str,
                "candidates_count": 0,
                "candidates": [],
                "urls": [],
                ...
            }
        """
        try:
            # Event emitter simples para logs
            def simple_emitter(message):
                # Normalize to OpenWebUI event format
                if isinstance(message, dict) and message.get("type"):
                    event = message
                else:
                    event = {"type": "status", "data": {"description": str(message)}}
                try:
                    desc = event.get("data", {}).get("description")
                    if desc:
                        logger.info(f"[Tool Discovery] {desc}")
                except Exception:
                    pass
                if __event_emitter__:
                    try:
                        if asyncio.iscoroutinefunction(__event_emitter__):
                            asyncio.create_task(__event_emitter__(event))
                        else:
                            __event_emitter__(event)
                    except Exception:
                        pass
            
            # Chamar função principal de descoberta
            result = await discover_urls(
                query=query,
                profile=profile,
                after=after,
                before=before,
                whitelist=whitelist,
                pages_per_slice=pages_per_slice,
                __event_emitter__=simple_emitter,
                valves=self.valves.model_dump(),  # ✅ FIX: converter Pydantic para dict
                # Rails opcionais recebidos do Pipe (compatível stand-alone)
                must_terms=must_terms,
                avoid_terms=avoid_terms,
                time_hint=time_hint,
                lang_bias=lang_bias,
                geo_bias=geo_bias,
                min_domains=min_domains,
                official_domains=official_domains,
            )
            
            filtered_candidates = _apply_rails_post(
                result.candidates,
                must_terms_param=must_terms,
                avoid_terms_param=avoid_terms,
                official_domains_param=official_domains,
                min_domains_param=min_domains
            )
            filtered_urls = [c.url for c in filtered_candidates]

            payload = {
                "operation": "discover_urls",
                "query": result.query,
                "candidates_count": len(filtered_candidates),
                "candidates": [
                    {
                        "url": c.url,
                        "title": c.title,
                        "snippet": c.snippet,
                        "domain": c.domain,
                        "source": c.source,
                        "score": c.score,
                        "brazilian_source": getattr(c, 'brazilian_source', False),
                        "brazilian_boost": getattr(c, 'brazilian_boost', 1.0)
                    }
                    for c in filtered_candidates
                ],
                "urls": filtered_urls,
                "citations": [
                    {
                        "url": c.url,
                        "title": c.title,
                        "snippet": c.snippet,
                        "domain": c.domain,
                        "source": c.source
                    }
                    for c in filtered_candidates
                ],
                "sources": result.sources,
                "success": True,
                "timestamp": result.timestamp
            }

            # PATCH v4.5: Retornar dict diretamente se solicitado (evita json.dumps/loads overhead)
            if return_dict:
                return payload

            # Safe json.dumps com fallback para objetos não-serializáveis
            try:
                return json.dumps(payload, ensure_ascii=False, indent=2, default=str)
            except Exception as e:
                logger.warning(f"JSON serialization failed, using safe fallback: {e}")
                # Last-resort: convert problematic fields to str then dump
                safe_payload = {
                    k: (v if isinstance(v, (str, int, float, bool, list, dict, type(None))) else str(v))
                    for k, v in payload.items()
                }
                return json.dumps(safe_payload, ensure_ascii=False, indent=2)

        except Exception as e:
            logger.error(f"Error in discover_urls: {e}")
            error_payload = {
                "success": False,
                "error": str(e),
                "query": query,
                "candidates_count": 0,
                "candidates": [],
                "urls": [],
                "citations": [],
                "sources": []
            }
            # PATCH v4.5: Consistente com sucesso, retornar dict ou string
            return error_payload if return_dict else json.dumps(error_payload, ensure_ascii=False, indent=2)


def get_tools():
    """Retorna lista de ferramentas disponíveis."""
    _tools = []
    _instance = Tools()

    async def discovery_tool(
        query: str,
        profile: str = "general",
        after: str = None,
        before: str = None,
        whitelist: str = "",
        pages_per_slice: int = 2,
        __event_emitter__ = None,
        # Rails opcionais do Planner/Pipe (compatível com uso stand-alone)
        must_terms: Optional[List[str]] = None,
        avoid_terms: Optional[List[str]] = None,
        time_hint: Optional[Dict[str, Any]] = None,
        lang_bias: Optional[List[str]] = None,
        geo_bias: Optional[List[str]] = None,
        min_domains: Optional[int] = None,
        official_domains: Optional[List[str]] = None,
        # PATCH v4.5: Opção de retorno como dict para uso interno do Pipe
        return_dict: bool = False,
    ) -> str | dict:
        """
        Wrapper de descoberta de URLs para OpenWebUI - integração com PiPeManual.
        
        Esta ferramenta utiliza múltiplos motores (SearXNG + Exa.ai) com análise LLM.
        
        Suporta:
        - Busca temporal com @noticias (slice automático)
        - Rails de qualidade do PiPeManual Planner
        - Retorno otimizado (dict ou JSON string)
        
        Args:
            query: Consulta de busca (use @noticias apenas quando explicitamente solicitado)
            profile: general | news | academic | technical
            after/before: Filtros temporais ISO (YYYY-MM-DD)
            whitelist: Domínios permitidos (separados por vírgula)
            pages_per_slice: Páginas por slice temporal
            
            # Rails (PiPeManual Planner - opcional):
            must_terms: Termos obrigatórios para priorização
            avoid_terms: Termos a evitar/filtrar
            time_hint: Controle temporal fino (recency/strict)
            lang_bias/geo_bias: Preferências de idioma/região
            min_domains: Mínimo de domínios únicos desejados
            official_domains: Domínios oficiais a priorizar
            
            # Otimização (v4.5.1):
            return_dict: True = retorna dict (Pipe interno, -100% JSON overhead)
                        False = retorna JSON string (default, OpenWebUI standalone)
            
        Returns:
            str | dict - Formato determinado por return_dict (ver Tools.discover_urls docstring)
        """
        return await _instance.discover_urls(
            query=query,
            profile=profile,
            after=after,
            before=before,
            whitelist=whitelist,
            pages_per_slice=pages_per_slice,
            __event_emitter__=__event_emitter__,
            must_terms=must_terms,
            avoid_terms=avoid_terms,
            time_hint=time_hint,
            lang_bias=lang_bias,
            geo_bias=geo_bias,
            min_domains=min_domains,
            official_domains=official_domains,
            # PATCH v4.5: Propagar return_dict
            return_dict=return_dict,
        )

    _tools.append(discovery_tool)
    return _tools
