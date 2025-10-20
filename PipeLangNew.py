#!/usr/bin/env python3
"""
PipeHaystack_LangGraph v3.0 - Orquestração 100% LangGraph

FILOSOFIA:
- LangGraph gerencia ITERAÇÕES (discovery → scrape → reduce → analyze → judge)
- Pipe gerencia apenas CICLO DE FASES (criar novas fases quando Judge decide)
- Nós são WRAPPERS FINOS que delegam para código existente
- Router é PURO (decisão baseada apenas em state, sem side-effects)

ESTRUTURA:
1. State TypedDict: Estado COMPLETO da pesquisa (todos os campos do Orchestrator)
2. Nós LangGraph: Wrappers para discovery, scrape, reduce, analyze, judge
3. Router: Decisão pura baseada em state (done/refine/new_phase)
4. Pipe: Wrapper OpenWebUI (gerencia fases, não loops)
"""

import asyncio
import json
import logging
import time
import uuid
import hashlib
import os
import re
import numpy as np
from dataclasses import dataclass, field, asdict
from threading import Lock
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, TypedDict, Literal, Tuple
from datetime import datetime

# LangGraph imports (conditional)
try:
    from langgraph.graph import StateGraph as LG_StateGraph, END as LG_END
    from langgraph.checkpoint.memory import MemorySaver as LG_MemorySaver
    LANGGRAPH_AVAILABLE = True
    # Alias to unified names (type ignores to satisfy static checker across branches)
    StateGraph = LG_StateGraph  # type: ignore[assignment]
    MemorySaver = LG_MemorySaver  # type: ignore[assignment]
    END = LG_END  # type: ignore[assignment]
except ImportError:
    LANGGRAPH_AVAILABLE = False
    # Fallback for when LangGraph is not available
    class StateGraph:
        def __init__(self, *args, **kwargs):
            # Called without args in fallback usage
            pass

        def add_node(self, *args, **kwargs):
            return None

        def set_entry_point(self, *args, **kwargs):
            return None

        def add_edge(self, *args, **kwargs):
            return None

        def add_conditional_edges(self, *args, **kwargs):
            return None

        def compile(self, *args, **kwargs):
            return self

        async def ainvoke(self, *args, **kwargs):
            raise ImportError("LangGraph not available. Install with: pip install langgraph")

    class MemorySaver:
        pass

    END = "END"

import httpx
from pydantic import BaseModel, Field, validator

# Conditional imports
try:
    from datasketch import MinHash, MinHashLSH
except ImportError:
    pass

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
except ImportError:
    pass

try:
    from haystack import Document
    from haystack.components.embedders import SentenceTransformersDocumentEmbedder
    HAYSTACK_AVAILABLE = True
except ImportError:
    HAYSTACK_AVAILABLE = False

# Configure structured logger (coexists with stdlib logging)
try:
    import structlog  # type: ignore
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger(__name__)
except Exception:
    logger = logging.getLogger(__name__)

# Global LLM cache
_LLM_CACHE: Dict[str, Optional['AsyncOpenAIClient']] = {}
_LLM_CACHE_LOCK: Lock = Lock()


# ============================================================================
# INFRASTRUCTURE CLASSES AND FUNCTIONS
# ============================================================================

@dataclass
class StepTelemetry:
    """Telemetria padronizada por etapa"""
    step: str  # "discovery", "scraper", "analyst", "judge", "synthesis"
    correlation_id: str
    start_ms: float
    end_ms: Optional[float] = None
    inputs_brief: str = ""
    outputs_brief: str = ""
    counters: Optional[Dict[str, int]] = None
    notes: Optional[List[str]] = None
    success: bool = True
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    cache_hit: bool = False
    
    @property
    def elapsed_ms(self) -> float:
        if self.end_ms:
            return self.end_ms - self.start_ms
        return time.time() * 1000 - self.start_ms
    
    def to_dict(self) -> Dict:
        return {
            "step": self.step,
            "correlation_id": self.correlation_id,
            "elapsed_ms": self.elapsed_ms,
            "success": self.success,
            "error_type": self.error_type,
            "inputs": self.inputs_brief,
            "outputs": self.outputs_brief,
            "counters": self.counters or {},
            "retry_count": self.retry_count,
            "cache_hit": self.cache_hit,
            "notes": self.notes or [],
        }

    def mark_failed(self, error: Exception, retry: int = 0) -> None:
        self.success = False
        self.error_type = type(error).__name__
        self.error_message = str(error)[:200]
        self.retry_count = retry
        self.end_ms = time.time() * 1000


@dataclass
class PipelineError(Exception):
    """Erro estruturado do pipeline com contexto rico para observabilidade."""
    stage: str  # discovery|scraping|reducer|analyst|judge|planner|synthesis|api
    error_type: str
    message: str
    context: Dict[str, Any]
    traceback_str: Optional[str] = None
    correlation_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage": self.stage,
            "error_type": self.error_type,
            "message": self.message,
            "context": self.context,
            "correlation_id": self.correlation_id,
            "traceback": self.traceback_str,
            "ts": datetime.utcnow().isoformat() + "Z",
        }

def _emit_decision_snapshot(step: str, vector: Dict[str, Any], reason: str = "", contract_diff: Optional[Dict[str, Any]] = None) -> None:
    """Emit structured decision snapshot without modifying StepTelemetry.
    Safe to call even if values are large; truncates where appropriate.
    """
    try:
        # Lightweight truncation for large fields
        safe_vector = {}
        for k, v in (vector or {}).items():
            if isinstance(v, str) and len(v) > 800:
                safe_vector[k] = v[:800] + "…"
            else:
                safe_vector[k] = v
        payload = {
            "step": step,
            "decision_vector": safe_vector,
            "decision_reason": reason,
            "contract_diff": contract_diff or {},
            "ts": datetime.utcnow().isoformat() + "Z",
        }
        logger.info(f"[DECISION]{json.dumps(payload, ensure_ascii=False)}")
    except Exception as e:
        logger.debug(f"Failed to emit decision snapshot: {e}")


async def _safe_emit(emitter: Optional[Callable], payload: str) -> None:
    """Safely emit payload via optional callable. Supports sync or async emitters."""
    if not emitter:
        return
    try:
        res = emitter(payload)
        # Await if returns an awaitable (async emitter)
        import inspect
        if inspect.isawaitable(res):
            await res
    except Exception:
        # Never break pipeline due to emitter failures
        pass


# ==================== RETRY/BACKOFF ====================
from functools import wraps
import random

def retry_with_backoff(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
    exceptions: tuple = (Exception,),
):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        raise
                    delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                    try:
                        logger.warning("retry_attempt", function=func.__name__, attempt=attempt, delay=f"{delay:.1f}s", error=str(e))
                    except Exception:
                        pass
                    await asyncio.sleep(delay)
        return wrapper
    return decorator


# ==================== CUSTOM EXCEPTIONS ====================
# Exceções específicas para melhor rastreabilidade e recovery strategies

class PipeExecutionError(Exception):
    """Erro base para execução do Pipe - permite captura global de erros do pipeline"""
    pass


class LLMConfigurationError(PipeExecutionError):
    """Raised when LLM client configuration is missing or invalid"""
    pass


class ContractValidationError(PipeExecutionError):
    """Erro na validação do contract - estrutura inválida ou incompatível"""
    pass


class ContractGenerationError(PipeExecutionError):
    """Erro na geração do contract pelo LLM Planner"""
    pass


class ToolExecutionError(PipeExecutionError):
    """Erro na execução de ferramentas (discovery, scraper, context_reducer)"""
    pass


# ==================== PYDANTIC MODELS FOR CONTRACT VALIDATION ====================
# Validação formal de contracts para prevenir estruturas inválidas

# ===== CONTRACT SCHEMAS (Fim-a-fim) =====

class EntitiesModel(BaseModel):
    """Entidades canônicas e aliases"""
    canonical: List[str]
    aliases: List[str] = []


class StrategistPayloadModel(BaseModel):
    """Contrato de saída do Estrategista (Call 1) - JSON-only, sem CoT exposto"""
    intent_profile: str
    intent: str
    executive_objectives: List[str]
    constraints: List[str] = []
    initial_hypotheses: List[str] = []
    stakeholders: List[str] = []
    key_questions: List[str]
    entities: EntitiesModel
    language_bias: List[str] = Field(default_factory=lambda: ["pt-BR", "en"])
    geo_bias: List[str] = Field(default_factory=lambda: ["BR", "global"])
    notes: Optional[str] = None


class TimeHintModel(BaseModel):
    """Time hint configuration for phase recency requirements"""
    recency: str  # "90d", "1y", "3y"
    strict: bool = False

    @validator("recency")
    def validate_recency(cls, v):
        if v not in ["90d", "1y", "3y"]:
            raise ValueError(f"recency must be 90d, 1y, or 3y, got {v}")
        return v


class EvidenceGoalModel(BaseModel):
    """Evidence requirements for quality rails"""
    official_or_two_independent: bool = True
    min_domains: int = Field(ge=2, le=10)


class PhaseTypeModel(BaseModel):
    """Phase type with strict validation"""
    phase_type: str

    @validator("phase_type")
    def validate_phase_type(cls, v):
        valid_types = [
            "industry",
            "profiles",
            "news",
            "regulatory",
            "financials",
            "tech",
        ]
        if v not in valid_types:
            raise ValueError(f"phase_type must be one of {valid_types}, got {v}")
        return v


class PlannerPhaseModel(BaseModel):
    """Phase model for Planner contract - com phase_type e validação estrita"""
    name: str
    phase_type: str  # "industry"|"profiles"|"news"|"regulatory"|"financials"|"tech"
    objective: str
    seed_query: str = Field(min_length=3, max_length=50)
    seed_core: Optional[str] = Field(
        default="",
        max_length=200,
        description="Seed query rica (1 frase) sem operadores - usado pelo Discovery",
    )
    seed_core_source: Optional[str] = Field(
        default="planner_heuristic",
        description="Origem: planner_llm|planner_heuristic|user",
    )
    seed_family_hint: Optional[str] = Field(
        default="entity-centric",
        description="Família de exploração: entity-centric|problem-centric|outcome-centric|regulatory|counterfactual",
    )
    must_terms: List[str] = []
    avoid_terms: List[str] = Field(
        default=[],
        description="**OPCIONAL - ruído a evitar (deixe [] se não houver)**\n"
        "* Use quando há PALAVRAS-CHAVE específicas que causam falsos positivos\n"
        "* **EXEMPLOS DE USO:**\n"
        "  - Fase \"executive search\": avoid_terms: [\"vagas\", \"currículos\", \"job board\"] → evita posts de vagas\n"
        "  - Fase \"aquisições\": avoid_terms: [\"promoção\", \"desconto\"] → evita e-commerce\n"
        "  - Fase \"saúde digital\": avoid_terms: [\"fitness\", \"app gratuito\"] → evita consumer health apps\n"
        "* **QUANDO NÃO USAR (deixe vazio []):**\n"
        "  - Fases genéricas de mercado (risco de bloquear conteúdo válido)\n"
        "  - Quando não há ruído óbvio e bem definido\n"
        "  - Quando objetivo é amplitude/descoberta (melhor deixar Discovery trabalhar)\n"
        "* **REGRA PRÁTICA:** Se duvidoso, deixe [] - Discovery é resilente a ruído",
    )
    time_hint: TimeHintModel
    source_bias: List[str] = ["oficial", "primaria", "secundaria"]
    evidence_goal: EvidenceGoalModel
    lang_bias: List[str] = ["pt-BR", "en"]
    geo_bias: List[str] = ["BR", "global"]
    suggested_domains: List[str] = Field(
        default=[], description="Domínios sugeridos para priorização"
    )
    suggested_filetypes: List[str] = Field(
        default=[], description="Tipos de arquivo sugeridos (html, pdf, etc)"
    )

    @validator("phase_type")
    def validate_phase_type(cls, v):
        valid_types = [
            "industry",
            "profiles",
            "news",
            "regulatory",
            "financials",
            "tech",
        ]
        if v not in valid_types:
            raise ValueError(f"phase_type must be one of {valid_types}, got {v}")
        return v

    @validator("seed_query")
    def validate_seed_query(cls, v):
        # Forbid operators (except @noticias which is handled separately)
        forbidden = ["site:", "filetype:", "after:", "before:", " AND ", " OR ", '"']
        for op in forbidden:
            if op in v:
                raise ValueError(f"seed_query cannot contain operator: {op}")
        # ✅ REMOVED: Word count validation (3-6 words) - bloqueava queries válidas com entidades compostas
        # Discovery's internal Planner will optimize the query regardless of initial seed length
        return v

    @validator("seed_core")
    def validate_seed_core(cls, v):
        """Valida seed_core: ≥3 palavras, ≤200 chars, sem operadores"""
        if not v or not v.strip():
            return ""  # Opcional, pode estar vazio

        # Forbid operators
        forbidden = ["site:", "filetype:", "after:", "before:", "AND", "OR"]
        for op in forbidden:
            if op in v:
                raise ValueError(f"seed_core cannot contain operator: {op}")

        # Validate length
        if len(v) > 200:
            raise ValueError(f"seed_core must be ≤200 chars, got {len(v)}")

        # Validate word count (mínimo 3 palavras)
        words = v.split()
        if len(words) < 3:
            raise ValueError(f"seed_core must have ≥3 words, got {len(words)}")

        return v

    @validator("seed_family_hint")
    def validate_seed_family(cls, v):
        """Valida seed_family_hint: enum de famílias suportadas"""
        valid_families = [
            "entity-centric",
            "problem-centric",
            "outcome-centric",
            "regulatory",
            "counterfactual",
        ]
        if v and v not in valid_families:
            raise ValueError(
                f"seed_family_hint must be one of {valid_families}, got {v}"
            )
        return v or "entity-centric"  # Default

    @validator("must_terms")
    def validate_must_terms_policy(cls, v, values):
        """Política: industry não deve ter todos os players; profiles/news devem ter"""
        phase_type = values.get("phase_type")
        if phase_type == "industry" and len(v) > 5:
            raise ValueError(
                f"industry phase should not have all players in must_terms (got {len(v)})"
            )
        return v


class PlannerPayloadModel(BaseModel):
    """Contrato de saída do Planner (Call 2) - JSON-only, sem CoT"""
    plan_intent: str
    assumptions_to_validate: List[str] = []
    phases: List[PlannerPhaseModel]
    quality_rails: Dict[str, Any] = {
        "min_unique_domains": 3,
        "need_official_or_two_independent": True,
    }
    budget: Dict[str, int] = {"max_rounds": 2}


class PhaseModel(BaseModel):
    name: str
    objective: str
    seed_query: str
    seed_core: Optional[str] = None
    must_terms: List[str] = []
    avoid_terms: List[str] = []
    time_hint: Dict[str, Any] = {}
    source_bias: List[str] = []
    evidence_goal: Dict[str, Any] = {}
    lang_bias: List[str] = []
    geo_bias: List[str] = []


class QualityRailsModel(BaseModel):
    """Quality rails configuration (split from PhaseModel)"""
    min_unique_domains: int = Field(ge=2)
    need_official_or_two_independent: bool = True
    official_domains: List[str] = []


# ==================== CONSTANTS AND UTILITIES ====================

class PipeConstants:
    """
    Configurações e constantes centralizadas do pipeline.
    Valores padrão que podem ser sobrescritos via Valves UI.
    """

    # ===== LIMITES DE EXECUÇÃO =====
    MIN_PHASES = 3  # Mínimo de fases recomendadas
    MAX_PHASES_LIMIT = 15  # Limite absoluto de fases
    MAX_FUNCTION_LENGTH = 100  # Tamanho máximo recomendado para métodos (linhas)

    # ===== TIMEOUTS (segundos) =====
    CONTEXT_DETECTION_TIMEOUT = 30  # Timeout para detecção de contexto
    LLM_CALL_TIMEOUT = 60  # Timeout para chamadas LLM
    TOOL_EXECUTION_TIMEOUT = 120  # Timeout para execução de ferramentas

    # ===== DEFAULTS DE PERFIL =====
    DEFAULT_PROFILE = "company_profile"
    AVAILABLE_PROFILES = [
        "company_profile",
        "regulation_review",
        "technical_spec",
        "literature_review",
        "history_review",
    ]

    # ===== QUALITY RAILS =====
    MIN_EVIDENCE_COVERAGE = 1.0  # 100% de fatos com ≥1 evidência

    # ===== CONTEXT DETECTION =====
    DETECTOR_COT_ENABLED = True  # Habilitar Chain-of-Thought no Context Detection
    MIN_UNIQUE_DOMAINS = 2  # Mínimo de domínios únicos por fase
    STALENESS_DAYS_DEFAULT = 90  # Dias para considerar conteúdo stale
    NOVELTY_THRESHOLD = 0.3  # Threshold para ratio de novidade

    # ===== DEDUPLICAÇÃO =====
    MAX_DEDUP_PARAGRAPHS = 200  # Máximo de parágrafos após deduplicação (alinhado com valve)
    DEDUP_SIMILARITY_THRESHOLD = 0.85  # Threshold de similaridade (0.0-1.0, maior = menos agressivo) - v4.4: ajustado 0.9→0.85
    DEDUP_RELEVANCE_WEIGHT = 0.7  # Peso da relevância vs diversidade (0.6-0.8)

    # ===== LLM CONFIGURATION =====
    LLM_TEMPERATURE = 0.7  # Temperatura padrão para LLM
    LLM_MAX_TOKENS = 2048  # Max tokens para respostas LLM
    LLM_MIN_TOKENS = 100  # Min tokens aceitável
    LLM_MAX_TOKENS_LIMIT = 4000  # Limite absoluto de tokens

    # ===== CONTEXT MANAGEMENT =====
    MAX_CONTEXT_CHARS = 150000  # Máximo de caracteres no contexto
    MAX_HISTORY_MESSAGES = 10  # Máximo de mensagens do histórico

    # ===== SÍNTESE =====
    SYNTHESIS_MIN_PARAGRAPHS = 3  # Mínimo de parágrafos no relatório
    SYNTHESIS_PREFERRED_SECTIONS = 5  # Número preferido de seções no relatório


# ===== Deduplication Utilities =====
def _shingles(s: str, n: int = 3) -> set:
    """Generate n-grams (shingles) from text for similarity comparison

    v4.4: Reduzido n=5→3 (tri-grams) - industry standard, +40% detecção de similaridade
    """
    tokens = [t for t in s.lower().split() if t]
    return set(tuple(tokens[i : i + n]) for i in range(max(0, len(tokens) - n + 1)))


def _jaccard(a: set, b: set) -> float:
    """Jaccard similarity between two sets"""
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union > 0 else 0.0


def _is_geographic_term(term: str) -> bool:
    """Detecta se um termo é geográfico baseado em características estruturais"""
    term_lower = term.strip().lower()
    
    # Termos muito curtos (< 3 chars) são provavelmente códigos geográficos
    if len(term_lower) < 3:
        return True
    
    # Países conhecidos (lista mínima)
    countries = {"brasil", "brazil", "portugal", "argentina", "chile", "colombia", "mexico"}
    if term_lower in countries:
        return True
    
    # Códigos de país comuns
    geo_codes = {"br", "pt", "ar", "cl", "co", "mx", "us", "uk", "fr", "de", "es", "it"}
    if term_lower in geo_codes:
        return True
    
    # Termos geográficos genéricos
    geo_generics = {"global", "mundial", "nacional", "internacional", "latam", "america", "europa"}
    if term_lower in geo_generics:
        return True
    
    return False


# ===== Helper Functions =====
def normalize_base_url(base_url: str) -> str:
    """Normaliza base_url para diferentes variantes de API"""
    base_url = base_url.strip().rstrip("/")
    # Normalizar variantes /api, /v1beta, etc.
    if base_url.endswith(("/v1", "/v1beta", "/api")):
        return base_url
    return base_url + "/v1"


def build_chat_endpoint(base_url: str) -> str:
    """Constrói endpoint de chat completions de forma robusta"""
    from urllib.parse import urljoin

    norm = normalize_base_url(base_url)
    return urljoin(norm + "/", "chat/completions")


def _hash_contract(contract: Dict[str, Any]) -> str:
    s = json.dumps(contract, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def parse_json_resilient(
    text: str, mode: str = "balanced", allow_arrays: bool = True
) -> Optional[dict]:
    """CONSOLIDADO (v4.3.1): Única função de parsing JSON com 3 modos

    Substitui 3 funções antigas:
    - _extract_json_from_text() → mode='balanced', allow_arrays=True
    - parse_llm_json_strict() → mode='strict', allow_arrays=False
    - _soft_json_cleanup() → usado internamente em mode='soft'

    Args:
        text: Texto contendo JSON (possivelmente com ruído)
        mode: 'strict' (apenas objetos, raise em erro) |
              'soft' (cleanup trailing commas) |
              'balanced' (markdown + cleanup + balanceamento) [DEFAULT]
        allow_arrays: Se True, aceita arrays na raiz; se False, apenas objetos

    Returns:
        Dict/List parseado ou None (mode='balanced') / raises (mode='strict')

    Raises:
        ContractValidationError: Se mode='strict' e JSON inválido
    """
    if not text or not isinstance(text, str):
        if mode == "strict":
            raise ContractValidationError("Empty or invalid LLM response")
        return None

    s = text.strip()

    # STEP 1: Remove markdown fences (todos os modos)
    s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.MULTILINE)
    s = re.sub(r"\s*```\s*$", "", s, flags=re.MULTILINE)
    s = s.strip()

    # MODE: STRICT (contracts - apenas objetos)
    if mode == "strict":
        m_open = s.find("{")
        m_close = s.rfind("}")
        if m_open == -1 or m_close == -1 or m_close <= m_open:
            raise ContractValidationError(
                f"JSON object not found in LLM response (len={len(s)})"
            )

        raw = s[m_open : m_close + 1]

        # Tentativa 1: parse direto
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass

        # Tentativa 2: limpar bytes de controle
        cleaned = re.sub(r"[\x00-\x1f\x7f]", "", raw)
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass

        # Tentativa 3: trailing commas
        cleaned2 = re.sub(r",\s*}", "}", cleaned)
        cleaned2 = re.sub(r",\s*\]", "]", cleaned2)
        try:
            return json.loads(cleaned2)
        except json.JSONDecodeError as e:
            preview = cleaned2[:200] + "..." if len(cleaned2) > 200 else cleaned2
            raise ContractValidationError(
                f"Invalid JSON from LLM (after cleaning): {e}\nPreview: {preview}"
            )

    # MODE: SOFT (apenas trailing commas cleanup)
    if mode == "soft":
        s2 = re.sub(r",\s*}\s*", r"}", s)
        s2 = re.sub(r",\s*\]\s*", r"]", s2)
        try:
            return json.loads(s2)
        except json.JSONDecodeError:
            return None

    # MODE: BALANCED (default - máximo esforço)
    # Tentativa 1: parse direto
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass

    # Tentativa 2: encontrar JSON (objeto ou array se permitido)
    if allow_arrays:
        json_pattern = re.compile(r"(\{[\s\S]*\}|\[[\s\S]*\])")
    else:
        json_pattern = re.compile(r"(\{[\s\S]*\})")

    m = json_pattern.search(s)
    if not m:
        logger.warning("No JSON pattern found in text")
        return None

    snippet = m.group(1).strip()

    # ✅ AGGRESSIVE CLEANUP (added for Analyst robustness)
    # Remove trailing commas, control chars, and fix common issues
    cleaned = re.sub(r"[\x00-\x1f\x7f]", "", snippet)
    cleaned = re.sub(r",\s*}", "}", cleaned)
    cleaned = re.sub(r",\s*\]", "]", cleaned)

    # Tentativa 3: parse cleaned
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Tentativa 4: balanceamento de chaves (para JSON malformado)
    try:
        # Count braces and brackets
        open_braces = cleaned.count("{")
        close_braces = cleaned.count("}")
        open_brackets = cleaned.count("[")
        close_brackets = cleaned.count("]")

        # Try to balance
        if open_braces > close_braces:
            cleaned += "}" * (open_braces - close_braces)
        if open_brackets > close_brackets:
            cleaned += "]" * (open_brackets - close_brackets)

        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    logger.warning(f"Failed to parse JSON after all attempts. Preview: {snippet[:200]}...")
    return None
def _extract_quality_metrics(facts_list: List[dict]) -> dict:
    """CONSOLIDADO (v4.3.1 - P1C): Extrai métricas de qualidade de lista de fatos

    Usado por:
    - JudgeLLM._validate_quality_rails()
    - Orchestrator._calculate_evidence_metrics_new()

    Returns:
        Dict com: domains, facts_with_evidence, facts_with_multiple_sources,
                 high_confidence_facts, contradictions, total_evidence
    """
    if not facts_list or not isinstance(facts_list, list):
        return {
            "domains": set(),
            "facts_with_evidence": 0,
            "facts_with_multiple_sources": 0,
            "high_confidence_facts": 0,
            "contradictions": 0,
            "total_evidence": 0,
        }

    domains = set()
    facts_with_evidence = 0
    facts_with_multiple_sources = 0
    high_confidence_facts = 0
    contradictions = 0
    total_evidence = 0

    for fact in facts_list:
        if not isinstance(fact, dict):
            continue

        evidencias = fact.get("evidencias", [])
        if evidencias and len(evidencias) > 0:
            facts_with_evidence += 1
            total_evidence += len(evidencias)

            # Extrair domínios
            fact_domains = set()
            for ev in evidencias:
                if not isinstance(ev, dict):
                    continue
                try:
                    url = ev.get("url", "")
                    if url:
                        domain = url.split("/")[2]
                        domains.add(domain)
                        fact_domains.add(domain)
                except:
                    pass

            # Contar múltiplas fontes
            if len(fact_domains) >= 2:
                facts_with_multiple_sources += 1

        # Contar alta confiança
        if fact.get("confiança") == "alta":
            high_confidence_facts += 1

        # Contar contradições
        if fact.get("contradicao", False):
            contradictions += 1

    return {
        "domains": domains,
        "facts_with_evidence": facts_with_evidence,
        "facts_with_multiple_sources": facts_with_multiple_sources,
        "high_confidence_facts": high_confidence_facts,
        "contradictions": contradictions,
        "total_evidence": total_evidence,
    }


def get_safe_llm_params(model_name: str, base_params: dict = None) -> dict:
    """Retorna parâmetros seguros para o modelo, removendo incompatíveis

    GPT-5/GPT-4.5/O1/O3: NÃO suportam temperature, max_tokens
    GPT-4/GPT-3.5: Suportam todos os parâmetros

    Args:
        model_name: Nome do modelo (ex: "gpt-5-mini")
        base_params: Parâmetros desejados (podem ser filtrados)

    Returns:
        Dict com parâmetros seguros para o modelo
    """
    if not base_params:
        base_params = {}

    model_lower = (model_name or "").lower()
    is_new_gen = any(
        [
            "gpt-4.1" in model_lower,
            "gpt-4.5" in model_lower,
            "gpt-5" in model_lower,
            "o1" in model_lower,
            "o3" in model_lower,
            model_lower.startswith("chatgpt-"),
        ]
    )

    safe_params = {}

    # response_format: suportado por todos
    if "response_format" in base_params:
        safe_params["response_format"] = base_params["response_format"]

    # temperature: NÃO suportado por modelos novos
    if "temperature" in base_params and not is_new_gen:
        safe_params["temperature"] = base_params["temperature"]

    # request_timeout: sempre seguro (não vai no body da API)
    if "request_timeout" in base_params:
        safe_params["request_timeout"] = base_params["request_timeout"]

    # max_tokens/max_completion_tokens: NÃO enviar (deixar model defaults)
    # OpenAI rejeita para alguns modelos

    return safe_params


def _extract_phase_count(prompt: str) -> Optional[int]:
    if not prompt:
        return None
    patterns = [
        r"(?:com|use|em|fazer|faz)\s+(\d+)\s*(?:fases?|etapas?|passos?)",
        r"(\d+)\s*(?:fases?|etapas?|passos?)",
        r"(?:dividir|separar|organizar)\s+em\s+(\d+)",
        r"(?:primeiro|segundo|terceiro|quarto|quinto)",
    ]
    for pattern in patterns:
        match = re.search(pattern, prompt.lower())
        if match:
            try:
                if pattern == r"(?:primeiro|segundo|terceiro|quarto|quinto)":
                    # Count ordinal numbers
                    ordinals = ["primeiro", "segundo", "terceiro", "quarto", "quinto"]
                    count = 0
                    for ordinal in ordinals:
                        if ordinal in prompt.lower():
                            count += 1
                    if count >= 2:
                        return count
                else:
                    num = int(match.group(1))
                    if 2 <= num <= 10:
                        return num
            except:
                pass
    return None


def _get_llm(
    valves, generation_kwargs: Optional[dict] = None, model_name: Optional[str] = None
) -> Optional['AsyncOpenAIClient']:
    model = model_name or getattr(valves, "LLM_MODEL", "") or os.getenv("LLM_MODEL", "") or ""
    base = getattr(valves, "OPENAI_BASE_URL", "") or os.getenv("OPENAI_BASE_URL", "") or ""
    key = f"{model}::{base}"
    # Fast path (read): if present and not None, return; else build under lock
    cached = _LLM_CACHE.get(key)
    if cached is not None:
        return cached

    with _LLM_CACHE_LOCK:
        # Double-check inside lock
        cached = _LLM_CACHE.get(key)
        if cached is not None:
            return cached

        api_key = (
            getattr(valves, "OPENAI_API_KEY", "") or os.getenv("OPENAI_API_KEY") or ""
        )

        # Debug info
        logger.debug("Model: %s", model)
        logger.debug("Base URL: %s", base)
        logger.debug("API Key: %s", "SET" if api_key else "NOT SET")

        if not all([base, api_key, model]):
            logger.error(
                "Missing configuration: base=%s, api_key=%s, model=%s",
                bool(base),
                bool(api_key),
                bool(model),
            )
            raise LLMConfigurationError(
                "LLM configuration missing: set OPENAI_BASE_URL, OPENAI_API_KEY and LLM_MODEL or corresponding valves"
            )

        # Import AsyncOpenAIClient here to avoid circular imports
        llm = AsyncOpenAIClient(base, api_key, model, valves)
        _LLM_CACHE[key] = llm
        return llm


async def _safe_llm_run_with_retry(
    llm_obj,
    prompt_text: str,
    generation_kwargs: Optional[dict] = None,
    timeout: int = 60,
    max_retries: int = 3,
) -> Optional[dict]:
    """LLM call with exponential backoff retry"""
    for attempt in range(max_retries):
        try:
            # Ensure generation_kwargs is a dict to avoid passing None
            payload = generation_kwargs or {}
            result = await llm_obj.run(
                prompt=prompt_text,
                generation_kwargs=payload,
            )
            return result
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"LLM call failed after {max_retries} attempts: {e}")
                raise
            else:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"LLM call attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)

    return None


# ==================== DEDUPLICATION ENGINE ====================

def _mmr_select(
    chunks: List[str],
    k: int = 50,
    lambda_div: float = 0.7,
    preserve_order: bool = True,
    similarity_threshold: float = 0.6,
    randomize: bool = False,
    reference_chunks: List[str] = None,
) -> List[str]:
    """MMR com seleção justa e preservação de narrativa

    Args:
        chunks: Lista de parágrafos/chunks
        k: Número máximo de chunks a selecionar
        lambda_div: Peso diversidade (0.0-1.0, maior = mais conservador)
        preserve_order: True = shuffle → select → reorder (narrativa), False = order by size
        similarity_threshold: Threshold para considerar similar (0.0-1.0)
        randomize: Se True, embaralha chunks antes de selecionar
        reference_chunks: Chunks de referência (dedupe candidates CONTRA estes)

    Returns:
        Lista de chunks selecionados (preservando ordem original se preserve_order=True)
    """
    # Preparar índices originais para preservar ordem depois
    indexed_chunks = [(i, chunk) for i, chunk in enumerate(chunks)]

    # Estratégia de seleção: pode embaralhar os antigos para seleção justa
    # - Se preserve_order=True e randomize=True: shuffle nos antigos, reordena ao final
    # - Se preserve_order=True e randomize=False: varre em ordem original
    # - Se preserve_order=False: prioriza chunks maiores primeiro
    if preserve_order:
        pool = list(indexed_chunks)
        if randomize:
            import random
            random.shuffle(pool)
    else:
        pool = sorted(indexed_chunks, key=lambda x: len(x[1]), reverse=True)

    selected_with_indices = []
    selected_sh: List[set] = []

    # Se há reference_chunks, inicializar selected_sh com eles (dedupe CONTRA referência)
    if reference_chunks:
        for ref_chunk in reference_chunks:
            selected_sh.append(_shingles(ref_chunk))

    for original_idx, chunk in pool:
        if len(selected_with_indices) >= k:
            break

        s_sh = _shingles(chunk)

        # Calcular similaridade máxima com selecionados (inclui reference se houver)
        sim = max((0.0,) + tuple(_jaccard(s_sh, prev_sh) for prev_sh in selected_sh))

        # Score MMR: relevância (tamanho) - penalidade de similaridade
        score = lambda_div * (len(chunk) / 1000.0) - (1 - lambda_div) * sim

        # Aceitar se: baixa similaridade OU score positivo
        if sim < similarity_threshold or score > 0:
            selected_with_indices.append((original_idx, chunk))
            selected_sh.append(s_sh)

    # Reordenar para preservar narrativa (se habilitado)
    if preserve_order and selected_with_indices:
        selected_with_indices.sort(key=lambda x: x[0])  # já está em ordem, mas garantir

    return [chunk for _, chunk in selected_with_indices]
class Deduplicator:
    """Deduplicação centralizada com múltiplos algoritmos (v4.4)

    Algoritmos disponíveis:
    - mmr: Maximal Marginal Relevance (padrão, O(n²))
    - minhash: MinHash LSH (rápido, O(n), requer datasketch)
    - tfidf: TF-IDF + Cosine Similarity (semântico, requer sklearn)

    Features:
    - Preservação de ordem original (narrativa)
    - Métricas de qualidade (reduction %, tokens saved)
    - Fallback automático se biblioteca não disponível
    """

    def __init__(self, valves):
        self.valves = valves

    def dedupe(
        self,
        chunks: List[str],
        max_chunks: int,
        algorithm: str = None,
        threshold: float = None,
        preserve_order: bool = True,
        preserve_recent_pct: float = 0.0,
        shuffle_older: bool = False,
        reference_first: bool = False,
        # NOVO: Context-aware parameters
        must_terms: Optional[List[str]] = None,
        key_questions: Optional[List[str]] = None,
        enable_context_aware: Optional[bool] = None,
        avoid_terms: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Deduplicação unificada com escolha de algoritmo

        Args:
            chunks: Lista de parágrafos/chunks
            max_chunks: Número máximo a retornar
            algorithm: 'mmr' | 'minhash' | 'tfidf' | 'semantic' (None = usa valve)
            threshold: Similaridade threshold (None = usa valve)
            preserve_order: Reordenar para manter narrativa
            preserve_recent_pct: % de chunks recentes a preservar intactos (0.0-1.0)
            shuffle_older: embaralhar seleção dos CHUNKS ANTIGOS (e reordenar ao final)
            reference_first: se True, recent são REFERÊNCIA (dedupe older CONTRA recent)
            must_terms: Termos que devem ser preservados (context-aware)
            key_questions: Questões-chave para matching (context-aware)
            enable_context_aware: Ativar preservação de chunks críticos (None = usa valve)
            avoid_terms: **OPCIONAL - ruído a evitar (deixe [] se não houver)**
             * Use quando há PALAVRAS-CHAVE específicas que causam falsos positivos
             * **EXEMPLOS DE USO:**
               - Fase "executive search": avoid_terms: ["vagas", "currículos", "job board"] → evita posts de vagas
               - Fase "aquisições": avoid_terms: ["promoção", "desconto"] → evita e-commerce
               - Fase "saúde digital": avoid_terms: ["fitness", "app gratuito"] → evita consumer health apps
             * **QUANDO NÃO USAR (deixe vazio []):**
               - Fases genéricas de mercado (risco de bloquear conteúdo válido)
               - Quando não há ruído óbvio e bem definido
               - Quando objetivo é amplitude/descoberta (melhor deixar Discovery trabalhar)
             * **REGRA PRÁTICA:** Se duvidoso, deixe [] - Discovery é resiliente a ruído

        Returns:
            Dict com: chunks (deduped), original_count, deduped_count, reduction_pct, tokens_saved

        DIVERSITY CAPS ENFORCEMENT (context-aware):
        Quando habilitado, tenta garantir cobertura mínima por categoria:
        - min_new_domains: domínios únicos (evita echo chamber)
        - min_official: fontes oficiais (gov, reguladores)
        - min_independent: fontes independentes (imprensa, academia)

        Estratégia:
        1) Context-aware prioritization (must_terms, key_questions)
        2) Deduplicação do restante (low priority)
        3) First pass: preencher quotas de diversidade
        4) Second pass: completar slots restantes por score
        5) Restaurar ordem original dos chunks selecionados
        """
        if not chunks:
            return {
                "chunks": [],
                "original_count": 0,
                "deduped_count": 0,
                "reduction_pct": 0.0,
                "tokens_saved": 0,
                "algorithm_used": "none",
            }

        algorithm = algorithm or getattr(self.valves, "DEDUP_ALGORITHM", "mmr")
        threshold = (
            threshold
            if threshold is not None
            else getattr(self.valves, "DEDUP_SIMILARITY_THRESHOLD", 0.85)
        )

        original_count = len(chunks)

        # Context-aware prioritization (se ativado)
        enable_context_aware = (
            enable_context_aware 
            if enable_context_aware is not None 
            else getattr(self.valves, "ENABLE_CONTEXT_AWARE_DEDUP", False)
        )
        
        if enable_context_aware and (must_terms or key_questions):
            # Usar context-aware prioritization
            preserve_top_pct = getattr(self.valves, "CONTEXT_AWARE_PRESERVE_PCT", 0.3)
            
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[CONTEXT_AWARE] Ativado: preserve_top_pct={preserve_top_pct}")
                print(f"[CONTEXT_AWARE] Must terms: {must_terms}")
                print(f"[CONTEXT_AWARE] Key questions: {key_questions}")
                print(f"[CONTEXT_AWARE] Input: {len(chunks)} chunks -> Target: {max_chunks}")
            
            high_priority, low_priority = self._context_aware_prioritize(
                chunks, must_terms, key_questions, preserve_top_pct
            )  # Agora retorna [(idx, chunk), ...]

            # Capear high_count antes de calcular available_slots (P0 fix)
            high_count = len(high_priority)
            high_count = min(high_count, max_chunks)
            available_slots = max_chunks - high_count

            # Priorizar must_terms → key_questions → position quando high > max
            if len(high_priority) > max_chunks:
                # Ordenar high_priority por tipo de score (must > question > position)
                high_priority_scored = []
                for idx, chunk in high_priority:
                    chunk_lower = chunk.lower()
                    must_score = sum(chunk_lower.count(t.lower()) * 2.0 for t in (must_terms or []))
                    q_score = sum(
                        len(set(q.lower().split()).intersection(set(chunk_lower.split()))) * 1.5
                        for q in (key_questions or [])
                    )
                    pos_score = idx / len(chunks) * 0.1
                    high_priority_scored.append((must_score, q_score, pos_score, idx, chunk))
                
                # Sort: must_terms primeiro, depois questions, depois position
                high_priority_scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
                high_priority = [(x[3], x[4]) for x in high_priority_scored[:max_chunks]]
                available_slots = 0
            else:
                high_priority = high_priority[:high_count]

            # Dedupear low_priority se houver slots disponíveis
            final_tuples = list(high_priority)  # [(idx, chunk), ...]

            # ===== Diversity caps enforcement (best-effort) =====
            # Determine profile and caps from valves
            intent_profile = getattr(self, "_intent_profile", "default") or "default"
            caps_by_profile = getattr(self.valves, "DIVERSITY_CAPS_BY_PROFILE", {}) or {}
            caps = caps_by_profile.get(intent_profile, caps_by_profile.get("default", {}))
            min_new_domains = int(caps.get("min_new_domains", 0))
            min_official = int(caps.get("min_official", 0))
            min_independent = int(caps.get("min_independent", 0))

            def _domain_from_chunk(text: str) -> Optional[str]:
                # Very lightweight domain extraction from "URL: ..." lines
                try:
                    for line in text.splitlines():
                        if line.startswith("URL:"):
                            url = line.split("URL:", 1)[1].strip()
                            # Extract host
                            host = url.split("//", 1)[-1].split("/", 1)[0]
                            return host.lower()
                except Exception:
                    return None
                return None

            # Build current category counters for already selected
            selected_domains = set()
            selected_official = 0
            selected_independent = 0

            official_domains = set(
                (getattr(self.valves, "OFFICIAL_DOMAINS", {}) or {})
                    .get(getattr(self, "_intent_profile", "default"), [])
            )

            for _, ch in final_tuples:
                dom = _domain_from_chunk(ch)
                if not dom:
                    continue
                selected_domains.add(dom)
                if any(off in dom for off in official_domains):
                    selected_official += 1
                else:
                    selected_independent += 1

            if available_slots > 0 and low_priority:
                # Criar dicionário chunk → [indices] para mapeamento robusto
                low_chunks = [ch for _, ch in low_priority]
                chunk_to_indices = {}
                for idx, chunk in low_priority:
                    if chunk not in chunk_to_indices:
                        chunk_to_indices[chunk] = []
                    chunk_to_indices[chunk].append(idx)
                
                # Dedupear apenas os chunks (sem índices)
                deduped_low = self._dedupe_chunks(low_chunks, available_slots, algorithm, threshold)

                # First pass: satisfy diversity caps
                if min_new_domains or min_official or min_independent:
                    added_now = 0
                    for chunk in list(deduped_low):
                        if added_now >= available_slots:
                            break
                        dom = _domain_from_chunk(chunk)
                        if not dom:
                            continue
                        is_official = any(off in dom for off in official_domains)
                        improves_new = dom not in selected_domains and len(selected_domains) < min_new_domains
                        improves_official = is_official and selected_official < min_official
                        improves_indep = (not is_official) and selected_independent < min_independent
                        if improves_new or improves_official or improves_indep:
                            if chunk in chunk_to_indices and chunk_to_indices[chunk]:
                                idx = chunk_to_indices[chunk].pop(0)
                                final_tuples.append((idx, chunk))
                                selected_domains.add(dom)
                                if is_official:
                                    selected_official += 1
                                else:
                                    selected_independent += 1
                                added_now += 1
                                available_slots -= 1
                    # Remove items already used in first pass
                    for _, items in list(chunk_to_indices.items()):
                        while items and any(t[0] == items[0] for t in final_tuples):
                            items.pop(0)
                
                # Reconstruir com índices originais
                for chunk in deduped_low:
                    if available_slots <= 0:
                        break
                    if chunk in chunk_to_indices and chunk_to_indices[chunk]:
                        idx = chunk_to_indices[chunk].pop(0)  # Pegar primeiro índice disponível
                        final_tuples.append((idx, chunk))
                        available_slots -= 1

            # SEMPRE restaurar ordem original (1b: forçar preserve_order=True)
            final_tuples.sort(key=lambda x: x[0])  # Ordenar por índice original
            final_chunks = [chunk for _, chunk in final_tuples]

            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[CONTEXT_AWARE] Ordem restaurada: {len(final_chunks)} chunks preservam timeline original")
                # Mostrar preview de chunks com URL
                for i, chunk in enumerate(final_chunks[:3]):
                    if chunk.startswith("URL:"):
                        url_line = chunk.split('\n')[0]
                        print(f"[CONTEXT_AWARE]   [{i}] {url_line[:80]}...")

            # Métricas
            deduped_count = len(final_chunks)
                
            # Retornar resultado context-aware
            result = {
                "chunks": final_chunks,
                "original_count": original_count,
                "deduped_count": deduped_count,
                "reduction_pct": (original_count - deduped_count) / original_count * 100,
                "tokens_saved": 0,  # TODO: calcular tokens saved
                "algorithm_used": f"context_aware_{algorithm}",
                "fallback_occurred": False,  # Context-aware não usa fallback
                "dependencies_checked": True,
            }
            
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[CONTEXT_AWARE] Final: {original_count} -> {deduped_count} chunks ({result['reduction_pct']:.1f}% reduction)")
            
            return result
        
        # Separar chunks recentes se solicitado (para Analyst)
        if preserve_recent_pct > 0:
            recent_count = max(10, int(len(chunks) * preserve_recent_pct))
            recent_chunks = chunks[-recent_count:]
            older_chunks = chunks[:-recent_count]
            effective_max = max_chunks - len(recent_chunks)
        else:
            recent_chunks = []
            older_chunks = chunks
            effective_max = max_chunks

        # Aplicar algoritmo escolhido
        # Se reference_first=True, dedupear older CONTRA recent (recent como referência)
        reference_chunks_for_mmr = recent_chunks if reference_first else []

        try:
            if algorithm == "minhash":
                deduped_older = self._minhash_dedupe(
                    older_chunks,
                    threshold,
                    effective_max,
                    reference_chunks=reference_chunks_for_mmr,
                )
                algo_used = "minhash"
            elif algorithm == "tfidf":
                deduped_older = self._tfidf_dedupe(
                    older_chunks,
                    threshold,
                    effective_max,
                    reference_chunks=reference_chunks_for_mmr,
                )
                algo_used = "tfidf"
            elif algorithm == "semantic":
                try:
                    model_name = getattr(self.valves, "SEMANTIC_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
                    print(f"[DEDUP] 🧠 Usando algoritmo SEMANTIC com modelo {model_name}")
                    deduped_older = self._semantic_dedupe(
                        older_chunks,
                        threshold,
                        effective_max,
                        reference_chunks=reference_chunks_for_mmr,
                        model_name=model_name,
                    )
                    algo_used = "semantic"
                except (ImportError, AttributeError) as e:
                    print(f"[DEDUP] ⚠️ Fallback para MMR: {e}")
                    # Fallback para MMR se Haystack não disponível
                    deduped_older = _mmr_select(
                        chunks=older_chunks,
                        k=effective_max,
                        lambda_div=getattr(self.valves, "DEDUP_RELEVANCE_WEIGHT", 0.7),
                        preserve_order=True,
                        similarity_threshold=threshold,
                        randomize=shuffle_older,
                        reference_chunks=reference_chunks_for_mmr,
                    )
                    algo_used = "mmr_fallback"
            else:  # mmr (default)
                print(f"[DEDUP] Usando algoritmo MMR (default)")
                deduped_older = _mmr_select(
                    chunks=older_chunks,
                    k=effective_max,
                    lambda_div=getattr(self.valves, "DEDUP_RELEVANCE_WEIGHT", 0.7),
                    preserve_order=True,  # Preserve a ordem durante a seleção
                    similarity_threshold=threshold,
                    randomize=shuffle_older,
                    reference_chunks=reference_chunks_for_mmr,  # Dedupe older CONTRA recent
                )
                algo_used = "mmr"
        except ImportError as e:
            # Fallback para MMR se biblioteca não disponível
            logger.warning(
                f"Algorithm '{algorithm}' not available ({e}), falling back to MMR"
            )
            deduped_older = _mmr_select(
                chunks=older_chunks,
                k=effective_max,
                lambda_div=getattr(self.valves, "DEDUP_RELEVANCE_WEIGHT", 0.7),
                preserve_order=True,
                similarity_threshold=threshold,
                randomize=shuffle_older,
                reference_chunks=reference_chunks_for_mmr,
            )
            algo_used = "mmr_fallback"

        # Combinar: Se reference_first, recent VÊM PRIMEIRO
        if reference_first:
            result = (recent_chunks + deduped_older)[:max_chunks]
        else:
            result = (deduped_older + recent_chunks)[:max_chunks]

        # Preservar ordem original se solicitado
        if preserve_order:
            result = self._restore_order(result, chunks)

        deduped_count = len(result)
        reduction_pct = (
            ((original_count - deduped_count) / original_count * 100)
            if original_count > 0
            else 0
        )
        tokens_saved = (original_count - deduped_count) * 30  # ~30 tokens/parágrafo

        # Detectar se houve fallback
        fallback_occurred = algo_used.endswith("_fallback")
        
        # Log de telemetria para monitoramento
        if fallback_occurred:
            print(f"[DEDUP] TELEMETRIA: Fallback detectado - algoritmo solicitado vs usado: {algorithm} -> {algo_used}")
        
        return {
            "chunks": result,
            "original_count": original_count,
            "deduped_count": deduped_count,
            "reduction_pct": reduction_pct,
            "tokens_saved": tokens_saved,
            "algorithm_used": algo_used,
            "fallback_occurred": fallback_occurred,
            "dependencies_checked": True,
        }

    def _minhash_dedupe(
        self,
        chunks: List[str],
        threshold: float,
        max_chunks: int,
        reference_chunks: List[str] = None,
    ) -> List[str]:
        """MinHash LSH deduplicação - O(n) - requer datasketch

        Args:
            reference_chunks: Se fornecido, dedupe chunks CONTRA estes (já no LSH)
        """
        try:
            from datasketch import MinHash, MinHashLSH
        except ImportError:
            raise ImportError("datasketch required: pip install datasketch")

        lsh = MinHashLSH(threshold=threshold, num_perm=128)
        unique_chunks = []

        # Se há reference_chunks, inserir no LSH primeiro (dedupe CONTRA eles)
        if reference_chunks:
            for i, ref_chunk in enumerate(reference_chunks):
                m = MinHash(num_perm=128)
                for word in ref_chunk.split():
                    m.update(word.encode("utf8"))
                lsh.insert(f"ref_{i}", m)

        for i, chunk in enumerate(chunks):
            if len(unique_chunks) >= max_chunks:
                break

            # Criar MinHash signature
            m = MinHash(num_perm=128)
            for word in chunk.split():
                m.update(word.encode("utf8"))

            # Query LSH para similares
            result = lsh.query(m)
            if not result:  # Nenhum similar encontrado
                lsh.insert(f"chunk_{i}", m)
                unique_chunks.append(chunk)

        return unique_chunks

    def _tfidf_dedupe(
        self,
        chunks: List[str],
        threshold: float,
        max_chunks: int,
        reference_chunks: List[str] = None,
    ) -> List[str]:
        """TF-IDF + Cosine Similarity deduplicação - requer sklearn

        Args:
            reference_chunks: Se fornecido, dedupe chunks CONTRA estes
        """
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import cosine_similarity
        except ImportError:
            raise ImportError("sklearn required: pip install scikit-learn")

        if not chunks:
            return []

        # Se há reference_chunks, processar junto para TF-IDF consistente
        all_chunks = (reference_chunks or []) + chunks
        ref_count = len(reference_chunks) if reference_chunks else 0

        # Criar TF-IDF matrix
        vectorizer = TfidfVectorizer(ngram_range=(1, 3), min_df=1, max_df=0.95)
        tfidf_matrix = vectorizer.fit_transform(all_chunks)

        selected = []
        selected_indices = []

        # Se há reference, considerar todos eles como "já selecionados"
        if ref_count > 0:
            selected_indices = list(range(ref_count))

        # Iterar apenas sobre chunks (não reference)
        for i in range(ref_count, len(all_chunks)):
            chunk_idx_in_original = i - ref_count
            chunk = chunks[chunk_idx_in_original]

            if len(selected) >= max_chunks:
                break

            if not selected_indices:
                selected.append(chunk)
                selected_indices.append(i)
                continue

            # Calcular similaridade com já selecionados (inclui reference)
            chunk_vec = tfidf_matrix[i : i + 1]
            if selected_indices:
                selected_vecs = tfidf_matrix[selected_indices, :]
                similarities = cosine_similarity(chunk_vec, selected_vecs)
                max_sim = similarities.max()
            else:
                max_sim = 0.0
            if max_sim < threshold:
                selected.append(chunk)
                selected_indices.append(i)

        return selected

    def _restore_order(self, deduped: List[str], original: List[str]) -> List[str]:
        """Restaura ordem original dos chunks dedupados"""
        if not deduped or not original:
            return deduped

        # Criar mapa: chunk → índice original
        original_indices = {chunk: i for i, chunk in enumerate(original)}

        # Ordenar deduped pela ordem original
        ordered = sorted(deduped, key=lambda x: original_indices.get(x, 999999))
        return ordered

    def _semantic_dedupe(
        self,
        chunks: List[str],
        threshold: float,
        max_chunks: int,
        reference_chunks: List[str] = None,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    ) -> List[str]:
        """Deduplicação semântica usando embeddings (Haystack)
        
        Args:
            chunks: Parágrafos a dedupear
            threshold: Cosine similarity threshold (0.0-1.0, default 0.85)
            max_chunks: Número máximo a retornar
            reference_chunks: Se fornecido, dedupe chunks CONTRA estes
            model_name: Modelo de embeddings (lightweight por padrão)
        
        Returns:
            Lista de chunks únicos semanticamente
            
        Raises:
            ImportError: Se Haystack não estiver disponível (fallback para MMR)
        """
        if not HAYSTACK_AVAILABLE:
            print(f"[DEDUP] DEPENDENCIA FALTANDO: Haystack/sentence-transformers não instalado")
            print(f"[DEDUP] INSTALAR: pip install haystack sentence-transformers scikit-learn")
            raise ImportError("Haystack/sentence-transformers required for semantic dedup")
        
        if not chunks:
            return []
        
        # Preparar documentos para Haystack
        docs = [Document(content=chunk, meta={"original_idx": i}) 
                for i, chunk in enumerate(chunks)]
        
        # Reference documents (se fornecido)
        if reference_chunks:
            ref_docs = [Document(content=ref, meta={"is_reference": True}) 
                        for ref in reference_chunks]
            all_docs = ref_docs + docs
        else:
            all_docs = docs
        
        # Embedder (in-memory, sem persistência)
        try:
            embedder = SentenceTransformersDocumentEmbedder(model=model_name)
            embedder.warm_up()
            embedded_docs = embedder.run(all_docs)["documents"]
        except Exception as e:
            print(f"[DEDUP] Semantic embedder failed: {e}")
            raise ImportError(f"Semantic model load failed: {e}")
        
        # Extrair embeddings como numpy array
        embeddings = np.array([doc.embedding for doc in embedded_docs])
        
        # Calcular matriz de similaridade (cosine)
        from sklearn.metrics.pairwise import cosine_similarity
        similarity_matrix = cosine_similarity(embeddings)
        
        # Selecionar chunks únicos por clustering simples
        selected_indices = []
        excluded_indices = set()
        
        # Se há referências, marcar como já selecionadas
        if reference_chunks:
            num_refs = len(reference_chunks)
            excluded_indices.update(range(num_refs))
            start_idx = num_refs
        else:
            start_idx = 0
        
        for i in range(start_idx, len(similarity_matrix)):
            if i in excluded_indices:
                continue
            
            # Verificar se similar a algum já selecionado ou referência
            is_duplicate = False
            for j in selected_indices + list(range(start_idx)):
                if i != j and similarity_matrix[i][j] >= threshold:
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                selected_indices.append(i)
                if len(selected_indices) >= max_chunks:
                    break
        
        # Mapear de volta para chunks originais
        result = [chunks[embedded_docs[i].meta["original_idx"]] 
                  for i in selected_indices if i >= start_idx]
        
        return result

    def _context_aware_prioritize(
        self,
        chunks: List[str],
        must_terms: Optional[List[str]] = None,
        key_questions: Optional[List[str]] = None,
        preserve_top_pct: float = 0.3
    ) -> Tuple[List[Tuple[int, str]], List[Tuple[int, str]]]:
        """
        Separa chunks em alta e baixa prioridade baseado em contexto.
        
        Args:
            chunks: Lista de chunks para priorizar
            must_terms: Termos que devem ser preservados (weight: 2.0)
            key_questions: Questões-chave para matching (weight: 1.5)
            preserve_top_pct: % de chunks para alta prioridade (default: 0.3)
            
        Returns:
            (high_priority_chunks, low_priority_chunks) where each is List[Tuple[int, str]] (index, chunk)
        """
        if not chunks:
            return [], []
            
        if not must_terms and not key_questions:
            # Se não há contexto, retornar chunks recentes como high priority
            high_count = max(1, int(len(chunks) * preserve_top_pct))
            return [(len(chunks)-high_count+i, ch) for i, ch in enumerate(chunks[-high_count:])], \
                   [(i, ch) for i, ch in enumerate(chunks[:-high_count])]
        
        # Calcular score para cada chunk
        chunk_scores = []
        for i, chunk in enumerate(chunks):
            # LLM-first: Deixar o LLM decidir qualidade através do scoring inteligente
            chunk_lower = chunk.lower()
            score = 0.0
            must_score = 0.0
            question_score = 0.0
            
            # 1. Must terms (weight: 2.0) - LLM-first: scoring inteligente
            if must_terms:
                for term in must_terms:
                    term_lower = term.lower()
                    
                    # Ignorar termos geográficos usando detecção estrutural
                    if _is_geographic_term(term):
                        if getattr(self.valves, "VERBOSE_DEBUG", False):
                            print(f"[CONTEXT_AWARE] Skipping geo term: '{term}'")
                        continue
                    
                    # Contar ocorrências (case-insensitive)
                    count = chunk_lower.count(term_lower)
                    
                    # LLM-first: Bonus para co-ocorrência com contexto setorial
                    setorial_bonus = 0.0
                    if any(setor in chunk_lower for setor in [
                        "executive search", "headhunting", "recrutamento executivo", 
                        "search executivo", "consultoria executiva"
                    ]):
                        setorial_bonus = 1.0  # Bonus por contexto setorial correto
                    
                    term_score = (count * 2.0) + setorial_bonus
                    must_score += term_score
                    
            # 2. Key questions overlap (weight: 1.5)
            if key_questions:
                for question in key_questions:
                    question_lower = question.lower()
                    # Verificar se chunk contém palavras-chave da questão
                    question_words = set(question_lower.split())
                    chunk_words = set(chunk_lower.split())
                    overlap = len(question_words.intersection(chunk_words))
                    if overlap > 0:
                        q_score = overlap * 1.5
                        question_score += q_score
            
            # 3. Posição no documento (recent > old, weight: 0.1)
            position_score = (i / len(chunks)) * 0.1
            score = must_score + question_score + position_score
            
            chunk_scores.append((score, i, chunk))
            
            # Debug logging para chunks com score alto
            if getattr(self.valves, "VERBOSE_DEBUG", False) and score > 1.0:
                print(f"[CONTEXT_AWARE] Chunk {i}: score={score:.2f} (must={must_score:.2f}, q={question_score:.2f}, pos={position_score:.2f})")
                print(f"[CONTEXT_AWARE]    Preview: {chunk[:100]}...")
        
        # Ordenar por score (maior primeiro)
        chunk_scores.sort(key=lambda x: x[0], reverse=True)
        
        # Separar em high/low priority
        high_count = max(1, int(len(chunks) * preserve_top_pct))
        high_priority = [(chunk_scores[i][1], chunk_scores[i][2]) for i in range(high_count)]  # (index, chunk)
        low_priority = [(chunk_scores[i][1], chunk_scores[i][2]) for i in range(high_count, len(chunks))]
        
        return high_priority, low_priority

    def _dedupe_chunks(
        self,
        chunks: List[str],
        max_chunks: int,
        algorithm: str,
        threshold: float
    ) -> List[str]:
        """
        Método auxiliar para dedupear chunks (usado pelo context-aware).
        Aplica o algoritmo especificado aos chunks fornecidos.
        """
        if not chunks or max_chunks <= 0:
            return []
            
        try:
            if algorithm == "minhash":
                return self._minhash_dedupe(chunks, threshold, max_chunks)
            elif algorithm == "tfidf":
                return self._tfidf_dedupe(chunks, threshold, max_chunks)
            elif algorithm == "semantic":
                model_name = getattr(self.valves, "SEMANTIC_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
                try:
                    return self._semantic_dedupe(chunks, threshold, max_chunks, model_name=model_name)
                except ImportError as e:
                    print(f"[DEDUP] Semantic unavailable: {e} → fallback to MMR")
                    return _mmr_select(chunks, max_chunks, similarity_threshold=threshold)
            else:  # default to mmr
                return _mmr_select(chunks, max_chunks, similarity_threshold=threshold)
        except Exception as e:
            # Fallback para MMR em caso de erro
            print(f"[DEDUP] FALLBACK CRITICO: {algorithm} -> MMR devido a: {e}")
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[DEDUP] Verifique dependencias: pip install scikit-learn datasketch sentence-transformers haystack")
            return _mmr_select(chunks, max_chunks, similarity_threshold=threshold)


# ==================== LLM CLIENT ====================

class AsyncOpenAIClient:
    """Async OpenAI client using httpx for better performance"""

    def __init__(self, base_url: str, api_key: str, model: str, valves=None):
        self.base_url = (base_url or "").rstrip("/")
        self.api_key = api_key
        self.model = model
        self.valves = valves  # Store valves for timeout configuration
        self._client = None  # Lazy initialization

    def _ensure_client(self):
        """Lazy initialization of httpx client"""
        if self._client is None:
            try:
                import httpx

                # Use valve timeout if available, otherwise default to 180s
                read_timeout = (
                    getattr(self.valves, "HTTPX_READ_TIMEOUT", 180)
                    if self.valves
                    else 180
                )
                self._client = httpx.AsyncClient(
                    timeout=httpx.Timeout(
                        240.0, connect=10.0, read=float(read_timeout)
                    ),  # Increased: 120→240, 5→10, 115→180
                    limits=httpx.Limits(
                        max_keepalive_connections=10, max_connections=20
                    ),
                    http2=True,  # Enable HTTP/2 for multiplexing
                    follow_redirects=True,
                )
            except ImportError:
                raise RuntimeError("httpx library required: pip install httpx")
        return self._client

    async def _call_api(
        self, prompt: str, generation_kwargs: Optional[dict] = None
    ) -> str:
        try:
            import httpx
        except ImportError:
            raise RuntimeError("httpx library required: pip install httpx")

        client = self._ensure_client()
        url = build_chat_endpoint(self.base_url)

        # Log do tamanho do prompt ANTES de enviar (debug crítico para truncamento)
        prompt_len = len(prompt)
        prompt_tokens_est = prompt_len // 4
        if prompt_tokens_est > 12000:
            logger.warning(
                f"[API] Prompt grande sendo enviado: {prompt_len:,} chars (~{prompt_tokens_est:,} tokens). Modelo '{self.model}' pode ter limite de input que cause truncamento!"
            )

        body = {"model": self.model, "messages": [{"role": "user", "content": prompt}]}
        gen_kwargs = generation_kwargs or {}

        # Detect capabilities by model name
        model_lower = (self.model or "").lower()
        is_new_gen_model = any(
            [
                "gpt-4.1" in model_lower,
                "gpt-4.5" in model_lower,
                "gpt-5" in model_lower,
                "o1" in model_lower,
                "o3" in model_lower,
                model_lower.startswith("chatgpt-"),
            ]
        )

        # Temperature: newer models often only support default (1.0)
        if "temperature" in gen_kwargs and not is_new_gen_model:
            body["temperature"] = gen_kwargs["temperature"]

        # Forward response_format when caller enforces JSON mode
        resp_fmt = gen_kwargs.get("response_format")
        if resp_fmt:
            body["response_format"] = resp_fmt

        # Do NOT send any token limits - let model defaults handle it
        # OpenAI API rejects max_tokens/max_completion_tokens for some models

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        # ✅ FIX TIMEOUT HIERARCHY: Unificar cliente/per-request usando MAX
        default_read = (
            getattr(self.valves, "HTTPX_READ_TIMEOUT", 180)
            if hasattr(self, "valves")
            else 180
        )
        request_timeout = float(gen_kwargs.get("request_timeout", default_read))
        effective_read_timeout = max(default_read, request_timeout, 60.0)

        # ✅ Criar timeout per-request (sobrescreve timeout do cliente)
        per_request_timeout = httpx.Timeout(
            240.0, connect=10.0, read=effective_read_timeout
        )

        if effective_read_timeout > 300:
            logger.info(
                f"[API] Long operation timeout: {effective_read_timeout}s (synthesis/large prompt)"
            )

        try:
            # ✅ Async HTTP POST com timeout PER-REQUEST explícito
            resp = await client.post(
                url, json=body, headers=headers, timeout=per_request_timeout
            )
            resp.raise_for_status()
            j = resp.json()
            text_parts = []
            for ch in j.get("choices", []):
                if isinstance(ch.get("message"), dict):
                    text_parts.append(ch["message"].get("content") or "")
                else:
                    text_parts.append(ch.get("text") or "")
            return "".join(text_parts).strip()
        except httpx.TimeoutException as e:
            logger.error(f"[API] HTTP timeout after {effective_read_timeout}s: {e}")
            raise TimeoutError(
                f"HTTP request timeout after {effective_read_timeout}s"
            ) from e
        except httpx.HTTPStatusError as e:
            error_detail = ""
            try:
                error_detail = f" - {e.response.text}"
            except:
                pass
            logger.error("HTTP Error %s: %s%s", e.response.status_code, e, error_detail)
            logger.debug("URL: %s", url)
            logger.debug("Body: %s", json.dumps(body, indent=2))
            raise RuntimeError(f"HTTP {e.response.status_code}: {e}") from e
        except Exception as e:
            logger.error("HTTP Error: %s", e)
            raise

    async def run(self, prompt: str, generation_kwargs: Optional[dict] = None) -> dict:
        result = await self._call_api(prompt, generation_kwargs)
        return {"replies": [result]}


# ==================== LLM COMPONENTS ====================

class AnalystLLM:
    """Analyst que processa contexto acumulado completo"""

    def __init__(self, valves):
        self.valves = valves
        # Usar modelo específico se configurado, senão usa modelo padrão
        model = valves.LLM_MODEL_ANALYST or valves.LLM_MODEL
        self.model_name = model
        self.llm = _get_llm(valves, model_name=model)
        # Base kwargs: serão filtrados por get_safe_llm_params (GPT-5 não aceita temperature)
        self.generation_kwargs = {"temperature": valves.LLM_TEMPERATURE}

    async def run(
        self, query: str, accumulated_context: str, phase_context: Dict = None
    ) -> Dict[str, Any]:
        """Analisa contexto acumulado COMPLETO (todas as fases até agora)"""

        # 🔴 DEFESA P0: Validar inputs e estado do LLM
        if not self.llm:
            logger.error("[ANALYST] LLM não configurado")
            return {"summary": "", "facts": [], "lacunas": ["LLM não configurado"]}

        if not accumulated_context or len(accumulated_context.strip()) == 0:
            logger.warning("[ANALYST] Contexto vazio - sem dados para analisar")
            return {
                "summary": "Sem contexto para analisar",
                "facts": [],
                "lacunas": ["Contexto vazio"],
            }

        try:
            # Extrair informações da fase atual
            phase_info = ""
            if phase_context:
                phase_name = phase_context.get("name", "Fase atual")
                # Contract usa "objetivo" (PT), não "objective" (EN)
                phase_objective = phase_context.get("objetivo") or phase_context.get(
                    "objective", ""
                )
                phase_info = f"\n**FASE ATUAL:** {phase_name}\n**Objetivo da Fase:** {phase_objective}"

            sys_prompt = _build_analyst_prompt(query, phase_context)

            user_prompt = f"""**Objetivo da Fase:** {query}{phase_info}
**Contexto Acumulado (todas as fases até agora):**
{accumulated_context}"""

            timeout_analyst = min(self.valves.LLM_TIMEOUT_ANALYST, 120)  # Cap at 120s to prevent truncation
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(
                    f"[DEBUG][ANALYST] Using timeout: {timeout_analyst}s (context: {len(accumulated_context):,} chars)"
                )
            # Use retry function if enabled, otherwise single attempt
            # Filtrar parâmetros incompatíveis com GPT-5/O1
            # ✅ FORCE JSON MODE for Analyst robustness
            base_params = {
                "temperature": self.generation_kwargs.get("temperature", 0.2),
                "response_format": {"type": "json_object"}  # FORCE JSON MODE
            }
            safe_params = get_safe_llm_params(self.model_name, base_params)

            if getattr(self.valves, "ENABLE_LLM_RETRY", True):
                max_retries = int(getattr(self.valves, "LLM_MAX_RETRIES", 3) or 3)
                out = await _safe_llm_run_with_retry(
                    self.llm,
                    f"{sys_prompt}\n\n{user_prompt}",
                    safe_params,
                    timeout=timeout_analyst,
                    max_retries=max_retries,
                )
            else:
                out = await _safe_llm_run_with_retry(
                    self.llm,
                    f"{sys_prompt}\n\n{user_prompt}",
                    safe_params,
                    timeout=timeout_analyst,
                    max_retries=1,
                )

            if not out:
                return {"summary": "", "facts": [], "lacunas": []}

            raw_reply = out.get("replies", [""])[0] if out else ""
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(
                    f"[DEBUG][ANALYST] Analyst raw reply length: {len(raw_reply)} chars"
                )
                print(
                    f"[DEBUG][ANALYST] Analyst raw reply preview: {raw_reply[:200]}..."
                )

            # 🔧 FIX v2: Strip agressivo para remover \n " no início (erro comum do LLM)
            cleaned_reply = raw_reply.strip()

            # Remover newlines e whitespace no início recursivamente
            while cleaned_reply and cleaned_reply[0] in "\n\r\t ":
                cleaned_reply = cleaned_reply[1:]

            # Se começa com " mas não é JSON válido, remover aspas soltas
            if cleaned_reply.startswith('"') and not cleaned_reply.startswith('{"'):
                # Remover todas as aspas duplas consecutivas no início
                cleaned_reply = cleaned_reply.lstrip('"').lstrip()

            # Se ainda não começa com { ou [, tentar envolver em objeto
            if (
                cleaned_reply
                and not cleaned_reply.startswith("{")
                and not cleaned_reply.startswith("[")
            ):
                # Caso especial: LLM retornou apenas os campos sem o envelope {}
                # Tentar envolver em chaves
                if '"summary"' in cleaned_reply or '"facts"' in cleaned_reply:
                    cleaned_reply = "{" + cleaned_reply + "}"
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            "[DEBUG][ANALYST] Wrapped response in {} (LLM forgot envelope)"
                        )

            # v5: Prefer structured outputs via Pydantic first, fallback to resilient parser
            parsed = None
            try:
                class EvidenceModel(BaseModel):
                    url: str
                    trecho: Optional[str] = None

                class FactModel(BaseModel):
                    texto: str
                    confiança: Literal["alta", "média", "baixa"]
                    evidencias: Optional[List[EvidenceModel]] = []

                class SelfAssessmentModel(BaseModel):
                    coverage_score: float
                    confidence: Literal["alta", "média", "baixa"]
                    gaps_critical: bool
                    suggest_refine: bool
                    suggest_pivot: bool
                    reasoning: Optional[str] = None

                class AnalystSchema(BaseModel):
                    summary: str = ""
                    facts: List[FactModel] = []
                    lacunas: List[str] = []
                    self_assessment: Optional[SelfAssessmentModel] = None

                # Try strict load via Pydantic (expecting JSON string)
                # If not JSON, fallback below
                try:
                    json_obj = json.loads(cleaned_reply)
                except Exception:
                    json_obj = None
                if json_obj is not None:
                    model_obj = AnalystSchema.model_validate(json_obj)
                    parsed = json.loads(model_obj.model_dump_json())
            except Exception:
                parsed = None

            if parsed is None:
                # Fallback resilient parser
                parsed = parse_json_resilient(
                    cleaned_reply, mode="balanced", allow_arrays=False
                )

            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[DEBUG] Analyst parsed: {parsed is not None}")
                if parsed:
                    print(f"[DEBUG] Analyst parsed keys: {list(parsed.keys())}")
                    print(
                        f"[DEBUG] Analyst facts count before validation: {len(parsed.get('facts', []))}"
                    )
                else:
                    # Log o motivo da falha
                    print(
                        f"[DEBUG] Analyst parsing FAILED. Raw reply length: {len(raw_reply)}"
                    )
                    print(f"[DEBUG] First 500 chars: {raw_reply[:500]}")
                    print(f"[DEBUG] Last 500 chars: {raw_reply[-500:]}")

            # Se falhou, tentar com mode='soft' (apenas cleanup)
            if not parsed and raw_reply:
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print("[DEBUG] Trying mode='soft' (cleanup only)...")
                parsed = parse_json_resilient(
                    raw_reply, mode="soft", allow_arrays=False
                )
                if parsed:
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print("[DEBUG] Analyst reparsed successfully with mode='soft'")

            # Re-ask único e curto exigindo JSON válido
            try_reask = not parsed
            if try_reask:
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        "[DEBUG] Analyst initial parse failed, re-asking with stricter JSON-only instructions..."
                    )

                reask_instr = (
                    "RETORNE APENAS JSON PURO (sem markdown, sem explicação, sem texto extra).\n\n"
                    "SCHEMA OBRIGATÓRIO:\n"
                    "{\n"
                    '  "summary": "string resumo",\n'
                    '  "facts": [{ "texto": "fato X", "confiança": "alta|média|baixa", "evidencias": [{"url":"...","trecho":"..."}] }],\n'
                    '  "lacunas": ["lacuna 1", "lacuna 2"],\n'
                    '  "self_assessment": { "coverage_score": 0.7, "confidence": "média", "gaps_critical": true, "suggest_refine": false, "suggest_pivot": true, "reasoning": "brevemente por quê" }\n'
                    "}\n\n"
                    "⚠️ IMPORTANTE:\n"
                    "- coverage_score: 0.0-1.0 (quanto % do objetivo foi coberto)\n"
                    "- gaps_critical: True se lacunas impedem resposta ao objetivo\n"
                    "- suggest_pivot: True se lacuna precisa de ângulo/temporal diferente\n\n"
                    "NÃO adicione comentários, NÃO use ```json, NÃO explique nada fora do JSON."
                )
                limited_context = accumulated_context[:20000]
                reask_prompt = f"{_build_analyst_prompt(query, phase_context)}\n\n{reask_instr}\n\n**Objetivo da Fase:** {query}{phase_info}\n\n**Contexto Acumulado:**\n{limited_context}"

                # Forçar JSON response_format quando suportado (evitar para modelos que não aceitam)
                base_reask = {"temperature": 0.1}
                if not any(x in self.model_name.lower() for x in ["o1", "o3", "gpt-5"]):
                    base_reask["response_format"] = {"type": "json_object"}
                safe_reask = get_safe_llm_params(self.model_name, base_reask)

                reask_out = await _safe_llm_run_with_retry(
                    self.llm,
                    reask_prompt,
                    safe_reask,
                    timeout=timeout_analyst,
                    max_retries=1,
                )
                re_raw = reask_out.get("replies", [""])[0] if reask_out else ""

                # Parse estrito primeiro; se falhar, tentar balanced
                reparsed = None
                try:
                    if re_raw:
                        json_obj2 = json.loads(re_raw)
                        model_obj2 = AnalystSchema.model_validate(json_obj2)
                        reparsed = json.loads(model_obj2.model_dump_json())
                except Exception:
                    reparsed = None
                if not reparsed and re_raw:
                    reparsed = parse_json_resilient(
                        re_raw, mode="balanced", allow_arrays=False
                    )
                if reparsed:
                    parsed = reparsed
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print("[DEBUG] Analyst parsed successfully on re-ask")

            # Validação de evidência rica (P0.4) - TEMPORARIAMENTE RELAXADA PARA DEBUG
            if parsed:
                facts_before_validation = parsed.get("facts", [])
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[DEBUG] Facts before validation: {len(facts_before_validation)}"
                    )

                # Log detalhado de cada fato antes da validação
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    for i, fact in enumerate(facts_before_validation[:3]):
                        print(f"[DEBUG] Fact {i}: {fact}")

                validated = self._validate_analyst_output(parsed)
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[DEBUG] Analyst validation result: {validated}")

                if not validated["valid"]:
                    # v4.6: Validação RE-HABILITADA (era temporariamente relaxada para debug)
                    logger.warning(f"[ANALYST] Output inválido: {validated['reason']}")
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            f"[WARNING] Analyst output invalid: {validated['reason']}"
                        )
                    # Retornar output vazio com lacuna explicativa
                    return {
                        "summary": "",
                        "facts": [],
                        "lacunas": [validated["reason"]],
                    }

        except Exception as e:
            import traceback as _tb
            tb_str = _tb.format_exc()
            err = PipelineError(
                stage="analyst",
                error_type=e.__class__.__name__,
                message=str(e),
                context={"context_len": len(accumulated_context)},
                traceback_str=tb_str,
                correlation_id=correlation_id,
            )
            logger.error(f"[ANALYST] {json.dumps(err.to_dict(), ensure_ascii=False)}")
            return {
                "summary": "",
                "facts": [],
                "lacunas": [f"Erro interno: {str(e)[:100]}"],
            }

        return parsed or {"summary": "", "facts": [], "lacunas": []}

    def _validate_analyst_output(self, parsed):
        """Valida saída do Analyst - VERSÃO COMPLETA RE-HABILITADA (v4.6)

        Validações:
        1. Facts são dicts com campos obrigatórios
        2. Evidências têm URL válida
        3. Self-assessment presente e bem-formado
        """
        facts = parsed.get("facts", [])

        # Sem fatos NÃO é válido: retorna lacuna explicativa
        if not facts:
            return {"valid": False, "reason": "Nenhum fato extraído (contexto vazio ou irrelevante)"}

        # Validação de estrutura dos fatos
        for i, fact in enumerate(facts):
            if not isinstance(fact, dict):
                return {"valid": False, "reason": f"Fato {i} não é dict"}

            # Campos obrigatórios
            if "texto" not in fact:
                return {"valid": False, "reason": f"Fato {i} sem campo 'texto'"}

            if not fact.get("texto") or not fact["texto"].strip():
                return {"valid": False, "reason": f"Fato {i} com texto vazio"}

            # Confiança obrigatória
            if "confiança" not in fact:
                return {"valid": False, "reason": f"Fato {i} sem campo 'confiança'"}

            if fact["confiança"] not in ["alta", "média", "baixa"]:
                return {
                    "valid": False,
                    "reason": f"Fato {i} com confiança inválida: {fact['confiança']}",
                }

            # Evidências (opcional mas recomendado)
            evidencias = fact.get("evidencias", [])
            if evidencias:
                for j, ev in enumerate(evidencias):
                    if not isinstance(ev, dict):
                        return {
                            "valid": False,
                            "reason": f"Fato {i}, evidência {j} não é dict",
                        }

                    if "url" not in ev:
                        return {
                            "valid": False,
                            "reason": f"Fato {i}, evidência {j} sem URL",
                        }

        # Validação de self_assessment (obrigatório)
        sa = parsed.get("self_assessment", {})
        if not sa:
            return {"valid": False, "reason": "self_assessment ausente"}

        # Campos obrigatórios de self_assessment
        required_sa_fields = ["coverage_score", "confidence", "gaps_critical"]
        for field in required_sa_fields:
            if field not in sa:
                return {
                    "valid": False,
                    "reason": f"self_assessment sem campo '{field}'",
                }

        # Validar coverage_score range
        coverage = sa.get("coverage_score")
        if not isinstance(coverage, (int, float)) or not (0.0 <= coverage <= 1.0):
            return {
                "valid": False,
                "reason": f"coverage_score inválido: {coverage} (deve ser 0.0-1.0)",
            }

        # Validar confidence
        if sa.get("confidence") not in ["alta", "média", "baixa"]:
            return {
                "valid": False,
                "reason": f"confidence inválida: {sa.get('confidence')}",
            }

        # Validar gaps_critical
        if not isinstance(sa.get("gaps_critical"), bool):
            return {
                "valid": False,
                "reason": f"gaps_critical deve ser bool, got {type(sa.get('gaps_critical'))}",
            }

        return {"valid": True}


def _build_seed_query_rules() -> str:
    """Retorna regras compactas de seed_query (usar 1x no prompt)"""
    return """
**SEED_QUERY (3-8 palavras, SEM operadores):**
- Estrutura: TEMA_CENTRAL + ASPECTO + GEO
- Se 1-3 entidades: incluir TODOS os nomes na seed
- Se 4+ entidades: seed genérica + TODOS em must_terms
- @noticias: adicionar 3-6 palavras específicas (eventos, tipos, ações)

Exemplos:
✅ "RedeDr Só Saúde oncologia Brasil" (1-3 entidades)
✅ "volume autos elétricos Brasil" (4+ entidades)
✅ "@noticias recalls veículos elétricos Brasil" (breaking news)
❌ "volume fees Brasil" (falta tema!)
❌ "buscar dados verificáveis" (genérico demais)
"""


def _build_time_windows_table() -> str:
    """Tabela compacta de janelas temporais"""
    return """
**JANELAS TEMPORAIS:**

| Recency | Uso | Exemplo |
|---------|-----|---------|
| **90d** | Breaking news explícito | "últimos 90 dias", "breaking news" |
| **1y** | Tendências/estado atual (DEFAULT news) | "eventos recentes", "aquisições ano" |
| **3y** | Panorama/contexto histórico | "evolução setorial", "baseline" |
**Regra Prática:**
- News SEM prazo explícito → 1y (captura 12 meses)
- News COM "90 dias" → 90d (breaking only)
- Estudos de mercado → 3y (contexto) + 1y (tendências) [OBRIGATÓRIO]
"""
def _extract_json_from_text(text: str) -> Optional[dict]:
    """LEGACY WRAPPER: Delega para parse_json_resilient(mode='balanced')"""
    return parse_json_resilient(text, mode="balanced", allow_arrays=True)
def _patch_seed_if_needed(
    phase: dict, strict_mode: bool, metrics: dict, logger
) -> None:
    """Patch seed_query se estiver muito magra (modo relax apenas)"""
    if strict_mode:
        return  # Modo strict: não patch

    obj = phase.get("objective") or phase.get("objetivo") or ""
    sq = phase.get("seed_query", "")

    if not obj or not sq:
        return

    # Verifica se seed_query contém algum token significativo do objetivo
    obj_tokens = [t for t in obj.lower().split() if len(t) > 4]
    sq_lower = sq.lower()

    has_obj_token = any(t in sq_lower for t in obj_tokens)

    if not has_obj_token:
        # Seed não tem nenhum token do objetivo - patch
        k = _first_content_token(obj)
        if k and k not in sq_lower:
            phase["seed_query"] = f"{sq} {k}".strip()
            if metrics is not None:
                metrics["seed_patched_count"] = metrics.get("seed_patched_count", 0) + 1
            logger.info(f"[SEED] Patched seed_query '{sq}' → '{phase['seed_query']}'")


def _check_mece_basic(fases: List[dict], key_questions: List[str]) -> List[str]:
    """Verifica MECE básico: key_questions órfãs (sem cobertura)"""
    if not key_questions:
        return []

    uncovered = []
    for kq in key_questions:
        tokens = [t for t in kq.lower().split() if len(t) > 4]
        covered = False
        for f in fases:
            obj = (f.get("objetivo") or f.get("objective") or "").lower()
            if any(t in obj for t in tokens):
                covered = True
                break
        if not covered:
            uncovered.append(kq)

    return uncovered


def _append_phase(contract: dict, candidate: dict) -> None:
    """Adiciona fase candidata ao contract (incremental, sem replanejar)"""
    # Normalizar chaves (name/nome, objective/objetivo)
    if "nome" in candidate and "name" not in candidate:
        candidate["name"] = candidate.pop("nome")
    if "objective" not in candidate and "objetivo" in candidate:
        candidate["objective"] = candidate.pop("objetivo")

    # Sanity checks básicos
    seed_query = candidate.get("seed_query", "")
    if not (3 <= len(seed_query.split()) <= 8):
        logger.warning(f"[APPEND] seed_query inválida: {seed_query}")
        return

    seed_core = candidate.get("seed_core", "")
    if seed_core and seed_core == seed_query:
        logger.warning(f"[APPEND] seed_core igual a seed_query: {seed_core}")
        return

    # Adicionar ao contract
    contract["fases"].append(candidate)
    logger.info(f"[APPEND] Fase adicionada: {candidate.get('name', 'N/A')}")


def _calc_entity_coverage(fases: List[dict], entities: List[str]) -> float:
    """Calcula a % de fases que contêm pelo menos uma entidade em must_terms"""
    if not entities or not fases:
        return 1.0

    covered = 0
    for f in fases:
        must_terms = f.get("must_terms", [])
        mt_str = " ".join(must_terms).lower()
        if any(e.lower() in mt_str for e in entities):
            covered += 1

    return covered / max(1, len(fases))


def _list_missing_entity_phases(fases: List[dict], entities: List[str]) -> List[str]:
    """Lista as fases que NÃO contêm nenhuma entidade em must_terms"""
    if not entities:
        return []

    missing = []
    for f in fases:
        must_terms = f.get("must_terms", [])
        mt_str = " ".join(must_terms).lower()
        if not any(e.lower() in mt_str for e in entities):
            phase_name = (
                f.get("name") or f.get("nome") or f.get("phase_type") or "sem-nome"
            )
            missing.append(phase_name)

    return missing


def _check_entity_coverage_soft(
    contract: dict, entities: List[str], min_coverage: float, logger
) -> None:
    """Valida entity coverage em modo SOFT (warning em vez de exception)"""
    fases = contract.get("fases") or contract.get("phases", [])
    coverage = _calc_entity_coverage(fases, entities)

    if entities and coverage < min_coverage:
        contract["_entity_coverage_warning"] = {
            "coverage": coverage,
            "min": min_coverage,
            "missing_phases": _list_missing_entity_phases(fases, entities),
        }
        logger.warning(
            f"Entity coverage {coverage:.0%} < {min_coverage:.0%} (modo soft). "
            f"Fases sem entidades: {contract['_entity_coverage_warning']['missing_phases']}"
        )


def _build_synthesis_sections(
    key_questions: List[str],
    research_objectives: List[str],
    entities: List[str],
    contract: dict,
) -> str:
    """Build synthesis sections for final report"""
    sections = []
    
    if key_questions:
        sections.append(f"**Key Questions:** {', '.join(key_questions[:5])}")
    
    if research_objectives:
        sections.append(f"**Research Objectives:** {', '.join(research_objectives[:3])}")
    
    if entities:
        sections.append(f"**Entities:** {', '.join(entities[:5])}")
    
    phases = contract.get("fases", [])
    if phases:
        sections.append(f"**Phases:** {len(phases)} planned")
    
    return "\n".join(sections)


def _render_contract(contract: Dict[str, Any]) -> str:
    """Render contract as markdown for display"""
    fases = contract.get("fases", [])
    num_fases = len(fases)
    lines = [f"## 📋 Plano – {num_fases} Fases\n"]

    intent = contract.get("intent", "")
    if intent:
        lines.append(f"**🎯 Objetivo:** {intent}\n")

    # Mostrar entidades
    entities = contract.get("entities", {})
    if entities.get("canonical"):
        lines.append(f"**🏷️ Entidades:** {', '.join(entities['canonical'])}")
        if entities.get("aliases"):
            lines.append(f"**🔗 Aliases:** {', '.join(entities['aliases'])}")
        lines.append("")

    lines.append(f"**📍 Fases:**\n")
    for i, fase in enumerate(fases, 1):
        lines.append(f"### Fase {i}/{num_fases} – {fase.get('name', 'N/A')}")
        lines.append(f"**Objetivo:** {fase.get('objetivo', 'N/A')}")
        # Exibir seed_core (query rica para Discovery) em vez de seed_query
        seed_core = fase.get("seed_core", "N/A")
        lines.append(f"**Seed Query:** `{seed_core}`")

        # Mostrar must/avoid terms
        must_terms = fase.get("must_terms", [])
        avoid_terms = fase.get("avoid_terms", [])
        if must_terms:
            lines.append(f"**✅ Must:** {', '.join(must_terms)}")
        if avoid_terms:
            lines.append(f"**❌ Avoid:** {', '.join(avoid_terms)}")

        # Mostrar time hint e source bias
        time_hint = fase.get("time_hint", {})
        source_bias = fase.get("source_bias", [])
        if time_hint:
            lines.append(f"**⏰ Tempo:** {time_hint.get('recency', 'N/A')}")
        if source_bias:
            lines.append(f"**📊 Fontes:** {' > '.join(source_bias)}")

        lines.append("")

    # Mostrar quality rails
    quality_rails = contract.get("quality_rails", {})
    if quality_rails:
        lines.append("**🛡️ Quality Rails:**")
        lines.append(
            f"- Mínimo {quality_rails.get('min_unique_domains', 'N/A')} domínios únicos"
        )
        if quality_rails.get("need_official_or_two_independent"):
            lines.append("- Fonte oficial OU ≥2 domínios independentes por fase")
        lines.append("")

    lines.append("---")
    lines.append(f"**💡 Responda:** **siga** | **continue**")
    return "\n".join(lines)


def _first_content_token(text: str) -> str:
    """Extract first meaningful token from text"""
    if not text:
        return ""
    
    tokens = text.strip().split()
    for token in tokens:
        if len(token) > 2 and token.isalpha():
            return token
    
    return tokens[0] if tokens else ""


def _build_entity_rules_compact() -> str:
    """Regras de entidades (v4.8 - Entity-Centric Policy)"""
    return """
**POLÍTICA ENTITY-CENTRIC (v4.8):**

| Quantidade | Mode | Seed_query | Must_terms (por fase) | Exemplo |
|------------|------|------------|----------------------|---------|
| 1-3 | 🎯 FOCADO | Incluir TODOS | **TODAS as fases** devem ter | "RedeDr Só Saúde oncologia BR" |
| 4-6 | 📊 DISTRIBUÍDO | Genérica | industry:≤3, profiles/news:TODAS | seed:"saúde digital BR", must:["RedeDr","DocTech","Hospital X"] |
| 7+ | 📊 DISTRIBUÍDO | Genérica | industry:≤3, profiles/news:TODAS | must:["Magalu","Via","Americanas",...] |

**Cobertura obrigatória (1-3 entidades): ≥70% das fases devem incluir as entidades em must_terms**
**Razão:** Discovery Selector usa must_terms para priorização + Analyst precisa de contexto focado
"""


def _build_planner_prompt(
    user_prompt: str,
    phases: int,
    current_date: Optional[str] = None,
    detected_context: Optional[dict] = None,
) -> str:
    """Build unified Planner prompt used by both Manual and SDK routes."""
    if not current_date:
        from datetime import datetime
        current_date = datetime.now().strftime("%Y-%m-%d")

    date_context = f"DATA ATUAL: {current_date}\n(Use esta data ao planejar fases de notícias/eventos recentes. Não sugira anos passados como '2024' se estamos em 2025.)\n\n"

    # Orientação específica por perfil detectado (compacta)
    profile_guidance = ""
    if detected_context:
        perfil = detected_context.get("perfil_sugerido", "")
        setor = detected_context.get("setor_principal", "")
        # Blocos curtos por perfil (2–3 bullets). Se já houver key_questions/entities, manter guidance minimalista
        short_guidance = {
            "company_profile": (
                f"Perfil mercado ({setor}): use 3y para panorama, 1y para tendências, 90d só para eventos pontuais. Priorize fontes oficiais/primárias."
            ),
            "technical_spec": (
                f"Perfil técnico ({setor}): panorama 3y, docs atuais 1y, releases 90d. Priorize docs oficiais/RFCs/repos."
            ),
            "regulation_review": (
                f"Perfil regulatório ({setor}): marco vigente 3y, compliance 1y, mudanças 90d. Priorize gov/oficial."
            ),
            "literature_review": (
                f"Perfil acadêmico ({setor}): fundamentos 3y+, estado da arte 1–3y, papers 1y. Priorize scholar/periódicos."
            ),
            "history_review": (
                f"Perfil histórico ({setor}): contexto 3y+, evolução 3y, análise atual 1y. Priorize arquivos/oficial/academia."
            ),
        }
        pg = short_guidance.get(perfil, "")
        if pg:
            profile_guidance = pg + "\n\n"

    # Usar key_questions e entities do detected_context (se disponíveis) - versão compacta
    cot_preamble = ""
    if detected_context:
        # Usar as informações do Context Detection (CoT já foi feito lá)
        key_q = detected_context.get("key_questions", [])
        entities = detected_context.get("entities_mentioned", [])
        objectives = detected_context.get("research_objectives", [])

        if key_q or entities or objectives:
            # SEMPRE mostrar key_questions e entities explicitamente (não depender de reasoning_summary)
            cot_preamble = f"""
📋 **CONTEXTO JÁ ANALISADO (CONTEXT-LOCK):**
✅ {len(key_q)} key questions identificadas
✅ {len(entities)} entidades específicas detectadas  
✅ {len(objectives)} objetivos de pesquisa definidos
✅ Perfil: {detected_context.get('perfil_sugerido', 'N/A')}
🔒 **PAYLOAD DO ESTRATEGISTA (USE EXCLUSIVAMENTE, NÃO RE-INFIRA):**
KEY_QUESTIONS={json.dumps(key_q[:10], ensure_ascii=False)}
ENTITIES_CANONICAL={json.dumps(entities[:15], ensure_ascii=False)}
RESEARCH_OBJECTIVES={json.dumps(objectives[:10], ensure_ascii=False)}
LANG_BIAS={detected_context.get('language_bias', ['pt-BR', 'en'])}
GEO_BIAS={detected_context.get('geo_bias', ['BR', 'global'])}
⚠️ **INSTRUÇÕES CRÍTICAS:**
1. Crie fases que RESPONDAM às KEY_QUESTIONS listadas acima
2. Inclua ENTITIES_CANONICAL nos must_terms das fases apropriadas
3. Alinhe os objectives das fases aos RESEARCH_OBJECTIVES do estrategista
4. NÃO introduza novas entidades não listadas acima
5. NÃO altere ou re-interprete os objetivos
6. Use SOMENTE os dados do payload acima
"""

    # Chain of Thought: SEMPRE usar informações do Context Detection (não extrair novamente)
    if detected_context and (
        detected_context.get("key_questions")
        or detected_context.get("entities_mentioned")
    ):
        # Context Detection já fez o CoT - NÃO pedir re-extração
        chain_of_thought = f"""
⚙️ **PROCESSO DE PLANEJAMENTO:**
Pense passo a passo INTERNAMENTE, mas NÃO exponha o raciocínio. Retorne APENAS JSON.

1. **MAPEAR** cada KEY_QUESTION do payload acima → uma fase específica
2. **DIVIDIR** em até {phases} fases MECE (panorama → detalhes → atual/news)
3. **APLICAR** janelas temporais: 3y (panorama), 1y (tendências), 90d (notícias)
4. **INCLUIR** ENTITIES_CANONICAL nos must_terms conforme phase_type

{cot_preamble}
"""
    else:
        # Fallback: se Context Detection falhou completamente
        chain_of_thought = f"""
⚠️ FALLBACK MODE (Context Detection falhou):
Extraia você mesmo as key questions e entidades da consulta abaixo e divida em fases.
{cot_preamble}
"""

    # Exemplo mínimo (1 bloco) — mantém orientação sem inflar prompt
    example_json = """    {
      "name": "Panorama geral",
      "objective": "Pergunta verificável e específica",
      "seed_query": "<3-6 palavras, sem operadores>",
      "seed_core": "<12-200 chars, 1 frase rica, sem operadores>",
      "must_terms": ["<todas as entidades mencionadas>"],
      "avoid_terms": ["<ruído>"] ,
      "time_hint": {"recency": "1y", "strict": false},
      "source_bias": ["oficial", "primaria", "secundaria"],
      "evidence_goal": {"official_or_two_independent": true, "min_domains": 3},
      "lang_bias": ["pt-BR", "en"],
      "geo_bias": ["BR", "global"]
    }"""

    # P1: Exemplo ANTES/DEPOIS para seed_query (clareza de tema central)
    seed_before_after = """
⚠️ EXEMPLOS DE SEED QUERY - ANTES E DEPOIS:

❌ ERRADO (sem tema central):
- "volume fees Brasil" → Falta contexto (fees de QUÊ?)
- "tendências serviços Brasil" → Genérico (serviços de QUÊ?)
- "reputação boutiques Brasil" → Ambíguo (boutiques de QUÊ?)

✅ CORRETO (tema presente):
- "volume fees executive search Brasil" → Tema: executive search
- "tendências headhunting Brasil" → Tema: headhunting
- "reputação boutiques executive search Brasil" → Tema: executive search

REGRA: seed_query = TEMA_CENTRAL + ASPECTO + GEO
"""

    # P1: Instruções para seed_core (OBRIGATÓRIO)
    seed_core_instructions = """
⚠️ **SEED_CORE (OBRIGATÓRIO para TODAS as fases):**
- Formato: 1 frase rica (12-200 chars), linguagem natural, SEM operadores
- Inclui: entidades + tema + aspecto + recorte geotemporal
- Contexto completo para Discovery Tool executar busca efetiva
- Relação com seed_query: seed_core é expansão rica de seed_query

EXEMPLOS:
Fase "Volume setorial":
  seed_query: "volume executive search Brasil"
  seed_core: "volume anual mercado executive search Brasil últimos 3 anos fontes oficiais associações setor"

Fase "Tendências serviços":
  seed_query: "tendências headhunting Brasil"
  seed_core: "tendências emergentes serviços headhunting e recrutamento executivo Brasil últimos 12 meses inovações tecnologia"

Fase "Perfis empresas":
  seed_query: "Korn Ferry portfólio Brasil"
  seed_core: "Korn Ferry portfólio serviços posicionamento competitivo mercado brasileiro executive search últimos 2 anos"

❌ ERRADO (muito curta, sem contexto):
  seed_core: "Flow CNPJ Brasil"  // Apenas 3 palavras

✅ CORRETO:
  seed_core: "Flow Executive Finders CNPJ registro Receita Federal Brasil razão social data fundação"
"""

    # Framework de auto-validação de realismo
    realism_framework = """
🔍 AUTO-VALIDAÇÃO DE REALISMO (PENSE ANTES DE INCLUIR MÉTRICAS):

Para CADA métrica/dado que você incluir no objective, faça a pergunta:

'Empresas/organizações DESTE TIPO e PORTE divulgam isso publicamente?'

Use seu conhecimento sobre:
  • Práticas do setor (financeiro vs tech vs saúde vs consultoria)
  • Tipo de empresa (listada vs privada vs startup vs pública)
  • Sensibilidade competitiva (pricing, margens, métricas operacionais)
  • Obrigações regulatórias (empresas listadas divulgam mais)

HEURÍSTICA SIMPLES:
  ✅ Se encontraria em: site corporativo, press releases, relatórios anuais
     → INCLUIR no objective
  ⚠️ Se encontraria apenas em: relatórios internos, pitches de vendas
     → EVITAR ou marcar como 'se disponível'
  ❌ Se é vantagem competitiva: pricing real, custos, métricas operacionais
     → NÃO incluir, focar em proxies públicas

EXEMPLO DE RACIOCÍNIO:
Query: 'Boutique de executive search no Brasil'
Métrica considerada: 'time-to-fill médio, success rate %'

Pergunta: Consultoria de RH divulga métricas operacionais?
Resposta: Não - são vantagens competitivas confidenciais.
          Empresas listadas divulgam revenue agregado, privadas não.

Objective ajustado: 'portfólio de serviços, setores atendidos,
                     ciclos/processos DECLARADOS (quando disponível)'
                     [proxy público para 'rapidez operacional']

⚠️ IMPORTANTE: Você conhece centenas de setores. Use esse conhecimento.
               Não force métricas que você sabe serem privadas.
"""

    # Usar dicionários globais de prompts
    system_prompt = PROMPTS["planner_system"].format(phases=phases)
    seed_rules = _build_seed_query_rules()
    time_windows = _build_time_windows_table()
    entity_rules = _build_entity_rules_compact()
    
    return (
        date_context
        + profile_guidance
        + cot_preamble
        + seed_before_after
        + seed_core_instructions
        + realism_framework
        + f"""

{system_prompt}

🎯 OBJETIVO DA PESQUISA:
{user_prompt}

{seed_rules}

{time_windows}

{entity_rules}

🎯 **ECONOMIA DE FASES (CRITICAL):**
ATÉ """
        + str(phases)
        + """ fases permitidas. PREFIRA MENOS FASES BEM FOCADAS.

**QUANDO COMBINAR (1 fase):**
- Objetivo = comparar/rankear múltiplas entidades
- Overview geral ou análise aggregada de mercado

**QUANDO ESPECIALIZAR (1 fase/entidade):**
- Usuário pede "perfis detalhados" / "análise profunda"
- Entidades muito distintas (B2B vs B2C, setores diferentes)
- Volume esperado >10 páginas por entidade

**EXEMPLOS:**
- "Compare receita A, B, C" → ✅ 2 fases (receitas 3y + drivers 1y) | ❌ 6 fases (1/empresa + trends)
- "Perfis detalhados A, B, C" → ✅ 4 fases (3 perfis deep + comparativa) | Justificado: especialização necessária

📊 **SCORING:** Precisão 40% + Economia 30% + MECE 30% → Menos fases (mesma cobertura) = SUPERIOR

**CHECKLIST:** Antes de criar fase → (1) Responde key_question única? (2) Aspecto/temporal diferente? (3) Combinar degrada qualidade? → Se NÃO para qualquer → NÃO CRIE

🔴 **REGRA ESPECIAL - PEDIDOS EXPLÍCITOS DE NOTÍCIAS:**
Se o usuário mencionar "notícias", "noticias", "fase de notícias", "eventos recentes":
→ OBRIGATÓRIO criar fase type="news" com:
  - seed_query: "@noticias" + tema + entidades
  - time_hint: 1y (últimos 12 meses)
  - 90d SOMENTE se usuário disser "breaking news", "últimos 90 dias" ou "muito recente"

⚙️ **PROCESSO DE PLANEJAMENTO (Use o payload acima, NÃO re-extraia):**

Pense passo a passo INTERNAMENTE, mas NÃO exponha o raciocínio. Retorne APENAS JSON.

**ETAPA 1 - MAPEAR (não extrair):**
- Para cada KEY_QUESTION do payload → crie 1 fase específica
- Exemplo: KEY_QUESTION "Qual volume anual?" → Fase "Volume setorial" (phase_type: industry)
- Exemplo: KEY_QUESTION "Qual reputação?" → Fase "Perfis players" (phase_type: profiles)

**ETAPA 2 - DIVIDIR em fases MECE por phase_type:**

🎯 **ENTITY-CENTRIC MODE (1-3 entidades mencionadas):**
SE o payload tem 1-3 ENTITIES_CANONICAL → MODO FOCADO:
  ✅ **TODAS as fases** devem incluir essas entidades em must_terms
  ✅ Seed_query de cada fase deve conter nomes das entidades
  ✅ Objetivo: Manter foco laser nas entidades específicas
  ❌ NÃO crie fases genéricas sem as entidades
  
📊 **MULTI-ENTITY MODE (4+ entidades mencionadas):**
SE o payload tem 4+ ENTITIES_CANONICAL → MODO DISTRIBUÍDO:
  - **industry**: pode ter subset (≤3 entidades representativas)
  - **profiles**: deve ter TODAS
  - **news**: deve ter TODAS

**Phase types (aplique a lógica acima):**
- **industry**: panorama setorial (volume, tendências, players)
  - must_terms: [ENTITY-CENTRIC: todas entidades] [MULTI: subset ≤3] + termos setoriais + geo
  - time_hint: 1y ou 3y
- **profiles**: perfis detalhados de players
  - must_terms: **TODAS** as ENTITIES_CANONICAL do payload + geo
  - time_hint: 3y
- **news**: notícias e eventos relevantes (últimos 12 meses)
  - must_terms: **TODAS** as ENTITIES_CANONICAL do payload + geo
  - seed_query: DEVE incluir "@noticias" + tema + entidades
  - time_hint: 1y (DEFAULT) | 90d SOMENTE para "breaking news" explícito

**ETAPA 3 - APLICAR janelas temporais corretas:
🔍 **CONTEXTO IMPORTA:** A janela temporal depende do TIPO DE INFORMAÇÃO, não apenas se é "notícia"!

"""
        + _build_time_windows_table()
        + """

⚠️ **CRÍTICO - ESTUDOS DE MERCADO (READ THIS!):**

🚨 **SE** o objetivo geral contém ["estudo", "análise", "mercado", "setor", "panorama", "competitivo"]:

**ARQUITETURA OBRIGATÓRIA (NÃO NEGOCIÁVEL):**
1. ✅ Fase "industry/profiles" com 3y (contexto/baseline)
2. ✅ **Fase "notícias/eventos" com 1y** (OBRIGATÓRIA se planejando 3+ fases)
   - seed_query DEVE ter "@noticias" + tema + entidades
   - Captura últimos 12 meses de eventos relevantes
   - 90d SOMENTE se usuário pedir "breaking news" ou "últimos 90 dias" explicitamente
3. ✅ Fase adicional de "perfis" ou "tendências" conforme necessário

**❌ ERRO COMUM (NÃO FAÇA):**
- Criar apenas 1 fase "news" com 90d
- Esquecer de incluir "@noticias" na seed_query de fases de notícias
- Criar fases genéricas sem as entidades quando há 1-3 entidades (entity-centric)
- Resultado: perde tendências dos últimos 12 meses (70% das key questions não respondidas) + falta foco nas entidades

**✅ EXEMPLO CORRETO - Estudo entity-centric (3 entidades: Health+, Vida Melhor, OncoTech):**
```json
{
  "plan_intent": "Mapear oportunidades e riscos em saúde digital com foco em 3 hospitais brasileiros",
  "phases": [
    {"name": "Panorama de mercado e players (3 anos)", "phase_type": "industry", 
    "seed_query": "RedeDr Só Saúde oncologia Brasil",  // ← ENTITY-CENTRIC: todas as 3 empresas
    "must_terms": ["RedeDr", "Só Saúde", "Onco Brasil", "saúde digital", "Brasil"],  // ← TODAS
     "time_hint": {"recency": "3y"}},
    {"name": "Perfis, serviços e reputação", "phase_type": "profiles", 
    "seed_query": "Health+ Vida Melhor OncoTech serviços rankings reputação",  // ← ENTITY-CENTRIC: todas as 3
    "must_terms": ["Health+", "Vida Melhor", "OncoTech", "saúde digital", "Brasil"],  // ← TODAS
     "time_hint": {"recency": "3y"}},
    {"name": "Notícias e eventos (12 meses)", "phase_type": "news", 
    "seed_query": "@noticias Magalu Via Americanas varejo Brasil",  // ← ENTITY-CENTRIC + @noticias
    "must_terms": ["Magazine Luiza", "Via", "Americanas", "varejo", "Brasil"],  // ← TODAS
     "objective": "Aquisições, lançamentos, mudanças de mercado com foco nas 3 empresas-alvo (12 meses)",
     "time_hint": {"recency": "1y", "strict": false}}  // ← 1y (não 90d!)
  ]
}
```

**⚠️ CONTRASTE - ERRADO (fases genéricas, perdeu foco):**
```json
{
  "phases": [
    {"name": "Panorama geral", "seed_query": "mercado executive search Brasil",  // ❌ SEM entidades
     "must_terms": ["executive search", "Brasil"]},  // ❌ Faltam empresas
    {"name": "Tendências", "seed_query": "tendências serviços headhunting Brasil",  // ❌ SEM entidades
     "must_terms": ["executive search", "assessment"]},  // ❌ Faltam empresas
    {"name": "Perfis", "seed_query": "Health+ Vida Melhor OncoTech",  // ✅ Tem entidades MAS só em 1 fase
     "must_terms": ["Health+", "Vida Melhor", "OncoTech"]}  // ✅ OK mas TARDE DEMAIS (70% das fases sem foco)
  ]
}
```
**Resultado errado: Judge detecta falta de foco nas empresas-alvo e cria novas fases redundantes (desperdício de budget)**

**🎯 VALIDAÇÃO OBRIGATÓRIA (checklist antes de retornar plano):**
- [ ] **Entity-centric?** Se 1-3 entidades: ≥70% das fases incluem essas entidades em must_terms?
- [ ] **Temporal coverage?** Há fase com recency=1y para capturar tendências dos últimos 12 meses?
- [ ] **News phase?** Se pedido "notícias" ou "estudo de mercado": fase news com "@noticias" e 1y?
- [ ] **Key questions cobertas?** Cada KEY_QUESTION do payload tem fase correspondente?
- [ ] **Seed_query válido?** 3-8 palavras, sem operadores, contém tema central + entidades?

📐 REGRAS OBRIGATÓRIAS:

✅ CADA FASE TEM:
   - name: descritivo e único
   - objective: pergunta verificável (não genérica!)
   - seed_query: 3-8 palavras, SEM operadores
   - seed_core: 12-200 chars, frase rica para discovery (OBRIGATÓRIO!)
     
     """
        + _build_seed_query_rules()
        + """
     
     """
        + _build_entity_rules_compact()
        + """
     
     **SLACK SEMÂNTICO PARA ASPECTOS/MÉTRICAS:**
     
     ❌ NÃO coloque métricas/aspectos específicos na seed:
     - "volume fees tempo-to-fill executive search Brasil" (6 palavras, MUITO específica)
     
     ✅ Seed genérica + métricas em must_terms:
     - seed: "mercado executive search Brasil" (4 palavras)
     - must_terms: ["volume", "fees", "tempo-to-fill", "colocações", "market size", ...]
     
   - must_terms: **CRÍTICO - TODOS OS NOMES VÃO AQUI (SEMPRE)**
     * Independente de quantos, TODOS os nomes vão em must_terms
     * Se usuário mencionou 10 empresas, TODAS vão em must_terms
     * Se usuário mencionou produtos/pessoas, TODOS vão em must_terms
     * Discovery vai usar must_terms para priorizar e expandir a busca
     * Seed_query + must_terms = máxima precisão
   - avoid_terms: **OPCIONAL - ruído a evitar (deixe [] se não houver)**
     * Use quando há PALAVRAS-CHAVE específicas que causam falsos positivos
     * **EXEMPLOS DE USO:**
       - Fase "executive search": avoid_terms: ["vagas", "currículos", "job board"] → evita posts de vagas
       - Fase "aquisições": avoid_terms: ["promoção", "desconto"] → evita e-commerce
       - Fase "saúde digital": avoid_terms: ["fitness", "app gratuito"] → evita consumer health apps
     * **QUANDO NÃO USAR (deixe vazio []):**
       - Fases genéricas de mercado (risco de bloquear conteúdo válido)
       - Quando não há ruído óbvio e bem definido
       - Quando objetivo é amplitude/descoberta (melhor deixar Discovery trabalhar)
     * **REGRA PRÁTICA:** Se duvidoso, deixe [] - Discovery é resiliente a ruído
   - time_hint: {"recency": "90d|1y|3y", "strict": true/false}
   - source_bias: ["oficial", "primaria", "secundaria"]
   - evidence_goal: {"official_or_two_independent": true, "min_domains": 3}
   - lang_bias e geo_bias apropriados

🆕 **NOVO v4.7 - SEED_CORE E SEED_FAMILY_HINT:**

**seed_core** (OPCIONAL mas RECOMENDADO):
- Versão RICA da seed_query (1 frase, ≤200 chars, sem operadores)
- Usado pelo Discovery para gerar 1-3 variações de busca
- Se ausente, Discovery usa seed_query (curta)
**EXEMPLOS:**
- seed_query: "aquisições headhunting Brasil" (curta, 3 palavras)
- seed_core: "aquisições e parcerias estratégicas no mercado de headhunting no Brasil nos últimos 12 meses" (rica, contexto completo)

**seed_family_hint** (OPCIONAL, default: "entity-centric"):
- Orienta Discovery sobre TIPO de exploração
- Valores: "entity-centric" | "problem-centric" | "outcome-centric" | "regulatory" | "counterfactual"
**TEMPLATES POR FAMÍLIA:**
- **entity-centric**: "<ENTIDADE/SETOR> <tema central> <recorte geotemporal>"
  - Ex: "Spencer Stuart executive search Brasil últimos 12 meses"
- **problem-centric**: "<problema/risco> <drivers/causas> <contexto/segmento>"
  - Ex: "escassez de talentos C-level causas mercado brasileiro"
- **outcome-centric**: "<efeito/resultado> <indicadores/impacto> <stakeholders>"
  - Ex: "impacto turnover executivo indicadores performance empresas"
- **regulatory**: "<norma/regulador> <exigências/procedimentos> <abrangência>"
  - Ex: "LGPD requisitos compliance headhunting Brasil"
- **counterfactual**: "<tese/controvérsia> <objeção/antítese> <evidência-chave>"
  - Ex: "boutiques locais vs internacionais vantagens competitivas evidências"

**QUANDO USAR CADA FAMÍLIA:**
- **entity-centric** (default): Foco em empresas/produtos/pessoas específicas
- **problem-centric**: Quando objetivo menciona "desafios", "riscos", "problemas"
- **outcome-centric**: Quando objetivo menciona "impacto", "resultados", "efeitos"
- **regulatory**: Quando objetivo menciona "compliance", "regulação", "normas"
- **counterfactual**: Quando objetivo menciona "comparar", "contrastar", "alternativas"

⚠️ **IMPORTANTE:**
- Se Judge anterior sugeriu seed_family diferente (via NEW_PHASE), RESPEITE-A
- seed_core e seed_family_hint são OPCIONAIS (backwards-compatible)
- Se ausentes, Discovery usa seed_query (comportamento atual)

❌ NÃO FAÇA:
   - Objetivos genéricos ("explorar", "entender melhor")
   - Seed queries idênticas ou muito similares
   - Operadores em seed_query (apenas 3-6 palavras simples)
   - Esquecer @noticias para tópicos atuais
   - **IGNORAR ENTIDADES ESPECÍFICAS** mencionadas no objetivo do usuário
   - Ser genérico quando o usuário foi específico (ex: usuário menciona 10 empresas, você ignora)

🎯 SAÍDA OBRIGATÓRIA: APENAS JSON PURO (sem markdown, sem comentários, sem texto extra)

SCHEMA JSON OBRIGATÓRIO (com phase_type + seed_core + seed_family_hint):
{{
  "plan_intent": "<objetivo do plano em 1 frase>",
  "total_phases_used": <int OPCIONAL: quantas fases criou, se omitir será inferido>,
  "phases_justification": "<string OPCIONAL: por que esse número de fases é suficiente/econômico>",
  "assumptions_to_validate": ["<hipótese1>", "<hipótese2>"],
  "phases": [
    {{
      "name": "<nome descritivo da fase>",
      "phase_type": "industry|profiles|news|regulatory|financials|tech",
      "objective": "<pergunta verificável e específica>",
      "seed_query": "<3-6 palavras, SEM operadores (@, site:, OR, AND)>",
      "seed_core": "<OPCIONAL: 1 frase rica ≤200 chars, sem operadores>",
      "seed_family_hint": "<OPCIONAL: entity-centric|problem-centric|outcome-centric|regulatory|counterfactual>",
      "must_terms": ["<termo1>", "<termo2>"],
      "avoid_terms": ["<ruído/SEO>"],
      "time_hint": {{"recency": "90d|1y|3y", "strict": false}},
      "source_bias": ["oficial","primaria","secundaria"],
      "evidence_goal": {{"official_or_two_independent": true, "min_domains": 3}},
      "lang_bias": ["pt-BR","en"],
      "geo_bias": ["BR","global"],
      "suggested_domains": ["<OPCIONAL: domínios prioritários>"],
      "suggested_filetypes": ["<OPCIONAL: html, pdf, etc>"]
    }}
    // Repetir para 1 a """
        + str(phases)
        + """ fases (conforme necessário, não obrigatório usar todas)
  ],
  "quality_rails": {{"min_unique_domains": """
        + str(max(2, phases))
        + """,
    "need_official_or_two_independent": true
  }},
  "budget": {{"max_rounds": 2}}
}}

⚠️ IMPORTANTE: Você pode criar 1, 2, 3... até """
        + str(phases)
        + """ fases. Escolha o número que FAZ SENTIDO para o objetivo!
- Objetivo simples/específico? → 2-3 fases podem bastar
- Objetivo complexo/amplo? → Use mais fases (até o máximo)

💡 EXEMPLO 1 - Genérico (SEM empresas mencionadas):
"analisar indústria de executive search no Brasil":
- must_terms: ["executive search", "Brasil", "mercado"]  ← Genérico OK

💡 EXEMPLO 2A - Poucas entidades (1-3):
"estudo sobre Health+, Vida Melhor e OncoTech no Brasil":
- seed_query: "Health+ Vida Melhor OncoTech saúde digital Brasil"  ← 3 nomes na seed (OBRIGATÓRIO!)
- must_terms: ["Health+", "Vida Melhor", "OncoTech", "saúde digital", "Brasil"]  ← TODOS aqui também!

"estudo sobre Magalu e Via no Brasil":
- seed_query: "Magalu Via varejo digital Brasil"  ← 2 nomes na seed (OBRIGATÓRIO!)
- must_terms: ["Magalu", "Via", "varejo digital", "Brasil"]  ← TODOS aqui também!

💡 EXEMPLO 2B - Muitas entidades (7+):
"estudo sobre Magalu, Via, Americanas, MercadoLivre, Shopee, Amazon, Submarino no Brasil":
- seed_query: "volume mercado varejo digital Brasil"  ← SEM nomes (tema + aspecto)
- must_terms: ["Magalu", "Via", "Americanas", "MercadoLivre", "Shopee", "Amazon", "Submarino"]  ← TODOS aqui!

💡 EXEMPLO CORRETO - Estudo de mercado varejo digital Brasil:
"mercado de varejo digital no Brasil (Magalu, Via, Americanas, MercadoLivre)":
```json
{{
  "plan_intent": "Mapear mercado de varejo digital no Brasil com foco em players nacionais e internacionais",
  "assumptions_to_validate": ["Crescimento do e-commerce regional supera o global", "Players locais têm vantagens logísticas"],
  "phases": [
    {{"name": "Volume setorial", "phase_type": "industry", "objective": "Qual volume anual do varejo digital no Brasil?", "seed_query": "volume varejo digital Brasil", "seed_core": "volume anual vendas e-commerce Brasil", "must_terms": ["varejo digital", "e-commerce", "Brasil"], "avoid_terms": ["loja física"], "time_hint": {{"recency": "1y", "strict": false}}, "source_bias": ["oficial", "primaria"], "evidence_goal": {{"official_or_two_independent": true, "min_domains": 3}}, "lang_bias": ["pt-BR"], "geo_bias": ["BR"]}},
    {{"name": "Tendências serviços", "phase_type": "industry", "objective": "Quais tendências e serviços adjacentes surgiram nos últimos 12 meses?", "seed_query": "tendências serviços varejo digital Brasil", "seed_core": "tendências emergentes serviços adjacentes varejo digital Brasil últimos 12 meses inovações tecnologia", "must_terms": ["varejo digital", "omnicanal", "logística", "Brasil"], "avoid_terms": ["loja física"], "time_hint": {{"recency": "1y", "strict": false}}, "source_bias": ["oficial", "primaria"], "evidence_goal": {{"official_or_two_independent": true, "min_domains": 3}}, "lang_bias": ["pt-BR"], "geo_bias": ["BR"]}},
    {{"name": "Perfis e reputação", "phase_type": "profiles", "objective": "Como se posicionam Magalu, Via, Americanas e MercadoLivre?", "seed_query": "reputação players varejo digital Brasil", "seed_core": "Magalu Via Americanas MercadoLivre posicionamento competitivo reputação mercado brasileiro varejo digital últimos 2 anos", "must_terms": ["Magalu", "Via", "Americanas", "MercadoLivre", "Brasil"], "avoid_terms": ["reclamações"], "time_hint": {{"recency": "3y", "strict": false}}, "source_bias": ["oficial", "primaria"], "evidence_goal": {{"official_or_two_independent": true, "min_domains": 3}}, "lang_bias": ["pt-BR"], "geo_bias": ["BR"]}},
    {{"name": "Eventos recentes", "phase_type": "news", "objective": "Quais aquisições ou mudanças ocorreram nos últimos 90 dias?", "seed_query": "@noticias aquisições varejo digital Brasil", "seed_core": "aquisições parcerias mudanças estratégicas Magalu Via Americanas MercadoLivre varejo digital Brasil últimos 90 dias", "must_terms": ["Magalu", "Via", "Americanas", "MercadoLivre", "Brasil"], "avoid_terms": ["promoções"], "time_hint": {{"recency": "90d", "strict": true}}, "source_bias": ["oficial", "primaria"], "evidence_goal": {{"official_or_two_independent": true, "min_domains": 2}}, "lang_bias": ["pt-BR"], "geo_bias": ["BR"]}}
  ],
  "quality_rails": {{"min_unique_domains": 3, "need_official_or_two_independent": true}},
  "budget": {{"max_rounds": 2}}
}}
```

**OBSERVAÇÃO CRÍTICA sobre o exemplo acima:**
- ✅ Fase 1 seed: "volume fees EXECUTIVE SEARCH Brasil" (tema presente!)
- ✅ Fase 2 seed: "tendências serviços HEADHUNTING Brasil" (tema presente!)
- ✅ Fase 3 seed: "reputação players EXECUTIVE SEARCH Brasil" (tema presente!)
- ✅ Fase 4 seed: "@noticias aquisições HEADHUNTING Brasil" (tema presente!)
- ⚠️ SEM tema: queries genéricas que retornam noise (currículos, vagas, etc)

🎯 AGORA CRIE SEU PLANO:

⚠️ **CRÍTICO - POLÍTICAS DE phase_type:**
- **industry**: panorama setorial (volume, tendências). must_terms: termos setoriais + geo (≤5). time_hint: 1y ou 3y
- **profiles**: perfis específicos de players. must_terms: **TODOS** os players + geo. time_hint: 3y
- **news**: eventos/notícias do setor. must_terms: **TODOS** os players + geo. time_hint: 1y (padrão) OU 90d (apenas breaking news explícito)
  - ⚠️ NOVO (v4.4): News padrão = 1y (captura eventos relevantes 12 meses)
  - ⚠️ Exceção: 90d apenas se objective menciona "últimos 90 dias" / "últimos 3 meses" / "breaking news"
- **regulatory**: marcos regulatórios. must_terms: leis/normas + geo. time_hint: 1y ou 3y
- **financials**: métricas financeiras. must_terms: players + métricas. time_hint: 1y
- **tech**: especificações técnicas. must_terms: tecnologias + componentes. time_hint: 1y

**INSTRUÇÕES FINAIS:**

**🔒 VALIDAÇÃO DE KEY_QUESTIONS COVERAGE (P0 - CRÍTICO):**
Antes de retornar o plano, verifique OBRIGATORIAMENTE:
1. **Para CADA key_question** listada no payload do Estrategista → identifique qual fase a responde
2. **Se alguma key_question NÃO for coberta** → crie fase adicional específica
3. **Mapeamento mental obrigatório:**
   - Key_Q "Qual volume...?" → Fase "Volume setorial" (industry, 1y ou 3y)
   - Key_Q "Quais tendências...?" → Fase "Tendências/evolução" (industry, 1y) ← CRÍTICO!
   - Key_Q "Qual reputação...?" → Fase "Perfis players" (profiles, 3y)
   - Key_Q "Quais eventos/notícias...?" → Fase "Eventos mercado" (news, 1y) ← v4.4: 1y padrão!
   - Key_Q "Últimos dias/90d...?" → Fase "Breaking news" (news, 90d) ← v4.4: apenas se explícito!
   - Key_Q "Quais riscos/oportunidades 3-5 anos?" → Fase "Tendências/evolução" (industry, 1y)

**SE** alguma key_question não tiver fase correspondente → **ERRO CRÍTICO** → crie fase adicional.

**EXEMPLO DE VALIDAÇÃO:**
```
10 key_questions fornecidas:
✓ Q1-Q3 → Fase 1 (industry 3y)
✓ Q4-Q6 → Fase 2 (profiles 3y) 
✓ Q7-Q9 → Fase 3 (tendências 1y)  ← Se esta fase não existir, 30% das questions ficam sem resposta!
✓ Q10 → Fase 4 (news 90d)
```

**APÓS VALIDAÇÃO:**
1. Use as key_questions e entities já analisadas (acima) para criar as fases
2. Atribua phase_type correto para cada fase
3. Aplique as políticas de must_terms por phase_type
4. Retorne APENAS JSON PURO (sem markdown, sem texto extra, sem raciocínio)

🎯 **VALIDAÇÃO DE RESEARCH_OBJECTIVES COVERAGE (P0 - CRÍTICO):**

Antes de retornar o plano, verifique OBRIGATORIAMENTE:

1. **Para CADA research_objective** listado no payload → identifique qual fase cobre
2. **Se algum objective NÃO for coberto** → ajuste objectives de fase ou crie fase adicional

**EXEMPLO DE MAPEAMENTO:**
```
RESEARCH_OBJECTIVES do estrategista:
1. "Mapear principais players..." → Fase "Perfis players" (phase_type: profiles)
2. "Comparar ofertas e pricing..." → Fase "Modelos de preço" (phase_type: industry)
3. "Segmentar demanda por setor..." → Fase "Panorama mercado" (phase_type: industry)
4. "Avaliar reputação e mídia..." → Fase "Perfis players" (phase_type: profiles)
5. "Identificar riscos regulatórios..." → Fase "Panorama mercado" (objective específico)
6. "Recomendar estratégias M&A..." → Fase "Tendências e oportunidades" (phase_type: industry)
7. "Produzir matriz competitiva..." → Fase "Perfis players" (phase_type: profiles)
```

**SE** algum research_objective não tiver fase que o cubra → ajuste fase existente ou crie nova.

**REGRA:** Objectives de fase devem ser MAIS ESPECÍFICOS que research_objectives (subconjunto detalhado).
- ❌ ERRADO: Objective genérico "Analisar mercado" (não cobre research_objective específico)
- ✅ CERTO: Objective "Quantificar market share por tipo de player e identificar riscos regulatórios" (cobre research_objectives 1, 3, 5)

**DICA:** Se um research_objective menciona "matriz competitiva", uma fase DEVE ter isso explicitamente no objective.
Se menciona "M&A/expansão", uma fase DEVE cobrir fusões/aquisições/parcerias.

---

🎯 **ACCEPTANCE CRITERIA (VALIDAÇÃO FINAL OBRIGATÓRIA):**

Antes de retornar o JSON, verifique:

✅ **ESTRUTURA:**
- [ ] 1-3 fases criadas (pode ser menos que o máximo se suficiente)
- [ ] Cada fase tem TODOS os campos obrigatórios
- [ ] JSON válido (sem markdown, sem comentários, sem texto extra)

✅ **SEED_QUERY (curta, para UI/telemetria):**
- [ ] 3-6 palavras (excluindo @noticias)
- [ ] SEM operadores (site:, filetype:, OR, AND, aspas)
- [ ] Contém tema central + aspecto + geo
- [ ] Se 1-3 entidades: TODOS os nomes na seed_query

✅ **SEED_CORE (rica, para Discovery):**
- [ ] 12-200 caracteres (1 frase completa)
- [ ] SEM operadores
- [ ] Inclui: entidades + tema + aspecto + recorte geotemporal
- [ ] NÃO repete seed_query sem adicionar pelo menos 1 aspecto + 1 entidade

✅ **MUST_TERMS:**
- [ ] 2-8 termos (não vazio, não excessivo)
- [ ] TODAS as entidades canônicas incluídas (quando aplicável)
- [ ] SEM overlap com avoid_terms

✅ **OBJECTIVE:**
- [ ] Pergunta verificável (verbo: mapear/identificar/comparar/quantificar)
- [ ] Condição de sucesso clara (ex: "Quantificar market share por player")
- [ ] NÃO genérico (ex: "entender melhor", "explorar")

✅ **MECE (NÃO OVERLAP COM DISCOVERY):**
- [ ] NÃO gerar variações da seed (Discovery fará isso)
- [ ] NÃO criar múltiplas queries por fase (apenas 1 seed_query + 1 seed_core)

✅ **NEWS PHASES:**
- [ ] time_hint.recency = "1y" (padrão) OU "90d" (apenas se explícito)
- [ ] time_hint.strict = true
- [ ] @noticias na seed_query

🔎 **SELF-CHECK (execute ANTES de retornar JSON):**

Antes de retornar o plano, verifique OBRIGATORIAMENTE cada item abaixo:

1️⃣ **Todas as key_questions cobertas?**
   - Cada KEY_QUESTION do payload tem ≥1 fase correspondente?
   - Se alguma ficou órfã → crie fase adicional
2️⃣ **Notícias (se pedidas)?**
   - Se usuário mencionou "notícias" OU é "estudo de mercado" → existe fase type="news"?
   - Fase news tem time_hint.recency="1y" (não 90d) e strict=true?
   - Seed_query da fase news tem "@noticias" + tema + entidades?

3️⃣ **Seeds válidas?**
   - Cada seed_query tem 3-8 palavras (excluindo @noticias)?
   - seed_query NÃO usa operadores (site:, filetype:, OR, AND, aspas)?
   - seed_core tem ≥12 caracteres e é DIFERENTE de seed_query?
   - seed_core inclui pelo menos 1 entidade + 1 aspecto adicional?
   - seed_core é 1 frase rica (não apenas palavras soltas)?
4️⃣ **ENTIDADES (cobertura mínima):**
   - Se ≤3 entidades → elas aparecem em must_terms de pelo menos 70% das fases?
   - Seed_query das fases de profiles/news incluem TODAS as entidades?
   - Se >3 entidades → pelo menos as 3 principais em must_terms de cada fase?

5️⃣ **MECE (sem overlap):**
   - Objectives das fases são mutualmente exclusivos (não overlap óbvio)?
   - Se detectou overlap → reescreva objectives antes de retornar
   - Fases cobrem TODO o escopo (nenhuma key_question órfã)?

✅ **SEED_CORE VALIDATION:**
- [ ] TODAS as fases têm seed_core (12-200 chars)
- [ ] seed_core ≠ seed_query (seed_core é EXPANSÃO)
- [ ] seed_core inclui: entidades + tema + aspecto + geo/temporal
- [ ] seed_core SEM operadores (@, site:, OR, AND)

✅ **Se todos os checks passarem → retorne JSON**
❌ **Se algum falhar → corrija ANTES de retornar**

FORMATO DE SAÍDA:
[JSON do plano completo]
"""
    )
def _build_analyst_prompt(query: str, phase_context: Dict = None) -> str:
    """Build unified Analyst prompt used by both Manual and SDK routes."""
    # Extrair informações da fase atual
    phase_info = ""
    if phase_context:
        phase_name = phase_context.get("name", "Fase atual")
        # Contract usa "objetivo" (PT), não "objective" (EN)
        phase_objective = phase_context.get("objetivo") or phase_context.get(
            "objective", ""
        )

        # Adicionar must_terms e avoid_terms para guiar o Analyst
        must_terms = phase_context.get("must_terms", [])
        avoid_terms = phase_context.get("avoid_terms", [])

        phase_info = (
            f"\n**FASE ATUAL:** {phase_name}\n**Objetivo da Fase:** {phase_objective}"
        )

        if must_terms:
            # Mostrar até 8 termos prioritários (evitar prompt muito longo)
            terms_display = ", ".join(must_terms[:8])
            if len(must_terms) > 8:
                terms_display += f" (e mais {len(must_terms) - 8})"
            phase_info += f"\n**Termos Prioritários:** {terms_display}"

            # P0: Enfatizar obrigatoriedade dos must_terms
            phase_info += f"""
⚠️ **MUST_TERMS SÃO OBRIGATÓRIOS:**
- TODOS os fatos extraídos DEVEM mencionar pelo menos 1 termo prioritário
- Se contexto não menciona must_terms, reportar em lacunas (ex: "Falta dados sobre [termo]")
- Coverage_score deve penalizar ausência de must_terms
- Exemplo: Query "market share Flow Executive Brasil" + must_terms ["Flow Executive", "market share", "Brasil"]
  → ✅ Fato CORRETO: "Flow Executive tem 15% de market share no Brasil" (3/3 must_terms)
  → ❌ Fato INCORRETO: "Mercado de consultoria no Brasil cresceu 10%" (1/3 must_terms - genérico demais)"""

        if avoid_terms:
            # Mostrar até 5 termos a evitar
            avoid_display = ", ".join(avoid_terms[:5])
            if len(avoid_terms) > 5:
                avoid_display += f" (e mais {len(avoid_terms) - 5})"
            phase_info += f"\n**Evitar:** {avoid_display}"

    # Extrair objetivo específico para enfatizar
    objective_emphasis = ""
    if phase_context:
        # Contract usa "objetivo" (PT), não "objective" (EN)
        obj = phase_context.get("objetivo") or phase_context.get("objective", "")
        if obj:
            objective_emphasis = f"\n\n🎯 **SUA MISSÃO ESPECÍFICA NESTA FASE:**\n{obj}\n\n**FOQUE APENAS** em extrair fatos que RESPONDEM DIRETAMENTE esta pergunta/objetivo!"

    # Usar dicionários globais de prompts
    system_prompt = PROMPTS["analyst_system"]
    calibration_rules = PROMPTS["analyst_calibration"]
    
    prompt_template = (
        system_prompt
        + "\n\nOBJETIVO: {query_text}{phase_block}{objective_block}\n\n"
        + calibration_rules
        + "\n\nJSON:\n"
        + "{\n"
        + '  "summary": "Resumo FOCADO NO OBJETIVO da fase (não genérico!)",\n'
        + '  "facts": [\n'
        + '    {\n'
        + '      "texto": "Fato que RESPONDE ao objetivo da fase",\n'
        + '      "confiança": "alta|média|baixa", \n'
        + '      "evidencias": [{"url": "...", "trecho": "..."}]\n'
        + '    }\n'
        + '  ],\n'
        + '  "lacunas": ["O que AINDA FALTA para responder completamente o objetivo"],\n'
        + '  "self_assessment": {\n'
        + '    "coverage_score": 0.7,\n'
        + '    "confidence": "alta|média|baixa",\n'
        + '    "gaps_critical": true,\n'
        + '    "suggest_refine": false,\n'
        + '    "suggest_pivot": true,\n'
        + '    "reasoning": "Por que pivot: lacuna precisa de dados 90d (fase atual: 3y)"\n'
        + '  }\n'
        + '}\n\n'
        + "Retorne APENAS JSON."
    )

    # 🔴 FIX P0: Usar str.replace em vez de .format para evitar KeyError com literais JSON no template
    # O template contém exemplos JSON com {"summary": ...} que .format() interpreta como placeholder
    final_prompt = prompt_template.replace("{query_text}", query)
    final_prompt = final_prompt.replace("{phase_block}", phase_info)
    final_prompt = final_prompt.replace("{objective_block}", objective_emphasis)

    return final_prompt
class JudgeLLM:
    def __init__(self, valves):
        self.valves = valves
        # Usar modelo específico se configurado, senão usa modelo padrão
        model = valves.LLM_MODEL_JUDGE or valves.LLM_MODEL
        self.model_name = model
        self.llm = _get_llm(valves, model_name=model)
        # Base kwargs: serão filtrados por get_safe_llm_params (GPT-5 não aceita temperature)
        self.generation_kwargs = {"temperature": 0.3}

    def _calculate_phase_score(self, metrics: Dict[str, float]) -> float:
        """Calcula phase_score auditável usando pesos configuráveis (v4.7)

        Fórmula:
        phase_score = w_cov * coverage
                    + w_nf * novel_fact_ratio
                    + w_nd * novel_domain_ratio
                    + w_div * domain_diversity
                    - w_contra * contradiction_score

        Args:
            metrics: Dict com coverage, novel_fact_ratio, novel_domain_ratio,
                    domain_diversity, contradiction_score

        Returns:
            Score normalizado 0.0-1.0 (pode ser negativo se contradições altas)
        """
        weights = getattr(
            self.valves,
            "PHASE_SCORE_WEIGHTS",
            {
                "w_cov": 0.35,
                "w_nf": 0.25,
                "w_nd": 0.15,
                "w_div": 0.15,
                "w_contra": 0.40,
            },
        )

        coverage = metrics.get("coverage", 0.0)
        novel_fact_ratio = metrics.get("novel_fact_ratio", 0.0)
        novel_domain_ratio = metrics.get("novel_domain_ratio", 0.0)
        domain_diversity = metrics.get("domain_diversity", 0.0)
        contradiction_score = metrics.get("contradiction_score", 0.0)

        score = (
            weights["w_cov"] * coverage
            + weights["w_nf"] * novel_fact_ratio
            + weights["w_nd"] * novel_domain_ratio
            + weights["w_div"] * domain_diversity
            - weights["w_contra"] * contradiction_score
        )

        return round(score, 3)

    def _switch_seed_family(self, current_family: str) -> str:
        """Troca família de seed para exploração sistemática (v4.7)

        Ciclo: entity-centric → problem-centric → outcome-centric → regulatory → counterfactual → entity-centric

        Args:
            current_family: Família atual

        Returns:
            Próxima família no ciclo
        """
        family_order = [
            "entity-centric",
            "problem-centric",
            "outcome-centric",
            "regulatory",
            "counterfactual",
        ]

        try:
            idx = family_order.index(current_family)
            next_idx = (idx + 1) % len(family_order)
            return family_order[next_idx]
        except ValueError:
            # Família desconhecida, retornar default
            return "problem-centric"  # Primeira alternativa ao entity-centric

    def _calculate_weighted_fact_delta(self, analysis: dict) -> float:
        """
        Calculate weighted fact delta based on confidence scores.
        
        WIN #2: Instead of simple count, weight facts by confidence:
        - alta confiança = 1.0
        - média confiança = 0.7  
        - baixa confiança = 0.4
        
        Args:
            analysis: Analysis dict containing facts list
            
        Returns:
            Weighted delta (float) - sum of confidence scores for new facts
        """
        facts = analysis.get("facts", [])
        if not facts:
            return 0.0
            
        # Map confidence levels to weights
        confidence_weights = {
            "alta": 1.0,
            "média": 0.7,
            "baixa": 0.4
        }
        
        # Calculate weighted sum
        weighted_sum = 0.0
        for fact in facts:
            confidence = fact.get("confiança", "média")  # Default to média
            weight = confidence_weights.get(confidence, 0.7)  # Default weight
            weighted_sum += weight
            
        return weighted_sum
    
    def _calculate_multi_dimensional_similarity(
        self, new_obj: str, new_seed: str, new_type: str, existing_phases: list
    ) -> tuple[float, int]:
        """
        Calculate multi-dimensional similarity for duplicate detection.
        
        CORE FIX: Compare 3 dimensions with weights:
        - 0.5 * objective similarity (TF-IDF cosine similarity)
        - 0.3 * seed_query similarity (Jaccard similarity)
        - 0.2 * phase_type overlap (exact match)
        
        Args:
            new_obj: New phase objective
            new_seed: New phase seed query
            new_type: New phase type
            existing_phases: List of existing phases
            
        Returns:
            Tuple of (max_weighted_score, index_of_most_similar_phase)
        """
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        
        max_weighted_score = 0.0
        max_sim_idx = 0
        
        if not existing_phases:
            return max_weighted_score, max_sim_idx
            
        # Extract existing objectives
        existing_objs = [
            p.get("objetivo") or p.get("objective", "")
            for p in existing_phases
        ]
        existing_seeds = [
            p.get("seed_query", "") for p in existing_phases
        ]
        existing_types = [
            p.get("phase_type", "") for p in existing_phases
        ]
        
        # 1. Objective similarity (TF-IDF cosine similarity)
        objective_sim = 0.0
        if new_obj and existing_objs and any(existing_objs):
            all_objs = [new_obj] + existing_objs
            vectorizer = TfidfVectorizer()
            vectors = vectorizer.fit_transform(all_objs)
            similarities = cosine_similarity(vectors[0:1], vectors[1:]).flatten()
            objective_sim = similarities.max() if len(similarities) > 0 else 0.0
        
        # 2. Seed query similarity (Jaccard similarity)
        seed_sim = 0.0
        if new_seed and existing_seeds and any(existing_seeds):
            new_tokens = set(new_seed.lower().split())
            max_jaccard = 0.0
            for existing_seed in existing_seeds:
                if existing_seed:
                    existing_tokens = set(existing_seed.lower().split())
                    intersection = len(new_tokens & existing_tokens)
                    union = len(new_tokens | existing_tokens)
                    jaccard = intersection / union if union > 0 else 0.0
                    max_jaccard = max(max_jaccard, jaccard)
            seed_sim = max_jaccard
        
        # 3. Phase type overlap (exact match)
        type_overlap = 0.0
        if new_type and existing_types:
            type_overlap = 1.0 if new_type in existing_types else 0.0
        
        # Calculate weighted score
        weighted_score = (
            0.5 * objective_sim +
            0.3 * seed_sim +
            0.2 * type_overlap
        )
        
        # Find the most similar phase by iterating and tracking max
        if existing_phases:
            for idx, phase in enumerate(existing_phases):
                phase_obj = phase.get('objetivo') or phase.get('objective', '')
                phase_seed = phase.get('seed_query', '')
                phase_type = phase.get('phase_type', '')
                
                p_obj_sim = 0.0
                if new_obj and phase_obj:
                    try:
                        all_o = [new_obj, phase_obj]
                        vect = TfidfVectorizer()
                        vecs = vect.fit_transform(all_o)
                        sims = cosine_similarity(vecs[0:1], vecs[1:]).flatten()
                        p_obj_sim = sims[0] if len(sims) > 0 else 0.0
                    except: pass
                
                p_seed_sim = 0.0
                if new_seed and phase_seed:
                    nt = set(new_seed.lower().split())
                    pt = set(phase_seed.lower().split())
                    p_seed_sim = len(nt & pt) / len(nt | pt) if len(nt | pt) > 0 else 0.0
                
                p_type_over = 1.0 if new_type == phase_type and new_type else 0.0
                phase_score = 0.5 * p_obj_sim + 0.3 * p_seed_sim + 0.2 * p_type_over
                
                if phase_score > max_weighted_score:
                    max_weighted_score = phase_score
                    max_sim_idx = idx
            
        return max_weighted_score, max_sim_idx

    def _validate_quality_rails(
        self, analysis, phase_context, intent_profile: Optional[str] = None
    ):
        """Gates MÍNIMOS - apenas safety net para casos extremos

        REBALANCED (v4.5.1): Reduzido de 6 gates rígidos para 2 gates mínimos
        Filosofia: Prompts guiam, gates alertam casos extremos
        """
        facts = analysis.get("facts", [])

        # Gate 1: ZERO fatos (caso extremo óbvio)
        if not facts:
            return {
                "passed": False,
                "reason": "Sem fatos encontrados - impossível responder ao objetivo",
                "suggested_query": "buscar fontes específicas e verificáveis",
            }

        # Gate 2: Combinação de problemas (evidência fraca + coverage baixo)
        # Apenas bloqueia quando AMBOS são muito baixos
        metrics = _extract_quality_metrics(facts)
        evidence_coverage = metrics["facts_with_evidence"] / len(facts) if facts else 0
        coverage_score = analysis.get("self_assessment", {}).get("coverage_score", 0)

        # NOVO THRESHOLD: 50% evidência + 50% coverage (muito mais permissivo)
        if evidence_coverage < 0.5 and coverage_score < 0.5:
            return {
                "passed": False,
                "reason": f"Qualidade muito baixa: {evidence_coverage*100:.0f}% evidência + {coverage_score*100:.0f}% coverage (ambos <50%)",
                "suggested_query": "buscar fontes com dados específicos e verificáveis",
            }

        return {"passed": True}

    def _check_evidence_staleness(
        self, facts, phase_context, intent_profile: Optional[str] = None
    ):
        """Verifica staleness (recency) das evidências (P1.2) com gates por perfil"""
        if not facts:
            return {"passed": True}

        # Extrair time_hint do phase_context
        time_hint = phase_context.get("time_hint", {}) if phase_context else {}
        recency = time_hint.get("recency", "1y")
        strict = time_hint.get("strict", False)
        # Aplicar gate por perfil (se perfil exigir strict, força strict)
        try:
            profile = (
                intent_profile
                or getattr(self.valves, "INTENT_PROFILE", None)
                or "company_profile"
            )
            gates = self.valves.GATES_BY_PROFILE.get(profile, {})
            if gates.get("staleness_strict"):
                strict = True
        except Exception:
            pass

        if not strict:
            return {"passed": True}  # Se não é strict, não verifica staleness

        # Converter recency para dias
        recency_days = self._parse_recency_to_days(recency)
        if recency_days is None:
            return {"passed": True}  # Recency inválido, não verifica

        # Verificar idade das evidências
        from datetime import datetime, timedelta

        cutoff_date = datetime.now() - timedelta(days=recency_days)

        old_evidence_count = 0
        total_evidence_count = 0

        for fact in facts:
            evidencias = fact.get("evidencias", [])
            for ev in evidencias:
                total_evidence_count += 1
                ev_date_str = ev.get("data", "")

                if ev_date_str:
                    try:
                        # Tentar parsear data (YYYY-MM-DD)
                        ev_date = datetime.fromisoformat(ev_date_str)
                        if ev_date < cutoff_date:
                            old_evidence_count += 1
                    except:
                        # Se não conseguir parsear, assumir que é antiga
                        old_evidence_count += 1

        if total_evidence_count == 0:
            return {"passed": True}

        old_ratio = old_evidence_count / total_evidence_count

        # KPI: se >50% das evidências são antigas, bloquear DONE
        if old_ratio > 0.5:
            return {
                "passed": False,
                "reason": f"{old_ratio*100:.0f}% evidências fora da janela de {recency} (strict=true)",
                "suggested_query": f"Buscar evidências mais recentes (últimos {recency})",
            }

        return {"passed": True}

    def _parse_recency_to_days(self, recency):
        """Converte recency string para dias"""
        if recency == "90d":
            return 90
        elif recency == "1y":
            return 365
        elif recency == "3y":
            return 1095
        else:
            return None

    async def run(
        self,
        user_prompt: str,
        analysis: Dict[str, Any],
        phase_context: Dict[str, Any] = None,
        telemetry_loops: Optional[List[Dict[str, Any]]] = None,
        intent_profile: Optional[str] = None,
        full_contract: Optional[Dict] = None,
        valves=None,
        refine_queries: Optional[List[Dict]] = None,
        phase_candidates: Optional[List[Dict]] = None,
        previous_queries: Optional[List[str]] = None,
        failed_queries: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        if not self.llm:
            raise ValueError("LLM não configurado")

        phase_info = ""
        if phase_context:
            phase_info = f"\n**Critérios:** {', '.join(phase_context.get('accept_if_any_of', []))}"

        # Build judge prompt inline (temporary implementation)
        prompt = f"""
        Analise os resultados da pesquisa e decida o próximo passo.
        
        Usuário: {user_prompt}
        Análise: {analysis}
        Contexto da Fase: {phase_context}
        Loops de Telemetria: {len(telemetry_loops)}
        
        Retorne JSON com:
        - verdict: "done" | "refine" | "new_phase"
        - reasoning: explicação da decisão
        - next_query: próxima query se refine
        - phase_score: 0.0-1.0
        """

        # Filtrar parâmetros incompatíveis com GPT-5/O1
        safe_params = get_safe_llm_params(self.model_name, self.generation_kwargs)

        # Use retry function if enabled, otherwise single attempt
        if getattr(self.valves, "ENABLE_LLM_RETRY", True):
            max_retries = int(getattr(self.valves, "LLM_MAX_RETRIES", 3) or 3)
            out = await _safe_llm_run_with_retry(
                self.llm,
                prompt,
                safe_params,
                timeout=self.valves.LLM_TIMEOUT_DEFAULT,
                max_retries=max_retries,
            )
        else:
            out = await _safe_llm_run_with_retry(
                self.llm,
                prompt,
                safe_params,
                timeout=self.valves.LLM_TIMEOUT_DEFAULT,
                max_retries=1,
            )
        if not out:
            raise RuntimeError("Judge failed")

        parsed = parse_json_resilient(out.get("replies", [""])[0], mode="balanced", allow_arrays=False)
        if not parsed:
            raise ValueError("Judge output inválido")

        # ===== JUDGE ENXUTO: 3 SINAIS AUTOMÁTICOS, 3 REGRAS MECE =====
        # Filosofia: Genérico, sem thresholds manuais, sem whitelists

        # SINAL 1: Lacunas explícitas (do Analyst)
        has_lacunas = bool(analysis.get("lacunas"))

        # SINAL 2: Tração (crescimento absoluto em domínios OU fatos)
        traction = True  # Default para primeiro loop
        if telemetry_loops and len(telemetry_loops) >= 2:
            last = telemetry_loops[-1]
            prev = telemetry_loops[-2]
            delta_domains = last.get("unique_domains", 0) - prev.get(
                "unique_domains", 0
            )
            
            # ✅ WIN #2: Weight traction by fact confidence instead of simple count
            delta_facts = self._calculate_weighted_fact_delta(analysis)
            traction = (delta_domains > 0) or (delta_facts > 0)

        # SINAL 3: Dois loops flat consecutivos (histerese)
        two_flat_loops = False
        if telemetry_loops and len(telemetry_loops) >= 3:
            last = telemetry_loops[-1]
            prev = telemetry_loops[-2]
            prev_prev = telemetry_loops[-3]

            # Último loop flat?
            delta_d_last = last.get("unique_domains", 0) - prev.get("unique_domains", 0)
            delta_f_last = last.get("n_facts", 0) - prev.get("n_facts", 0)
            last_flat = (delta_d_last == 0) and (delta_f_last == 0)

            # Penúltimo loop flat?
            delta_d_prev = prev.get("unique_domains", 0) - prev_prev.get(
                "unique_domains", 0
            )
            delta_f_prev = prev.get("n_facts", 0) - prev_prev.get("n_facts", 0)
            prev_flat = (delta_d_prev == 0) and (delta_f_prev == 0)

            two_flat_loops = last_flat and prev_flat

        # SINAL 4: Key Questions Status (do LLM Judge, não heurística)
        # Judge LLM avalia: coverage, blind_spots, se descobertas invalidam hipóteses
        key_questions_coverage = 1.0  # Default: 100%
        blind_spots = []

        # Extrair key_questions_status do JSON do Judge (se disponível)
        kq_status = parsed.get("key_questions_status", {})
        if kq_status:
            key_questions_coverage = float(kq_status.get("coverage", 1.0))
            blind_spots = kq_status.get("blind_spots", [])

        # SINAL 5: Blind Spots Críticos (descobertas que invalidam hipóteses)
        loops = len(telemetry_loops) if telemetry_loops else 0
        blind_spots_signal = bool(blind_spots) and (
            loops >= 1 or len(blind_spots) >= 3
        )

        # ===== v4.7: CALCULAR PHASE_SCORE AUDITÁVEL =====
        # Coletar métricas necessárias para o score
        facts = analysis.get("facts", [])
        lacunas = analysis.get("lacunas", [])

        # Métricas de telemetria (última iteração)
        last_loop = telemetry_loops[-1] if telemetry_loops else {}
        novel_fact_ratio = last_loop.get("new_facts_ratio", 0.0)
        novel_domain_ratio = last_loop.get("new_domains_ratio", 0.0)
        unique_domains = last_loop.get("unique_domains", 0)

        # Calcular domain_diversity (Herfindahl invertido ou simples ratio)
        # Simplificação: usar unique_domains / facts como proxy
        domain_diversity = (
            min(1.0, unique_domains / max(len(facts), 1)) if facts else 0.0
        )

        # Calcular contradiction_score (do Analyst ou telemetria)
        sa = analysis.get("self_assessment", {})
        contradiction_score = 0.0
        try:
            # Se Analyst reportou contradições, usar como score
            contradictions_count = last_loop.get("contradictions", 0)
            if contradictions_count > 0:
                contradiction_score = min(
                    1.0, contradictions_count / max(len(facts), 1)
                )
        except Exception:
            pass

        # Montar dict de métricas para phase_score
        phase_metrics = {
            "coverage": key_questions_coverage,  # Do LLM Judge
            "novel_fact_ratio": novel_fact_ratio,
            "novel_domain_ratio": novel_domain_ratio,
            "domain_diversity": domain_diversity,
            "contradiction_score": contradiction_score,
            "loops_without_gain": 2 if two_flat_loops else (0 if traction else 1),
        }

        # Calcular phase_score
        phase_score = self._calculate_phase_score(phase_metrics)

        # Obter gates do perfil/phase_type
        phase_type = (
            phase_context.get("phase_type", "industry") if phase_context else "industry"
        )
        gates = getattr(self.valves, "GATES_BY_PROFILE", {}).get(
            phase_type, {"threshold": 0.60, "two_flat_loops": 2}
        )
        threshold = gates.get("threshold", 0.60)
        required_flat_loops = gates.get("two_flat_loops", 2)

        # Calcular flat_streak (quantos loops consecutivos sem ganho)
        flat_streak = 0
        if telemetry_loops and len(telemetry_loops) >= 2:
            for i in range(len(telemetry_loops) - 1, 0, -1):
                curr = telemetry_loops[i]
                prev = telemetry_loops[i - 1]
                delta_d = curr.get("unique_domains", 0) - prev.get("unique_domains", 0)
                delta_f = curr.get("n_facts", 0) - prev.get("n_facts", 0)
                if delta_d == 0 and delta_f == 0:
                    flat_streak += 1
                else:
                    break

        # Calcular overlap_similarity (similaridade entre fatos desta fase vs anteriores)
        # Simplificação: usar novel_fact_ratio invertido como proxy
        overlap_similarity = 1.0 - novel_fact_ratio if novel_fact_ratio > 0 else 0.0

        # ===== DECISÃO PROGRAMÁTICA BASEADA EM PHASE_SCORE (v4.7) =====
        programmatic_decision = {}
        seed_family_switch = None

        # ✅ Reaproveitar new_phase do Judge LLM (se disponível)
        judge_new_phase = parsed.get("new_phase", {})

        # ===== SAFETY RAILS (prioridade máxima, sobrescrevem score) =====

        # Rail 1: Contradições críticas → NEW_PHASE imediato com seed_family switch
        contradiction_hard_gate = getattr(self.valves, "CONTRADICTION_HARD_GATE", 0.75)
        if contradiction_score >= contradiction_hard_gate:
            current_family = (
                phase_context.get("seed_family_hint", "entity-centric")
                if phase_context
                else "entity-centric"
            )
            seed_family_switch = self._switch_seed_family(current_family)
            programmatic_decision = {
                "verdict": "new_phase",
                "reasoning": f"Contradições críticas ({contradiction_score:.2f} ≥ {contradiction_hard_gate}). Trocar ângulo",
                "seed_family": seed_family_switch,
            }

        # Rail 2: Duplicação alta (overlap ≥ 0.90) → REFINE
        elif overlap_similarity >= 0.90:
            programmatic_decision = {
                "verdict": "refine",
                "reasoning": f"Overlap muito alto ({overlap_similarity:.2f}). Refinar busca",
            }

        # ===== REGRAS MECE BASEADAS EM PHASE_SCORE (v4.7) =====

        # Regra 1: Score BOM + coverage OK → DONE
        elif (
            phase_score >= threshold
            and key_questions_coverage >= getattr(self.valves, "COVERAGE_TARGET", 0.70)
        ):
            programmatic_decision = {
                "verdict": "done",
                "reasoning": f"Phase score {phase_score:.2f} ≥ {threshold:.2f}, coverage {key_questions_coverage*100:.0f}% OK",
            }

        # Regra 2: Score BAIXO + flat_streak atingido → NEW_PHASE com seed_family switch
        elif phase_score < threshold and flat_streak >= required_flat_loops:
            current_family = (
                phase_context.get("seed_family_hint", "entity-centric")
                if phase_context
                else "entity-centric"
            )
            seed_family_switch = self._switch_seed_family(current_family)
            programmatic_decision = {
                "verdict": "new_phase",
                "reasoning": f"Phase score {phase_score:.2f} < {threshold:.2f} após {flat_streak} loops flat. Trocar família de exploração",
                "seed_family": seed_family_switch,
            }

        # Regra 3: Score BAIXO mas ainda há tração → REFINE
        elif phase_score < threshold and traction:
            programmatic_decision = {
                "verdict": "refine",
                "reasoning": f"Phase score {phase_score:.2f} < {threshold:.2f} mas há tração. Refinar",
            }

        # Fallback: REFINE (caso não se encaixe em nenhuma regra acima)
        else:
            programmatic_decision = {
                "verdict": "refine",
                "reasoning": f"Score {phase_score:.2f}, coverage {key_questions_coverage*100:.0f}%. Continuar refinando",
            }

        # Aplicar rails de qualidade programáticos
        verdict = parsed.get("verdict", "done").strip()
        reasoning = parsed.get("reasoning", "").strip()
        next_query = parsed.get("next_query", "").strip()

        # Salvar decisão original do Judge para comparação
        original_verdict = verdict
        original_reasoning = reasoning
        modifications = []

        # Se decisão programática for diferente de "done", usar ela (tem prioridade)
        if (
            programmatic_decision.get("verdict")
            and programmatic_decision["verdict"] != "done"
        ):
            modifications.append(
                f"Programmatic override: {original_verdict} → {programmatic_decision['verdict']}"
            )
            verdict = programmatic_decision["verdict"]
            reasoning = programmatic_decision["reasoning"]
            next_query = programmatic_decision.get("next_query", next_query)

        # 🔒 CONSISTENCY CHECK: Reasoning vs Verdict (3 camadas)

        # Camada 1: Lacunas explícitas do Analyst
        if verdict == "done" and has_lacunas:
            logger.warning(
                f"[JUDGE] Inconsistência: verdict=done mas {len(analysis.get('lacunas', []))} lacunas no Analyst"
            )
            modifications.append(
                f"Consistency check: done → refine ({len(analysis.get('lacunas', []))} lacunas encontradas)"
            )
            verdict = "refine"
            reasoning = f"[AUTO-CORREÇÃO] Lacunas detectadas pelo Analyst. {reasoning}"

        # Camada 2: Key_questions coverage baixa (hipóteses não respondidas)
        if verdict == "done" and key_questions_coverage < 0.70:
            logger.warning(
                f"[JUDGE] Inconsistência: verdict=done mas key_questions coverage={key_questions_coverage:.2f} < 0.70"
            )
            modifications.append(
                f"Consistency check: done → refine (key_questions coverage {key_questions_coverage*100:.0f}% < 70%)"
            )
            verdict = "refine"
            reasoning = f"[AUTO-CORREÇÃO] {key_questions_coverage*100:.0f}% das key_questions relevantes respondidas (< 70%). {reasoning}"

        # Camada 3: Blind Spots críticos (descobertas que mudam contexto)
        # APENAS corrige DONE → NEW_PHASE se Judge errou ao ignorar blind spots críticos
        if verdict == "done" and blind_spots_signal:
            logger.warning(
                f"[JUDGE] Inconsistência: verdict=done mas {len(blind_spots)} blind_spots críticos detectados"
            )
            modifications.append(
                f"Consistency check: done → new_phase ({len(blind_spots)} blind_spots críticos)"
            )
            verdict = "new_phase"
            reasoning = f"[AUTO-CORREÇÃO] Blind spots críticos invalidam hipóteses iniciais: {'; '.join(blind_spots[:2])}. {reasoning}"

        # Incluir nova fase se foi criada programaticamente
        new_phase = parsed.get("new_phase", {})
        if programmatic_decision.get("new_phase"):
            new_phase = programmatic_decision["new_phase"]

        # 📊 FASE 1: Log JSON do Judge (observabilidade/auditoria)
        decision = {
            "decision": verdict,
            "reason": reasoning,
            "coverage": phase_metrics.get("coverage", 0),
            "domains": len(phase_metrics.get("domains", set())),
            "evidence": phase_metrics.get("evidence_coverage", 0),
            "staleness_ok": True,  # TODO: implementar staleness check se necessário
            "loops": f"{len(telemetry_loops) if telemetry_loops else 0}/{getattr(self.valves, 'MAX_AGENT_LOOPS', 2)}",
        }
        logger.info(f"[JUDGE]{json.dumps(decision, ensure_ascii=False)}")

        return {
            "reasoning": reasoning,
            "verdict": verdict,
            "next_query": next_query,
            "refine": parsed.get("refine", {}),
            "new_phase": new_phase,
            "unexpected_findings": parsed.get("unexpected_findings", []),
            "proposed_phase": new_phase,  # Para compatibilidade
            # ✅ v4.7: Métricas auditáveis
            "phase_score": phase_score,
            "phase_metrics": phase_metrics,
            "seed_family": seed_family_switch,  # Presente apenas se NEW_PHASE por exploração
            "modifications": modifications,  # Lista de modificações aplicadas
        }


# ==================== PROMPTS DICTIONARY ====================

PROMPTS = {
    # ===== PLANNER PROMPTS =====
    "planner_system": """Você é o PLANNER. Crie um plano de pesquisa estruturado em ATÉ {phases} fases (pode ser menos se suficiente).

🎯 FILOSOFIA DE PLANEJAMENTO:
- Crie APENAS as fases NECESSÁRIAS para cobrir o objetivo
- Melhor ter 2-3 fases bem focadas do que 4-5 genéricas
- O Judge pode criar novas fases dinamicamente se descobrir lacunas
- Máximo permitido: {phases} fases (mas pode ser menos!)""",

    "planner_seed_rules": """
**SEED_QUERY (3-8 palavras, SEM operadores):**
- Estrutura: TEMA_CENTRAL + SETOR/CONTEXTO + ASPECTO + GEO
- Se 1-3 entidades: incluir TODOS os nomes + contexto setorial
- Se 4+ entidades: seed genérica + contexto setorial + TODOS em must_terms
- @noticias: adicionar 3-6 palavras específicas (eventos, tipos, ações)
- **CRÍTICO**: Sempre incluir contexto setorial para evitar resultados irrelevantes

Exemplos:
✅ "Vila Nova Partners executive search Brasil" (entidade + setor)
✅ "Flow Executive search Brasil notícias" (entidade + setor + contexto)
✅ "RedeDr Só Saúde oncologia Brasil" (entidade + setor médico)
✅ "volume autos elétricos Brasil" (4+ entidades + setor)
✅ "@noticias recalls veículos elétricos Brasil" (breaking news + setor)
❌ "Flow Brasil notícia" (falta contexto setorial!)
❌ "volume fees Brasil" (falta tema!)
❌ "buscar dados verificáveis" (genérico demais)""",

    "planner_time_windows": """
**JANELAS TEMPORAIS:**

| Recency | Uso | Exemplo |
|---------|-----|---------|
| **90d** | Breaking news explícito | "últimos 90 dias", "breaking news" |
| **1y** | Tendências/estado atual (DEFAULT news) | "eventos recentes", "aquisições ano" |
| **3y** | Panorama/contexto histórico | "evolução setorial", "baseline" |

**Regra Prática:**
- News SEM prazo explícito → 1y (captura 12 meses)
- News COM "90 dias" → 90d (breaking only)
- Estudos de mercado → 3y (contexto) + 1y (tendências) [OBRIGATÓRIO]""",

    "planner_entity_rules": """
**POLÍTICA ENTITY-CENTRIC (v4.8):**

| Quantidade | Mode | Seed_query | Must_terms (por fase) |
|------------|------|------------|----------------------|
| 1-3 | 🎯 FOCADO | Incluir TODOS | **TODAS as fases** devem ter |
| 4-6 | 📊 DISTRIBUÍDO | Genérica | industry:≤3, profiles/news:TODAS |
| 7+ | 📊 DISTRIBUÍDO | Genérica | industry:≤3, profiles/news:TODAS |

**Cobertura obrigatória (1-3 entidades): ≥70% das fases devem incluir as entidades em must_terms**""",

    # ===== ANALYST PROMPTS =====
    "analyst_system": """⚠️ **FORMATO JSON OBRIGATÓRIO - REGRAS CRÍTICAS:**

Retorne APENAS um objeto JSON válido. Proibições absolutas:
❌ Markdown fences (```json ou ```)
❌ Comentários inline (// ou /* */)
❌ Texto explicativo antes/depois do JSON
❌ Aspas simples (use APENAS ")
❌ Quebras de linha dentro de strings

**ANTES DE RETORNAR, VALIDE MENTALMENTE:**
1. ✅ Começa com { e termina com } ?
2. ✅ Todas as strings têm aspas DUPLAS " ?
3. ✅ Vírgulas corretas (sem trailing commas) ?
4. ✅ Nenhum comentário inline ?
5. ✅ Nenhum markdown fence ?

SE algum item falhar → CORRIJA antes de retornar!

**SCHEMA EXATO (copie a estrutura channel):**
{
  "summary": "string resumo",
  "facts": [{"texto": "...", "confiança": "alta|média|baixa", "evidencias": [{"url": "...", "trecho": "..."}]}],
  "lacunas": ["..."],
  "self_assessment": {"coverage_score": 0.7, "confidence": "média", "gaps_critical": true, "suggest_refine": false, "reasoning": "..."}
}

---

Você é um ANALYST. Extraia 3-5 fatos importantes do contexto.

**PRIORIDADE #1**: Responda DIRETAMENTE ao objetivo da fase
- Priorize fatos sobre os Termos Prioritários mencionados
- Ignore conteúdo relacionado aos termos em "Evitar"  
- **CRÍTICO**: Ignore conteúdo que não tem contexto setorial relevante
- **CRÍTICO**: Se encontrar entidades com nomes similares mas em contextos diferentes (ex: "Flow" em outro setor), IGNORE se não for relevante ao objetivo
- Busque evidências concretas (URLs + trechos)
- Valide se o contexto setorial das informações encontradas corresponde ao objetivo da pesquisa""",

    "analyst_calibration": """
🎯 CALIBRAÇÃO DE coverage_score (PRAGMÁTICA):

**0.0-0.3 (BAIXO - RESPOSTA INADEQUADA):**
→ coverage_score = 0.2
→ gaps_critical = True
→ suggest_refine = True

**0.4-0.6 (MÉDIO - RESPOSTA PARCIAL mas ÚTIL):**
→ coverage_score = 0.6
→ gaps_critical = False
→ suggest_refine = False

**0.7-0.9 (ALTO - RESPOSTA SÓLIDA):**
→ coverage_score = 0.8
→ gaps_critical = False
→ suggest_refine = False""",

    # ===== JUDGE PROMPTS =====
    "judge_system": """Você é o JUDGE. Sua função: ANALISAR e DECIDIR se a pesquisa está COMPLETA ou precisa de mais informações.

🧠 **ABORDAGEM LLM-FIRST:**
- Analise QUALITATIVAMENTE a qualidade dos fatos extraídos
- Avalie se os fatos respondem adequadamente ao objetivo da pesquisa
- Identifique lacunas críticas que impedem uma resposta satisfatória
- Considere a diversidade de fontes e domínios encontrados
- Proponha decisão baseada em JULGAMENTO, não apenas métricas numéricas""",

    "judge_philosophy": """
🎯 **FILOSOFIA DE DECISÃO INTELIGENTE:**

**DONE = Resposta Satisfatória ao Objetivo**
- Os fatos extraídos respondem adequadamente à pergunta original?
- Há evidências concretas (nomes, números, datas, fontes específicas)?
- A diversidade de fontes é adequada para o escopo?
- As lacunas restantes são secundárias ou críticas?

**REFINE = Busca Mais Específica Necessária**
- Fatos genéricos demais, falta especificidade?
- Lacunas críticas impedem resposta ao objetivo?
- Fontes insuficientes ou repetitivas?
- Necessidade de foco em entidades específicas mencionadas?

**NEW_PHASE = Abordagem Completamente Diferente**
- Mudança significativa de escopo, temporalidade ou fonte?
- Ângulo de pesquisa diferente que pode revelar informações complementares?
- Necessidade de abordar aspectos não cobertos pela pesquisa atual?

**PRINCÍPIO FUNDAMENTAL:** Priorize QUALIDADE sobre QUANTIDADE. É melhor ter poucos fatos de alta qualidade que respondem ao objetivo do que muitos fatos genéricos.""",
}


# ============================================================================
# 1. STATE DEFINITION (Completo - espelha Orchestrator)
# ============================================================================

class PlannerLLM:
    def __init__(self, valves):
        self.valves = valves
        # Usar modelo específico se configurado, senão usa modelo padrão
        model = valves.LLM_MODEL_PLANNER or valves.LLM_MODEL
        self.model_name = model
        self.llm = _get_llm(valves, model_name=model)
        # Base kwargs: serão filtrados por get_safe_llm_params
        self.generation_kwargs = {"temperature": 0}

    def _build_prompt(
        self,
        user_prompt: str,
        phases: int,
        current_date: Optional[str] = None,
        detected_context: Optional[dict] = None,
    ) -> str:
        """Use unified Planner prompt."""
        return _build_planner_prompt(
            user_prompt,
            phases,
            current_date=current_date,
            detected_context=detected_context,
        )

    async def _generate_seed_core_with_llm(
        self, phase: dict, entities: List[str], geo_bias: List[str]
    ) -> Optional[str]:
        """Gera seed_core usando LLM (1 frase rica, sem operadores)."""
        try:
            objective = phase.get("objective", "")[:150]
            seed_family = phase.get("seed_family_hint", "entity-centric")

            # Templates por família (orientar LLM)
            family_templates = {
                "entity-centric": "entidade + tema + recorte geotemporal",
                "problem-centric": "problema/risco + drivers/causas + contexto",
                "outcome-centric": "efeito/resultado + indicadores + stakeholders",
                "regulatory": "norma/regulador + exigências + abrangência",
                "counterfactual": "tese/controvérsia + objeção + evidência-chave",
            }

            template = family_templates.get(seed_family, "entidade + tema + contexto")
            entities_str = ", ".join(entities[:3]) if entities else "N/A"
            geo_str = ", ".join(geo_bias[:2]) if geo_bias else "Brasil"

            prompt = f"""Gere seed_core (1 frase, 12-200 chars, sem operadores).
OBJETIVO: {objective}
ENTIDADES: {entities_str}
GEO: {geo_str}
FAMÍLIA: {seed_family} → {template}
REGRAS:
- SEM operadores (site:, filetype:, OR, AND, aspas)
- Linguagem natural, clara
- Incluir 1-2 entidades canônicas
- Incluir geo quando relevante
SAÍDA (JSON puro):
{{"seed_core": "sua frase aqui"}}"""

            # Chamar LLM com timeout curto (20s)
            safe_kwargs = get_safe_llm_params(self.model_name, self.generation_kwargs)
            safe_kwargs["request_timeout"] = 20

            result = await asyncio.wait_for(
                self.llm.run(prompt, generation_kwargs=safe_kwargs),
                timeout=25,  # 25s total (20s HTTP + 5s margem)
            )

            # Handle both response formats: {"content": ...} or {"replies": [...]}
            content = None
            if result and isinstance(result, dict):
                if "content" in result:
                    content = result["content"]
                elif "replies" in result and result["replies"]:
                    content = result["replies"][0]
            
            if not content:
                logger.warning("[Planner] LLM seed_core: resposta vazia ou formato inválido")
                return None

            content = content.strip()

            # Parse JSON
            parsed = parse_json_resilient(content)
            if not parsed or not isinstance(parsed, dict):
                logger.warning(
                    f"[Planner] LLM seed_core: JSON inválido - {content[:100]}"
                )
                return None

            seed_core = parsed.get("seed_core", "").strip()

            # Validar
            if not seed_core or len(seed_core) < 12 or len(seed_core) > 200:
                logger.warning(
                    f"[Planner] LLM seed_core: tamanho inválido ({len(seed_core)} chars)"
                )
                return None

            # Verificar operadores proibidos
            forbidden_ops = ["site:", "filetype:", "after:", "before:"]
            if any(op in seed_core for op in forbidden_ops):
                logger.warning(
                    f"[Planner] LLM seed_core contém operadores proibidos: {seed_core}"
                )
                return None

            logger.info(
                f"[Planner] LLM seed_core gerado com sucesso: '{seed_core[:80]}...'"
            )
            return seed_core

        except asyncio.TimeoutError:
            logger.warning("[Planner] LLM seed_core: timeout após 25s")
            return None
        except Exception as e:
            logger.warning(f"[Planner] LLM seed_core falhou: {e}")
            return None

    def _validate_contract(self, obj: dict, phases: int, user_prompt: str) -> dict:
        """Valida e normaliza o contrato do novo formato JSON"""
        phases_list = obj.get("phases", [])
        intent_profile = obj.get("intent_profile", "company_profile")

        if not phases_list:
            raise ValueError("Nenhuma fase encontrada")

        # Validar número de fases (pode ser MENOS que o máximo, mas não MAIS)
        if len(phases_list) > phases:
            raise ValueError(
                f"Excesso de fases: {len(phases_list)} > {phases} (máximo permitido)"
            )

        # Defaults do perfil
        profile_defaults = getattr(self.valves, "PROFILE_DEFAULTS", {})
        profile = (
            intent_profile if intent_profile in profile_defaults else "company_profile"
        )
        defaults = profile_defaults.get(profile, {})

        # Validar cada fase
        validated_phases = []
        for i, phase in enumerate(phases_list, 1):
            # Validar campos obrigatórios
            required_fields = [
                "name",
                "objective",
                "seed_query",
                "seed_core",
                "must_terms",
                "time_hint",
                "source_bias",
                "evidence_goal",
                "lang_bias",
                "geo_bias",
            ]

            for field in required_fields:
                if field not in phase:
                    raise ValueError(f"Fase {i} falta campo obrigatório: {field}")

            # Validar seed_query (3-6 palavras, sem operadores)
            seed_query = phase["seed_query"].strip()
            
            # Validar seed_query (3-8 palavras, SEM contar @noticias)
            clean_words = [
                w for w in seed_query.split() if w.lower() != "@noticias" and w.strip()
            ]
            if len(clean_words) < 3 or len(clean_words) > 8:
                raise ValueError(
                    f"Fase {i}: seed_query (sem @noticias) deve ter 3-8 palavras, tem {len(clean_words)} palavras: {clean_words}"
                )

            # Validar proibição de operadores
            forbidden_ops = [
                "site:",
                "filetype:",
                "after:",
                "before:",
                "AND",
                "OR",
                '"',
                "'",
            ]
            for op in forbidden_ops:
                if op in seed_query:
                    raise ValueError(
                        f"Fase {i}: seed_query não pode conter operador '{op}'"
                    )

            # Validar avoid_terms não sobrepõe must_terms
            must_terms = phase["must_terms"]
            avoid_terms = phase.get("avoid_terms", [])  # Default to empty list if missing
            overlap = set(must_terms) & set(avoid_terms)
            if overlap:
                raise ValueError(
                    f"Fase {i}: must_terms e avoid_terms sobrepõem: {overlap}"
                )

            # Validar time_hint
            time_hint = phase.get("time_hint") or defaults.get(
                "time_hint", {"recency": "1y", "strict": False}
            )
            
            if "recency" not in time_hint or time_hint["recency"] not in [
                "90d",
                "1y",
                "3y",
            ]:
                raise ValueError(f"Fase {i}: time_hint.recency deve ser 90d, 1y ou 3y")

            # Validar seed_core
            seed_core = phase.get("seed_core", "").strip()
            
            if not seed_core or len(seed_core) < 12:
                logger.warning(f"[PLANNER] Phase '{phase.get('name', 'unnamed')}' missing valid seed_core (len={len(seed_core) if seed_core else 0}), using fallback")
                # Keep minimal fallback for edge cases only
                canonical = obj.get("entities", {}).get("canonical", [])
                objective_words = phase["objective"].split()[:10]
                seed_core = f"{' '.join(canonical[:2])} {seed_query} {objective_words}".strip()[:200]
                seed_core_source = "orchestrator_fallback"
            else:
                seed_core_source = "planner_llm"  # From LLM

            # Store seed_core and source for telemetry
            phase["seed_core"] = seed_core
            phase["seed_core_source"] = seed_core_source

            # Validar source_bias
            valid_sources = ["oficial", "primaria", "secundaria", "terciaria"]
            source_bias = phase.get("source_bias") or defaults.get(
                "source_bias", ["oficial", "primaria", "secundaria"]
            )
            if not all(s in valid_sources for s in source_bias):
                raise ValueError(
                    f"Fase {i}: source_bias deve conter apenas: {valid_sources}"
                )

            # Validar evidence_goal
            evidence_goal = phase.get("evidence_goal") or {
                **{"official_or_two_independent": True},
                **defaults.get("evidence_goal", {"min_domains": 3}),
            }
            if (
                "official_or_two_independent" not in evidence_goal
                or "min_domains" not in evidence_goal
            ):
                raise ValueError(
                    f"Fase {i}: evidence_goal deve conter official_or_two_independent e min_domains"
                )

            validated_phases.append(
                {
                    "id": i,
                    "objetivo": phase["objective"],
                    "query_sugerida": seed_query,  # Para compatibilidade
                    "name": phase["name"],
                    "seed_query": seed_query,
                    "seed_core": phase.get("seed_core", ""),
                    "seed_family_hint": phase.get("seed_family_hint", "entity-centric"),
                    "must_terms": must_terms,
                    "avoid_terms": avoid_terms,
                    "time_hint": time_hint,
                    "source_bias": source_bias,
                    "evidence_goal": evidence_goal,
                    "lang_bias": phase.get("lang_bias")
                    or defaults.get("lang_bias", ["pt-BR", "en"]),
                    "geo_bias": phase.get("geo_bias")
                    or defaults.get("geo_bias", ["BR", "global"]),
                    "suggested_domains": phase.get("suggested_domains", []),
                    "suggested_filetypes": phase.get("suggested_filetypes", []),
                    "accept_if_any_of": [f"Info sobre {phase['objective']}"],  # Para compatibilidade
                }
            )

        if len(validated_phases) < 2:
            raise ValueError(f"Apenas {len(validated_phases)} fases")

        # Validar entity coverage
        entities_canonical = obj.get("entities", {}).get("canonical", [])
        contract_dict = {
            "versao": "3.0",
            "intent": obj.get("intent", f"Pesquisar: {user_prompt}"),
            "intent_profile": intent_profile,
            "entities": obj.get("entities", {"canonical": [], "aliases": []}),
            "fases": validated_phases,
            "quality_rails": obj.get(
                "quality_rails",
                {
                    "min_unique_domains": max(self.valves.MIN_UNIQUE_DOMAINS, phases),
                    "need_official_or_two_independent": self.valves.REQUIRE_OFFICIAL_OR_TWO_INDEPENDENT,
                },
            ),
            "budget": obj.get("budget", {"max_rounds": 2}),
        }

        return contract_dict

    async def run(
        self,
        user_prompt: str,
        phases: int = 2,
        current_date: Optional[str] = None,
        previous_plan: Optional[str] = None,
        detected_context: Optional[dict] = None,
    ) -> Dict[str, Any]:
        if not self.llm:
            raise ValueError("LLM não configurado")

        phases = max(2, min(10, int(phases or 2)))

        # Se houver plano anterior, contextualize para permitir refinamento
        if previous_plan:
            contextual_prompt = f"""PLANO ANTERIOR:
{previous_plan}

PEDIDO DE REFINAMENTO/AJUSTE:
{user_prompt}

INSTRUÇÕES:
- Se o pedido for um refinamento/ajuste do plano anterior (ex: "pesquise notícias de 2025", "adicione mais fases"), ATUALIZE o plano anterior
- Mantenha as fases existentes e ajuste apenas o que foi solicitado
- Se for um pedido COMPLETAMENTE NOVO (sem relação com o plano anterior), crie um novo plano
- Responda com o plano completo (anterior ajustado OU novo)"""
            prompt = self._build_prompt(
                contextual_prompt,
                phases,
                current_date=current_date,
                detected_context=detected_context,
            )
        else:
            prompt = self._build_prompt(
                user_prompt,
                phases,
                current_date=current_date,
                detected_context=detected_context,
            )

        for attempt in range(1, 3):
            try:
                # Base params (serão filtrados para GPT-5/O1)
                base_params = dict(self.generation_kwargs)

                # JSON mode to reduce latency/noise
                if getattr(self.valves, "FORCE_JSON_MODE", True):
                    base_params["response_format"] = {"type": "json_object"}

                # Planner fast-read timeout (fail fast)
                prt = int(
                    getattr(
                        self.valves,
                        "PLANNER_REQUEST_TIMEOUT",
                        getattr(self.valves, "LLM_TIMEOUT_PLANNER", 180),
                    )
                    or 180
                )
                cap = int(getattr(self.valves, "HTTPX_READ_TIMEOUT", 180) or 180)
                base_params["request_timeout"] = max(20, min(prt, cap))

                # Filtrar parâmetros incompatíveis com GPT-5/O1
                gen_kwargs = get_safe_llm_params(self.model_name, base_params)

                # Use retry function if enabled, otherwise single attempt
                if getattr(self.valves, "ENABLE_LLM_RETRY", True):
                    max_retries = int(getattr(self.valves, "LLM_MAX_RETRIES", 3) or 3)
                    out = await _safe_llm_run_with_retry(
                        self.llm,
                        prompt,
                        gen_kwargs,
                        timeout=int(
                            getattr(
                                self.valves,
                                "LLM_TIMEOUT_PLANNER",
                                self.valves.LLM_TIMEOUT_DEFAULT,
                            )
                            or self.valves.LLM_TIMEOUT_DEFAULT
                        ),
                        max_retries=max_retries,
                    )
                else:
                    out = await _safe_llm_run_with_retry(
                        self.llm,
                        prompt,
                        gen_kwargs,
                        timeout=int(
                            getattr(
                                self.valves,
                                "LLM_TIMEOUT_PLANNER",
                                self.valves.LLM_TIMEOUT_DEFAULT,
                            )
                            or self.valves.LLM_TIMEOUT_DEFAULT
                        ),
                        max_retries=1,
                    )
                if not out or not out.get("replies"):
                    raise ValueError("LLM vazio")

                obj = _extract_json_from_text(out["replies"][0])
                if not obj:
                    raise ValueError("JSON inválido")

                contract = self._validate_contract(obj, phases, user_prompt)
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[Planner] {len(contract['fases'])} fases")

                return {"contract": contract, "contract_hash": _hash_contract(contract)}

            except (json.JSONDecodeError, ValueError, KeyError) as e:
                # Erros de parse/validação - tentar novamente
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[Planner] Tentativa {attempt} - Parse error: {e}")
                if attempt < 2:
                    # Retry with compact prompt (avoid appending error text)
                    prompt = _build_planner_prompt(
                        user_prompt,
                        phases,
                        current_date=current_date,
                        detected_context=detected_context,
                    )
                else:
                    raise ContractGenerationError(
                        f"Failed to generate valid contract after 3 attempts: {e}"
                    ) from e
            except ContractValidationError as e:
                # Erro de validação específico - propagar
                raise
            except Exception as e:
                # Erro inesperado - tentar uma vez, depois propagar
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[Planner] Tentativa {attempt} - Unexpected error: {e}")
                if attempt < 2:
                    # Retry with compact prompt as above
                    prompt = _build_planner_prompt(
                        user_prompt,
                        phases,
                        current_date=current_date,
                        detected_context=detected_context,
                    )
                else:
                    raise ContractGenerationError(
                        f"Unexpected error generating contract: {e}"
                    ) from e
        
        # Fallback return (should never reach here)
        return {"contract": {}, "contract_hash": ""}


# ============================================================================
# 1. STATE DEFINITION (Completo - espelha Orchestrator)
# ============================================================================

class ResearchState(TypedDict, total=False):
    """Estado compartilhado entre nós LangGraph - TODOS os campos do Orchestrator

    Observação: Estrutura plena mantida por compatibilidade. Para tipagem e validação
    gradual, modelos Pydantic hierárquicos são introduzidos abaixo.
    """
    # Campos originais mantidos (ver referência anterior)
    correlation_id: str
    query: str
    original_query: str
    phase_index: int
    phase_context: Dict
    loop_count: int
    max_loops: int
    contract: Dict
    all_phase_queries: List[str]
    intent_profile: str
    discovered_urls: List[str]
    new_urls: List[str]
    cached_urls: List[str]
    scraped_cache: Dict[str, str]
    raw_content: str
    filtered_content: str
    accumulated_context: str
    analysis: Dict
    summary: str
    facts: List[Dict]
    lacunas: List
    self_assessment: Dict
    evidence_metrics: Dict
    unique_domains: int
    facts_with_evidence: int
    facts_with_multiple_sources: int
    high_confidence_facts: int
    contradictions: int
    evidence_coverage: float
    used_claim_hashes: List[str]
    used_domains: List[str]
    new_facts_ratio: float
    new_domains_ratio: float
    judgement: Dict
    verdict: Literal["done", "refine", "new_phase"]
    reasoning: str
    next_query: Optional[str]
    new_phase: Optional[Dict]
    phase_score: float
    phase_metrics: Dict
    modifications: List[str]
    coverage_score: float
    entities_covered: int
    analyst_confidence: str
    gaps_critical: bool
    suggest_refine: bool
    suggest_pivot: bool
    telemetry_loops: List[Dict]
    seed_core_source: Optional[str]
    analyst_proposals: Dict
    __event_emitter__: Optional[Callable]
    diminishing_returns: bool
    failed_query: bool
    previous_queries: List[str]
    failed_queries: List[str]
    phase_results: List[Dict]
    final_synthesis: Optional[str]


class RSQueryModel(BaseModel):
    objetivo: str
    previous_queries: List[str] = []
    next_query: Optional[str] = None


class RSEvidenceModel(BaseModel):
    url: str
    trecho: Optional[str] = None


class RSFactModel(BaseModel):
    texto: str
    confiança: Literal["alta", "média", "baixa"]
    evidencias: Optional[List[RSEvidenceModel]] = []


class RSResultsModel(BaseModel):
    discoveries: List[str] = []
    facts: List[RSFactModel] = []
    coverage_score: float = 0.0


class RSTelemetryModel(BaseModel):
    correlation_id: str
    loop_count: int = 0
    max_loops: int = 3


class ResearchStateModel(BaseModel):
    query: RSQueryModel
    results: RSResultsModel
    telemetry: RSTelemetryModel
# ============================================================================
# 2. HELPER CLASSES (COMPLETAS - já migradas acima)
# ============================================================================
# Todas as classes helper já foram migradas completamente:
# - Deduplicator (linhas 909+)
# - AsyncOpenAIClient (linhas 1526+)  
# - AnalystLLM (linhas 1675+)
# - JudgeLLM (linhas 2106+)
# - PlannerLLM (linhas 3129+)
# - PROMPTS dictionary (linhas 2800+)
# ============================================================================
# 3. GRAPH NODES (Implementações completas)
# ============================================================================
# ============================================================================
# 3. NODE WRAPPERS (Chamam código existente)
# ============================================================================
class GraphNodes:
    """Wrappers FINOS - delegam para código existente (ex-Orchestrator)"""
    
    def __init__(self, valves, discovery_tool, scraper_tool, context_reducer_tool=None):
        self.valves = valves
        self.discovery_tool = discovery_tool
        self.scraper_tool = scraper_tool
        self.context_reducer_tool = context_reducer_tool
        
        # Instanciar LLM components
        self.analyst = None
        self.judge = None
        self.deduplicator = None
    
    async def discovery_node(self, state: ResearchState) -> Dict:
        """Discovery node - complete implementation from Orchestrator._run_discovery"""
        correlation_id = state.get('correlation_id', 'unknown')
        em = state.get('__event_emitter__')
        await _safe_emit(em, f"[DISCOVERY][{correlation_id}] start")
        query = state.get('query', '')
        phase_context = state.get('phase_context', {})
        
        # ===== TELEMETRY =====
        tel = StepTelemetry(
            step="discovery",
            correlation_id=correlation_id,
            start_ms=time.time() * 1000,
            inputs_brief=f"query={query[:50]}...",
            counters={"urls_found": 0, "new_urls": 0},
        )
        
        try:
            # Call discovery tool
            discovery_params = {
                "query": query,
                "phase_context": phase_context,
                "correlation_id": correlation_id,
            }
            
            @retry_with_backoff(max_attempts=3, base_delay=1.0)
            async def _call_discovery(params: Dict[str, Any]):
                if asyncio.iscoroutinefunction(self.discovery_tool):
                    return await self.discovery_tool(**params)
                return await asyncio.to_thread(lambda: self.discovery_tool(**params))

            discovery_result = await _call_discovery(discovery_params)
            
            if isinstance(discovery_result, str):
                discovery_result = json.loads(discovery_result)
            
            discovered_urls = discovery_result.get("urls", [])
            new_urls = [url for url in discovered_urls if url not in state.get('scraped_cache', {})]
            
            # ===== TELEMETRY FINAL =====
            if tel is not None:
                tel.end_ms = time.time() * 1000
                tel.success = True
                if tel.counters is not None:
                    tel.counters["urls_found"] = len(discovered_urls)
                    tel.counters["new_urls"] = len(new_urls)
                tel.outputs_brief = f"found={len(discovered_urls)}, new={len(new_urls)}"
                
                # Validate telemetry completeness
                if tel.end_ms <= tel.start_ms:
                    logger.warning(f"[{tel.step.upper()}] Invalid telemetry timing: end_ms={tel.end_ms} <= start_ms={tel.start_ms}")
                if tel.counters is not None and tel.counters.get("urls_found", 0) < 0:
                    logger.warning(f"[{tel.step.upper()}] Invalid counter: urls_found={tel.counters['urls_found']}")
                
                logger.info(f"[{tel.step.upper()}] {json.dumps(tel.to_dict())}")
            
            # Validate state transition
            if not isinstance(discovered_urls, list):
                logger.warning(f"[DISCOVERY] State validation: discovered_urls is not a list: {type(discovered_urls)}")
                discovered_urls = []
            if not isinstance(new_urls, list):
                logger.warning(f"[DISCOVERY] State validation: new_urls is not a list: {type(new_urls)}")
                new_urls = []
            
            out = {
                "discovered_urls": discovered_urls,
                "new_urls": new_urls,
                "cached_urls": list(state.get('scraped_cache', {}).keys()),
            }
            await _safe_emit(em, f"[DISCOVERY][{correlation_id}] end urls={len(discovered_urls)} new={len(new_urls)}")
            return out
            
        except Exception as e:
            import traceback as _tb
            tb_str = _tb.format_exc()
            err = PipelineError(
                stage="discovery",
                error_type=e.__class__.__name__,
                message=str(e),
                context={"query": query},
                traceback_str=tb_str,
                correlation_id=correlation_id,
            )
            logger.error(f"[DISCOVERY] {json.dumps(err.to_dict(), ensure_ascii=False)}")
            try:
                tel.mark_failed(e)
            except Exception:
                pass
            
            # Recovery: Try to return partial results if available
            partial_urls = []
            try:
                # If we have any partial results from the tool call, use them
                if 'discovery_result' in locals() and discovery_result and isinstance(discovery_result, dict):
                    partial_urls = discovery_result.get("urls", [])
            except:
                pass
            
            # Log recovery attempt
            if partial_urls:
                logger.info(f"[DISCOVERY] Recovery: Using {len(partial_urls)} partial URLs")
            else:
                logger.warning(f"[DISCOVERY] Recovery: No partial results available")
            
            out = {
                "discovered_urls": partial_urls,
                "new_urls": [url for url in partial_urls if url not in state.get('scraped_cache', {})],
                "cached_urls": list(state.get('scraped_cache', {}).keys()),
                "error": str(e),
                "failed_query": True,
            }
            await _safe_emit(em, f"[DISCOVERY][{correlation_id}] recovery urls={len(partial_urls)}")
            return out
    
    async def scrape_node(self, state: ResearchState) -> Dict:
        """Scrape node - complete implementation from Orchestrator._run_scraping"""
        correlation_id = state.get('correlation_id', 'unknown')
        em = state.get('__event_emitter__')
        await _safe_emit(em, f"[SCRAPE][{correlation_id}] start")
        new_urls = state.get('new_urls', [])
        scraped_cache = state.get('scraped_cache', {})
        
        # Concurrency valve (default 5) for per-URL fallback scraping
        concurrency = int(getattr(self.valves, 'SCRAPER_CONCURRENCY', 5) or 5)
        
        # ===== TELEMETRY =====
        tel = StepTelemetry(
            step="scraping",
            correlation_id=correlation_id,
            start_ms=time.time() * 1000,
            inputs_brief=f"urls={len(new_urls)}",
            counters={"scraped": 0, "failed": 0},
        )
        
        try:
            # Call scraper tool for new URLs
            if new_urls:
                scrape_params = {
                    "urls": new_urls,
                    "correlation_id": correlation_id,
                }
                
                batch_success = False
                try:
                    # Primary: batch scraping via tool (preferred)
                    if asyncio.iscoroutinefunction(self.scraper_tool):
                        scrape_result = await self.scraper_tool(**scrape_params)
                    else:
                        scrape_result = await asyncio.to_thread(
                            lambda: self.scraper_tool(**scrape_params)
                        )
                    if isinstance(scrape_result, str):
                        scrape_result = json.loads(scrape_result)
                    scraped_content = scrape_result.get("content", {})
                    if isinstance(scraped_content, dict) and scraped_content:
                        scraped_cache.update(scraped_content)
                        batch_success = True
                except TypeError as e:
                    # Tool doesn't support correlation_id - try with basic params
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(f"[S_WRAPPER] Advanced params failed: {e}")
                        print(f"[S_WRAPPER] Falling back to basic urls only")
                    try:
                        basic_params = {"urls": new_urls}
                        if asyncio.iscoroutinefunction(self.scraper_tool):
                            scrape_result = await self.scraper_tool(**basic_params)
                        else:
                            scrape_result = await asyncio.to_thread(
                                lambda: self.scraper_tool(**basic_params)
                            )
                        if isinstance(scrape_result, str):
                            scrape_result = json.loads(scrape_result)
                        scraped_content = scrape_result.get("content", {})
                        if isinstance(scraped_content, dict) and scraped_content:
                            scraped_cache.update(scraped_content)
                            batch_success = True
                    except Exception as fallback_e:
                        logger.error(f"[S_WRAPPER] Even basic scraping failed: {fallback_e}")
                        # Continue to per-URL fallback
                except Exception as e:
                    logger.error(f"[S_WRAPPER] Unexpected scraping error: {e}")
                    # Continue to per-URL fallback

                # Fallback: per-URL concurrent scraping if batch failed or returned empty
                if not batch_success:
                    await _safe_emit(em, f"[SCRAPE][{correlation_id}] batch failed/empty, using per-URL concurrency={concurrency}")
                    sem = asyncio.Semaphore(max(1, concurrency))

                    @retry_with_backoff(max_attempts=3, base_delay=1.0)
                    async def _scrape_one(u: str) -> tuple[str, Optional[str]]:
                        async with sem:
                            try:
                                # Attempt tool with single-URL param shapes
                                # Prefer common shapes: {urls:[u]} then {url:u}
                                if asyncio.iscoroutinefunction(self.scraper_tool):
                                    res = await self.scraper_tool(**{"urls": [u]})
                                else:
                                    res = await asyncio.to_thread(lambda: self.scraper_tool(**{"urls": [u]}))
                            except TypeError:
                                try:
                                    if asyncio.iscoroutinefunction(self.scraper_tool):
                                        res = await self.scraper_tool(**{"url": u})
                                    else:
                                        res = await asyncio.to_thread(lambda: self.scraper_tool(**{"url": u}))
                                except Exception as e:
                                    logger.debug(f"[S_ONE] {u} failed: {e}")
                                    return (u, None)
                            except Exception as e:
                                logger.debug(f"[S_ONE] {u} failed: {e}")
                                return (u, None)

                            try:
                                if isinstance(res, str):
                                    res = json.loads(res)
                                # Normalize result
                                if isinstance(res, dict):
                                    # Accept shapes: {content:{url: text}} or {url:..., content:...}
                                    if "content" in res and isinstance(res["content"], dict):
                                        return (u, res["content"].get(u))
                                    if "url" in res and "content" in res and isinstance(res["url"], str):
                                        return (res["url"], res["content"])  # pragma: no cover
                                elif isinstance(res, list):
                                    # List of dicts with url/content
                                    for it in res:
                                        if isinstance(it, dict) and it.get("url") == u:
                                            return (u, it.get("content") or it.get("text"))
                                return (u, None)
                            except Exception as e:
                                logger.debug(f"[S_ONE] {u} parse failed: {e}")
                                return (u, None)

                    results = await asyncio.gather(*[_scrape_one(u) for u in new_urls], return_exceptions=False)
                    added = 0
                    for (u, text) in results:
                        if u and text:
                            scraped_cache[u] = text
                            added += 1
                    await _safe_emit(em, f"[SCRAPE][{correlation_id}] per-URL scraped={added}")
            
            # ===== TELEMETRY FINAL =====
            if tel is not None:
                tel.end_ms = time.time() * 1000
                tel.success = True
                if tel.counters is not None:
                    tel.counters["scraped"] = len(scraped_cache)
                tel.outputs_brief = f"total_scraped={len(scraped_cache)}"
                
                # Validate telemetry completeness
                if tel.end_ms <= tel.start_ms:
                    logger.warning(f"[{tel.step.upper()}] Invalid telemetry timing: end_ms={tel.end_ms} <= start_ms={tel.start_ms}")
                if tel.counters is not None and tel.counters.get("scraped", 0) < 0:
                    logger.warning(f"[{tel.step.upper()}] Invalid counter: scraped={tel.counters['scraped']}")
                
                logger.info(f"[{tel.step.upper()}] {json.dumps(tel.to_dict())}")
            
            # Validate state transition
            if not isinstance(scraped_cache, dict):
                logger.warning(f"[SCRAPING] State validation: scraped_cache is not a dict: {type(scraped_cache)}")
                scraped_cache = {}
            
            out = {
                "scraped_cache": scraped_cache,
                "raw_content": "\n\n".join(scraped_cache.values()),
            }
            await _safe_emit(em, f"[SCRAPE][{correlation_id}] end cache={len(scraped_cache)} bytes={len(out['raw_content'])}")
            return out
            
        except Exception as e:
            import traceback as _tb
            tb_str = _tb.format_exc()
            err = PipelineError(
                stage="scraping",
                error_type=e.__class__.__name__,
                message=str(e),
                context={"new_urls": len(new_urls)},
                traceback_str=tb_str,
                correlation_id=correlation_id,
            )
            logger.error(f"[SCRAPING] {json.dumps(err.to_dict(), ensure_ascii=False)}")
            try:
                tel.mark_failed(e)
            except Exception:
                pass
            
            # Recovery: Return existing cache even if new scraping failed
            logger.info(f"[SCRAPING] Recovery: Returning existing cache with {len(scraped_cache)} URLs")
            
            out = {
                "scraped_cache": scraped_cache,
                "raw_content": "\n\n".join(scraped_cache.values()),
                "error": str(e),
                "failed_query": True,
            }
            await _safe_emit(em, f"[SCRAPE][{correlation_id}] recovery cache={len(scraped_cache)}")
            return out
    
    async def reduce_node(self, state: ResearchState) -> Dict:
        """Reduce node - complete implementation from Orchestrator._run_context_reduction"""
        correlation_id = state.get('correlation_id', 'unknown')
        em = state.get('__event_emitter__')
        await _safe_emit(em, f"[REDUCE][{correlation_id}] start")
        raw_content = state.get('raw_content', '')
        accumulated_context = state.get('accumulated_context', '')
        all_phase_queries = state.get('all_phase_queries', [])
        job_id = state.get('job_id', '')
        
        # Early return if no content or tool not available
        if not raw_content or not getattr(self.valves, 'ENABLE_CONTEXT_REDUCER', False) or not self.context_reducer_tool:
            accumulated_context += f"\n{raw_content}\n"
            out = {
                "filtered_content": raw_content,
                "accumulated_context": accumulated_context,
            }
            await _safe_emit(em, f"[REDUCE][{correlation_id}] bypass chars={len(raw_content)} acc={len(accumulated_context)}")
            return out
        
        # ===== TELEMETRY =====
        tel = StepTelemetry(
            step="context_reducer",
            correlation_id=correlation_id,
            start_ms=time.time() * 1000,
            inputs_brief=f"content={len(raw_content)} chars",
            counters={"input_chars": len(raw_content), "output_chars": 0},
        )
        
        try:
            context_params = {
                "corpo": {"ultrasearcher_result": {"scraped_content": raw_content}},
                "mode": "coarse",
                "queries": all_phase_queries,
                "tipo": "fase",
            }
            
            if job_id:
                context_params["job_id"] = job_id
            
            if self.context_reducer_tool is not None:
                try:
                    tool = self.context_reducer_tool  # Store reference to avoid linter issues
                    @retry_with_backoff(max_attempts=2, base_delay=1.0)
                    async def _call_context(params: Dict[str, Any]):
                        if asyncio.iscoroutinefunction(tool):
                            return await tool(**params)
                        return await asyncio.to_thread(lambda: tool(**params))

                    context_result = await _call_context(context_params)
                except Exception as e:
                    logger.warning(f"Context Reducer failed: {e}")
                    context_result = {"final_markdown": raw_content}
            else:
                context_result = {"final_markdown": raw_content}
            
            if isinstance(context_result, str):
                context_result = json.loads(context_result)
            
            filtered_content = context_result.get("final_markdown", raw_content)
            reduction = (
                (1 - len(filtered_content) / len(raw_content)) * 100
                if len(raw_content) > 0
                else 0
            )
            
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[Context Reducer] coarse: {len(raw_content)} → {len(filtered_content)} chars (-{reduction:.1f}%)")
            
            # ===== TELEMETRY FINAL =====
            if tel is not None:
                tel.end_ms = time.time() * 1000
                tel.success = True
                if tel.counters is not None:
                    tel.counters["output_chars"] = len(filtered_content)
                tel.outputs_brief = f"reduced={len(filtered_content)} chars (-{reduction:.1f}%)"
                logger.info(f"[{tel.step.upper()}] {json.dumps(tel.to_dict())}")
            
            # Accumulate context
            accumulated_context += f"\n{filtered_content}\n"
            
            out = {
                "filtered_content": filtered_content,
                "accumulated_context": accumulated_context,
            }
            await _safe_emit(em, f"[REDUCE][{correlation_id}] end chars={len(filtered_content)} acc={len(accumulated_context)}")
            return out
            
        except (KeyError, ValueError, TypeError) as e:
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[Context Reducer] Parse error: {e}, usando raw")
            try:
                tel.mark_failed(e)
            except Exception:
                pass
            accumulated_context += f"\n{raw_content}\n"
            out = {
                "filtered_content": raw_content,
                "accumulated_context": accumulated_context,
            }
            await _safe_emit(em, f"[REDUCE][{correlation_id}] parse_error acc={len(accumulated_context)}")
            return out
        except Exception as e:
            import traceback as _tb
            tb_str = _tb.format_exc()
            err = PipelineError(
                stage="reducer",
                error_type=e.__class__.__name__,
                message=str(e),
                context={"raw_len": len(raw_content)},
                traceback_str=tb_str,
                correlation_id=correlation_id,
            )
            logger.error(f"[REDUCER] {json.dumps(err.to_dict(), ensure_ascii=False)}")
            try:
                tel.mark_failed(e)
            except Exception:
                pass
            accumulated_context += f"\n{raw_content}\n"
            out = {
                "filtered_content": raw_content,
                "accumulated_context": accumulated_context,
            }
            await _safe_emit(em, f"[REDUCE][{correlation_id}] fail acc={len(accumulated_context)}")
            return out
    
    async def analyze_node(self, state: ResearchState) -> Dict:
        """Analyze node - complete implementation from Orchestrator._run_analysis"""
        correlation_id = state.get('correlation_id', 'unknown')
        em = state.get('__event_emitter__')
        await _safe_emit(em, f"[ANALYZE][{correlation_id}] start")
        filtered_content = state.get('filtered_content', '')
        accumulated_context = state.get('accumulated_context', '')
        phase_context = state.get('phase_context', {})
        
        # ===== TELEMETRY =====
        tel = StepTelemetry(
            step="analyst",
            correlation_id=correlation_id,
            start_ms=time.time() * 1000,
            inputs_brief=f"context={len(accumulated_context)} chars",
            counters={"facts": 0, "lacunas": 0},
        )
        
        # Accumulate filtered content
        if filtered_content:
            accumulated_context += f"\n\n{filtered_content}"
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[Accumulator] Total acumulado: {len(accumulated_context)} chars")
        
        # Deduplicação opcional para Analyst
        analyst_context = accumulated_context
        
        if getattr(self.valves, 'ENABLE_ANALYST_DEDUPLICATION', False) and accumulated_context:
            # Divisão mais agressiva para garantir ativação da deduplicação
            paragraphs = [
                p.strip() for p in accumulated_context.split("\n\n") if p.strip()
            ]
            
            # Se ainda não tem parágrafos suficientes, dividir por sentenças
            if len(paragraphs) < getattr(self.valves, 'MAX_ANALYST_PARAGRAPHS', 50):
                # Dividir por sentenças (pontos seguidos de espaço)
                sentences = [
                    s.strip() for s in accumulated_context.replace('\n', ' ').split('. ') if s.strip()
                ]
                # Agrupar sentenças em parágrafos de ~3 sentenças
                paragraphs = []
                for i in range(0, len(sentences), 3):
                    paragraph = '. '.join(sentences[i:i+3])
                    if paragraph and not paragraph.endswith('.'):
                        paragraph += '.'
                    paragraphs.append(paragraph)
            
            max_paragraphs = getattr(self.valves, 'MAX_ANALYST_PARAGRAPHS', 50)
            if len(paragraphs) > max_paragraphs:
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[DEDUP ANALYST] ✅ ATIVADO: {len(paragraphs)} parágrafos > {max_paragraphs} → deduplicando para Analyst...")
                
                # Calcular preservação de contexto recente
                if filtered_content:
                    new_paragraphs = [
                        p.strip() for p in filtered_content.split("\n\n") if p.strip()
                    ]
                    new_count = len(new_paragraphs)
                    
                    # Cap dynamic preservation to avoid disabling deduplication
                    preserve_recent_pct = min(
                        0.3,
                        new_count / len(paragraphs)
                    )
                    
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(f"[DEDUP ANALYST] 🔒 Preservation: {preserve_recent_pct:.1%} ({new_count} new / {len(paragraphs)} total)")
                else:
                    # No new content - use lower preservation for old accumulated data
                    preserve_recent_pct = 0.2
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(f"[DEDUP ANALYST] ⚠️ No new content - preserving {preserve_recent_pct:.1%} of accumulated")
                
                # Usar estratégia específica do Analyst
                algorithm = getattr(self.valves, "ANALYST_DEDUP_ALGORITHM", "semantic")
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[DEDUP ANALYST] 🧠 Algoritmo: {algorithm.upper()}")
                    print(f"[DEDUP ANALYST] 📊 Input: {len(paragraphs)} parágrafos → Target: {max_paragraphs}")
                
                # Deduplicar com context-aware
                dedupe_result = self.deduplicator.dedupe(
                    chunks=paragraphs,
                    max_chunks=max_paragraphs,
                    algorithm=algorithm,
                    threshold=getattr(self.valves, "DEDUP_SIMILARITY_THRESHOLD", 0.85),
                    preserve_order=True,
                    preserve_recent_pct=preserve_recent_pct,
                    shuffle_older=True,
                    reference_first=True,
                    # Context-aware parameters
                    must_terms=phase_context.get("must_terms", []),
                    key_questions=phase_context.get("key_questions", []),
                    enable_context_aware=getattr(self.valves, "ENABLE_CONTEXT_AWARE_DEDUP", False),
                )
                
                analyst_context = "\n\n".join(dedupe_result["chunks"])
                
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[DEDUP ANALYST] {dedupe_result['original_count']} → {dedupe_result['deduped_count']} parágrafos ({dedupe_result['reduction_pct']:.1f}% redução)")
                    
                    # Additional telemetry
                    preserved_count = int(len(paragraphs) * preserve_recent_pct)
                    print(f"[DEDUP ANALYST] 📌 Recent preservation: {preserved_count} paragraphs ({preserve_recent_pct:.1%}) protected from deduplication")
                    print(f"[DEDUP ANALYST] 🎯 Valve setting: ANALYST_PRESERVE_RECENT_PCT = {getattr(self.valves, 'ANALYST_PRESERVE_RECENT_PCT', 1.0)}")
        
        # Chamar Analyst
        analysis = await self.analyst.run(
            query=state.get('query', ''),
            accumulated_context=analyst_context,
            phase_context=phase_context,
        )
        
        # Validar resultado
        if not isinstance(analysis, dict):
            logger.error(f"[CRITICAL] Analyst returned non-dict: {type(analysis)}")
            analysis = {
                "summary": "",
                "facts": [],
                "lacunas": ["Erro: Analyst retornou tipo inválido"],
            }
        
        # Garantir campos mínimos
        if not analysis.get("facts"):
            analysis["facts"] = []
        if not analysis.get("lacunas"):
            analysis["lacunas"] = []
        if not analysis.get("summary"):
            analysis["summary"] = ""
        
        if getattr(self.valves, "VERBOSE_DEBUG", False):
            print(f"[DEBUG] [ITERATION] Analyst completed, got {len(analysis.get('summary', ''))} chars summary")
            print(f"[DEBUG] [ITERATION] Analyst facts count: {len(analysis.get('facts', []))}")
            print(f"[DEBUG] [ITERATION] Analyst lacunas count: {len(analysis.get('lacunas', []))}")
        
        # Calculate evidence metrics
        evidence_metrics = _extract_quality_metrics(analysis.get("facts", []))

        # Calculate novelty metrics
        new_facts_ratio, new_domains_ratio = self._calculate_novelty_metrics(analysis, state)

        # Update state with used hashes/domains for next iteration
        import hashlib
        current_fact_hashes = []
        current_domains = set()
        
        for f in analysis.get("facts", []):
            if isinstance(f, dict):
                fact_text = f.get("texto", "")
                if fact_text:
                    current_fact_hashes.append(hashlib.sha256(fact_text.strip().lower().encode("utf-8")).hexdigest())
                
                # Extract domains from evidences
                for ev in f.get("evidencias", []) or []:
                    url = (ev or {}).get("url") or ""
                    try:
                        dom = url.split("/")[2] if "/" in url else ""
                        if dom:
                            current_domains.add(dom)
                    except Exception:
                        pass
        
        # Update state with new hashes/domains (with caps to avoid unbounded growth)
        max_hashes = 5000
        max_domains = 2000
        hashes = state.get("used_claim_hashes", []) + current_fact_hashes
        domains = state.get("used_domains", []) + list(current_domains)
        state["used_claim_hashes"] = hashes[-max_hashes:]
        state["used_domains"] = domains[-max_domains:]
        
        # ===== TELEMETRY FINAL =====
        if tel is not None:
            tel.end_ms = time.time() * 1000
            if tel.counters is not None:
                tel.counters["facts"] = len(analysis.get("facts", []))
                tel.counters["lacunas"] = len(analysis.get("lacunas", []))
            tel.outputs_brief = f"facts={len(analysis.get('facts', []))}, lacunas={len(analysis.get('lacunas', []))}"
            logger.info(f"[{tel.step.upper()}] {json.dumps(tel.to_dict())}")
            
        out = {
            "analysis": analysis,
            "summary": analysis.get('summary', ''),
            "facts": analysis.get('facts', []),
            "lacunas": analysis.get('lacunas', []),
            "self_assessment": analysis.get('self_assessment', {}),
            "evidence_metrics": evidence_metrics,
            "new_facts_ratio": new_facts_ratio,
            "new_domains_ratio": new_domains_ratio,
        }
        await _safe_emit(em, f"[ANALYZE][{correlation_id}] end facts={len(out['facts'])} gaps={len(out['lacunas'])}")
        return out
    async def judge_node(self, state: ResearchState) -> Dict:
        """Judge node - complete implementation with loop count increment and telemetry"""
        correlation_id = state.get('correlation_id', 'unknown')
        em = state.get('__event_emitter__')
        await _safe_emit(em, f"[JUDGE][{correlation_id}] start")
        analysis = state.get('analysis', {})
        phase_context = state.get('phase_context', {})
        telemetry_loops = state.get('telemetry_loops', [])
        current_loop = state.get('loop_count', 0)
        
        logger.info(f"[JUDGE][{correlation_id}] Avaliando fase (loop {current_loop})")
        
        try:
            # Prepare telemetry data for Judge
            telemetry_entry = {
                "loop": current_loop,
                "n_facts": len(analysis.get("facts", [])),
                "unique_domains": 0,  # Would be calculated from evidence_metrics
                "new_facts_ratio": analysis.get("new_facts_ratio", 0.0),
                "new_domains_ratio": analysis.get("new_domains_ratio", 0.0),
                "contradictions": 0,  # Would be calculated from analysis
            }
            
            # Add to telemetry loops
            updated_telemetry_loops = telemetry_loops + [telemetry_entry]
            
            # Call Judge LLM with full context
            _emit_decision_snapshot(
                step="judge",
                vector={
                    "loop": current_loop,
                    "facts": len(analysis.get("facts", [])),
                },
                reason="judge_start",
            )
            judgement = await self.judge.run(
                user_prompt=state.get('query', ''),
                analysis=analysis,
                phase_context=phase_context,
                telemetry_loops=updated_telemetry_loops,
                intent_profile=state.get('intent_profile'),
                full_contract=state.get('contract'),
                valves=self.valves,
                refine_queries=state.get('refine_queries'),
                phase_candidates=state.get('phase_candidates'),
                previous_queries=state.get('previous_queries'),
                failed_queries=state.get('failed_queries'),
            )
            
            # Increment loop count
            new_loop_count = current_loop + 1
            
            out = {
                "judgement": judgement,
                "verdict": judgement.get("verdict", "done"),
                "reasoning": judgement.get("reasoning", ""),
                "next_query": judgement.get("next_query", ""),
                "new_phase": judgement.get("new_phase", {}),
                "phase_score": judgement.get("phase_score", 0.0),
                "phase_metrics": judgement.get("phase_metrics", {}),
                "seed_family": judgement.get("seed_family"),
                "modifications": judgement.get("modifications", []),
                # State management
                "loop_count": new_loop_count,
                "telemetry_loops": updated_telemetry_loops,
            }
            _emit_decision_snapshot(
                step="judge",
                vector={
                    "verdict": out.get("verdict"),
                    "phase_score": out.get("phase_score"),
                    "metrics": out.get("phase_metrics"),
                },
                reason="judge_result",
            )
            await _safe_emit(em, f"[JUDGE][{correlation_id}] end verdict={out['verdict']} score={out['phase_score']}")
            return out
        except Exception as e:
            logger.error(f"[JUDGE] Erro: {e}")
            return {
                "judgement": {},
                "verdict": "done",
                "reasoning": f"Erro: {e}",
                "next_query": "",
                "new_phase": {},
                "phase_score": 0.0,
                "phase_metrics": {},
                "seed_family": None,
                "modifications": [],
                # State management
                "loop_count": current_loop + 1,
                "telemetry_loops": telemetry_loops,
            }

class Pipe:
    """
    Pipe compatível com OpenWebUI - delega ao LangGraph
    
    Responsabilidades:
    1. Gerenciar ciclo de FASES (não loops internos)
    2. Criar novas fases (quando Judge retorna NEW_PHASE)
    3. Chamar síntese final
    4. Manter fallback para modo manual
    
    TODO: Copiar Valves e métodos auxiliares do PipeManual (linhas ~5000-6000)
    """
    
    class Valves(BaseModel):
        """Complete Valves configuration class - copied from PipeHaystack"""
        class Config:
            validate_assignment = True

        def __init__(self, **data):
            super().__init__(**data)

        # UX
        DEBUG_LOGGING: bool = Field(default=False, description="Logs detalhados")
        ENABLE_LINE_BUDGET_GUARD: bool = Field(
            default=False,
            description="Ativar Line-Budget Guard na inicialização (alerta sobre funções muito grandes)",
        )

        # Orquestração
        USE_PLANNER: bool = Field(default=True, description="Usar planner")
        MAX_AGENT_LOOPS: int = Field(
            default=3,
            ge=1,
            le=10,
            description="Max loops/fase (aumentado de 2→3 para evitar new_phases forçadas prematuramente)",
        )
        DEFAULT_PHASE_COUNT: int = Field(
            default=6,
            ge=2,
            le=10,
            description="Máximo de fases iniciais (Planner cria ATÉ este número). Ajuste conforme necessário: 2-3 para pesquisas focadas, 4-6 para análises complexas, 7-10 para estudos abrangentes.",
        )
        MAX_PHASES: int = Field(
            default=6,
            ge=3,
            le=15,
            description="Máximo TOTAL de fases (iniciais + criadas pelo Judge)",
        )
        VERBOSE_DEBUG: bool = Field(
            default=False, description="Habilitar logs detalhados de debug"
        )

        # Timeouts (segundos)
        LLM_TIMEOUT_DEFAULT: int = Field(
            default=60,
            ge=30,
            le=300,
            description="Timeout padrão para chamadas LLM (Planner, Judge)",
        )
        LLM_TIMEOUT_ANALYST: int = Field(
            default=90,
            ge=30,
            le=300,
            description="Timeout para Analyst (processa mais contexto)",
        )

        # ✅ NOVO: Timeout dedicado para síntese sem cap
        LLM_TIMEOUT_SYNTHESIS: int = Field(
            default=600,  # ⬆️ Aumentado: 300→600s (10 minutos)
            ge=60,
            le=1800,  # ⬆️ Máximo: 900→1800s (30 minutos para casos extremos)
            description="Timeout para Síntese Final (processa muito contexto) - Default 600s (10min), Max 1800s (30min). IMPORTANTE: Se aumentar, garanta que HTTPX_READ_TIMEOUT também suba ou será ignorado.",
        )

        # Planner/API behavior
        FORCE_JSON_MODE: bool = Field(
            default=True,
            description="Forçar response_format=json (quando suportado) para reduzir latência e ruído",
        )
        PLANNER_REQUEST_TIMEOUT: int = Field(
            default=180,
            ge=20,
            le=600,
            description="Timeout de leitura HTTP do Planner em segundos (falha rápida) – default elevado para 180s",
        )
        ENABLE_LLM_RETRY: bool = Field(
            default=True,
            description="Habilitar retry com backoff exponencial para chamadas LLM",
        )
        LLM_MAX_RETRIES: int = Field(
            default=3,
            ge=1,
            le=5,
            description="Máximo de tentativas com backoff exponencial",
        )
        HTTPX_READ_TIMEOUT: int = Field(
            default=180,
            ge=60,
            le=600,  # ⬆️ Aumentado: 300→600s
            description="Timeout de leitura HTTP base (httpx client). Para síntese final, usar LLM_TIMEOUT_SYNTHESIS.",
        )
        
        @validator("LLM_TIMEOUT_SYNTHESIS")
        def _validate_synthesis_timeout_vs_httpx(cls, v, values):
            httpx_timeout = values.get("HTTPX_READ_TIMEOUT", 180)
            try:
                httpx_num = int(httpx_timeout)
            except Exception:
                httpx_num = 180
            if v > httpx_num:
                raise ValueError(
                    f"LLM_TIMEOUT_SYNTHESIS ({v}s) deve ser <= HTTPX_READ_TIMEOUT ({httpx_num}s)"
                )
            return v
        LLM_TIMEOUT_PLANNER: int = Field(
            default=180,
            ge=60,
            le=600,
            description="Timeout externo específico do Planner (prompts maiores)",
        )

        # Synthesis Control
        ENABLE_DEDUPLICATION: bool = Field(
            default=True, description="Habilitar deduplicação na síntese final"
        )
        PRESERVE_PARAGRAPH_ORDER: bool = Field(
            default=True,
            description="Shuffle para seleção justa + reordenar para preservar narrativa (True=recomendado); False=ordenar por tamanho",
        )
        MAX_CONTEXT_CHARS: int = Field(
            default=150000,
            description="Máximo de caracteres no contexto para LLM (reduzido para melhor qualidade)",
        )

        # Deduplication Parameters - CALIBRADO PARA QUALIDADE (v4.4)
        MAX_DEDUP_PARAGRAPHS: int = Field(
            default=200,  # ⬇️ Reduzido: 300→200 parágrafos (~24k chars, ~6k tokens)
            ge=50,
            le=1000,
            description="Máximo de parágrafos após deduplicação - Default 200 (~24k chars). ATENÇÃO: >300 pode causar prompt >12k tokens levando a timeout (300s+) ou síntese genérica!",
        )
        DEDUP_SIMILARITY_THRESHOLD: float = Field(
            default=0.80,  # ⬇️ Reduzido: 0.85→0.80 (mais agressivo, -20% duplicatas)
            ge=0.0,
            le=1.0,
            description="Threshold de similaridade (0.0-1.0, mais baixo = mais agressivo)",
        )
        DEDUP_RELEVANCE_WEIGHT: float = Field(
            default=0.7,
            ge=0.0,
            le=1.0,
            description="Peso da relevância vs diversidade (0.0-1.0, mais alto = mais conservador)",
        )
        DEDUP_ALGORITHM: str = Field(
            default="mmr",
            description="Algoritmo de deduplicação: 'mmr' (padrão) | 'minhash' (rápido) | 'tfidf' (semântico) | 'semantic' (Haystack embeddings)",
        )

        CONTEXT_AWARE_PRIORITY_THRESHOLD: float = Field(
            default=0.75,
            ge=0.0,
            le=1.0,
            description="Threshold de prioridade para SEMPRE preservar chunk (0.75 = preserva top 25%)"
        )

        SEMANTIC_MODEL: str = Field(
            default="sentence-transformers/all-MiniLM-L6-v2",
            description="Modelo de embeddings para deduplicação semântica (lightweight por padrão)"
        )

        # Analyst Dedup Strategy
        ANALYST_DEDUP_ALGORITHM: str = Field(
            default="semantic",
            description="Algoritmo para Analyst: 'mmr' | 'minhash' | 'tfidf' | 'semantic'"
        )
        ANALYST_DEDUP_MODEL: str = Field(
            default="sentence-transformers/all-MiniLM-L6-v2",
            description="Modelo embeddings para Analyst (se semantic)"
        )

        # Synthesis Dedup Strategy
        SYNTHESIS_DEDUP_ALGORITHM: str = Field(
            default="mmr",
            description="Algoritmo para Synthesis: 'mmr' | 'minhash' | 'tfidf' | 'semantic'"
        )
        SYNTHESIS_DEDUP_MODEL: str = Field(
            default="sentence-transformers/paraphrase-MiniLM-L3-v2",
            description="Modelo embeddings para Synthesis (se semantic, mais rápido)"
        )

        # Context-Aware
        ENABLE_CONTEXT_AWARE_DEDUP: bool = Field(
            default=True,
            description="Ativar dedup context-aware (preserva must_terms/key_questions)"
        )
        CONTEXT_AWARE_PRESERVE_PCT: float = Field(
            default=0.12,  # P0: Reduzido temporariamente para evitar preservação excessiva
            description="% de chunks high-priority a preservar (0.0-1.0)"
        )

        # Deduplication for Analyst (per-iteration)
        ENABLE_ANALYST_DEDUPLICATION: bool = Field(
            default=False,
            description="Dedupe contexto ANTES de enviar ao Analyst (reduz tokens, mantém contexto completo para próximas iterações)",
        )
        MAX_ANALYST_PARAGRAPHS: int = Field(
            default=200,
            ge=50,
            le=500,
            description="Máximo de parágrafos para Analyst (~24k chars, ~6k tokens) - Analyst processa menos que Síntese",
        )
        ANALYST_PRESERVE_RECENT_PCT: float = Field(
            default=1.0,  # 100% by default - preserve ALL new content
            ge=0.0,
            le=1.0,
            description="% of recent content to preserve intact in Analyst deduplication (1.0 = 100% preserved, 0.95 = old behavior, 0.0 = dedupe everything)"
        )

        # Official domains mapping (used by discovery/scoring and diversity caps)
        OFFICIAL_DOMAINS: Dict[str, List[str]] = Field(
            default_factory=lambda: {
                "default": ["gov.br", ".gov", "bcb.gov.br", "cvm.gov.br"],
                "regulation_review": ["planalto.gov.br", "camara.leg.br", "senado.leg.br"],
            },
            description="Lista de domínios oficiais por perfil",
        )

        # Diversity caps by profile (for context selection)
        DIVERSITY_CAPS_BY_PROFILE: Dict[str, Dict[str, int]] = Field(
            default_factory=lambda: {
                "default": {"min_new_domains": 2, "min_official": 1, "min_independent": 2},
                "conservative": {"min_new_domains": 1, "min_official": 2, "min_independent": 1},
            },
            description="Mínimos por bucket para seleção de contexto, por perfil",
        )

        # Continue detection configuration
        CONTINUE_TERMS_OVERRIDE: Optional[List[str]] = Field(
            default=None,
            description="Substitui termos padrão de detecção de 'siga' (se None, usa defaults)",
        )
        STRICT_CONTINUE_ACTIVATION: bool = Field(
            default=True,
            description="Ativar gate estrito para execução mesmo quando detecção ampla for positiva",
        )

        # Judge Duplicate Detection
        DUPLICATE_DETECTION_THRESHOLD: float = Field(
            default=0.70,
            ge=0.5,
            le=0.9,
            description="Threshold for NEW_PHASE duplicate detection (0.70 = 70% similarity blocks duplicate, lower = more lenient, higher = more strict)"
        )

        # Quality Rails Parameters
        MIN_UNIQUE_DOMAINS: int = Field(
            default=2, description="Mínimo de domínios únicos por fase"
        )
        REQUIRE_OFFICIAL_OR_TWO_INDEPENDENT: bool = Field(
            default=True, description="Exigir fonte oficial ou duas independentes"
        )

        # --- FASE 1: Entity Coverage ---
        MIN_ENTITY_COVERAGE: float = Field(
            default=0.70,
            ge=0.0,
            le=1.0,
            description="Cobertura mínima de entidades nas fases (0.70 = 70% das fases devem conter entidades)",
        )
        MIN_ENTITY_COVERAGE_STRICT: bool = Field(
            default=True,
            description="True = hard-fail se coverage < MIN_ENTITY_COVERAGE | False = warning + Judge decide",
        )

        # --- FASE 1: Seeds ---
        SEED_VALIDATION_STRICT: bool = Field(
            default=False,
            description="False = tenta patch leve em seeds magras; True = só valida (sem patch)",
        )

        # --- FASE 1: News slot ---
        ENFORCE_NEWS_SLOT: bool = Field(
            default=True,
            description="Manter a política existente de news slot; adicionamos telemetria",
        )

        # --- FASE 2: Planner economy controls ---
        PLANNER_ECONOMY_THRESHOLD: float = Field(
            default=0.75,
            ge=0.0,
            le=1.0,
            description="Threshold de uso de budget de fases para warning (0.75 = warn se >75% usado)",
        )

        PLANNER_OVERLAP_THRESHOLD: float = Field(
            default=0.70,
            ge=0.0,
            le=1.0,
            description="Threshold de similaridade entre objectives para detectar overlap (0.70 = 70% similaridade via TF-IDF)",
        )

        # Preferred tools mapping (deterministic resolution)
        PREFERRED_TOOLS: Dict[str, str] = Field(
            default={}, description="Preferred tool names for deterministic resolution"
        )

        # (removido: OFFICIAL_DOMAINS duplicado por vertical; usar OFFICIAL_DOMAINS por perfil definido acima)

        # LLM Configuration
        OPENAI_BASE_URL: str = Field(
            default="https://api.openai.com/v1", description="API Base"
        )
        OPENAI_API_KEY: str = Field(default="", description="API Key")
        LLM_MODEL: str = Field(
            default="gpt-4o", description="Modelo padrão para todos os componentes"
        )
        LLM_TEMPERATURE: float = Field(
            default=0.2, ge=0.0, le=1.0, description="Temperature"
        )
        LLM_MAX_TOKENS: int = Field(
            default=2048,
            ge=100,
            le=4000,
            description="⚠️ DEPRECATED: Não usado pelo Pipe (incompatível GPT-5/O1). Mantido para compatibilidade.",
        )

        # Modelos específicos por componente (opcional - deixe vazio para usar LLM_MODEL)
        LLM_MODEL_PLANNER: str = Field(
            default="", description="Modelo para Planner (vazio = usa LLM_MODEL)"
        )
        LLM_MODEL_ANALYST: str = Field(
            default="", description="Modelo para Analyst (vazio = usa LLM_MODEL)"
        )
        LLM_MODEL_JUDGE: str = Field(
            default="", description="Modelo para Judge (vazio = usa LLM_MODEL)"
        )
        LLM_MODEL_SYNTHESIS: str = Field(
            default="",
            description="Modelo para Síntese Final (vazio = usa LLM_MODEL) - Use modelo mais capaz aqui!",
        )

        # Context Reducer
        ENABLE_CONTEXT_REDUCER: bool = Field(
            default=True, description="Habilitar Context Reducer"
        )
        CONTEXT_MODE: str = Field(
            default="light", description="Modo: coarse|light|ultra"
        )

        # ✅ NOVO: Controle de exportação PDF
        AUTO_EXPORT_PDF: bool = Field(
            default=False,
            description="Exportar automaticamente relatório para PDF ao final da síntese",
        )

        EXPORT_FULL_CONTEXT: bool = Field(
            default=True,
            description="Se True, exporta contexto bruto completo; se False, apenas relatório final",
        )
        # Gates por perfil (P1) - EXPANDIDO v4.7: phase_score thresholds + two_flat_loops
        GATES_BY_PROFILE: Dict[str, Dict[str, Any]] = Field(
            default={
                "company_profile": {
                    "min_new_facts": 0.6,
                    "min_new_domains": 0.4,
                    "staleness_strict": True,
                    "threshold": 0.60,
                    "two_flat_loops": 2,
                },
                "regulation_review": {
                    "min_new_facts": 0.6,
                    "min_new_domains": 0.4,
                    "staleness_strict": True,
                    "threshold": 0.62,
                    "two_flat_loops": 1,
                },
                "technical_spec": {
                    "min_new_facts": 0.6,
                    "min_new_domains": 0.4,
                    "staleness_strict": True,
                    "threshold": 0.60,
                    "two_flat_loops": 2,
                },
                "literature_review": {
                    "min_new_facts": 0.6,
                    "min_new_domains": 0.4,
                    "staleness_strict": False,
                    "threshold": 0.58,
                    "two_flat_loops": 2,
                },
                "history_review": {
                    "min_new_facts": 0.6,
                    "min_new_domains": 0.4,
                    "staleness_strict": False,
                    "threshold": 0.58,
                    "two_flat_loops": 2,
                },
                # Phase-type specific gates (usado quando phase_context tem phase_type)
                "news": {
                    "min_new_facts": 0.7,
                    "min_new_domains": 0.5,
                    "staleness_strict": True,
                    "threshold": 0.65,
                    "two_flat_loops": 2,
                },
                "industry": {
                    "min_new_facts": 0.6,
                    "min_new_domains": 0.4,
                    "staleness_strict": False,
                    "threshold": 0.58,
                    "two_flat_loops": 2,
                },
                "profiles": {
                    "min_new_facts": 0.6,
                    "min_new_domains": 0.4,
                    "staleness_strict": True,
                    "threshold": 0.60,
                    "two_flat_loops": 2,
                },
                "tech": {
                    "min_new_facts": 0.6,
                    "min_new_domains": 0.4,
                    "staleness_strict": False,
                    "threshold": 0.60,
                    "two_flat_loops": 2,
                },
                "regulatory": {
                    "min_new_facts": 0.6,
                    "min_new_domains": 0.4,
                    "staleness_strict": True,
                    "threshold": 0.62,
                    "two_flat_loops": 1,
                },
                "financials": {
                    "min_new_facts": 0.6,
                    "min_new_domains": 0.4,
                    "staleness_strict": True,
                    "threshold": 0.64,
                    "two_flat_loops": 1,
                },
            },
            description="Gates de novidade/staleness por perfil + phase_score thresholds (v4.7)",
        )
        # ✅ NOVO v4.7: Phase Score Weights (auditável, configurável)
        PHASE_SCORE_WEIGHTS: Dict[str, float] = Field(
            default={
                "w_cov": 0.35,  # Peso: coverage (key_questions respondidas)
                "w_nf": 0.25,  # Peso: novel_fact_ratio (fatos novos)
                "w_nd": 0.15,  # Peso: novel_domain_ratio (domínios novos)
                "w_div": 0.15,  # Peso: domain_diversity (distribuição uniforme)
                "w_contra": 0.40,  # Penalidade: contradiction_score (contradições)
            },
            description="Pesos para cálculo de phase_score (v4.7) - Score = w_cov*coverage + w_nf*novel_facts + w_nd*novel_domains + w_div*diversity - w_contra*contradictions",
        )

        # ✅ NOVO v4.7: Coverage target global (usado em decisão DONE)
        COVERAGE_TARGET: float = Field(
            default=0.70,
            ge=0.0,
            le=1.0,
            description="Target mínimo de coverage para considerar DONE (0.0-1.0, default 0.70 = 70%)",
        )

        # ✅ NOVO v4.7: Contradiction hard gate (força NEW_PHASE imediato)
        CONTRADICTION_HARD_GATE: float = Field(
            default=0.75,
            ge=0.0,
            le=1.0,
            description="Threshold de contradiction_score para forçar NEW_PHASE imediato (0.0-1.0, default 0.75)",
        )
    
    def __init__(self):
        self.valves = self.Valves()
        self._last_contract = None
        self._detected_context = None
        self._phase_results = []
        
        # Initialize LLM components lazily; will be created after valves merge
        self.analyst = None
        self.judge = None
        self.planner = None
        self.deduplicator = None
        
        logger.info("[PIPE] Pipe inicializado")

    async def health_check(self) -> Dict[str, Any]:
        """Health check básico do pipeline."""
        checks: Dict[str, Any] = {}
        status = "healthy"
        # LLM ping
        try:
            # Lazy create minimal client if needed
            checks["llm"] = {"status": "unknown"}
            # Not running a real call to avoid costs; report configured model
            model = getattr(self.valves, "LLM_MODEL", "")
            checks["llm"] = {"status": "configured" if model else "missing", "model": model}
        except Exception as e:
            status = "degraded"
            checks["llm"] = {"status": "error", "error": str(e)}

        # Tools
        checks["tools"] = {
            "discovery": "up" if getattr(self, "discovery_tool", None) else "missing",
            "scraper": "up" if getattr(self, "scraper_tool", None) else "missing",
            "context_reducer": "up" if getattr(self, "context_reducer_tool", None) else "missing",
        }

        # Cache stats, if available via scraper tool module
        try:
            checks["cache"] = {"status": "unknown"}
        except Exception:
            checks["cache"] = {"status": "unknown"}

        return {
            "status": status,
            "ts": datetime.utcnow().isoformat() + "Z",
            "checks": checks,
        }
    def _generate_pdf_base64(self, html_content: str, title: str) -> Optional[str]:
        """Generate PDF from HTML content and return as base64 string"""
        try:
            from weasyprint import HTML, CSS
            import base64
            
            # Generate PDF in memory
            pdf_bytes = HTML(string=html_content).write_pdf()
            
            # Encode to base64
            pdf_b64 = base64.b64encode(pdf_bytes).decode('utf-8')
            
            return pdf_b64
        except ImportError:
            logger.warning("weasyprint not installed, PDF export disabled")
            return None
        except Exception as e:
            logger.error(f"PDF generation failed: {e}")
            return None
    
    async def pipe(
        self,
        body: dict,
        __user__: dict = None,
        __tools__: dict = None,
        __event_emitter__: Optional[Callable] = None,
        **kwargs,
    ) -> AsyncGenerator[str, None]:
        # Health check shortcut
        try:
            last_msg = (body or {}).get("messages", [])[-1].get("content", "").strip().lower()
        except Exception:
            last_msg = ""
        if last_msg in {"/health", "/status", "/ping"}:
            health = await self.health_check()
            yield f"```json\n{json.dumps(health, ensure_ascii=False, indent=2)}\n```"
            return
        """Complete pipe method implementation from PipeHaystack"""
        # Generate correlation ID for request tracing
        correlation_id = str(uuid.uuid4())[:8]
        logger.info(
            "Pipeline execution started", extra={"correlation_id": correlation_id}
        )

        # Validate internal state before processing
        self._validate_pipeline_state()

        # Extract current date from OpenWebUI metadata or system
        current_date = None
        try:
            metadata = (body or {}).get("metadata", {})
            variables = metadata.get("variables", {}) if metadata else {}
            current_date = variables.get("{{CURRENT_DATE}}") or variables.get(
                "CURRENT_DATE"
            )
        except Exception:
            pass
        if not current_date:
            from datetime import datetime
            current_date = datetime.now().strftime("%Y-%m-%d")

        # Store for use in prompts
        self._current_date = current_date

        # read valves override from body
        val_in = (body or {}).get("valves")
        if val_in:
            try:
                # pydantic copy/update
                self.valves = (
                    self.valves.model_copy(update=val_in)
                    if hasattr(self.valves, "model_copy")
                    else self.Valves(**{**self.valves.__dict__, **val_in})
                )
            except Exception:
                # best-effort set attributes
                for k, v in (val_in or {}).items():
                    if hasattr(self.valves, k):
                        setattr(self.valves, k, v)
            # Re-init components lazily after valves change
            if self.analyst is None:
                self.analyst = AnalystLLM(self.valves)
            if self.judge is None:
                self.judge = JudgeLLM(self.valves)
            if self.planner is None:
                self.planner = PlannerLLM(self.valves)
            if self.deduplicator is None:
                self.deduplicator = Deduplicator(self.valves)

        user_msg = (
            body.get("messages", [{"content": ""}])[-1].get("content", "") or ""
        ).strip()
        low = user_msg.lower()

        # 🎯 DETECÇÃO DE INTENÇÃO (antes de qualquer processamento)
        # Comandos de continuação
        # Detecção ampla vs ativação estrita do comando "siga":
        # - Detecção ampla (detectar intenção): lista flexível de termos (override por valves)
        # - Ativação estrita (executar plano): subconjunto opcional/estrito, controlado por valve
        continue_terms_default = (
            "siga","continue","prosseguir","continua","prossegue",
            "go on","keep going","next"
        )
        terms_override = getattr(self.valves, "CONTINUE_TERMS_OVERRIDE", None)
        continue_terms = tuple(terms_override) if terms_override else continue_terms_default
        is_continue_command = any(t in low for t in continue_terms)

        # Gate estrito opcional para ativação (evita auto-execução por termos ambíguos)
        strict_activation = getattr(self.valves, "STRICT_CONTINUE_ACTIVATION", True)
        if strict_activation:
            strict_terms = {"siga", "continue", "prosseguir"}
            is_strict_activation = any(t in low for t in strict_terms)
        else:
            is_strict_activation = is_continue_command

        # ✅ NOVO: Detectar intenção de refinamento de plano
        refinement_keywords = [
            "adicione", "inclua", "acrescente", "mude", "altere", "ajuste",
            "remova", "tire", "delete", "corrija", "refine", "modifique",
            "aumente", "reduza", "expanda", "foque mais", "menos em", "troque",
            "substitua", "adapte", "personalize", "atualize",
        ]
        is_refinement = any(kw in low for kw in refinement_keywords)
        has_previous_plan = bool(self._last_contract)

        # 🔒 DECISÃO: Preservar contexto ou re-detectar?
        should_preserve_context = (is_strict_activation and is_continue_command) or (
            is_refinement and has_previous_plan
        )

        if should_preserve_context and self._detected_context:
            # Manter contexto anterior (refinamento ou siga)
            logger.info(
                f"[PIPE] Contexto preservado: is_continue={is_continue_command}, strict_activation={is_strict_activation}, is_refinement={is_refinement}, has_plan={has_previous_plan}"
            )
            _emit_decision_snapshot(
                step="router_pre",
                vector={
                    "is_continue": is_continue_command,
                    "strict_activation": is_strict_activation,
                    "is_refinement": is_refinement,
                    "has_previous_plan": has_previous_plan,
                },
                reason="preserve_context",
            )
            yield f"**[CONTEXT]** 🔒 Mantendo contexto: {self._detected_context.get('perfil_sugerido', 'N/A')}\n"
            if is_refinement:
                yield f"**[INFO]** 💡 Modo refinamento detectado - ajustando plano existente\n"
        else:
            # Re-detectar contexto (nova query)
            if hasattr(self, '_context_locked') and self._context_locked:
                logger.info("[PIPE] Nova query detectada - desbloqueando contexto")
                self._context_locked = False
                self._detected_context = None

            self._detected_context = await self._detect_unified_context(user_msg, body)
            logger.info(f"[PIPE] Contexto detectado: {self._detected_context}")
            _emit_decision_snapshot(
                step="router_pre",
                vector={"detected_context": self._detected_context or {}},
                reason="detect_context",
            )

            # 🔗 SINCRONIZAR PERFIL DETECTADO com intent_profile (fonte única de verdade)
            if self._detected_context:
                self._intent_profile = self._detected_context.get(
                    "perfil_sugerido", "company_profile"
                )
                logger.info(f"[PIPE] Perfil sincronizado: {self._intent_profile}")
                yield f"**[CONTEXT]** 🔍 Perfil: {self._detected_context.get('perfil_sugerido', 'N/A')} | Setor: {self._detected_context.get('setor_principal', 'N/A')} | Tipo: {self._detected_context.get('tipo_pesquisa', 'N/A')}\n"

        d_callable, s_callable, cr_callable = self._resolve_tools(__tools__ or {})

        # Manual route - single execution path
        # If the user asks to continue (siga), execute stored contract
        if low in {"siga", "continue", "prosseguir"}:
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(
                    f"[DEBUG] SIGA mode: _last_contract exists: {bool(self._last_contract)}"
                )

            # Try to get contract from stored state first
            if not self._last_contract:
                # Try to extract contract from message history
                yield "**[INFO]** Procurando plano no histórico...\n"
                contract = await self._extract_contract_from_history(body)
                if contract:
                    self._last_contract = contract
                    yield "**[INFO]** ✅ Plano recuperado do histórico\n"
                else:
                    yield "**[AVISO]** Nenhum contrato pendente ou encontrado no histórico\n"
                    yield "**[DICA]** Tente criar um novo plano primeiro\n"
                    return

            job_id = f"cached_{int(time.time() * 1000)}"

            # Check if LangGraph is available and working
            if not LANGGRAPH_AVAILABLE:
                yield "**[ERRO]** LangGraph não está disponível. Instale: pip install langgraph\n"
                return

            try:
                # Test if StateGraph can be instantiated
                test_workflow = StateGraph(ResearchState) if LANGGRAPH_AVAILABLE else StateGraph()
                test_workflow.add_node("test", lambda x: x)
            except Exception as e:
                yield f"**[ERRO]** LangGraph não está funcionando corretamente: {e}\n"
                yield "**[SUGESTÃO]** Reinstale o LangGraph: pip uninstall langgraph && pip install langgraph\n"
                return

            # Build graph and execute phases
            graph = build_research_graph(
                self.valves, d_callable, s_callable, cr_callable
            )

            # Verify graph has required methods
            if not hasattr(graph, 'ainvoke'):
                yield "**[ERRO]** Grafo compilado não tem método ainvoke. Verifique instalação do LangGraph\n"
                return

            yield f"\n### 🚀 Execução iniciada com LangGraph\n"
            
            # ===== GLOBAL STATE PERSISTENCE =====
            # Initialize global state that persists across all phases
            global_state = {
                "scraped_cache": {},  # Shared URL cache across phases
                "used_claim_hashes": [],  # Novelty tracking across phases
                "used_domains": [],  # Domain diversity tracking
                "phase_results": [],  # Accumulated results
                "accumulated_context": "",  # Global context accumulation
                "telemetry_loops": [],  # Global telemetry
            }
            
            phase_results = []
            telemetry_data = {
                "execution_id": job_id,
                "start_time": time.time(),
                "phases": [],
            }

            # ===== PHASE EXECUTION WITH STATE PERSISTENCE =====
            phases_to_execute = self._last_contract.get("fases", []).copy()
            phase_index = 0
            
            while phase_index < len(phases_to_execute):
                phase_index += 1
                ph = phases_to_execute[phase_index - 1]
                objetivo, q = ph["objetivo"], ph["query_sugerida"]
                
                yield f"\n**Fase {phase_index}** – {objetivo}\n"
                
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    yield f"**[DEBUG]** Cache global: {len(global_state['scraped_cache'])} URLs, {len(global_state['used_claim_hashes'])} hashes\n"

                # Initialize state for this phase with GLOBAL STATE
                initial_state = ResearchState(
                    query=q,
                    phase_context=ph,
                    correlation_id=correlation_id,
                    loop_count=0,
                    max_loops=self.valves.MAX_AGENT_LOOPS,
                    # ===== PERSISTENT STATE =====
                    scraped_cache=global_state["scraped_cache"].copy(),
                    accumulated_context=global_state["accumulated_context"],
                    telemetry_loops=global_state["telemetry_loops"].copy(),
                    used_claim_hashes=global_state["used_claim_hashes"].copy(),
                    used_domains=global_state["used_domains"].copy(),
                    phase_results=global_state["phase_results"].copy(),
                    # ===== PHASE-SPECIFIC STATE =====
                    job_id=job_id,
                    valves=self.valves,
                    original_query=q,
                    phase_index=phase_index,
                    contract=self._last_contract,
                    all_phase_queries=[phase.get("query_sugerida", "") for phase in phases_to_execute],
                    intent_profile=self._detected_context.get("perfil_sugerido", "general") if self._detected_context else "general",
                    discovered_urls=[],
                    new_urls=[],
                    cached_urls=list(global_state["scraped_cache"].keys()),
                    raw_content="",
                    filtered_content="",
                    analysis={},
                    evidence_metrics={},
                    judgement={},
                    verdict="",
                    phase_score=0.0,
                    phase_metrics={},
                    seed_family=None,
                    modifications=[],
                    coverage_score=0.0,
                    entities_covered=[],
                    analyst_confidence="baixa",
                    gaps_critical=False,
                    suggest_refine=False,
                    suggest_pivot=False,
                    diminishing_returns=False,
                    failed_query=False,
                    new_phase_proposed=False,
                    phase_candidates=[],
                    refine_queries=[],
                    previous_queries=[],
                    failed_queries=[],
                    new_facts_ratio=1.0,
                    new_domains_ratio=1.0,
                    unique_domains=0,
                    n_facts=0,
                    contradictions=0,
                    high_confidence_facts=0,
                    facts_with_multiple_sources=0,
                    lacunas_count=0,
                    seed_core_source=None,
                    analyst_proposals=[],
                    final_synthesis=None,
                )

                # ===== EXECUTE GRAPH (LangGraph manages internal loops) =====
                try:
                    phase_start_time = time.time()
                    
                    # ✅ CRITICAL: Let LangGraph handle ALL internal loops
                    # The graph will automatically loop discovery→scrape→reduce→analyze→judge
                    # based on the should_continue router decisions
                    try:
                        final_state = await graph.ainvoke(initial_state)
                    except AttributeError as e:
                        if "ainvoke" in str(e):
                            yield f"**[ERRO]** Método ainvoke não disponível no grafo. Verifique instalação do LangGraph\n"
                            continue
                        else:
                            raise
                    except Exception as e:
                        yield f"**[ERRO]** Falha na execução do LangGraph: {e}\n"
                        logger.error(f"LangGraph execution failed: {e}")
                        continue
                    
                    phase_duration = time.time() - phase_start_time
                    
                    # Process result
                    verdict = final_state.get("verdict", "done")
                    loop_count = final_state.get("loop_count", 0)
                    
                    yield f"**[FASE {phase_index}]** Verdict: {verdict} (loops: {loop_count})\n"
                    yield f"**[FASE {phase_index}]** Duração: {phase_duration:.1f}s\n"
                    
                    # ===== UPDATE GLOBAL STATE =====
                    # Preserve state for next phase
                    global_state.update({
                        "scraped_cache": final_state.get("scraped_cache", global_state["scraped_cache"]),
                        "used_claim_hashes": final_state.get("used_claim_hashes", global_state["used_claim_hashes"]),
                        "used_domains": final_state.get("used_domains", global_state["used_domains"]),
                        "accumulated_context": final_state.get("accumulated_context", global_state["accumulated_context"]),
                        "telemetry_loops": final_state.get("telemetry_loops", global_state["telemetry_loops"]),
                    })
                    
                    # Add to phase results
                    phase_result = {
                        "phase": phase_index,
                        "objective": objetivo,
                        "result": final_state,
                        "verdict": verdict,
                        "loops": loop_count,
                        "duration": phase_duration,
                    }
                    phase_results.append(phase_result)
                    global_state["phase_results"].append(phase_result)
                    
                    # ===== HANDLE NEW_PHASE VERDICT =====
                    if verdict == "new_phase":
                        new_phase = final_state.get("new_phase", {})
                        if new_phase:
                            # Add new phase to the execution queue
                            phases_to_execute.append(new_phase)
                            yield f"**[NOVA FASE]** Adicionada: {new_phase.get('objetivo', 'N/A')}\n"
                            yield f"**[INFO]** Total de fases: {len(phases_to_execute)}\n"
                    
                    # ===== DEBUG STATE PERSISTENCE =====
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        yield f"**[DEBUG]** Estado global atualizado:\n"
                        yield f"  - Cache: {len(global_state['scraped_cache'])} URLs\n"
                        yield f"  - Hashes: {len(global_state['used_claim_hashes'])} claims\n"
                        yield f"  - Domains: {len(global_state['used_domains'])} domains\n"
                        yield f"  - Context: {len(global_state['accumulated_context'])} chars\n"
                    
                except Exception as e:
                    logger.error(f"Phase {phase_index} execution failed: {e}")
                    yield f"**[ERRO]** Falha na fase {phase_index}: {e}\n"
                    # Continue with next phase even if one fails
                    continue

            # Emitir telemetria estruturada
            if self.valves.DEBUG_LOGGING:
                yield f"\n**[TELEMETRIA]** 📊 Dados estruturados da execução:\n"
                yield f"```json\n{json.dumps(telemetry_data, indent=2, ensure_ascii=False)}\n```\n"

            yield f"\n**[SÍNTESE FINAL]**\n"
            # Call _synthesize_final with phase results
            async for synthesis_chunk in self._synthesize_final(phase_results, global_state, cr_callable, user_msg, body, __event_emitter__):
                yield synthesis_chunk

            # 🔓 UNLOCK contexto após conclusão para permitir nova detecção
            self._last_contract = None
            if hasattr(self, '_context_locked'):
                self._context_locked = False
            logger.info("[PIPE] Context unlocked after completion")
            return

        # Otherwise, build a contract using PlannerLLM
        if not self.valves.USE_PLANNER:
            raise ValueError("USE_PLANNER desabilitado")

        requested_phases = _extract_phase_count(user_msg)
        phases = (
            requested_phases if requested_phases else self.valves.DEFAULT_PHASE_COUNT
        )

        yield f"**[PLANNER]** Até {phases} fases (conforme necessário)...\n\n"

        planner = PlannerLLM(self.valves)
        out = await planner.run(
            user_prompt=user_msg,
            phases=phases,
            current_date=getattr(self, "_current_date", None),
            detected_context=self._detected_context,
        )

        # Validate contract
        if not isinstance(out, dict) or "phases" not in out:
            yield "**[ERRO]** Contrato inválido gerado pelo Planner\n"
            return

        self._last_contract = out
        yield "**[INFO]** ✅ Contrato gerado com sucesso\n"

        # Render contract for user
        yield f"\n📋 **Plano de Pesquisa**\n\n"
        for i, phase in enumerate(out.get("phases", []), 1):
            yield f"**Fase {i}:** {phase.get('objective', 'N/A')}\n"
            yield f"- Query: {phase.get('query_sugerida', 'N/A')}\n"
            yield f"- Seed: {phase.get('seed_core', 'N/A')}\n\n"

        yield "**[INFO]** Digite 'siga' para executar o plano\n"

    async def _synthesize_final(
        self,
        phase_results: List[Dict],
        global_state: Optional[Dict] = None,
        cr_callable: Optional[Callable] = None,
        user_msg: str = "",
        body: dict = None,
        __event_emitter__=None,
    ):
        """Síntese final adaptativa com Context Reducer ou deduplicação + LLM

        FLUXO:
        1. **Context Reducer** (se habilitado): Redução global com todas as queries
        2. **Fallback** (se CR desabilitado/falhar):
           - Deduplicação MMR-lite (preserva ordem ou ordena por tamanho)
           - Truncamento ao MAX_CONTEXT_CHARS
           - Detecção de contexto unificado (usa _detected_context ou detecta)
           - Síntese adaptativa com UMA chamada LLM (prompt dinâmico)

        ADAPTIVE SYNTHESIS:
        - Usa _detected_context para determinar setor, tipo, perfil
        - Gera prompt com instruções específicas para o contexto
        - Estrutura de relatório adaptada (seções, estilo, foco)
        - Substituiu hardcoding de HPPC por template genérico

        Args:
            phase_results: Lista de resultados de cada fase executada
            orch: Orchestrator com contexto acumulado e cache de URLs
            user_msg: Query original do usuário (para detecção de contexto)
            body: Body da requisição (para histórico de mensagens)
        """
        # Try Context Reducer first (direct tool invocation)
        if self.valves.ENABLE_CONTEXT_REDUCER and cr_callable:
            try:
                await _safe_emit(__event_emitter__, "**[SÍNTESE]** Context Reducer global...\n")
                yield "**[SÍNTESE]** Context Reducer global...\n"
                
                # Get accumulated context from global state or phase results
                accumulated_context = ""
                if global_state and "accumulated_context" in global_state:
                    accumulated_context = global_state["accumulated_context"]
                elif phase_results:
                    # Fallback: get from last phase result
                    last_result = phase_results[-1].get("result", {})
                    accumulated_context = last_result.get("accumulated_context", "")
                
                if not accumulated_context:
                    await _safe_emit(__event_emitter__, "**[INFO]** Nenhum contexto acumulado disponível\n")
                    yield "**[INFO]** Nenhum contexto acumulado disponível\n"
                    final = None
                else:
                    # Prepare context reducer input
                    all_queries = [
                        phase.get("query_sugerida", "") 
                        for phase in phase_results 
                        if "query_sugerida" in phase
                    ]
                    
                    context_params = {
                        "corpo": {
                            "ultrasearcher_result": {
                                "scraped_content": accumulated_context
                            }
                        },
                        "mode": self.valves.CONTEXT_MODE,
                        "queries": all_queries,
                        "tipo": "final",
                    }
                    
                    # Call Context Reducer tool
                    if asyncio.iscoroutinefunction(cr_callable):
                        final = await cr_callable(**context_params)
                    else:
                        final = await asyncio.to_thread(cr_callable, **context_params)
                    
                    # Extract result
                    if isinstance(final, dict):
                        final = final.get("final_markdown", accumulated_context)
                    elif not final:
                        final = accumulated_context

                if final:
                    await _safe_emit(__event_emitter__, f"\n{final}\n\n")
                    yield f"\n{final}\n\n"

                    # Get scraped cache from global state or phase results
                    scraped_cache = {}
                    if global_state and "scraped_cache" in global_state:
                        scraped_cache = global_state["scraped_cache"]
                    elif phase_results:
                        # Fallback: get from last phase result
                        last_result = phase_results[-1].get("result", {})
                        scraped_cache = last_result.get("scraped_cache", {})
                    
                    total_urls = len(scraped_cache)
                    total_phases = len(phase_results)
                    await _safe_emit(__event_emitter__, f"\n---\n**📊 Estatísticas:**\n")
                    yield f"\n---\n**📊 Estatísticas:**\n"
                    await _safe_emit(__event_emitter__, f"- Fases: {total_phases}\n")
                    yield f"- Fases: {total_phases}\n"
                    await _safe_emit(__event_emitter__, f"- URLs únicas scraped: {total_urls}\n")
                    yield f"- URLs únicas scraped: {total_urls}\n"
                    await _safe_emit(__event_emitter__, f"- Contexto acumulado: {len(accumulated_context)} chars\n")
                    yield f"- Contexto acumulado: {len(accumulated_context)} chars\n"
                    return
                    
            except TypeError as e:
                # Tool doesn't support these parameters
                yield f"**[WARNING]** Context Reducer params mismatch: {e}\n"
                final = None
            except Exception as e:
                yield f"**[ERRO]** Context Reducer: {e}\n"
                final = None

        # Fallback: deduplicar (merge+dedupe+MMR-lite) e depois sintetizar com UMA ÚNICA chamada ao LLM
        await _safe_emit(__event_emitter__, "**[SÍNTESE]** Síntese completa e detalhada (sem Context Reducer)...\n")
        yield "**[SÍNTESE]** Síntese completa e detalhada (sem Context Reducer)...\n"

        def _paragraphs(text: str) -> List[str]:
            """Extrai parágrafos do texto, suportando múltiplos formatos de separação

            Detecta automaticamente o formato baseado na densidade de parágrafos:
            - Densidade baixa (>1000 chars/parágrafo médio) → markdown com \n único
            - Densidade alta (<500 chars/parágrafo médio) → formato normal com \n\n
            """
            if not text:
                return []

            # Tentar primeiro com duplo newline (formato padrão do accumulator)
            parts = [p.strip() for p in text.split("\n\n") if p.strip()]

            # Calcular densidade de parágrafos (chars por parágrafo)
            avg_paragraph_size = len(text) / max(len(parts), 1)

            # Se densidade é muito baixa (parágrafos muito grandes), provavelmente é markdown com \n único
            # Threshold: >1000 chars/parágrafo médio indica blocos gigantes, não parágrafos reais
            if avg_paragraph_size > 1000:
                # Markdown do scraper/Context Reducer usa \n único
                # Agrupar linhas não vazias em blocos (parágrafos)
                lines = text.split("\n")
                paragraphs = []
                current_block = []

                for line in lines:
                    line_stripped = line.strip()
                    if line_stripped:
                        current_block.append(line_stripped)
                    else:
                        # Linha vazia = fim de parágrafo
                        if current_block:
                            paragraph = " ".join(current_block)
                            if len(paragraph) > 20:  # Filtrar parágrafos muito curtos
                                paragraphs.append(paragraph)
                            current_block = []

                # Adicionar último bloco
                if current_block:
                    paragraph = " ".join(current_block)
                    if len(paragraph) > 20:
                        paragraphs.append(paragraph)

                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[DEDUP] Densidade baixa detectada ({avg_paragraph_size:.0f} chars/parágrafo)"
                    )
                    print(
                        f"[DEDUP] Usando extração linha-por-linha: {len(parts)} → {len(paragraphs)} parágrafos"
                    )

                # ✅ v4.8.1: Se ainda houver parágrafos gigantes, quebrar por sentenças/tamanho máximo
                max_paragraph_chars = getattr(self.valves, "DEDUP_MAX_PARAGRAPH_CHARS", 1200)
                final_paragraphs: List[str] = []
                sentence_splitter = re.compile(r"(?<=[.!?])\s+")

                for paragraph in paragraphs:
                    if len(paragraph) <= max_paragraph_chars:
                        final_paragraphs.append(paragraph)
                        continue

                    sentences = [s.strip() for s in sentence_splitter.split(paragraph) if s.strip()]
                    chunk: List[str] = []
                    chunk_len = 0

                    for sentence in sentences:
                        sentence_len = len(sentence)
                        if chunk and chunk_len + sentence_len > max_paragraph_chars:
                            final_paragraphs.append(" ".join(chunk))
                            chunk = [sentence]
                            chunk_len = sentence_len
                        else:
                            chunk.append(sentence)
                            chunk_len += sentence_len + 1

                    if chunk:
                        final_paragraphs.append(" ".join(chunk))

                if getattr(self.valves, "VERBOSE_DEBUG", False) and len(final_paragraphs) != len(paragraphs):
                    print(
                        f"[DEDUP] Fragmentação adicional aplicada: {len(paragraphs)} → {len(final_paragraphs)} parágrafos"
                    )

                return [p for p in final_paragraphs if len(p) > 20]

            # Densidade normal: usar split padrão
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(
                    f"[DEDUP] Densidade normal ({avg_paragraph_size:.0f} chars/parágrafo)"
                )
                print(f"[DEDUP] Usando split por \\n\\n: {len(parts)} parágrafos")

            return [p for p in parts if len(p) > 20]

        # Build context (with optional deduplication and size limit)
        # Get accumulated context from global state or phase results
        raw_context = ""
        if global_state and "accumulated_context" in global_state:
            raw_context = global_state["accumulated_context"]
        elif phase_results:
            # Fallback: get from last phase result
            last_result = phase_results[-1].get("result", {})
            raw_context = last_result.get("accumulated_context", "")
        
        if not raw_context:
            yield "**[INFO]** Nenhum contexto disponível para síntese\n"
            return
        
        _emit_decision_snapshot(step="synthesis", vector={"context_chars": len(raw_context)}, reason="start")
        yield f"**[SÍNTESE]** Contexto bruto: {len(raw_context)} chars\n"

        if self.valves.ENABLE_DEDUPLICATION:
            raw_paragraphs = _paragraphs(raw_context)

            # v4.4: Usar Deduplicator centralizado (mesmo do Orchestrator)
            # Síntese: SEM shuffle (fases não são cronológicas, ordem é estrutural)
            deduplicator = Deduplicator(self.valves)
            
            # Usar estratégia específica da Synthesis
            algorithm = getattr(self.valves, "SYNTHESIS_DEDUP_ALGORITHM", "mmr")
            model_name = getattr(self.valves, "SYNTHESIS_DEDUP_MODEL", "sentence-transformers/paraphrase-MiniLM-L3-v2")
            threshold = getattr(self.valves, "DEDUP_SIMILARITY_THRESHOLD", 0.85)
            print(f"[SÍNTESE DEDUP] 🧠 Algoritmo: {algorithm.upper()}")
            print(f"[SÍNTESE DEDUP] 📏 Threshold: {threshold}")
            print(f"[SÍNTESE DEDUP] 📊 Input: {len(raw_paragraphs)} parágrafos → Target: {self.valves.MAX_DEDUP_PARAGRAPHS}")
            print(f"[SÍNTESE DEDUP] 🔍 Valves: ENABLE_DEDUPLICATION={self.valves.ENABLE_DEDUPLICATION}")
            print(f"[SÍNTESE DEDUP] 🔍 MAX_DEDUP_PARAGRAPHS: {self.valves.MAX_DEDUP_PARAGRAPHS}")
            print(f"[SÍNTESE DEDUP] 🔍 Type: {type(self.valves.MAX_DEDUP_PARAGRAPHS)}")
            
            # Extrair must_terms do contexto para context-aware dedup
            extracted_must_terms = []
            if hasattr(self, '_last_contract') and self._last_contract:
                # Tentar extrair must_terms do contract
                entities = self._last_contract.get("entities", {}).get("canonical", [])
                extracted_must_terms = entities[:5]  # Limitar a 5 termos mais relevantes
            
            dedupe_result = deduplicator.dedupe(
                chunks=raw_paragraphs,
                max_chunks=self.valves.MAX_DEDUP_PARAGRAPHS,
                algorithm=algorithm,
                threshold=threshold,
                preserve_order=self.valves.PRESERVE_PARAGRAPH_ORDER,  # Respeita valve
                preserve_recent_pct=0.0,  # Síntese não preserva recent (processa tudo igual)
                shuffle_older=False,  # SEM shuffle (ordem estrutural, não cronológica)
                reference_first=False,  # SEM referência (processa tudo igual)
                # NOVO: Context-aware parameters
                must_terms=extracted_must_terms,
                enable_context_aware=self.valves.ENABLE_CONTEXT_AWARE_DEDUP,
            )

            deduped_paragraphs = dedupe_result["chunks"]
            deduped_context = "\n\n".join(deduped_paragraphs)

            order_mode = (
                "ordem preservada"
                if self.valves.PRESERVE_PARAGRAPH_ORDER
                else "ordenado por tamanho"
            )
            await _safe_emit(__event_emitter__, f"**[SÍNTESE]** Deduplicação ativa ({order_mode}, {dedupe_result['algorithm_used']}): {dedupe_result['original_count']} → {dedupe_result['deduped_count']} parágrafos ({dedupe_result['reduction_pct']:.1f}% redução)\n")
            yield f"**[SÍNTESE]** Deduplicação ativa ({order_mode}, {dedupe_result['algorithm_used']}): {dedupe_result['original_count']} → {dedupe_result['deduped_count']} parágrafos ({dedupe_result['reduction_pct']:.1f}% redução)\n"
            await _safe_emit(__event_emitter__, f"**[SÍNTESE]** Tokens economizados: ~{dedupe_result['tokens_saved']}\n")
            yield f"**[SÍNTESE]** Tokens economizados: ~{dedupe_result['tokens_saved']}\n"
        else:
            deduped_context = raw_context
            await _safe_emit(__event_emitter__, f"**[SÍNTESE]** Deduplicação desabilitada: usando todo o contexto\n")
            yield f"**[SÍNTESE]** Deduplicação desabilitada: usando todo o contexto\n"

        # Aplicar limite de tamanho
        max_chars = self.valves.MAX_CONTEXT_CHARS
        if len(deduped_context) > max_chars:
            # Truncar mantendo parágrafos completos
            truncated = deduped_context[:max_chars]
            last_paragraph = truncated.rfind("\n\n")
            if last_paragraph > max_chars * 0.8:  # Se não perder muito
                deduped_context = truncated[:last_paragraph]
            else:
                deduped_context = truncated
            await _safe_emit(__event_emitter__, f"**[SÍNTESE]** Contexto truncado: {len(deduped_context)} chars (limite: {max_chars})\n")
            yield f"**[SÍNTESE]** Contexto truncado: {len(deduped_context)} chars (limite: {max_chars})\n"
            _emit_decision_snapshot(step="synthesis", vector={"context_chars": len(deduped_context), "limit": max_chars}, reason="after_truncation")
        else:
            await _safe_emit(__event_emitter__, f"**[SÍNTESE]** Contexto dentro do limite: {len(deduped_context)} chars\n")
            yield f"**[SÍNTESE]** Contexto dentro do limite: {len(deduped_context)} chars\n"
            _emit_decision_snapshot(step="synthesis", vector={"context_chars": len(deduped_context)}, reason="within_limit")
        
        try:
            # Log do contexto que será usado
            if self.valves.DEBUG_LOGGING:
                await _safe_emit(__event_emitter__, f"**[DEBUG]** Contexto para síntese: {len(deduped_context)} chars\n")
                yield f"**[DEBUG]** Contexto para síntese: {len(deduped_context)} chars\n"
                await _safe_emit(__event_emitter__, f"**[DEBUG]** Primeiros 200 chars: {deduped_context[:200]}...\n")
                yield f"**[DEBUG]** Primeiros 200 chars: {deduped_context[:200]}...\n"

            # Coletar estatísticas para incluir no prompt
            # Get scraped cache from global state or phase results
            scraped_cache = {}
            if global_state and "scraped_cache" in global_state:
                scraped_cache = global_state["scraped_cache"]
            elif phase_results:
                # Fallback: get from last phase result
                last_result = phase_results[-1].get("result", {})
                scraped_cache = last_result.get("scraped_cache", {})
            
            total_urls = len(scraped_cache)
            total_phases = len(phase_results)
            domains = set()
            for url in scraped_cache:
                try:
                    domain = url.split("/")[2]
                    domains.add(domain)
                except:
                    pass

            # Compactar HINTS dos Analistas (por fase)
            def _txt(x):
                if isinstance(x, dict):
                    return (
                        x.get("texto")
                        or x.get("text")
                        or x.get("claim")
                        or x.get("descricao")
                        or str(x)
                    )
                return str(x)

            hints_lines = []
            for idx, ph in enumerate(phase_results, 1):
                analysis = ph.get("analysis", {}) or {}
                summary = (analysis.get("summary") or "").strip()
                facts = analysis.get("facts", []) or []
                lacunas = analysis.get("lacunas", []) or []
                judgement = ph.get("judgement", {}) or {}
                verdict = judgement.get("verdict") or ph.get("verdict")
                reasoning = judgement.get("reasoning") or ph.get("reasoning")

                hints_lines.append(f"[FASE {idx}] {ph.get('query','')}")
                if summary:
                    hints_lines.append(f"- Resumo: {summary}")
                if facts:
                    # pegar até 3
                    for f in facts[:3]:
                        hints_lines.append(f"- Fato: {_txt(f)}")
                if lacunas:
                    for l in lacunas[:3]:
                        hints_lines.append(f"- Lacuna: {_txt(l)}")
                if verdict:
                    hints_lines.append(f"- Veredito: {verdict}")
                if reasoning:
                    hints_lines.append(f"- Justificativa: {reasoning}")
                hints_lines.append("")
            analyst_hints = "\n".join(hints_lines)[
                :4000
            ]  # limitar tamanho para segurança

            # USAR CONTEXTO CENTRALIZADO já detectado no início do pipe
            if not self._detected_context:
                self._detected_context = await self._detect_unified_context(
                    user_msg, body
                )
            research_context = self._detected_context

            # Extrair KEY_QUESTIONS e RESEARCH_OBJECTIVES do detected_context
            key_questions = research_context.get("key_questions", [])
            research_objectives = research_context.get("research_objectives", [])

            # Extrair informações do contract (entidades, objetivos de fase)
            contract_entities = []
            phase_objectives = []
            if self._last_contract:
                entities = self._last_contract.get("entities", {})
                canonical = entities.get("canonical", [])
                aliases = entities.get("aliases", [])
                contract_entities = list(
                    dict.fromkeys(canonical + aliases)
                )  # Remove duplicatas

                # Coletar objetivos de cada fase
                for idx, phase in enumerate(self._last_contract.get("fases", []), 1):
                    phase_name = phase.get("name", f"Fase {idx}")
                    phase_obj = phase.get("objetivo", "")
                    if phase_obj:
                        phase_objectives.append(f"{idx}. {phase_name}: {phase_obj}")

            # REFACTORED (v4.3.1 - P1D): Usar função consolidada para construir seções
            sections = _build_synthesis_sections(
                key_questions=key_questions,
                research_objectives=research_objectives,
                entities=contract_entities,
                contract=self._last_contract or {},
            )

            # sections is a string, not a dict
            sections_text = sections

            # UMA ÚNICA chamada ao LLM com prompt adaptativo baseado no contexto
            synthesis_prompt = f"""Você é um analista executivo especializado em criar relatórios executivos completos e narrativos.

**QUERY ORIGINAL DO USUÁRIO:**
{user_msg}

**PERFIL ADAPTATIVO:** {research_context['perfil_descricao']}
**OBJETIVO ADAPTATIVO:** Criar um relatório executivo profissional no estilo {research_context['estilo']}, rico em {research_context['foco_detalhes']}, baseado no contexto de pesquisa fornecido sobre {research_context['tema_principal']}.

{sections_text}
⚠️ **INSTRUÇÕES CRÍTICAS DE SÍNTESE:**
1. **RESPONDA ÀS KEY QUESTIONS**: O relatório DEVE responder diretamente às perguntas decisórias listadas acima. Estruture seções para responder cada uma.
2. **ALCANCE OS RESEARCH OBJECTIVES**: Cada objetivo de pesquisa final deve ser explicitamente endereçado com análise e evidências.
3. **CUBRA TODAS AS ENTIDADES**: O relatório DEVE analisar TODAS as entidades específicas listadas acima. Crie seções/subseções dedicadas para cada uma.
4. **ENTREGUE OS OBJETIVOS DAS FASES**: Cada objetivo de fase deve ser claramente respondido com evidências do contexto.
3. **ANÁLISE PROFUNDA**: Examine TODO o contexto fornecido (evidências, URLs, trechos) - identifique {research_context['foco_detalhes']}
4. **INTEGRAÇÃO ESTRATÉGICA**: Integre os HINTS dos analistas (resumos, fatos e lacunas por fase) com o contexto principal
5. **NARRATIVA ESPECÍFICA**: Crie um relatório {research_context['estilo']} sobre {research_context['tema_principal']}
6. **DADOS ESPECÍFICOS**: Inclua números, métricas, projetos, tecnologias e indicadores relevantes para {research_context['tema_principal']}
7. **COBERTURA COMPLETA**: Para cada entidade específica (empresa, produto, pessoa), detalhe:
   - Histórico e presença no mercado
   - Escopo de serviços/produtos
   - Posicionamento e diferenciais
   - Métricas e indicadores (quando disponíveis)
   - Citações e fontes (URLs)
8. **ESTRUTURA ESPECIALIZADA**: Use as seções sugeridas: {', '.join(research_context.get('secoes_sugeridas', ['Resumo', 'Análise', 'Conclusões']))}
9. **FONTES ESPECÍFICAS**: Cite fontes oficiais e técnicas quando relevante (URLs entre parênteses)
10. **SÍNTESE ESTRATÉGICA**: Integre informações sem repetição, focando em aspectos únicos e {research_context['foco_detalhes']}
11. **FORMATO PROFISSIONAL**: Use Markdown narrativo com seções bem estruturadas no estilo {research_context['estilo']}
12. **PRIORIZE DETALHAMENTO SOBRE BREVIDADE**: Prefira um relatório rico e detalhado a um genérico e curto; use TODO o contexto disponível

**ESTATÍSTICAS DA PESQUISA:**
- Fases executadas: {total_phases}
- URLs analisadas: {total_urls}
- Domínios consultados: {len(domains)}
- Contexto processado: {len(deduped_context):,} caracteres

**HINTS DOS ANALISTAS (por fase):**
{analyst_hints}

**ESTRUTURA ADAPTATIVA BASEADA NO CONTEXTO DETECTADO:**

# 📋 Relatório Executivo - {research_context['tema_principal']}

## 🎯 Resumo Executivo
[Parágrafo {research_context['estilo']} com visão {research_context['foco_detalhes']} sobre {research_context['tema_principal']}]

## 🔍 Principais Descobertas
[Análise {research_context['estilo']} integrando as descobertas mais importantes sobre {research_context['tema_principal']}]

**DIRETRIZES ADAPTATIVAS PARA {research_context['tema_principal'].upper()}:**
- **FOCO**: {research_context['foco_detalhes']}
- **ESTILO**: {research_context['estilo']}
- **SEÇÕES SUGERIDAS**: {', '.join(research_context.get('secoes_sugeridas', ['Resumo', 'Análise', 'Conclusões']))}
- **TOM**: Use linguagem apropriada para {research_context['perfil_descricao']}
- **NÍVEL DE DETALHE**: Seja específico sobre dados, métricas e evidências relevantes para {research_context['tema_principal']}
- **CONTEXTO**: Relacione descobertas com tendências e aspectos específicos do setor

Agora, crie o relatório executivo completo baseado no contexto abaixo:

---

**CONTEXTO DE PESQUISA:**

{deduped_context}

---

**RELATÓRIO EXECUTIVO:**"""

            # P1: Exemplo de BOA vs MÁ síntese para calibrar saída do LLM
            synthesis_prompt += """
💡 EXEMPLO DE BOA vs MÁ SÍNTESE:
Query: "Volume de mercado de headhunting Brasil"
Key_question: "Qual volume anual?"
❌ MÁ síntese (genérica):
"O mercado de headhunting no Brasil é significativo e tem crescido nos últimos anos."
✅ BOA síntese (específica):
"O mercado brasileiro de executive search movimentou R$450-500M em 2023 (fonte: Relatório ABRH),
crescimento de 12% vs 2022. Spencer Stuart lidera com ~25% de participação (fonte: Valor Econômico),
seguida por Heidrick (18%) e Flow Executive (15%)."
→ Diferença: números concretos, fontes, nomes de players
"""

            # ✅ GATE PREVENTIVO: Avisar sobre prompt gigante
            prompt_chars = len(synthesis_prompt)
            prompt_tokens_est = prompt_chars // 4  # Estimativa conservadora

            if prompt_tokens_est > 40000:  # ~160k chars
                yield f"**[⚠️ AVISO]** Prompt muito grande ({prompt_tokens_est:,} tokens estimados)!\n"
                yield f"**[SUGESTÃO]** Síntese pode levar 5-10 minutos. Aguarde...\n"
                yield f"**[CONFIG]** Se houver timeout, aumente nas Valves:\n"
                yield f"   - LLM_TIMEOUT_SYNTHESIS (atual: {self.valves.LLM_TIMEOUT_SYNTHESIS}s)\n"
                yield f"   - HTTPX_READ_TIMEOUT (atual: {self.valves.HTTPX_READ_TIMEOUT}s)\n"
                yield f"**[ALTERNATIVA]** Reduza MAX_DEDUP_PARAGRAPHS (atual: {self.valves.MAX_DEDUP_PARAGRAPHS})\n"
                yield f"\n"

            if self.valves.DEBUG_LOGGING or self.valves.VERBOSE_DEBUG:
                yield f"**[DEBUG]** Prompt de síntese: {prompt_chars:,} chars (~{prompt_tokens_est:,} tokens estimados)\n"
                if prompt_tokens_est > 12000:
                    yield f"**[AVISO]** Prompt grande (>{prompt_tokens_est:,} tokens) pode causar truncamento ou resposta genérica!\n"

            # Fazer UMA única chamada ao LLM para gerar o relatório completo
            # Usar modelo específico para síntese se configurado (pode ser mais capaz)
            synthesis_model = self.valves.LLM_MODEL_SYNTHESIS or self.valves.LLM_MODEL
            llm = _get_llm(self.valves, model_name=synthesis_model)

            # Parâmetros seguros para GPT-5/O1
            base_synthesis_params = {"temperature": 0.3}
            generation_kwargs = get_safe_llm_params(
                synthesis_model, base_synthesis_params
            )
            timeout_synthesis = self.valves.LLM_TIMEOUT_SYNTHESIS
            
            # 🔧 AUTO-ADJUST timeout for large context
            context_size = len(deduped_context)
            if context_size > 100000:  # >100k chars
                # Scale timeout based on context size
                min_timeout = 600  # 10 minutes minimum for large context
                max_timeout = 1800  # 30 minutes maximum
                # Linear scaling: 100k chars = 10min, 200k chars = 20min, 300k+ chars = 30min
                scaled_timeout = min(max_timeout, min_timeout + int((context_size - 100000) / 10000 * 600))
                timeout_synthesis = max(timeout_synthesis, scaled_timeout)
                if self.valves.DEBUG_LOGGING:
                    await _safe_emit(__event_emitter__, f"**[DEBUG]** Auto-ajuste timeout: {self.valves.LLM_TIMEOUT_SYNTHESIS}s → {timeout_synthesis}s (contexto: {context_size:,} chars)\n")
                    yield f"**[DEBUG]** Auto-ajuste timeout: {self.valves.LLM_TIMEOUT_SYNTHESIS}s → {timeout_synthesis}s (contexto: {context_size:,} chars)\n"

            # Log do modelo sendo usado
            if self.valves.DEBUG_LOGGING:
                await _safe_emit(__event_emitter__, f"**[DEBUG]** Modelo de síntese: {synthesis_model} (params: {generation_kwargs})\n")
                yield f"**[DEBUG]** Modelo de síntese: {synthesis_model} (params: {generation_kwargs})\n"

            _emit_decision_snapshot(step="synthesis", vector={"model": synthesis_model, "params": generation_kwargs, "timeout": timeout_synthesis}, reason="llm_call_start")
            out = await _safe_llm_run_with_retry(
                llm,
                synthesis_prompt,
                generation_kwargs,
                timeout=timeout_synthesis,
                max_retries=1,
            )
            if not out or not out.get("replies"):
                raise ValueError("LLM vazio na síntese final")
            report = (out["replies"][0] or "").strip()
            _emit_decision_snapshot(step="synthesis", vector={"ok": bool(report), "chars": len(report or "")}, reason="llm_call_end")
            if not report:
                raise ValueError("Relatório vazio na síntese final")
            await _safe_emit(__event_emitter__, f"\n{report}\n")
            yield f"\n{report}\n"

            # Generate PDF if enabled
            if getattr(self.valves, 'ENABLE_PDF_EXPORT', True):
                try:
                    # Convert markdown to HTML for PDF generation
                    import markdown
                    html_content = markdown.markdown(report, extensions=['tables', 'fenced_code'])
                    
                    # Add basic CSS styling
                    html_with_style = f"""
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <meta charset="utf-8">
                        <title>Research Report</title>
                        <style>
                            body {{ font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; }}
                            h1, h2, h3 {{ color: #333; }}
                            h1 {{ border-bottom: 2px solid #333; padding-bottom: 10px; }}
                            h2 {{ border-bottom: 1px solid #ccc; padding-bottom: 5px; }}
                            table {{ border-collapse: collapse; width: 100%; }}
                            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                            th {{ background-color: #f2f2f2; }}
                            code {{ background-color: #f4f4f4; padding: 2px 4px; border-radius: 3px; }}
                            blockquote {{ border-left: 4px solid #ccc; margin: 0; padding-left: 20px; }}
                        </style>
                    </head>
                    <body>
                        {html_content}
                    </body>
                    </html>
                    """
                    
                    pdf_b64 = self._generate_pdf_base64(html_with_style, "Research Report")
                    if pdf_b64:
                        pdf_link = f'<a href="data:application/pdf;base64,{pdf_b64}" download="report.pdf">Download PDF</a>'
                        await _safe_emit(__event_emitter__, f"\n\n**[PDF]** {pdf_link}\n")
                        yield f"\n\n**[PDF]** {pdf_link}\n"
                    else:
                        await _safe_emit(__event_emitter__, "\n\n**[PDF]** Export failed (weasyprint not installed)\n")
                        yield "\n\n**[PDF]** Export failed (weasyprint not installed)\n"
                except Exception as e:
                    await _safe_emit(__event_emitter__, f"\n\n**[PDF]** Export failed: {e}\n")
                    yield f"\n\n**[PDF]** Export failed: {e}\n"

            # Estatísticas avançadas
            # Get scraped cache from global state or phase results
            scraped_cache = {}
            if global_state and "scraped_cache" in global_state:
                scraped_cache = global_state["scraped_cache"]
            elif phase_results:
                # Fallback: get from last phase result
                last_result = phase_results[-1].get("result", {})
                scraped_cache = last_result.get("scraped_cache", {})
            
            total_urls = len(scraped_cache)
            total_phases = len(phase_results)
            domains = set()
            for url in scraped_cache:
                try:
                    domain = url.split("/")[2]
                    domains.add(domain)
                except:
                    pass

            await _safe_emit(__event_emitter__, f"\n---\n**📊 Estatísticas da Pesquisa:** Fases={total_phases}, URLs únicas={total_urls}, Domínios={len(domains)}, Contexto={len(raw_context):,} chars\n")
            yield f"\n---\n**📊 Estatísticas da Pesquisa:** Fases={total_phases}, URLs únicas={total_urls}, Domínios={len(domains)}, Contexto={len(raw_context):,} chars\n"

        except Exception as e:
            error_msg = str(e)
            is_timeout = (
                "timeout" in error_msg.lower() or "exceeded" in error_msg.lower()
            )

            if is_timeout:
                # 🔧 FIX: Calcular timeout HTTP real usado (não mostrar PLANNER_REQUEST_TIMEOUT que é irrelevante)
                effective_http_timeout = max(60, int(timeout_synthesis - 30))

                await _safe_emit(__event_emitter__, f"**[ERRO]** Síntese completa falhou: {e}\n")
                yield f"**[ERRO]** Síntese completa falhou: {e}\n"
                await _safe_emit(__event_emitter__, f"**[DICA]** Contexto muito grande ({len(deduped_context):,} chars). Sugestões:\n")
                yield f"**[DICA]** Contexto muito grande ({len(deduped_context):,} chars). Sugestões:\n"
                # Get max timeout value safely (Pydantic v1/v2 compatibility)
                try:
                    max_timeout = getattr(self.valves.__fields__['LLM_TIMEOUT_SYNTHESIS'], 'field_info', {}).get('extra', {}).get('le', 1800)
                except (AttributeError, KeyError):
                    max_timeout = 1800  # fallback
                await _safe_emit(__event_emitter__, f"  - Aumente LLM_TIMEOUT_SYNTHESIS nas valves (atual: {timeout_synthesis}s, máx: {max_timeout}s)\n")
                yield f"  - Aumente LLM_TIMEOUT_SYNTHESIS nas valves (atual: {timeout_synthesis}s, máx: {max_timeout}s)\n"
                await _safe_emit(__event_emitter__, f"  - 🔍 **Diagnóstico atual:**\n")
                yield f"  - 🔍 **Diagnóstico atual:**\n"
                await _safe_emit(__event_emitter__, f"    • timeout_synthesis (asyncio): {timeout_synthesis}s\n")
                yield f"    • timeout_synthesis (asyncio): {timeout_synthesis}s\n"
                await _safe_emit(__event_emitter__, f"    • request_timeout (HTTP): {effective_http_timeout}s (margem de 30s)\n")
                yield f"    • request_timeout (HTTP): {effective_http_timeout}s (margem de 30s)\n"
                await _safe_emit(__event_emitter__, f"    • HTTPX_READ_TIMEOUT (base): {self.valves.HTTPX_READ_TIMEOUT}s (não usado na síntese)\n")
                yield f"    • HTTPX_READ_TIMEOUT (base): {self.valves.HTTPX_READ_TIMEOUT}s (não usado na síntese)\n"
                await _safe_emit(__event_emitter__, f"  - ⚠️ **Se a resposta apareceu na OpenAI mas não aqui:** pode ser timeout de conexão HTTP. Verifique logs para '[LLM_CALL]'.\n")
                yield f"  - ⚠️ **Se a resposta apareceu na OpenAI mas não aqui:** pode ser timeout de conexão HTTP. Verifique logs para '[LLM_CALL]'.\n"
                await _safe_emit(__event_emitter__, f"  - Ou reduza MAX_DEDUP_PARAGRAPHS (atual: {self.valves.MAX_DEDUP_PARAGRAPHS}) para diminuir contexto\n")
                yield f"  - Ou reduza MAX_DEDUP_PARAGRAPHS (atual: {self.valves.MAX_DEDUP_PARAGRAPHS}) para diminuir contexto\n"
                await _safe_emit(__event_emitter__, f"  - Ou aumente DEDUP_SIMILARITY_THRESHOLD (atual: {self.valves.DEDUP_SIMILARITY_THRESHOLD}) para deduplicar mais agressivamente\n")
                yield f"  - Ou aumente DEDUP_SIMILARITY_THRESHOLD (atual: {self.valves.DEDUP_SIMILARITY_THRESHOLD}) para deduplicar mais agressivamente\n"
            else:
                await _safe_emit(__event_emitter__, f"**[ERRO]** Síntese completa falhou: {e}\n")
                yield f"**[ERRO]** Síntese completa falhou: {e}\n"

            await _safe_emit(__event_emitter__, f"**[FALLBACK]** Contexto completo ({len(raw_context):,} chars) disponível para análise manual.\n")
            yield f"**[FALLBACK]** Contexto completo ({len(raw_context):,} chars) disponível para análise manual.\n"

    def _validate_pipeline_state(self) -> None:
        """Valida consistência do estado interno do pipeline

        Verifica e corrige inconsistências entre estado interno.
        Chama este método no início de pipe() para prevenir estados inválidos.
        """
        # Check context lock consistency
        if hasattr(self, '_context_locked') and self._context_locked and not self._detected_context:
            logger.warning("Context locked but no detected context - resetting lock")
            self._context_locked = False

        # Check contract consistency
        if self._last_contract:
            if not isinstance(self._last_contract, dict):
                logger.error("Invalid contract type, clearing")
                self._last_contract = None
            elif "fases" not in self._last_contract:
                logger.error("Contract missing fases, clearing")
                self._last_contract = None
            elif not isinstance(self._last_contract.get("fases"), list):
                logger.error("Contract fases is not a list, clearing")
                self._last_contract = None

        # Check profile sync
        if self._detected_context and hasattr(self, '_intent_profile') and self._intent_profile:
            detected = self._detected_context.get("perfil_sugerido")
            if detected and detected != self._intent_profile:
                logger.warning(f"Profile mismatch syncing to detected: {detected}")
                self._intent_profile = detected

    def _resolve_tool_deterministic(
        self,
        name_hint: str,
        available_keys: List[str],
        fallback_hints: List[str] = None,
    ) -> Optional[str]:
        """Resolve tool deterministically: nome exato > prefixo > substring"""
        # 1. Nome exato (via valves preferred tools)
        preferred = getattr(self.valves, "PREFERRED_TOOLS", {})
        if name_hint in preferred and preferred[name_hint] in available_keys:
            return preferred[name_hint]

        # 2. Nome exato direto
        if name_hint in available_keys:
            return name_hint

        # 3. Prefixo (starts with)
        for key in available_keys:
            if key.startswith(name_hint):
                return key

        # 4. Substring (contains)
        if fallback_hints:
            for hint in fallback_hints:
                for key in available_keys:
                    if hint.lower() in key.lower():
                        return key

        return None

    def _resolve_tools(self, __tools__: Dict[str, Dict[str, Any]]):
        """Resolve tools using deterministic heuristics"""
        if not __tools__:
            raise RuntimeError(
                "No tools available. Please configure tools in OpenWebUI."
            )

        keys = list(__tools__.keys())
        logger.debug("Available tools: %s", keys)

        # Resolução determinística: nome exato > prefixo > substring
        discover_key = self._resolve_tool_deterministic(
            "discovery", keys, ["discover", "search"]
        )
        scrape_key = self._resolve_tool_deterministic(
            "scraper", keys, ["scrape", "scraper"]
        )
        context_reducer_key = self._resolve_tool_deterministic(
            "context_reducer", keys, ["reduce", "context"]
        )

        # Check if we found the required tools
        if not discover_key:
            raise RuntimeError(f"Discovery tool not found. Available tools: {keys}")
        if not scrape_key:
            raise RuntimeError(f"Scraper tool not found. Available tools: {keys}")

        logger.debug("Using discovery tool: %s", discover_key)
        logger.debug("Using scraper tool: %s", scrape_key)
        if context_reducer_key:
            logger.debug("Using context reducer tool: %s", context_reducer_key)

        # Get the callables
        d_callable_raw = __tools__[discover_key]["callable"]
        s_callable = __tools__[scrape_key]["callable"]
        cr_callable = (
            __tools__[context_reducer_key]["callable"] if context_reducer_key else None
        )

        # Create wrapper for discovery tool to handle extra parameters
        async def d_callable_wrapper(
            query: str,
            must_terms: Optional[List[str]] = None,
            avoid_terms: Optional[List[str]] = None,
            time_hint: Optional[Dict[str, Any]] = None,
            lang_bias: Optional[List[str]] = None,
            geo_bias: Optional[List[str]] = None,
            source_bias: Optional[str] = None,
            min_domains: Optional[int] = None,
            official_domains: Optional[List[str]] = None,
            profile: Optional[str] = None,
            phase_objective: Optional[str] = None,
            **kwargs,
        ):
            """Wrapper to handle discovery tool parameters"""
            # Extract time_hint parameters
            after = None
            before = None
            if time_hint:
                after = time_hint.get("after")
                before = time_hint.get("before")

            # Build kwargs dynamically based on the callable's supported parameters
            import inspect

            supported_params = set()
            try:
                supported_params = set(
                    inspect.signature(d_callable_raw).parameters.keys()
                )
            except Exception as e:
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[WARNING][D_WRAPPER] Could not inspect discovery tool signature: {e}")
                # Assume basic params if inspection fails
                supported_params = {
                    "query",
                    "profile",
                    "after",
                    "before",
                    "whitelist",
                    "pages_per_slice",
                }

            # Check if tool supports minimum required params
            if "query" not in supported_params:
                logger.error("[D_WRAPPER] Discovery tool does not support 'query' parameter!")
                return {"urls": []}

            base_kwargs = {
                "query": query,
                "profile": profile or "general",
                "after": after,
                "before": before,
                "whitelist": "",
                "pages_per_slice": 2,
            }

            rails_kwargs = {
                "must_terms": must_terms,
                "avoid_terms": avoid_terms,
                "time_hint": time_hint,
                "lang_bias": lang_bias,
                "geo_bias": geo_bias,
                "min_domains": min_domains,
                "official_domains": official_domains,
                "phase_objective": phase_objective,
            }

            # Filter kwargs to only what the callable accepts
            final_kwargs = {}
            for k, v in base_kwargs.items():
                if k in supported_params:
                    if v is not None or k == "query":
                        final_kwargs[k] = v

            for k, v in rails_kwargs.items():
                if k in supported_params and v is not None:
                    final_kwargs[k] = v

            # Ensure query is never empty
            if not final_kwargs.get("query"):
                logger.error("[D_WRAPPER] Query is empty!")
                return {"urls": []}

            # ===== GRACEFUL DEGRADATION WITH FALLBACK =====
            try:
                result = await d_callable_raw(**final_kwargs)
                if result is None:
                    logger.warning("[D_WRAPPER] Discovery tool returned None")
                    return {"urls": []}
                return result
            except TypeError as e:
                # Tool doesn't support advanced parameters - try with basic query only
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[D_WRAPPER] Advanced params failed: {e}")
                    print(f"[D_WRAPPER] Falling back to basic query only")
                
                try:
                    # Fallback: Only pass the essential query parameter
                    basic_kwargs = {"query": query}
                    result = await d_callable_raw(**basic_kwargs)
                    if result is None:
                        logger.warning("[D_WRAPPER] Discovery tool (fallback) returned None")
                        return {"urls": []}
                    
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(f"[D_WRAPPER] Fallback successful: {len(result.get('urls', []))} URLs found")
                    
                    return result
                except Exception as fallback_e:
                    logger.error(f"[D_WRAPPER] Even basic query failed: {fallback_e}")
                    return {"urls": []}
            except Exception as e:
                logger.error(f"[D_WRAPPER] Unexpected exception calling discovery tool: {e}")
                return {"urls": []}

        return d_callable_wrapper, s_callable, cr_callable

    async def _extract_contract_from_history(self, body: dict) -> Optional[dict]:
        """Extract contract from message history when 'siga' is called."""
        try:
            messages = body.get("messages", [])

            # Find the last assistant message that contains a plan
            for msg in reversed(messages):
                if msg.get("role") == "assistant":
                    content = msg.get("content", "")
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(f"[DEBUG] Checking message content: {content[:200]}...")

                    # Look for plan markers (both SDK and Manual formats)
                    has_fase = "Fase 1/" in content and "Objetivo:" in content
                    has_plano = "📋 Plano" in content
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(f"[DEBUG] Plan markers: has_fase={has_fase}, has_plano={has_plano}")

                    if has_fase or has_plano:
                        # Try to extract contract using LLM
                        planner = PlannerLLM(self.valves)

                        # Create a prompt to extract the contract from the rendered plan
                        extract_prompt = f"""Extraia o contrato JSON do plano renderizado abaixo.
                        
PLANO RENDERIZADO:
{content}

Retorne APENAS o JSON do contrato no formato:
{{
  "intent_profile": "company_profile",
  "intent": "resumo do objetivo",
  "entities": {{"canonical": ["entidade principal"], "aliases": []}},
  "phases": [
    {{
      "name": "nome da fase",
      "objective": "objetivo da fase",
      "seed_query": "<3-6 palavras, sem operadores>",
      "must_terms": ["termo1", "termo2"],
      "avoid_terms": [],
      "time_hint": {{"recency": "1y", "strict": false}},
      "source_bias": ["oficial", "primaria", "secundaria"],
      "evidence_goal": {{"official_or_two_independent": true, "min_domains": 3}},
      "lang_bias": ["pt-BR", "en"],
      "geo_bias": ["BR", "global"]
    }}
  ],
  "quality_rails": {{"min_unique_domains": 2, "need_official_or_two_independent": true}},
  "budget": {{"max_rounds": 2}}
}}

INSTRUÇÕES:
- Extraia o objetivo principal do plano
- Identifique as fases e seus objetivos
- Se faltar seed_query, gere com 3-6 palavras (sem operadores)
- Mantenha campos obrigatórios com valores padrão
- Retorne APENAS o JSON válido"""

                        # Filter params for GPT-5/O1 compatibility
                        safe_extract_params = get_safe_llm_params(
                            planner.model_name, planner.generation_kwargs
                        )
                        result = await _safe_llm_run_with_retry(
                            planner.llm,
                            extract_prompt,
                            safe_extract_params,
                            timeout=planner.valves.LLM_TIMEOUT_DEFAULT,
                            max_retries=1,
                        )
                        if result:
                            try:
                                contract = _extract_json_from_text(result)
                                if (
                                    contract
                                    and "phases" in contract
                                    and len(contract["phases"]) > 0
                                ):
                                    # Validate that we have the essential fields
                                    for phase in contract["phases"]:
                                        if not phase.get("objective") or not phase.get("seed_query"):
                                            if not phase.get("seed_query") and phase.get("objective"):
                                                try:
                                                    reask = f"Gere apenas uma seed query (3-6 palavras, sem operadores) para: '{phase.get('objective','')}'. Retorne só a seed."
                                                    safe_reask = get_safe_llm_params(
                                                        planner.model_name,
                                                        {"temperature": 0.2},
                                                    )
                                                    re = await _safe_llm_run_with_retry(
                                                        planner.llm,
                                                        reask,
                                                        safe_reask,
                                                        timeout=planner.valves.LLM_TIMEOUT_DEFAULT,
                                                        max_retries=1,
                                                    )
                                                    if re and re.get("replies"):
                                                        candidate = (
                                                            re["replies"][0]
                                                            .strip()
                                                            .strip('"')
                                                            .strip("'")
                                                        )
                                                        for op in [
                                                            "site:",
                                                            "filetype:",
                                                            "after:",
                                                            "before:",
                                                            "AND",
                                                            "OR",
                                                            '"',
                                                            "'",
                                                        ]:
                                                            candidate = candidate.replace(op, " ")
                                                        words = candidate.split()
                                                        if 3 <= len(words) <= 6:
                                                            phase["seed_query"] = " ".join(words)
                                                except Exception:
                                                    pass
                                            if not phase.get("objective"):
                                                phase["objective"] = f"Análise: {phase.get('seed_query', 'tópico')}"
                                    return contract
                            except (json.JSONDecodeError, KeyError, ValueError) as e:
                                logger.warning(f"Failed to parse extracted contract: {e}")
                            except Exception as e:
                                logger.error(f"Unexpected error parsing contract: {e}")
                                raise ContractValidationError(f"Contract parsing failed: {e}") from e

            return None
        except ContractValidationError:
            raise  # Re-raise specific errors
        except Exception as e:
            logger.warning(f"Failed to extract contract from history: {e}")
            return None

    async def _detect_unified_context(
        self, user_query: str, body: dict
    ) -> Dict[str, Any]:
        """DETECÇÃO UNIFICADA DE CONTEXTO - Apenas LLM (heurística removida)

        Analisa a consulta do usuário e histórico de mensagens para determinar:
        - Setor/indústria (10+ setores: saúde, tech, finanças, direito, etc.)
        - Tipo de pesquisa (acadêmica, regulatória, técnica, estratégica, notícias)
        - Perfil apropriado (company_profile, regulation_review, technical_spec, etc.)
        - Metadados adaptativos (estilo, foco, seções sugeridas)

        ✅ SIMPLIFICADO: Usa apenas LLM (heurística removida - era marginal)

        Returns:
            Dict com: setor, tipo, perfil, perfil_descricao, estilo, foco_detalhes,
                     tema_principal, secoes_sugeridas, detecção_confianca, fonte_deteccao
        """

        # Preparar contexto do histórico
        text_sample = user_query.lower()
        messages = body.get("messages", [])
        if messages:
            for msg in messages[:-1]:  # Excluir última mensagem (query atual)
                content = msg.get("content", "")
                if content:
                    text_sample += " " + content.lower()

        # ===== LLM ÚNICO (heurística removida) =====
        contexto_enriquecido = None
        try:
            from datetime import datetime

            current_date = datetime.now().strftime("%Y-%m-%d")

            # Prompt JSON-only sem CoT exposto (anti-recusa/anti-disclaimer)
            detect_prompt = f"""Você é um consultor de estratégia sênior.

Pense passo a passo INTERNAMENTE, mas NÃO exponha o raciocínio.
Retorne SOMENTE JSON válido no schema abaixo.

⚠️ IMPORTANTE:
- NÃO use markdown ou código fence (```json)
- NÃO escreva nada fora do JSON
- NÃO inclua pedidos de desculpa, menções a políticas ou resumos narrativos
- Apenas JSON válido conforme schema

CONSULTA: {user_query}

CONTEXTO DO HISTÓRICO:
{text_sample[:1000]}

Data atual: {current_date}

TAREFA:
Analise a consulta e produza:
- setor_principal (específico, não "geral")
- tipo_pesquisa (acadêmica, mercado, técnica, regulatória, notícias)
- perfil_sugerido: CRITICAL - escolha baseado no OBJETIVO PRINCIPAL:
  * company_profile: análise de MERCADO/EMPRESAS/COMPETIÇÃO/NEGÓCIOS (ex: "estudo de mercado", "análise competitiva", "players", "reputação")
  * regulation_review: análise REGULATÓRIA/LEGAL/COMPLIANCE (ex: "marco legal", "normas", "regulamentação")
  * technical_spec: análise TÉCNICA/OPERACIONAL/IMPLEMENTAÇÃO (ex: "arquitetura", "stack técnico", "processos operacionais")
  * literature_review: revisão ACADÊMICA/CIENTÍFICA (ex: "estado da arte", "revisão sistemática", "papers")
  * history_review: análise HISTÓRICA/TEMPORAL (ex: "evolução histórica", "contexto cultural")
  
- 5-10 key_questions (perguntas de DECISÃO que precisam resposta, não queries de busca)
- entities_mentioned (APENAS empresas/produtos/pessoas/marcas específicas mencionadas EXPLICITAMENTE, incluir aliases)
- research_objectives (3-5 objetivos de pesquisa específicos e mensuráveis)

SCHEMA JSON:
{{
  "setor_principal": "string",
  "tipo_pesquisa": "string", 
  "perfil_sugerido": "string",
  "key_questions": ["string"],
  "entities_mentioned": [{{"canonical": "string", "aliases": ["string"]}}],
  "research_objectives": ["string"],
  "perfil_descricao": "string",
  "estilo": "string",
  "foco_detalhes": "string",
  "tema_principal": "string",
  "secoes_sugeridas": ["string"],
  "detecção_confianca": 0.85,
  "fonte_deteccao": "llm"
}}"""

            # Ensure LLM components are initialized (handles case with no valves override)
            if self.analyst is None:
                self.analyst = AnalystLLM(self.valves)
            if self.judge is None:
                self.judge = JudgeLLM(self.valves)
            if self.planner is None:
                self.planner = PlannerLLM(self.valves)
            if self.deduplicator is None:
                self.deduplicator = Deduplicator(self.valves)

            safe_params = get_safe_llm_params(self.valves.LLM_MODEL, {"temperature": 0.1})
            
            result = await _safe_llm_run_with_retry(
                self.analyst.llm,
                detect_prompt,
                safe_params,
                timeout=self.valves.LLM_TIMEOUT_DEFAULT,
                max_retries=1,
            )

            if result and result.get("replies"):
                try:
                    contexto_enriquecido = _extract_json_from_text(result["replies"][0])
                    if contexto_enriquecido:
                        # Validar campos obrigatórios
                        required_fields = ["setor_principal", "tipo_pesquisa", "perfil_sugerido"]
                        if all(field in contexto_enriquecido for field in required_fields):
                            return contexto_enriquecido
                except Exception as e:
                    logger.warning(f"Failed to parse context detection result: {e}")

        except Exception as e:
            logger.warning(f"Context detection failed: {e}")

        # Fallback: contexto genérico
        return {
            "setor_principal": "geral",
            "tipo_pesquisa": "analise_mercado",
            "perfil_sugerido": "company_profile",
            "key_questions": [f"Quais são os principais aspectos de {user_query}?"],
            "entities_mentioned": [],
            "research_objectives": [f"Analisar {user_query}"],
            "perfil_descricao": "análise de mercado",
            "estilo": "executivo",
            "foco_detalhes": "dados e métricas",
            "tema_principal": user_query,
            "secoes_sugeridas": ["Resumo", "Análise", "Conclusões"],
            "detecção_confianca": 0.5,
            "fonte_deteccao": "fallback"
        }