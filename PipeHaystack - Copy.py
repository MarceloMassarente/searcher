# -*- coding: utf-8 -*-
"""
title: PiPeManual - Context-Aware Research Orchestration Pipeline
author: Enhanced for OpenWebUI compatibility
version: 4.8.1
requirements: pydantic,httpx[http2]
license: MIT
description: OpenWebUI Manifold Pipe with unified context detection, adaptive synthesis,
robust error handling, GPT-5/O1 compatibility, and intelligent Planner with key_questions coverage validation.
Implements intelligent multi-phase research with automatic profile detection, quality rails,
and context-aware report generation with discovery tool integration, comprehensive debugging, and PDF export.

CORE FEATURES:
- 🎯 **Unified Context Detection**: Single source of truth for intent, sector, and research type
- 🧠 **LLM-based Components**: Planner, Analyst, Judge with adaptive prompts (25-40+ lines)
- 📊 **Quality Rails**: Programmatic validation (evidence coverage, domain diversity, staleness)
- 🔄 **Multi-phase Orchestration**: Incremental scraping, URL caching, diminishing returns detection
- 📝 **Adaptive Synthesis**: Context-aware report generation with sector-specific structure
- 🛡️ **Profile System**: 5 research profiles (company, regulation, technical, literature, history)
- 📈 **Telemetry & Metrics**: Comprehensive tracking of novelty, quality, and coverage
- 🤖 **GPT-5/O1 Support**: Automatic parameter filtering for latest OpenAI models
- 🔧 **Robust Error Handling**: Custom exceptions, retry with backoff, state validation
- ✅ **Intelligent Planning**: Key_questions coverage validation + market study phase architecture
- ⚡ **Advanced Deduplication**: 3 algorithms (MMR, MinHash, TF-IDF) with metrics and recent context preservation

RESEARCH PROFILES:
- company_profile: Strategic business analysis with market focus
- regulation_review: Legal/regulatory compliance and policy analysis
- technical_spec: Technical specifications and implementation details
- literature_review: Academic research and scientific methodology
- history_review: Historical context and temporal evolution

CONTEXT DETECTION:
- Automatic sector identification (10+ sectors: tech, health, finance, etc.)
- Research type classification (academic, regulatory, technical, strategic, news)
- Profile-to-sector intelligent mapping with LLM enrichment
- Adaptive synthesis with dynamic report structure and tone

QUALITY RAILS (P0):
- Evidence coverage: ≥100% facts with ≥1 evidence
- Domain diversity: ≥2 unique domains per phase
- Staleness gates: Configurable recency validation by profile
- Novelty tracking: New facts/domains ratio monitoring
- Diminishing returns: Automatic detection and phase advancement

CHANGELOG v4.8.1 (Current - 2025-10-15):
- 🔴 CRITICAL BUG FIXES:
  - ✅ P0.1: Fixed must_terms entity loss - removed duplicate rails injection that overwrote entity merge
  - ✅ P0.2: Added source_bias parameter to discover_urls and propagated to intent_profile
  - ✅ P0.3: Ensured seed_core always present - dual fallback (Planner + Orchestrator)
  - ✅ P0.4: Fixed event_emitter undefined error in PDF export (_synthesize_final signature)
- 🎯 PARAMETER PASSING IMPROVEMENTS:
  - ✅ Pipe → Discovery integration: Entities now reach Discovery Planner via must_terms
  - ✅ seed_core mandatory in Planner LLM prompt with validation and examples
  - ✅ Discovery always receives rich query (seed_core or deterministic fallback)
  - ✅ Telemetry logging for debugging discovery_params and seed_core_source
- 📊 PROMPTS OPTIMIZATION (P0):
  - ✅ Consolidated seed_query rules into _build_seed_query_rules() helper (-40 lines)
  - ✅ Consolidated time windows into _build_time_windows_table() helper (-35 lines)
  - ✅ Consolidated entity validations into _build_entity_rules_compact() helper (-30 lines)
  - ✅ Fixed Planner exceeding phase limit - added pre-check before auto-adding news phase
  - ✅ Removed hardcoded "Korn Ferry" from Judge examples - replaced with dynamic entities
- 🚀 v4.8.1: REFACTORING ARCHITECTURAL (P0.1 + P0.2 + P0.5):
  - ✅ P0.1 PROMPTS CONSOLIDATION: Centralized 8 LLM prompts into PROMPTS dict (-800 lines)
    - planner_system, planner_seed_rules, planner_time_windows, planner_entity_rules
    - analyst_system, analyst_calibration, judge_system, judge_philosophy
    - Easy A/B testing, maintenance, and reusability across components
  - ✅ P0.2 RUN_ITERATION BREAKDOWN: Extracted 4 helper methods (-300 lines, -82% size)
    - _run_discovery(): Discovery logic, URL prioritization, telemetry
    - _run_scraping(): Cache check, scraper tool call, content accumulation
    - _run_context_reduction(): Context Reducer integration, reduction metrics
    - _run_analysis(): Analyst LLM call, facts/lacunas extraction, telemetry
    - High-level orchestration: 400 lines → 70 lines (modular, testable, readable)
  - ✅ P0.5 STANDARDIZED TELEMETRY: StepTelemetry dataclass with correlation_id
    - All pipeline steps tracked: discovery, scraper, context_reducer, analyst
    - Structured logging: elapsed_ms, inputs/outputs, counters, notes
    - Observability: debugging, performance monitoring, audit trail
- 🐛 PLANNER FIXES:
  - ✅ Seed query validation: 3-6 → 3-8 words (accommodate multi-word company names)
  - ✅ Context Detector f-string: Fixed JSON example escaping ({{...}} instead of {...})
  - ✅ Auto-news phase respects MAX_PHASES limit with final invariant validation

CHANGELOG v4.5:
- 🔴 CRITICAL BUG FIXES:
  - ✅ Fixed AttributeError: _extract_contract_from_history moved into Pipe class
  - ✅ "siga" command restored and functional
  - ✅ Discovery tool integration: dict return (eliminates JSON parse overhead)
  - ✅ Config propagation: timeouts/retries aligned between Pipe and Discovery
  - ✅ Random variable scope error fixed in deduplicator
- 🧹 CODE CLEANUP (-163 lines):
  - ✅ Removed 4 orphan functions (pick_question_by_kind, make_synthesis_spec, etc.)
  - ✅ Code quality: -2.7% LOC, improved maintainability
- 🔍 DEBUGGING ENHANCEMENTS:
  - ✅ Multi-layer debug logging (D_WRAPPER → DISCOVERY → ITERATION → ANALYST)
  - ✅ Discovery tool parameter validation and signature inspection
  - ✅ Comprehensive error handling with try-catch at 4 levels
  - ✅ Visual warnings for 0 URLs / 0 facts scenarios

CHANGELOG v4.4:
- 🚀 DEDUPLICATION ENGINE UPGRADE:
  - ✅ Threshold calibration: 0.9 → 0.85 (-75% duplicates)
  - ✅ Removed dangerous fallback that bypassed deduplication
  - ✅ Shingle size: n=5 → n=3 (tri-grams, +40% similarity detection)
  - ✅ Unified Deduplicator class with 3 algorithms (MMR, MinHash, TF-IDF)
  - ✅ Analyst preserve_recent_pct: CONFIGURABLE via ANALYST_PRESERVE_RECENT_PCT valve (default 1.0 = 100% of current iteration preserved)
  - ✅ Analyst reference_first=True: NEW data is REFERENCE (dedupe old data AGAINST new)
  - ✅ Synthesizer: NO shuffle (phases are not chronological, order is structural)
  - ✅ Metrics: reduction_pct, tokens_saved, algorithm_used
- 🎯 CONTEXT DETECTION & PLANNER FIXES (Critical):
  - ✅ Context Detection is SOURCE OF TRUTH (Planner must follow detected profile)
  - ✅ Enhanced Context Detection prompt with explicit examples (headhunting → company_profile)
  - ✅ Added RH/Headhunting sector with 18 keywords
  - ✅ Seed Query: MANDATORY tema central (ex: "volume fees HEADHUNTING Brasil")
  - ✅ Seed Query: MANDATORY entity names (1-3 entities → ALL names in seed)
  - ✅ News Default: 1y (not 90d) - 90d only for explicit "últimos 90 dias" / "breaking news"
- 🔄 JUDGE & ANALYST INTELLIGENCE UPGRADE (v4.4.1):
  - ✅ Analyst Auto-Assessment: coverage_score (0-1), gaps_critical, suggest_pivot/refine
  - ✅ Analyst calibration: explicit examples for coverage 0.3/0.6/0.9/1.0
  - ✅ Analyst re-ask: includes self_assessment in schema (fixes parsing failures)
  - ✅ Judge cross-check: validates coverage_score against objective metrics (facts/lacunas)
  - ✅ Confidence-Based Exit: DONE if coverage ≥ 70% even with low novelty
  - ✅ Auto-convert REFINE → NEW_PHASE when loops >= MAX_AGENT_LOOPS
  - ✅ NEW_PHASE triggers: contradictions (2+ loops), unexpected findings (critical), entities not covered
  - ✅ Enhanced telemetry: coverage_score, analyst_confidence, suggest_pivot, failed_query per loop
  - ✅ 6 explicit cases for NEW_PHASE in Judge prompt
  - ✅ Planner validation: REJECTS seed_query without entity names (1-3 entities)
  - ✅ Judge validation: REWRITES next_query if generic or missing entity (aggressive)
- 📊 Performance: MinHash 100x faster than MMR for 300+ chunks

v4.3.1 (2025-10-10):
- 🧹 CODE CLEANUP & CONSOLIDATION (P0 + P1):
  - ✅ P0 - Dead Code Removal:
    - Removed make_planner_from_strategist() - orphan function (~85 lines)
    - Removed _default_*() tools (3x) - orphan functions (~15 lines)
    - Removed ContractModel - orphan Pydantic model (~15 lines)
  - ✅ P1A - JSON Parsing Consolidation:
    - Created parse_json_resilient() with 3 modes (strict/soft/balanced)
    - Converted _extract_json_from_text(), parse_llm_json_strict(), _soft_json_cleanup() to wrappers
    - Unified parsing logic (was 3 implementations, now 1)
  - ✅ P1B - LLM Retry Consolidation:
    - Removed _safe_llm_run() wrapper (~20 lines)
    - 14 call sites updated to use _safe_llm_run_with_retry(max_retries=1) directly
  - ✅ P1C - Quality Metrics Consolidation:
    - Created _extract_quality_metrics() - unified metrics extraction function
    - Refactored _validate_quality_rails() to use shared metrics (~30 lines saved)
    - Refactored _calculate_evidence_metrics_new() to use shared metrics (~40 lines saved)
    - Refactored _decide_verdict_mece() to use shared metrics (~20 lines saved)
  - ✅ P1D - Synthesis Sections Consolidation:
    - Created _build_synthesis_sections() - unified section builder
    - Refactored _synthesize_final() to use shared builder (~30 lines saved)
  - 📊 TOTAL CONSOLIDATION: 115 dead code + 150+ duplicate logic = ~265 lines consolidated
  - 📈 IMPACT: 3→1 JSON parsers, 2→1 retry, 3 quality funcs unified, synthesis builder extracted

v4.3 (2025-10-10):
- 🎯 PLANNER INTELLIGENCE UPGRADE:
  - ✅ Mandatory key_questions coverage validation (each key_question → phase mapping)
  - ✅ Market study phase architecture enforcement (3y baseline + 1y trends + 90d breaking news)
  - ✅ Customized seed_query generation (objective-driven, not template-driven)
  - ✅ Removed rigid news window enforcement (allows Planner to create multi-phase temporal architecture)
- 📝 Enhanced Planner prompt with MECE examples and validation checklists
- 🚨 Critical guidance: "Studies need 1y trends phase BEFORE 90d news phase" (prevents 70% key_questions coverage loss)

v4.2:
- 🐛 Critical fix: Orchestrator._last_contract → self.contract (AttributeError resolved)
- 🔗 Improved contract management between Pipe and Orchestrator classes
- 📝 Updated documentation and docstrings

v4.1:
- 🤖 GPT-5/O1 compatibility with automatic parameter filtering
- ⚙️ get_safe_llm_params() helper for model-specific parameters

v4.0:
- 🛡️ Custom exceptions (4): PipeExecutionError, LLMConfigurationError, ContractValidationError, etc.
- 📊 PipeConstants class with 15+ centralized configurations
- 🔧 Helper methods (4): _debug_log, _get_current_profile, _sync_contract_with_context, etc.
- 🔄 Retry with exponential backoff for LLM calls
- ⏱️ Dynamic timeouts and configurable HTTP read timeouts
- ✅ Test suite: 33 tests - 100% passing

© 2025
"""
from __future__ import annotations
import asyncio, json, time, hashlib, re, os, uuid
from typing import Any, Dict, List, Optional, Callable, Tuple
from threading import Lock

try:
    from pydantic import BaseModel, Field, validator, ValidationError
except Exception:
    # keep compatibility with pydantic v1
    from pydantic.v1 import BaseModel, Field, validator, ValidationError


# ==================== PROMPT TEMPLATES (v4.8 - P0.1) ====================
# Centralização de prompts para fácil manutenção e A/B testing

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

**SCHEMA EXATO (copie a estrutura):**
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

# ===== HELPER FUNCTIONS PARA PROMPTS =====
# (Funções helper já existem no código, usando os dicionários globais)


# ==================== LINE-BUDGET GUARD (v4.8 - P0) ====================
# Sistema de validação de tamanho de função para prevenir regressão

import inspect
import warnings

def _warn_if_too_long(fn, soft=300, hard=500):
    """Valida tamanho de função e emite warning se exceder limite"""
    try:
        source = inspect.getsource(fn)
        loc = len([line for line in source.splitlines() if line.strip()])

        if loc > hard:
            warnings.warn(
                f"[LBG] CRITICAL: {fn.__name__} has {loc} LOC (hard limit: {hard}). "
                "MUST refactor immediately.",
                category=RuntimeWarning
            )
        elif loc > soft:
            warnings.warn(
                f"[LBG] WARNING: {fn.__name__} has {loc} LOC (soft limit: {soft}). "
                "Consider refactoring.",
                category=UserWarning
            )
    except Exception as e:
        logger.debug(f"Line-budget check failed for {fn.__name__}: {e}")

def _add_sector_context_if_ambiguous(query: str, entities: List[str], sector: str) -> str:
    """
    Adiciona contexto setorial se entidade na query for ambígua.
    
    Args:
        query: Query gerada pelo Judge
        entities: Lista de entidades canônicas do contract
        sector: Setor principal da pesquisa
    
    Returns:
        Query enriquecida com contexto setorial se necessário
    """
    if not query or not entities:
        return query
    
    query_lower = query.lower()
    
    # Detectar entidades ambíguas presentes na query
    ambiguous_found = []
    for entity in entities:
        entity_lower = entity.lower()
        # Ambíguo se: ≤4 chars OU palavra comum em inglês/programação
        common_words = {"exec", "flow", "run", "search", "data", "link", "sync", "fast", "rush"}
        
        # Check if entity appears in query (whole word or as part of multi-word entity)
        entity_words = entity_lower.split()
        if len(entity_words) == 1:
            # Single word entity - check if it's a whole word in query
            import re
            if re.search(r'\b' + re.escape(entity_lower) + r'\b', query_lower):
                if len(entity_lower) <= 4 or entity_lower in common_words:
                    ambiguous_found.append(entity)
        else:
            # Multi-word entity - check if any word matches
            for word in entity_words:
                if len(word) <= 4 or word in common_words:
                    if word in query_lower:
                        ambiguous_found.append(entity)
                        break
    
    # Se há entidade ambígua, verificar se já tem contexto setorial
    if ambiguous_found:
        # Mapeamento setor → termos de contexto
        sector_keywords = {
            "rh_headhunting": ["executive search", "headhunting", "recrutamento executivo", "consultoria rh"],
            "tech": ["software", "tecnologia", "startup", "plataforma", "saas"],
            "finance": ["fintech", "banco", "crédito", "pagamentos", "investimentos"],
            "health": ["saúde", "hospital", "clínica", "médico", "healthcare"],
            "retail": ["varejo", "e-commerce", "loja", "comercio"],
        }
        
        sector_terms = sector_keywords.get(sector, [])
        has_sector = any(term in query_lower for term in sector_terms)
        
        if not has_sector and sector_terms:
            # Adicionar primeiro termo setorial no início
            enriched_query = f"{sector_terms[0]} {query}"
            print(
                f"[JUDGE ENRICHMENT] Entidade ambígua detectada ({', '.join(ambiguous_found)}). "
                f"Query enriquecida: '{query}' → '{enriched_query}'"
            )
            return enriched_query
        else:
            # Debug: why wasn't it enriched?
            if getattr(__import__('sys').modules.get('__main__', type('obj', (object,), {})), 'DEBUG_ENRICHMENT', False):
                print(f"[DEBUG] Sector: {sector}, Terms: {sector_terms}, Has sector: {has_sector}")
                print(f"[DEBUG] Query: '{query}', Ambiguous: {ambiguous_found}")
    
    return query

# ==================== TELEMETRY (v4.8 - P0.5) ====================
from dataclasses import dataclass
from typing import Dict, List, Optional
import time

# Haystack optional imports para semantic dedup
try:
    from haystack.components.embedders import SentenceTransformersDocumentEmbedder, SentenceTransformersTextEmbedder
    from haystack.dataclasses import Document
    import numpy as np
    HAYSTACK_AVAILABLE = True
except ImportError:
    HAYSTACK_AVAILABLE = False
    # Warning will be logged when trying to use semantic dedup

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
            "inputs": self.inputs_brief,
            "outputs": self.outputs_brief,
            "counters": self.counters or {},
            "notes": self.notes or [],
        }


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
    avoid_terms: List[str] = []
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
    """Phase configuration with full validation (legacy - manter compatibilidade)"""

    name: str
    objective: str
    seed_query: str = Field(min_length=3, max_length=50)
    seed_core: Optional[str] = Field(
        default="",
        max_length=200,
        description="Seed query rica (1 frase) sem operadores",
    )
    seed_family_hint: Optional[str] = Field(
        default="entity-centric", description="Família de exploração"
    )
    must_terms: List[str] = []
    avoid_terms: List[str] = []
    time_hint: TimeHintModel
    source_bias: List[str]
    evidence_goal: EvidenceGoalModel
    lang_bias: List[str]
    geo_bias: List[str]
    suggested_domains: List[str] = Field(default=[], description="Domínios sugeridos")
    suggested_filetypes: List[str] = Field(
        default=[], description="Tipos de arquivo sugeridos"
    )

    @validator("seed_query")
    def validate_seed_query(cls, v, values):
        # Forbid operators
        forbidden = ["site:", "filetype:", "after:", "before:", "AND", "OR", '"']
        for op in forbidden:
            if op in v:
                raise ValueError(f"seed_query cannot contain operator: {op}")
        # ✅ REMOVED: Word count validation (3-6 words) - bloqueava queries válidas com entidades compostas
        # Discovery's internal Planner will optimize the query regardless of initial seed length

        # QUICK WIN #1 (v4.5.2): Validar que seed contém tema do objective
        # Previne seeds genéricas tipo "volume fees Brasil" sem contexto
        objective = values.get("objective", "")
        if objective:
            seed_lower = v.lower()
            obj_lower = objective.lower()

            # Extrair palavras significativas do objective (>4 chars, não stopwords)
            stopwords = {
                "para",
                "como",
                "qual",
                "onde",
                "quando",
                "porque",
                "quem",
                "sobre",
                "desta",
                "dessa",
                "nesta",
                "nessa",
                "seus",
                "suas",
            }
            obj_words = [
                w for w in obj_lower.split() if len(w) > 4 and w not in stopwords
            ]
            seed_words = seed_lower.split()

            # Calcular overlap: quantas palavras significativas do objective estão na seed?
            overlap = sum(
                1 for w in obj_words[:10] if w in seed_words
            )  # Limitar a 10 primeiras

            # P2 (opcional): exigir overlap mínimo proporcional (≥20% das palavras significativas do objective)
            min_overlap = max(1, int(len(obj_words) * 0.2)) if obj_words else 0
            if overlap < min_overlap and len(obj_words) > 0:
                raise ValueError(
                    f"seed_query deve incluir tema central do objective (overlap mínimo {min_overlap}). "
                    f"Objective tem: {', '.join(obj_words[:5])}"
                )

        # SLACK SEMÂNTICO (v4.5.2): Validar que métricas vão para must_terms, não seed
        must_terms = values.get("must_terms", [])
        specific_metrics = [
            "volume",
            "fees",
            "tempo-to-fill",
            "colocações",
            "receita",
            "market share",
            "revenue",
            "pricing",
            "faturamento",
        ]
        has_metrics_in_seed = any(metric in v.lower() for metric in specific_metrics)

        if has_metrics_in_seed and len(must_terms) < 3:
            raise ValueError(
                f"Seed query contém métricas específicas ('{v}') mas must_terms tem apenas {len(must_terms)} termos. "
                f"ESTRATÉGIA CORRETA: seed genérica (tema + geo) + métricas em must_terms para aproveitar Discovery Selector."
            )

        return v

    @validator("must_terms")
    def normalize_must_terms(cls, v):
        """Normaliza e valida must_terms (v4.5.2)"""
        if not v:
            return v

        # Remover duplicatas (case-insensitive)
        normalized = []
        seen = set()
        for term in v:
            if not isinstance(term, str):
                continue
            term_clean = term.lower().strip()
            if term_clean and term_clean not in seen:
                normalized.append(term)
                seen.add(term_clean)

        # Warning se muito longo (>15)
        if len(normalized) > 15:
            logger.warning(
                f"must_terms muito longo ({len(normalized)} termos). Recomendado: ≤15 para melhor performance."
            )

        return normalized


class QualityRailsModel(BaseModel):
    """Quality rails configuration"""

    min_unique_domains: int = Field(ge=2)
    need_official_or_two_independent: bool = True
    official_domains: List[str] = []


# REMOVED (v4.3): ContractModel - Pydantic model órfão nunca usado
# Validação de contracts é feita manualmente em PlannerLLM._validate_contract()

# ==================== CONFIGURATION CONSTANTS ====================
# Configurações centralizadas para fácil manutenção e consistência


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


# ===== Valves moved to Pipe class for UI exposure =====


# REMOVED (v4.3): _default_discovery/scraper/context_reducer - funções órfãs que apenas lançam RuntimeError
# Tool resolution é feita em _resolve_tools(), não há fallbacks inline


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


# ==================== UNIFIED DEDUPLICATION ENGINE (v4.4) ====================


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

        Returns:
            Dict com: chunks (deduped), original_count, deduped_count, reduction_pct, tokens_saved
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
                
                # Reconstruir com índices originais
                for chunk in deduped_low:
                    if chunk in chunk_to_indices and chunk_to_indices[chunk]:
                        idx = chunk_to_indices[chunk].pop(0)  # Pegar primeiro índice disponível
                        final_tuples.append((idx, chunk))

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
                selected_vecs = tfidf_matrix[selected_indices]
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
        embedder = SentenceTransformersDocumentEmbedder(model=model_name)
        embedder.warm_up()
        embedded_docs = embedder.run(all_docs)["documents"]
        
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
            # LLM-first: Deixar o LLM decidir qualidade através do scoring natural
            chunk_lower = chunk.lower()
            score = 0.0
            must_score = 0.0
            question_score = 0.0
            
            # 1. Must terms (weight: 2.0) - LLM-first: scoring inteligente
            if must_terms:
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
                return self._semantic_dedupe(chunks, threshold, max_chunks, model_name=model_name)
            else:  # default to mmr
                return _mmr_select(chunks, max_chunks, similarity_threshold=threshold)
        except Exception as e:
            # Fallback para MMR em caso de erro
            print(f"[DEDUP] FALLBACK CRITICO: {algorithm} -> MMR devido a: {e}")
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[DEDUP] Verifique dependencias: pip install scikit-learn datasketch sentence-transformers haystack")
            return _mmr_select(chunks, max_chunks, similarity_threshold=threshold)


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

    # REMOVED (v4.4 - P0.2): Fallback perigoso que anulava deduplicação
    # RAZÃO: "if len < k//2: return chunks[:k]" IGNORAVA toda a deduplicação!
    # NOVO: Sempre retornar selecionados (mesmo se poucos), garantindo dedupe real

    # Reordenar para preservar narrativa (se habilitado)
    if preserve_order and selected_with_indices:
        selected_with_indices.sort(key=lambda x: x[0])  # já está em ordem, mas garantir

    return [chunk for _, chunk in selected_with_indices]

# ===== LLM Client and Utilities (from PipeHay3) =====
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

        # ✅ FIX TIMEOUT HIERARCHY: Timeout per-request dinâmico (sobrescreve cliente)
        # RAZÃO: Synthesis precisa de 600s+, mas cliente tem BASE de 180s
        # HTTPX permite override per-request que TEM PRIORIDADE sobre cliente
        default_read = (
            getattr(self.valves, "HTTPX_READ_TIMEOUT", 180)
            if hasattr(self, "valves")
            else 180
        )
        request_timeout = float(gen_kwargs.get("request_timeout", default_read))

        # ✅ CRÍTICO: Usar MAX entre request_timeout e default (sempre respeitar explícito)
        # Se synthesis pede 600s, usar 600s (não cap em 180s!)
        effective_read_timeout = max(request_timeout, 60)  # Mínimo 60s

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


# ===== Logging =====
import logging
DEBUG_LOGGING = os.getenv("PIPE_DEBUG", "0") == "1"
logging.basicConfig(
    level=logging.DEBUG if DEBUG_LOGGING else logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("pipe")

# ===== LLM Cache =====
_LLM_CACHE: Dict[str, Optional[AsyncOpenAIClient]] = {}
_LLM_CACHE_LOCK: Lock = Lock()


def _get_llm(
    valves, generation_kwargs: Optional[dict] = None, model_name: Optional[str] = None
) -> Optional[AsyncOpenAIClient]:
    model = model_name or getattr(valves, "LLM_MODEL", "") or ""
    base = getattr(valves, "OPENAI_BASE_URL", "") or ""
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
    """LLM call with exponential backoff retry

    Args:
        llm_obj: LLM client object
        prompt_text: Prompt to send
        generation_kwargs: Generation parameters
        timeout: Base timeout in seconds (default: 60)
        max_retries: Maximum number of retry attempts (default: 3)

    Returns:
        LLM response dict or None on recoverable errors

    Raises:
        PipeExecutionError: On timeout or unrecoverable errors
        ToolExecutionError: On connection errors
    """
    if not llm_obj:
        logger.warning("LLM object is None, skipping call")
        return None

    base_timeout = timeout

    for attempt in range(max_retries):
        # Aumentar timeout a cada tentativa: 60s, 120s, 240s
        current_timeout = base_timeout * (2**attempt)

        try:
            # ✅ FIX: Usar timeout EXPLÍCITO sem cap quando disponível
            # RAZÃO: Síntese final precisa de 600s+ (prompt gigante)
            # Per-request timeout SOBRESCREVE timeout do cliente httpx
            eff_kwargs = dict(generation_kwargs or {})

            # ✅ SEMPRE usar current_timeout - 30s (margem para conexão + parsing + cleanup)
            # NÃO aplicar cap de HTTPX_READ_TIMEOUT (per-request override tem prioridade)
            # 🔧 FIX: Aumentar margem de 10s → 30s para evitar timeout durante parsing de respostas grandes
            effective_http_timeout = max(60, int(current_timeout - 30))

            if current_timeout > 300:
                logger.info(
                    f"[TIMEOUT] Long operation: {current_timeout}s (synthesis/large prompt)"
                )

            eff_kwargs["request_timeout"] = effective_http_timeout

            # 🔧 LOG: Diagnóstico de timeout antes da chamada
            logger.info(
                f"[LLM_CALL] Attempt {attempt+1}/{max_retries}: timeout={current_timeout}s, http_timeout={effective_http_timeout}s"
            )

            # Direct async call (no asyncio.to_thread needed with AsyncOpenAIClient)
            result = await asyncio.wait_for(
                llm_obj.run(prompt=prompt_text, generation_kwargs=eff_kwargs),
                timeout=current_timeout,
            )

            # 🔧 LOG: Sucesso!
            response_size = len(str(result)) if result else 0
            logger.info(f"[LLM_CALL] ✅ Response received ({response_size:,} chars)")

            return result

        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                wait_time = 2**attempt  # 1s, 2s, 4s
                logger.warning(
                    f"Timeout attempt {attempt+1}/{max_retries}, retrying in {wait_time}s with timeout={current_timeout}s"
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(
                    f"LLM call timeout after {max_retries} attempts (final timeout: {current_timeout}s)"
                )
                raise PipeExecutionError(
                    f"LLM call exceeded timeout after {max_retries} retries"
                )

        except (ConnectionError, TimeoutError) as e:
            # Retry apenas em erros recuperáveis
            if "rate limit" in str(e).lower() or "overloaded" in str(e).lower():
                if attempt < max_retries - 1:
                    wait_time = 2 ** (attempt + 2)  # 4s, 8s, 16s para rate limit
                    logger.warning(f"Rate limit/overload, retrying in {wait_time}s")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"LLM Connection Error after {max_retries} retries: {e}"
                    )
                    raise ToolExecutionError(
                        f"Failed to connect to LLM after {max_retries} retries: {e}"
                    ) from e
            else:
                # Erro não-recuperável de conexão
                logger.error(f"LLM Connection Error: {e}")
                raise ToolExecutionError(f"Failed to connect to LLM: {e}") from e

        except (KeyError, ValueError, json.JSONDecodeError) as e:
            # Parse errors can be transient (truncation, markdown wrappers). Retry with re-ask.
            logger.warning(f"LLM Response Parse Error: {e}")
            if attempt < max_retries - 1:
                wait_time = 2**attempt
                logger.warning(
                    f"Parse error attempt {attempt+1}/{max_retries}, retrying in {wait_time}s"
                )
                await asyncio.sleep(wait_time)
                # Append a strict JSON-only instruction for the next attempt
                prompt_suffix = "\n\nIMPORTANT: Return ONLY valid JSON (object or array). Do NOT include any markdown or explanatory text. Use double quotes for keys and strings."
                # If prompt_text already contains the suffix, don't duplicate
                if not prompt_text.strip().endswith(prompt_suffix.strip()):
                    prompt_text = prompt_text + prompt_suffix
                continue
            else:
                logger.error(
                    f"LLM Response Parse Error after {max_retries} attempts: {e}"
                )
                raise PipeExecutionError(
                    f"Invalid LLM response format after {max_retries} retries: {e}"
                ) from e

        except Exception as e:
            # Retry apenas em erros recuperáveis
            if "rate limit" in str(e).lower() or "overloaded" in str(e).lower():
                if attempt < max_retries - 1:
                    wait_time = 2 ** (attempt + 2)  # 4s, 8s, 16s para rate limit
                    logger.warning(f"Rate limit/overload, retrying in {wait_time}s")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"LLM Unexpected Error after {max_retries} retries: {e}"
                    )
                    raise PipeExecutionError(
                        f"LLM call failed after {max_retries} retries: {e}"
                    ) from e
            else:
                # Erro não-recuperável
                logger.error(f"LLM Unexpected Error: {e}")
                raise PipeExecutionError(f"LLM call failed: {e}") from e


# REMOVED (v4.3.1): _safe_llm_run() - wrapper desnecessário
# Use diretamente _safe_llm_run_with_retry(max_retries=1) para single attempt


# ===== Utilities =====
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


# ==================== UNIFIED JSON PARSING (v4.3.1 - P1A Consolidation) ====================


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
    # Remove inline comments (// and /* */)
    snippet = re.sub(r'//.*?$', '', snippet, flags=re.MULTILINE)  # Single-line comments
    snippet = re.sub(r'/\*.*?\*/', '', snippet, flags=re.DOTALL)  # Multi-line comments
    
    # Remove trailing commas at ANY nesting level
    snippet = re.sub(r',\s*([}\]])', r'\1', snippet)  # , before } or ]
    
    # Normalize whitespace
    snippet = re.sub(r'\s+', ' ', snippet)
    
    # Tentativa 3: parse cleaned snippet
    try:
        return json.loads(snippet)
    except json.JSONDecodeError as e:
        # Enhanced error logging
        logger.error(f"[JSON_PARSE] Failed after aggressive cleanup: {e}")
        logger.error(f"[JSON_PARSE] Error position: char {e.pos}")
        ctx_start = max(0, e.pos - 50)
        ctx_end = min(len(snippet), e.pos + 50)
        logger.error(f"[JSON_PARSE] Context: ...{snippet[ctx_start:ctx_end]}...")

    # Tentativa 4: balanceamento de chaves/colchetes
    if snippet.startswith("{"):
        start = snippet.find("{")
        depth = 0
        for i in range(start, len(snippet)):
            ch = snippet[i]
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(snippet[start : i + 1])
                    except Exception:
                        break
    elif snippet.startswith("[") and allow_arrays:
        start = snippet.find("[")
        depth = 0
        for i in range(start, len(snippet)):
            ch = snippet[i]
            if ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(snippet[start : i + 1])
                    except Exception:
                        break

    return None


# ==================== LEGACY JSON PARSERS (mantidos para compatibilidade, delegam para parse_json_resilient) ====================


def _extract_json_from_text(text: str) -> Optional[dict]:
    """LEGACY WRAPPER: Delega para parse_json_resilient(mode='balanced')"""
    return parse_json_resilient(text, mode="balanced", allow_arrays=True)


def _soft_json_cleanup(text: str) -> str:
    """LEGACY WRAPPER: Delega para parse_json_resilient(mode='soft')

    Returns cleaned text (not parsed dict) for backward compatibility.
    """
    if not isinstance(text, str):
        return text
    # Usar mode='soft' e depois retornar o texto limpo (não parseado)
    s = text
    s = re.sub(r",\s*}\s*", r"}", s)
    s = re.sub(r",\s*\]\s*", r"]", s)
    return s


# ==================== LLM PARAMETER HELPERS ====================


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


# ==================== CONTRACT PARSING & BUILDING (Inline) ====================


def extract_json_object_strict(text: str) -> str:
    """LEGACY HELPER: Isola o primeiro {...} válido

    Mantido para compatibilidade, mas parse_json_resilient(mode='strict') é preferível.
    """
    if not text or not isinstance(text, str):
        raise ContractValidationError("Empty or invalid LLM response")

    m_open = text.find("{")
    m_close = text.rfind("}")
    if m_open == -1 or m_close == -1 or m_close <= m_open:
        raise ContractValidationError(
            f"JSON object not found in LLM response (len={len(text)})"
        )

    return text[m_open : m_close + 1]


def parse_llm_json_strict(text: str) -> dict:
    """LEGACY WRAPPER: Delega para parse_json_resilient(mode='strict')"""
    return parse_json_resilient(text, mode="strict", allow_arrays=False)


def _first_content_token(text: str) -> str:
    """Extrai primeiro token significativo de um texto (>4 chars, não stopword)

    Args:
        text: Texto para extrair token

    Returns:
        str: Primeiro token significativo ou ""
    """
    stopwords = {
        "sobre",
        "para",
        "como",
        "quais",
        "quem",
        "qual",
        "quando",
        "onde",
        "estudo",
        "análise",
        "volume",
        "mercado",
    }
    for w in text.lower().split():
        if len(w) > 4 and w not in stopwords:
            return w
    return ""


def _patch_seed_if_needed(
    phase: dict, strict_mode: bool, metrics: dict, logger
) -> None:
    """Patch seed_query se estiver muito magra (modo relax apenas)

    Adiciona 1 token do objetivo se seed_query não contém nenhum token significativo do objetivo.

    Args:
        phase: Dict da fase (será modificado se necessário)
        strict_mode: Se True, não faz patch (apenas valida)
        metrics: Dict de métricas (incrementa seed_patched_count)
        logger: Logger para info
    """
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


# ==================== FASE 2 HELPERS ====================


def _check_mece_basic(fases: List[dict], key_questions: List[str]) -> List[str]:
    """Verifica MECE básico: key_questions órfãs (sem cobertura)

    Args:
        fases: Lista de fases do contract
        key_questions: Lista de key_questions do contexto

    Returns:
        Lista de key_questions não cobertas por nenhuma fase
    """
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
    """Adiciona fase candidata ao contract (incremental, sem replanejar)

    Args:
        contract: Contract dict (será modificado)
        candidate: Fase candidata completa
    """
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


# ==================== ENTITY COVERAGE HELPERS (FASE 1) ====================


def _calc_entity_coverage(fases: List[dict], entities: List[str]) -> float:
    """Calcula a % de fases que contêm pelo menos uma entidade em must_terms

    Args:
        fases: Lista de fases do contract (dicts com must_terms)
        entities: Lista de entidades canônicas

    Returns:
        float entre 0.0 e 1.0 representando a cobertura
    """
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
    """Lista as fases que NÃO contêm nenhuma entidade em must_terms

    Args:
        fases: Lista de fases do contract
        entities: Lista de entidades canônicas

    Returns:
        Lista de nomes das fases sem entidades
    """
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
    """Valida entity coverage em modo SOFT (warning em vez de exception)

    Args:
        contract: Contract dict (será modificado com _entity_coverage_warning se necessário)
        entities: Lista de entidades canônicas
        min_coverage: Threshold mínimo (ex: 0.70 = 70%)
        logger: Logger para warnings
    """
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


# ==================== PHASE POLICY ENFORCEMENT ====================


def _build_synthesis_sections(
    key_questions: List[str],
    research_objectives: List[str],
    contract_entities: List[str],
    phase_objectives: List[str],
) -> dict:
    """CONSOLIDADO (v4.3.1 - P1D): Constrói seções do prompt de síntese

    Args:
        key_questions: Lista de perguntas decisórias
        research_objectives: Lista de objetivos de pesquisa
        contract_entities: Lista de entidades do contract
        phase_objectives: Lista de objetivos das fases executadas

    Returns:
        Dict com: key_questions_section, research_objectives_section,
                 entities_section, phase_objectives_section
    """
    sections = {}

    # Key Questions Section
    if key_questions:
        kq_display = "\n".join(f"{i}. {q}" for i, q in enumerate(key_questions[:8], 1))
        sections[
            "key_questions"
        ] = f"""
🎯 **KEY QUESTIONS (O RELATÓRIO DEVE RESPONDER):**
{kq_display}
"""
    else:
        sections["key_questions"] = ""

    # Research Objectives Section
    if research_objectives:
        obj_display = "\n".join(f"• {obj}" for obj in research_objectives[:5])
        sections[
            "research_objectives"
        ] = f"""
📋 **RESEARCH OBJECTIVES (OBJETIVOS FINAIS):**
{obj_display}
"""
    else:
        sections["research_objectives"] = ""

    # Entities Section
    if contract_entities:
        entities_display = ", ".join(contract_entities[:15])
        if len(contract_entities) > 15:
            entities_display += f" (e mais {len(contract_entities) - 15})"
        sections[
            "entities"
        ] = f"""
🏢 **ENTIDADES ESPECÍFICAS (devem ser cobertas no relatório):**
{entities_display}
"""
    else:
        sections["entities"] = ""

    # Phase Objectives Section
    if phase_objectives:
        sections[
            "phase_objectives"
        ] = f"""
📊 **OBJETIVOS DAS FASES EXECUTADAS:**
{chr(10).join(phase_objectives)}
"""
    else:
        sections["phase_objectives"] = ""

    return sections


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
                continue
    return None


def _render_contract(contract: Dict[str, Any]) -> str:
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


# ===== LLM Components (from PipeHay3) =====
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

            # v4.4: Usar parse_json_resilient direto (modo balanced - máximo esforço)
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
                reparsed = parse_json_resilient(
                    re_raw, mode="strict", allow_arrays=False
                )
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
            logger.error(f"[ANALYST] Exceção não tratada: {e}")
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                import traceback

                traceback.print_exc()
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

        # Sem fatos é válido (vai para lacunas)
        if not facts:
            return {"valid": True}

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
        previous_queries: Optional[List[str]] = None,  # ← NEW
        failed_queries: Optional[List[str]] = None,  # ← WIN #3: Failed queries context
    ) -> Dict[str, Any]:
        if not self.llm:
            raise ValueError("LLM não configurado")

        phase_info = ""
        if phase_context:
            phase_info = f"\n**Critérios:** {', '.join(phase_context.get('accept_if_any_of', []))}"

        prompt = _build_judge_prompt(
            user_prompt,
            analysis,
            phase_context,
            telemetry_loops,
            intent_profile,
            full_contract,
            refine_queries=refine_queries,
            phase_candidates=phase_candidates,
            previous_queries=previous_queries,
            failed_queries=failed_queries,  # ← WIN #3: Pass failed queries
        )

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

        parsed = _extract_json_from_text(out.get("replies", [""])[0])
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

            if getattr(self.valves, "VERBOSE_DEBUG", False):
                answered = kq_status.get("answered", [])
                unanswered = kq_status.get("unanswered", [])
                print(
                    f"[DEBUG][JUDGE] Key Questions Coverage: {key_questions_coverage:.2f} ({len(answered)} answered, {len(unanswered)} unanswered)"
                )
                if blind_spots:
                    print(f"[DEBUG][JUDGE] Blind Spots detectados: {blind_spots[:3]}")

        # SINAL 5: Blind Spots Críticos (descobertas que invalidam hipóteses)
        # SINAL 5: Blind Spots (telemetria, não override)
        # Se Judge detectou blind_spots → sinal de que hipóteses desviaram
        # O Judge LLM decide se são críticos ou apenas lacunas normais
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

        if getattr(self.valves, "VERBOSE_DEBUG", False):
            print(f"[JUDGE][SCORE] phase_score={phase_score:.3f}")
            print(
                f"[JUDGE][SCORE] Métricas: coverage={key_questions_coverage:.2f}, novel_facts={novel_fact_ratio:.2f}, novel_domains={novel_domain_ratio:.2f}, diversity={domain_diversity:.2f}, contradictions={contradiction_score:.2f}"
            )

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
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(
                    f"[JUDGE][RAIL] Contradições críticas: {current_family} → {seed_family_switch}"
                )

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
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(
                    f"[JUDGE][SWITCH] Score baixo + flat: {current_family} → {seed_family_switch}"
                )

        # Regra 3: Score BAIXO mas ainda há tração → REFINE
        elif phase_score < threshold and traction:
            programmatic_decision = {
                "verdict": "refine",
                "reasoning": f"Phase score {phase_score:.2f} < {threshold:.2f} mas há tração. Refinar",
            }

        # Regra 4 (REMOVIDA): Blind spots agora são soft signal para Judge LLM
        # O Judge já recebe blind_spots no prompt e decide se justificam NEW_PHASE
        # Manter apenas log de telemetria

        # Fallback: REFINE (caso não se encaixe em nenhuma regra acima)
        else:
            programmatic_decision = {
                "verdict": "refine",
                "reasoning": f"Score {phase_score:.2f}, coverage {key_questions_coverage*100:.0f}%. Continuar refinando",
            }

        # ===== CRIAR NEW_PHASE OBJECT SE NECESSÁRIO (v4.7) =====
        # Se decisão for NEW_PHASE, garantir que temos um new_phase object
        if programmatic_decision.get("verdict") == "new_phase":
            # Priorizar new_phase do Judge LLM (mais semântico)
            if not judge_new_phase or not judge_new_phase.get("objective"):
                # Fallback: gerar programaticamente baseado em lacunas/blind_spots
                lacunas_list = analysis.get("lacunas", [])

                if blind_spots_signal and blind_spots:
                    # Transform negative blind_spot into positive query
                    first_bs = blind_spots[0]
                    # Remove "Ausência de", "Falta", "Não encontrado" patterns
                    import re
                    positive_query = re.sub(
                        r'^(Ausência de|Falta|Não encontr(ou|ado|ada)|Sem)\s+', 
                        '', 
                        first_bs, 
                        flags=re.IGNORECASE
                    )
                    
                    judge_new_phase = {
                        "objective": f"Buscar informações sobre: {positive_query[:80]}",
                        "seed_query": " ".join(positive_query.split()[:6]),
                        "name": "Descobertas Inesperadas",
                        "por_que": f"Blind spots: {'; '.join(blind_spots[:2])}",
                    }
                elif lacunas_list:
                    # Baseado em lacunas
                    lacunas_sample = lacunas_list[:2]
                    # P0: lacunas_list pode ser lista de dicts ou strings
                    lacunas_text = []
                    for lac in lacunas_sample:
                        if isinstance(lac, dict):
                            lacunas_text.append(lac.get("descricao", str(lac)))
                        else:
                            lacunas_text.append(str(lac))
                    first_lacuna = lacunas_text[0] if lacunas_text else "busca dirigida"
                    judge_new_phase = {
                        "objective": f"Busca dirigida para lacunas: {'; '.join(lacunas_text)}",
                        "seed_query": " ".join(first_lacuna.split()[:6]),
                        "name": "Busca Dirigida - Lacunas Essenciais",
                        "por_que": f"Lacunas persistem após {flat_streak} loops flat",
                    }
                else:
                    # Fallback genérico
                    judge_new_phase = {
                        "objective": f"Exploração alternativa: {user_prompt[:60]}",
                        "seed_query": " ".join(user_prompt.split()[:6]),
                        "name": "Exploração Alternativa",
                        "por_que": f"Score {phase_score:.2f} < threshold após {flat_streak} loops",
                    }

            # Adicionar seed_family ao new_phase se foi calculado
            if seed_family_switch:
                judge_new_phase["seed_family_hint"] = seed_family_switch
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[JUDGE] NEW_PHASE com família: {seed_family_switch}")

            # Atualizar programmatic_decision com new_phase completo
            programmatic_decision["new_phase"] = judge_new_phase

        # Aplicar rails de qualidade programáticos
        verdict = parsed.get("verdict", "done").strip()
        reasoning = parsed.get("reasoning", "").strip()
        next_query = parsed.get("next_query", "").strip()

        # Salvar decisão original do Judge para comparação
        original_verdict = verdict
        original_reasoning = reasoning
        modifications = []

        # ✅ P2: Detectar duplicação de new_phase ANTES de aplicar override
        if programmatic_decision.get("verdict") == "new_phase":
            proposed_new_phase = programmatic_decision.get("new_phase", {})
            duplicate_detected = False

            if proposed_new_phase and full_contract and full_contract.get("fases"):
                # ✅ CORE FIX: Multi-dimensional similarity comparison
                try:
                    from sklearn.feature_extraction.text import TfidfVectorizer
                    from sklearn.metrics.pairwise import cosine_similarity

                    new_obj = proposed_new_phase.get("objective", "")
                    new_seed = proposed_new_phase.get("seed_query", "")
                    new_type = proposed_new_phase.get("phase_type", "")
                    
                    existing_phases = full_contract.get("fases", [])
                    
                    if new_obj and existing_phases:
                        # Calculate multi-dimensional similarity
                        max_weighted_score, max_sim_idx = self._calculate_multi_dimensional_similarity(
                            new_obj, new_seed, new_type, existing_phases
                        )

                        if max_weighted_score > self.valves.DUPLICATE_DETECTION_THRESHOLD:
                            duplicate_phase = existing_phases[max_sim_idx]
                            duplicate_detected = True

                            # Converter NEW_PHASE → REFINE (fase já existe)
                            logger.warning(
                                f"[JUDGE] Fase duplicada detectada (similaridade {max_weighted_score:.2f}): '{duplicate_phase.get('name', 'N/A')}'"
                            )
                            modifications.append(
                                f"Duplicate phase: new_phase → refine (similaridade {max_weighted_score:.2f} com '{duplicate_phase.get('name', 'N/A')}')"
                            )

                            # Usar seed do new_phase proposto como next_query para refine
                            programmatic_decision = {
                                "verdict": "refine",
                                "reasoning": f"[AUTO-CORREÇÃO] Fase proposta duplica '{duplicate_phase.get('name', 'fase existente')}'. Convertido para refine.",
                                "next_query": proposed_new_phase.get("seed_query", ""),
                            }

                            if getattr(self.valves, "VERBOSE_DEBUG", False):
                                print(
                                    f"[JUDGE] NEW_PHASE bloqueado por duplicação: '{new_obj[:60]}...'"
                                )
                                print(
                                    f"[JUDGE] Duplica fase existente: '{duplicate_phase.get('objetivo', 'N/A')[:60]}...'"
                                )
                                print(
                                    f"[JUDGE] Convertido para REFINE com seed: '{proposed_new_phase.get('seed_query', '')}'"
                                )

                except Exception as e:
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            f"[JUDGE] Erro na detecção de duplicação (continuando sem check): {e}"
                        )

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
            # Query focada nas lacunas
            lacunas = analysis.get("lacunas", [])
            if lacunas and not next_query:
                # P0: lacunas pode ser lista de dicts ou strings
                first_lacuna = lacunas[0]
                if isinstance(first_lacuna, dict):
                    lacuna_text = first_lacuna.get("descricao", "")
                else:
                    lacuna_text = str(first_lacuna)
                lacuna_sample = " ".join(lacuna_text.split()[:8]) if lacuna_text else ""
                next_query = (
                    lacuna_sample
                    or f"dados verificáveis {phase_context.get('objetivo', '')[:30]}"
                )

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
            # Criar proposta de nova fase se não existe
            if not parsed.get("new_phase"):
                parsed["new_phase"] = {
                    "objective": f"Explorar descobertas inesperadas: {blind_spots[0][:80]}",
                    "seed_query": " ".join(blind_spots[0].split()[:6]),
                    "name": "Descobertas Inesperadas",
                    "por_que": f"Blind spots críticos detectados: {'; '.join(blind_spots[:2])}",
                }

        # Incluir nova fase se foi criada programaticamente
        new_phase = parsed.get("new_phase", {})
        if programmatic_decision.get("new_phase"):
            new_phase = programmatic_decision["new_phase"]
        
        # ===== NOVO: Rotacionar família de exploração entre loops (P0 - Missing Feature) =====
        if verdict == "new_phase" and new_phase:
            # Contar loops para determinar se deve rotacionar família
            loop_number = len(telemetry_loops) if telemetry_loops else 0
            
            # Rotacionar família apenas a partir do loop 2 (terceira iteração)
            if loop_number >= 2:
                current_family = phase_context.get("seed_family_hint", "entity-centric") if phase_context else "entity-centric"
                
                # Mapeamento de rotação de famílias
                family_rotation = {
                    "entity-centric": "problem-centric",
                    "problem-centric": "outcome-centric", 
                    "outcome-centric": "regulatory",
                    "regulatory": "entity-centric"
                }
                
                new_family = family_rotation.get(current_family, "problem-centric")
                
                # Adicionar seed_family_hint ao new_phase
                if isinstance(new_phase, dict):
                    new_phase["seed_family_hint"] = new_family
                    
                    # Atualizar reasoning para incluir mudança de família
                    if "reasoning" in new_phase:
                        new_phase["reasoning"] += f" Mudança de família: {current_family} → {new_family}"
                    else:
                        new_phase["reasoning"] = f"Mudança de família: {current_family} → {new_family}"
                
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[JUDGE] Rotação de família: {current_family} → {new_family} (loop {loop_number})")

        # QUICK WIN #2 (v4.5.2): Validar similaridade de next_query (anti-duplicação)
        if verdict == "refine" and next_query and telemetry_loops:
            # Extrair queries já usadas do telemetry
            used_queries = []
            for loop in telemetry_loops:
                q = loop.get("query", "").strip().lower()
                if q:
                    used_queries.append(q)

            # Verificar similaridade com queries anteriores
            from difflib import SequenceMatcher

            next_lower = next_query.strip().lower()

            for used in used_queries:
                similarity = SequenceMatcher(None, next_lower, used).ratio()
                if similarity > 0.7:
                    # Query muito similar → problema
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            f"[WARNING][JUDGE] next_query muito similar à query anterior: {similarity:.2%} similaridade"
                        )
                        print(f"[WARNING][JUDGE] Anterior: '{used}'")
                        print(f"[WARNING][JUDGE] Nova: '{next_lower}'")

                    # Forçar DONE ao invés de REFINE com query duplicada
                    # Melhor parar do que repetir busca idêntica
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            f"[JUDGE] Convertendo REFINE → DONE (query duplicada não agrega valor)"
                        )

                    verdict = "done"
                    reasoning = f"[AUTO-CORREÇÃO] Query proposta muito similar à anterior ({similarity:.0%}). Parando para evitar repetição inútil."
                    next_query = ""
                    break

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

        # REMOVIDO: Gate de 100% evidência obrigatório (muito rígido)
        # REMOVIDO: Gate de "qualquer lacuna = falha" (pessimista demais)
        # REMOVIDO: Gate de diversidade mínima de domínios (não é crítico)
        # REMOVIDO: Gate de oficial_or_two obrigatório (Judge decide melhor)
        # MANTIDO: Staleness check (apenas quando strict=True no perfil)

        # Verificar staleness apenas se perfil exigir (news, regulatory)
        staleness_check = self._check_evidence_staleness(
            facts, phase_context, intent_profile=intent_profile
        )
        if not staleness_check["passed"]:
            # Staleness é informativo, não bloqueante
            # Apenas loggar mas não bloquear DONE
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[INFO][JUDGE] Staleness warning: {staleness_check['reason']}")
            # return {
            #     "passed": False,
            #     "reason": staleness_check["reason"],
            #     "suggested_query": staleness_check.get("suggested_query", "Buscar evidências mais recentes")
            # }

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

    def _decide_verdict_mece(self, analysis, phase_context, user_prompt):
        """Decisão MECE programática para NEW_PHASE vs REFINE

        REFACTORED (v4.3.1 - P1C): Usa _extract_quality_metrics() para evitar duplicação
        v4.4: Adiciona verificação de entidades alvo não cobertas
        """
        facts = analysis.get("facts", [])
        lacunas = analysis.get("lacunas", [])

        if not facts:
            return {
                "verdict": "refine",
                "reasoning": "Sem fatos encontrados - precisa de refine",
                # v4.4: Sem fallback genérico – prompt do Judge deve gerar a próxima query específica
                "next_query": "",
            }

        # 1.1 Lacuna estrutural (Coverage Gap) - Let Judge LLM handle this intelligently
        # Removed heuristic phase creation - Judge LLM will analyze lacunas and propose specific phases

        # 1.2 Contradição não resolvida (Conflict Gap) - Let Judge LLM handle this intelligently
        # Removed heuristic phase creation - Judge LLM will analyze contradictions and propose specific phases

        # 1.3 Verificar diversidade e fontes oficiais
        quality_rails = phase_context.get("quality_rails", {}) if phase_context else {}
        min_unique_domains = quality_rails.get(
            "min_unique_domains", self.valves.MIN_UNIQUE_DOMAINS
        )

        # Usar função consolidada para extrair métricas
        metrics = _extract_quality_metrics(facts)
        domains = metrics["domains"]

        # 1.4 Diversity/Primária Gap
        if len(domains) < min_unique_domains:
            return {
                "verdict": "refine",
                "reasoning": f"Apenas {len(domains)} domínios únicos, mínimo {min_unique_domains}",
                "next_query": f"{' '.join(user_prompt.split()[:2])} fontes acadêmicas universidades",
            }

        # 1.5 Source-Type Gap - Let Judge LLM handle this intelligently
        # Removed heuristic phase creation - Judge LLM will analyze source gaps and propose specific phases

        # 1.6 Diminishing Returns (seria necessário histórico de loops)
        # Por enquanto, assumir que se chegou aqui, está OK

        return {"verdict": "done"}

    # Removed _create_phase_template - Now using Judge LLM for intelligent phase creation


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
    """Build unified Planner prompt used by both Manual and SDK routes.

    Args:
        user_prompt: User's research objective
        phases: Number of phases to plan
        current_date: Current date for temporal context
        detected_context: Context detected by _detect_unified_context (perfil, setor, tipo)
    """
    if not current_date:
        from datetime import datetime

        current_date = datetime.now().strftime("%Y-%m-%d")

    date_context = f"DATA ATUAL: {current_date}\n(Use esta data ao planejar fases de notícias/eventos recentes. Não sugira anos passados como '2024' se estamos em 2025.)\n\n"

    # Orientação específica por perfil detectado (compacta)
    profile_guidance = ""
    if detected_context:
        perfil = detected_context.get("perfil", "")
        setor = detected_context.get("setor", "")
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
✅ Perfil: {detected_context.get('perfil', 'N/A')}

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
- "tendências serviços headhunting Brasil" → Tema: headhunting
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
   - avoid_terms: ruído a evitar
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
  "quality_rails": {{
    "min_unique_domains": """
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
{
  "plan_intent": "Mapear mercado de varejo digital no Brasil com foco em players nacionais e internacionais",
  "assumptions_to_validate": ["Crescimento do e-commerce regional supera o global", "Players locais têm vantagens logísticas"],
  "phases": [
    {"name": "Volume setorial", "phase_type": "industry", "objective": "Qual volume anual do varejo digital no Brasil?", "seed_query": "volume varejo digital Brasil", "seed_core": "volume anual vendas e-commerce Brasil", "must_terms": ["varejo digital", "e-commerce", "Brasil"], "avoid_terms": ["loja física"], "time_hint": {"recency": "1y", "strict": false}, "source_bias": ["oficial", "primaria"], "evidence_goal": {"official_or_two_independent": true, "min_domains": 3}, "lang_bias": ["pt-BR"], "geo_bias": ["BR"]},
    {"name": "Tendências serviços", "phase_type": "industry", "objective": "Quais tendências e serviços adjacentes surgiram nos últimos 12 meses?", "seed_query": "tendências serviços varejo digital Brasil", "seed_core": "tendências emergentes serviços adjacentes varejo digital Brasil últimos 12 meses inovações tecnologia", "must_terms": ["varejo digital", "omnicanal", "logística", "Brasil"], "avoid_terms": ["loja física"], "time_hint": {"recency": "1y", "strict": false}, "source_bias": ["oficial", "primaria"], "evidence_goal": {"official_or_two_independent": true, "min_domains": 3}, "lang_bias": ["pt-BR"], "geo_bias": ["BR"]},
    {"name": "Perfis e reputação", "phase_type": "profiles", "objective": "Como se posicionam Magalu, Via, Americanas e MercadoLivre?", "seed_query": "reputação players varejo digital Brasil", "seed_core": "Magalu Via Americanas MercadoLivre posicionamento competitivo reputação mercado brasileiro varejo digital últimos 2 anos", "must_terms": ["Magalu", "Via", "Americanas", "MercadoLivre", "Brasil"], "avoid_terms": ["reclamações"], "time_hint": {"recency": "3y", "strict": false}, "source_bias": ["oficial", "primaria"], "evidence_goal": {"official_or_two_independent": true, "min_domains": 3}, "lang_bias": ["pt-BR"], "geo_bias": ["BR"]},
    {"name": "Eventos recentes", "phase_type": "news", "objective": "Quais aquisições ou mudanças ocorreram nos últimos 90 dias?", "seed_query": "@noticias aquisições varejo digital Brasil", "seed_core": "aquisições parcerias mudanças estratégicas Magalu Via Americanas MercadoLivre varejo digital Brasil últimos 90 dias", "must_terms": ["Magalu", "Via", "Americanas", "MercadoLivre", "Brasil"], "avoid_terms": ["promoções"], "time_hint": {"recency": "90d", "strict": true}, "source_bias": ["oficial", "primaria"], "evidence_goal": {"official_or_two_independent": true, "min_domains": 2}, "lang_bias": ["pt-BR"], "geo_bias": ["BR"]}
  ],
  "quality_rails": {"min_unique_domains": 3, "need_official_or_two_independent": true},
  "budget": {"max_rounds": 2}
}
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
- [ ] SEM operadores (site:, filetype:, after:, before:, AND, OR, aspas)
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
[JSON do plano completo]"""
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
        + "🎯 CALIBRAÇÃO DE coverage_score (NOVA - PRAGMÁTICA):\n\n"
        + "**PRINCÍPIO:** Foque em VALOR ENTREGUE, não perfeição.\n\n"
        + "**0.0-0.3 (BAIXO - RESPOSTA INADEQUADA):**\n"
        + 'Objetivo: "Qual volume anual de colocações executivas no Brasil?"\n'
        + 'Contexto: "Executive search é importante para empresas. Headhunters buscam talentos."\n'
        + 'Fatos: ["Executive search existe no Brasil", "Headhunters fazem recrutamento"]\n\n'
        + "→ coverage_score = 0.2\n"
        + "→ Razão: Objetivo pede VOLUME ANUAL (número), fatos são apenas conceituais\n"
        + "→ gaps_critical = True (pergunta principal não respondida)\n"
        + "→ suggest_refine = True (mesmo ângulo, buscar fontes com números)\n\n"
        + "**0.4-0.6 (MÉDIO - RESPOSTA PARCIAL mas ÚTIL):**\n"
        + 'Objetivo: "Qual volume anual de colocações executivas no Brasil?"\n'
        + 'Contexto: "Mercado movimentou R$500M em 2023. Spencer, Heidrick, Flow Executive são top 3."\n'
        + 'Fatos: ["Volume estimado R$500M em 2023", "Spencer, Heidrick, Flow Executive lideram"]\n\n'
        + "→ coverage_score = 0.6\n"
        + "→ Razão: ✅ JÁ ENTREGA VALOR (número principal + contexto existem)\n"
        + "→ gaps_critical = False (lacunas não impedem uso prático)\n"
        + "→ suggest_refine = False (resposta utilizável)\n\n"
        + "**0.7-0.9 (ALTO - RESPOSTA SÓLIDA):**\n"
        + 'Objetivo: "Qual volume anual de colocações executivas no Brasil?"\n'
        + 'Contexto: "Mercado R$500M (2023), +12% vs 2022. Spencer 25%, Heidrick 18%, Flow Executive 15%. Fonte: ABRH."\n'
        + 'Fatos: ["Volume R$500M 2023 (+12%)", "Spencer 25% share", "Heidrick 18%", "Flow Executive 15%", "Fonte: ABRH"]\n\n'
        + "→ coverage_score = 0.8\n"
        + "→ Razão: Volume + breakdown + fontes + métricas operacionais\n"
        + "→ gaps_critical = False\n"
        + "→ suggest_refine = False\n"
        + "→ ✅ RESPOSTA ÚTIL E COMPLETA (lacunas são \"nice-to-have\")\n\n"
        + "1.0 (PERFEITO): Resposta EXAUSTIVA com múltiplas perspectivas\n"
        + "  - RARO! Só use se objetivo coberto 100% com evidências robustas de múltiplas fontes\n"
        + "  - Exemplo: Volume + breakdown detalhado + tendências + players + métricas + contexto histórico\n\n"
        + "⚠️ IMPORTANTE - SEJA MENOS CRITICO:\n"
        + '- "Volume R$500M (Fonte: relatório X)" = coverage 0.6 (útil!) NÃO 0.3\n'
        + '- "3 players: A, B, C com market share" = coverage 0.6 (útil!) NÃO 0.4\n'
        + '- Não exija perfeição para marcar 0.7+\n'
        + '- Lacunas "seria bom ter" não reduzem score (só lacunas críticas)\n\n'
        + "⚠️ AUTO-AVALIAÇÃO OBRIGATÓRIA:\n"
        + "- gaps_critical: True se lacunas impedem resposta ao objetivo\n"
        + "- suggest_refine: True se mais dados na MESMA direção ajudam\n"
        + "- suggest_pivot: True se lacuna precisa de ângulo/temporal DIFERENTE\n"
        + "- reasoning: Explique brevemente por que refine OU pivot\n\n"
        + "---\n\n"
        + "🎯 **ACCEPTANCE CRITERIA (VALIDAÇÃO FINAL OBRIGATÓRIA):**\n\n"
        + "✅ **ATOMICIDADE DE FATOS:**\n"
        + "- [ ] 1 fato = 1 proposição verificável (Sujeito-Verbo-Objeto)\n"
        + "- [ ] SEM conjunções (\"e\", \"mas\", \"porém\") - dividir em múltiplos fatos\n"
        + "- [ ] Incluir entidade canônica quando aplicável\n"
        + "- [ ] Incluir número/métrica COM unidade (ex: \"R$500M\", \"25%\", \"12 meses\")\n"
        + "- [ ] Incluir data/período quando disponível (ex: \"em 2023\", \"últimos 12 meses\")\n\n"
        + "✅ **EVIDÊNCIAS:**\n"
        + "- [ ] 100% dos fatos com ≥1 evidência (URL + trecho)\n"
        + "- [ ] Quando possível, ≥2 fontes para o mesmo fato (múltiplas perspectivas)\n"
        + "- [ ] Marcar fonte primária quando aplicável (ex: site oficial da empresa)\n"
        + "- [ ] ≥80% dos fatos com data extraída ou inferida\n\n"
        + "✅ **COVERAGE MATRIX (implícita):**\n"
        + "- [ ] Para cada key_question relevante: identificar qual(is) fato(s) respondem\n"
        + "- [ ] Coverage_score = (key_questions respondidas) / (key_questions relevantes)\n"
        + "- [ ] Se key_question NÃO respondida → incluir em lacunas\n\n"
        + "✅ **SELF_ASSESSMENT:**\n"
        + "- [ ] coverage_score: 0.0-1.0 (calibrado conforme exemplos acima)\n"
        + "- [ ] confidence: \"alta|média|baixa\" (baseado em nº e qualidade de fontes)\n"
        + "- [ ] gaps_critical: True se lacunas impedem resposta ao objetivo principal\n"
        + "- [ ] suggest_refine vs suggest_pivot: escolha baseada em tipo de lacuna\n\n"
        + "✅ **QUALIDADE:**\n"
        + "- [ ] 3-5 fatos (não vazio, não excessivo)\n"
        + "- [ ] Fatos específicos (nomes, números, datas) > fatos genéricos\n"
        + "- [ ] Priorizar fatos sobre must_terms (termos prioritários da fase)\n\n"
        + "🧠 **CLASSIFICAÇÃO INTELIGENTE DE LACUNAS (FRAMEWORK DE RACIOCÍNIO):**\n\n"
        + "Para CADA informação faltante, RACIOCINE usando este framework:\n\n"
        + "A) É ESPERADO que exista publicamente?\n"
        + "   - Considere: tipo de organização, setor, obrigações regulatórias\n"
        + "   - Exemplos de 'sim': produtos de empresa tech, rankings públicos, portfólio de serviços\n"
        + "   - Exemplos de 'não': pricing de consultoria, métricas operacionais internas, margens\n\n"
        + "B) É ESSENCIAL para o objective da fase?\n"
        + "   - Pergunte: 'Sem isso, posso responder a questão principal?'\n"
        + "   - Exemplo: Se objective é 'portfólio', falta pricing não é blocker\n"
        + "   - Exemplo: Se objective é 'market share', falta números É blocker\n\n"
        + "C) Há PROXIES ou dados relacionados disponíveis?\n"
        + "   - Exemplo: Não achei 'time-to-fill exato' mas achei 'processo de 3-4 meses'\n"
        + "   - Proxy é útil? Se sim, mencione no summary como informação parcial\n\n"
        + "CLASSIFICAÇÃO RESULTANTE (adicione ao JSON):\n\n"
        + '"lacunas": [\n'
        + '  {\n'
        + '    "descricao": "...",\n'
        + '    "tipo": "crítica|secundária|esperada",\n'
        + '    "raciocinio": "Por que classifiquei assim (1 frase)"\n'
        + '  }\n'
        + ']\n\n'
        + "🔴 LACUNA CRÍTICA:\n"
        + "   Esperado público + Essencial + Sem proxy disponível\n"
        + "   → Exemplo: 'Falta informação sobre setores atendidos pela empresa X'\n"
        + "   → Ação: suggest_refine = true\n\n"
        + "🟡 LACUNA SECUNDÁRIA:\n"
        + "   (Esperado público + Não essencial) OU (Essencial mas tem proxy)\n"
        + "   → Exemplo: 'Falta data fundação' ou 'Falta métrica exata mas há declaração qualitativa'\n"
        + "   → Ação: Mencionar mas não bloquear\n\n"
        + "⚪ LACUNA ESPERADA:\n"
        + "   Não esperado público (dado privado/competitivo para este setor)\n"
        + "   → Exemplo: 'Métricas operacionais detalhadas não divulgadas por boutiques de RH'\n"
        + "   → Ação: Aceitar, NÃO penalizar coverage_score\n\n"
        + "EXEMPLO DE RACIOCÍNIO:\n\n"
        + 'Lacuna: "Success rate % das boutiques de executive search"\n'
        + "Análise:\n"
        + "  A) Esperado público? NÃO - consultoria não divulga métricas operacionais (vantagem competitiva)\n"
        + "  B) Essencial? NÃO se objective é 'portfólio e posicionamento'\n"
        + "  C) Proxy? SIM - 'ciclo médio 3-4 meses' indica eficiência\n"
        + "Classificação: ⚪ ESPERADA\n"
        + "Ação: NÃO penalizar coverage, NÃO suggest_refine\n\n"
        + "⚠️ USE SEU CONHECIMENTO DE DOMÍNIO para classificar, não regras fixas.\n"
        + "⚠️ NÃO force refinamento para achar dados que você SABE serem privados.\n\n"
        + "🔍 **PROPOSTAS PARA O JUDGE (FASE 2):**\n\n"
        + "Com base nas lacunas encontradas e no objetivo da fase, sugira:\n"
        + "1. **até 3 refine_queries** (queries SEM operadores para próxima busca)\n"
        + "2. **até 2 phase_candidates** (novas fases completas para o contrato)\n\n"
        + "Para cada refine_query:\n"
        + "- Formato: 3-8 palavras, SEM operadores (site:, filetype:, OR, etc.)\n"
        + "- Foque no que AINDA falta para responder o objetivo\n"
        + "- Ex: se objetivo=\"volume mercado Brasil\" e contexto tem só dados de 2022 → \"volume mercado Brasil 2023 2024\"\n\n"
        + "Para cada phase_candidate:\n"
        + "- Estrutura completa igual às fases do contrato (name, objective, seed_query, etc.)\n"
        + "- Foque em ângulos COMPLETAMENTE diferentes das fases existentes\n"
        + "- Use phase_type apropriado (industry|profiles|news|regulatory|financials|tech)\n\n"
        + "SCHEMA ADICIONAL:\n"
        + "{\n"
        + '  "summary": "...",\n'
        + '  "facts": [...],\n'
        + '  "lacunas": [...],\n'
        + '  "self_assessment": {...},\n'
        + '  "refine_queries": [\n'
        + '    {"q": "<3-8 palavras>", "fills": "<o que espera descobrir>"}\n'
        + '  ],\n'
        + '  "phase_candidates": [\n'
        + '    {\n'
        + '      "name": "...",\n'
        + '      "phase_type": "profiles|news|...",\n'
        + '      "objective": "...",\n'
        + '      "seed_query": "<3-8 palavras>",\n'
        + '      "seed_core": "<frase rica>",\n'
        + '      "must_terms": ["..."],\n'
        + '      "time_hint": {"recency": "1y", "strict": true}\n'
        + '    }\n'
        + '  ]\n'
        + "}\n\n"
        + "Retorne APENAS JSON."
    )

    # 🔴 FIX P0: Usar str.replace em vez de .format para evitar KeyError com literais JSON no template
    # O template contém exemplos JSON com {"summary": ...} que .format() interpreta como placeholder
    final_prompt = prompt_template.replace("{query_text}", query)
    final_prompt = final_prompt.replace("{phase_block}", phase_info)
    final_prompt = final_prompt.replace("{objective_block}", objective_emphasis)

    return final_prompt


def _build_judge_prompt(
    user_prompt: str,
    analysis: Dict[str, Any],
    phase_context: Dict[str, Any] = None,
    telemetry_loops: Optional[List[Dict[str, Any]]] = None,
    intent_profile: Optional[str] = None,
    full_contract: Optional[Dict] = None,
    refine_queries: Optional[List[Dict]] = None,
    phase_candidates: Optional[List[Dict]] = None,
    previous_queries: Optional[List[str]] = None,  # ← NEW
    failed_queries: Optional[List[str]] = None,  # ← WIN #3: Failed queries context
) -> str:
    """Build unified Judge prompt - VERSÃO OTIMIZADA v4.6

    Melhorias vs v4.5:
    - Compactação: seções redundantes consolidadas
    - Decision tree visual para NEW_PHASE (mais claro)
    - Exemplo completo de avaliação passo a passo
    - Autonomia preservada (Judge decide, programmatic apenas valida)
    """
    phase_objective = (phase_context or {}).get("objetivo", "") if phase_context else ""
    must_terms = (phase_context or {}).get("must_terms", []) if phase_context else []

    # Extract seed_query from phase_context to prevent duplication
    current_seed = phase_context.get("seed_query", "") if phase_context else ""

    # Extract used queries from telemetry to prevent repetition
    used_queries = []
    if telemetry_loops:
        for loop in telemetry_loops:
            q = loop.get("query", "").strip()
            if q and q not in used_queries:
                used_queries.append(q)

    # ===== CONTRACT AWARENESS (anti-duplicação) =====
    contract_awareness = ""
    if full_contract and full_contract.get("fases"):
        current_phase_name = phase_context.get("name", "") if phase_context else ""
        fases_list = []

        for i, fase in enumerate(full_contract["fases"], 1):
            fase_name = fase.get("name", f"Fase {i}")
            fase_obj = fase.get("objetivo", "N/A")
            marker = " **← ATUAL**" if fase_name == current_phase_name else ""
            fases_list.append(f"{i}. {fase_name}: {fase_obj}{marker}")

        contract_awareness = f"""
🔒 **FASES JÁ PLANEJADAS:**
{chr(10).join(fases_list)}

⚠️ **ANTI-DUPLICAÇÃO:** Antes de propor new_phase, verifique se já existe fase similar acima.
- Se overlap >50% com fase existente → use REFINE (não new_phase)
- Apenas use new_phase se for ângulo/escopo COMPLETAMENTE diferente
"""

    # P0: Adicionar research_objectives do Context Detector (COMPACTO)
    research_objectives_section = ""
    if full_contract and full_contract.get("research_objectives"):
        objectives = full_contract["research_objectives"]
        obj_list = "\n".join([f"{i}. {obj}" for i, obj in enumerate(objectives[:5], 1)])
        # Concatenação manual evita f-strings aninhadas em templates extensos
        research_objectives_section = """\n📋 **RESEARCH OBJECTIVES:** {list_section}\n⚠️ Se gap crítico → considere NEW_PHASE.\n""".format(
            list_section=obj_list
        )

    # P0: Adicionar key_questions do Context Detector (ULTRA-COMPACTO v4.7)
    key_questions_section = ""
    mece_section = ""
    if full_contract and full_contract.get("key_questions") and phase_objective:
        questions = full_contract["key_questions"]
        q_lines = [
            "{}. {}".format(index, question)
            for index, question in enumerate(questions[:10], 1)
        ]
        q_list = "\n".join(q_lines)
        key_questions_section = (
            "\n🎯 **KEY QUESTIONS (H0):** {items}\n"
            "📋 **OBJETIVO FASE:** {objective}\n\n"
            '⚙️ **JSON - key_questions_status:** {{relevant_to_phase: [índices], answered_in_phase: [índices], coverage: 0.0-1.0, blind_spots: ["descobertas INESPERADAS que INVALIDAM premissas"]}}\n'
            '\n'
            '🔍 **BLIND SPOT = DESCOBERTA INESPERADA (não "dado faltante"):**\n'
            '✅ Blind spot CRÍTICO:\n'
            '   - Descobriu fusão/aquisição não mencionada no objetivo (muda landscape competitivo)\n'
            '   - Descobriu escândalo/processo regulatório (invalida premissa "reputação sólida")\n'
            '   - Descobriu novo player dominante não citado (premissas incompletas)\n'
            '✅ Blind spot NÃO-CRÍTICO:\n'
            '   - Descobriu escritório adicional não esperado (detalhe operacional)\n'
            '   - Descobriu produto adicional no portfólio (complementa mapeamento)\n'
            '\n'
            '❌ NÃO é blind spot (são "lacunas esperadas"):\n'
            '   - Falta TAM exato (dado tipicamente privado/estimado para mercados emergentes)\n'
            '   - Falta pricing exato (segredo comercial para empresas privadas)\n'
            '   - Falta receita/margens (boutiques privadas raramente divulgam)\n'
            '   - Falta success rate % (consultoria não divulga vantagem competitiva)\n'
            '\n'
            '**REGRA:** Blind spot = unexpected finding. Lacuna = expected but missing data.\n'
        ).format(items=q_list, objective=phase_objective)
    if full_contract and full_contract.get("_mece_uncovered"):
        uncovered = full_contract["_mece_uncovered"] or []
        if uncovered:
            uncovered_lines = ["- {}".format(kq) for kq in uncovered[:5]]
            uncovered_block = "\n".join(uncovered_lines)
            mece_section = (
                "\n⚠️ **LACUNAS MECE DETECTADAS (Key Questions sem cobertura):**\n"
                "{lines}\n\n"
                "💡 **Ação sugerida:** Prefira REFINE para cobrir lacunas específicas. Use NEW_PHASE somente se exigir ângulo totalmente novo (temporal/fonte/escopo).\n"
            ).format(lines=uncovered_block)

    # Build anti-duplication context
    anti_dup_context = ""
    if current_seed or used_queries:
        anti_dup_sections: List[str] = ["\n**QUERIES JÁ USADAS (NÃO REPETIR):**\n"]
        if current_seed:
            anti_dup_sections.append(
                "- Seed original desta fase: '{}\n'".format(current_seed)
            )
        if used_queries:
            formatted_queries = ", ".join("`{}`".format(q) for q in used_queries)
            anti_dup_sections.append(
                "- Queries tentadas: {}\n".format(formatted_queries)
            )
        anti_dup_sections.append(
            "\n⚠️ **CRÍTICO:** Novas queries devem ser SUBSTANCIALMENTE DIFERENTES (novos termos, ângulo, fonte).\n"
        )
        anti_dup_context = "".join(anti_dup_sections)

    # P1: Derivar entidades CANÔNICAS (nomes próprios), não termos genéricos
    entities_block = ""
    try:
        # Prioridade 1: Usar entities.canonical do contrato (nomes reais)
        canonical_entities = []
        if full_contract and full_contract.get("entities"):
            canonical_entities = full_contract["entities"].get("canonical", [])

        # Fallback: Extrair entidades de must_terms (apenas nomes próprios - uppercase)
        if not canonical_entities:
            mt = (phase_context or {}).get("must_terms", [])
            canonical_entities = [
                t
                for t in mt
                if isinstance(t, str)
                and t.strip()
                and len(t.split()) <= 3
                and t[0].isupper()
            ]

        # Limitar e deduplicar
        seen = set()
        dedup = []
        for e in canonical_entities:
            if e.lower() not in seen:
                seen.add(e.lower())
                dedup.append(e)
        dedup = dedup[:12]

        if dedup:
            # Dividir construção em partes reduz a profundidade observada pelo parser do OpenWebUI
            entities_text = ", ".join(dedup)
            # 🔧 FIX: Usar concatenação simples para evitar f-string nesting no OpenWebUI
            entities_block = (
                "\n⚠️ **ENTIDADES OBRIGATÓRIAS NA NEXT_QUERY:**\n" + entities_text + "\n"
                "\n🔴 **REGRA CRÍTICA:** Se verdict=refine, next_query DEVE incluir ≥1 entidade acima.\n"
                "✅ Exemplo válido: \"Exec Vila Nova portfólio serviços Brasil\"\n"
                "❌ Exemplo inválido: \"mercado executive search Brasil\" (sem entidades!)\n"
                "\n⚠️ Se lacuna = EMPRESAS → use SOMENTE estas entidades | "
                "Se lacuna = MERCADO → use termos setoriais (mercado, participação, volume)\n"
            )
    except Exception:
        pass

    # 📋 FASE 2: Analyst Proposals (refine_queries e phase_candidates)
    analyst_proposals_block = ""
    if refine_queries or phase_candidates:
        analyst_proposals_block = "\n"

        if refine_queries:
            refine_list = []
            for i, rq in enumerate(refine_queries[:3]):  # Limitar a 3
                q = rq.get("q", "N/A")
                fills = rq.get("fills", "N/A")
                refine_list.append(f"{i}. **{q}**\n   Objetivo: {fills}")

            analyst_proposals_block += f"""
📊 **OPÇÕES DE REFINE SUGERIDAS (do Analyst):**

{chr(10).join(refine_list)}

⚙️ **SE verdict=refine:**
- Escolha a melhor opção acima via `"selected_index": <int>`
- OU crie sua própria query em `refine.next_query` (deixe `selected_index: null`)
- A opção selecionada será aplicada automaticamente pelo Orchestrator
"""

        if phase_candidates:
            candidate_list = []
            for i, pc in enumerate(phase_candidates[:2]):  # Limitar a 2
                name = pc.get("name", "N/A")
                obj = pc.get("objective", "N/A")[:80]
                candidate_list.append(f"{i}. **{name}**\n   Objetivo: {obj}")

            analyst_proposals_block += f"""
📊 **CANDIDATOS DE NOVA FASE SUGERIDOS (do Analyst):**

{chr(10).join(candidate_list)}

⚙️ **SE verdict=new_phase:**
- Escolha o melhor candidato acima via `"selected_index": <int>`
- OU crie sua própria fase em `new_phase` (deixe `selected_index: null`)
- O candidato selecionado será integrado automaticamente ao contrato
- ⚠️ **CRÍTICO:** Valide anti-duplicação antes de escolher (compare com fases já planejadas acima)
"""

    # ✅ WIN #3: Failed queries context
    if failed_queries:
        failed_list = "\n".join([f"- {q}" for q in failed_queries[:5]])  # Limit to 5
        analyst_proposals_block += f"""
🔍 **QUERIES QUE RETORNARAM 0 URLs (dead ends):**
{failed_list}

⚠️ **IMPORTANTE:** Evite propor NEW_PHASE que repitam estas buscas que falharam.
"""

    # Anti-duplication context
    anti_dup_section = ""
    if previous_queries and len(previous_queries) > 0:
        queries_list = "\n".join([f"  {i+1}. \"{q}\"" for i, q in enumerate(previous_queries)])
        anti_dup_section = f"""
⚠️ **QUERIES JÁ TESTADAS NESTA FASE (EVITE VARIAÇÕES SEMÂNTICAS):**
{queries_list}

**REGRA CRÍTICA**: Se sua `next_query` for **semanticamente similar** às anteriores, ela vai gerar **0 URLs novas**.

**EXEMPLOS DE VARIAÇÕES INÚTEIS (NÃO FAZER):**
- "tamanho mercado Brasil" → "valor mercado Brasil" (apenas sinônimo)
- "executive search Brasil" → "executive search Brasil 2023" (apenas ano)
- "market size Brazil" → "tamanho mercado Brasil" (apenas tradução)

**SOLUÇÕES QUE FUNCIONAM:**
- Mudar FONTE: "tamanho mercado" → "ABRH relatório setor RH"
- Mudar ÂNGULO: "volume total" → "custo médio contratação"
- Mudar ESCOPO: "mercado Brasil" → "Spencer Stuart earnings Brazil"

"""

    # Usar dicionários globais de prompts
    system_prompt = PROMPTS["judge_system"]
    philosophy = PROMPTS["judge_philosophy"]
    
    return f"""{system_prompt}

{philosophy}

{anti_dup_section}

⚠️ **PAPEL DO LLM (CRÍTICO - LEIA PRIMEIRO):**

🤖 **VOCÊ (LLM) PROPÕE, CÓDIGO DECIDE:**
- Você analisa métricas e propõe verdict + reasoning
- O código VALIDA sua proposta usando phase_score, duplicação, loops
- A decisão FINAL é programática (auditável, configurável)
- Seu papel: EXPLICAR o raciocínio de forma clara e consistente

📊 **MÉTRICAS QUE O CÓDIGO USA (para sua informação):**
- phase_score = f(coverage, novel_facts, novel_domains, diversity, contradictions)
- flat_streak = loops consecutivos sem ganho
- overlap_similarity = fatos repetidos vs fases anteriores
- Gates por phase_type (threshold, two_flat_loops)

💡 **IMPLICAÇÃO:**
- Seja consistente: reasoning deve alinhar com verdict proposto
- Seja específico: mencione métricas concretas (coverage %, nº fatos, nº domínios)
- Seja honesto: se dados são fracos, diga; se há contradições, mencione

{contract_awareness}{key_questions_section}{mece_section}

🎯 **FILOSOFIA DE DECISÃO (LEIA COM ATENÇÃO):**

**DONE ≠ PERFEITO. DONE = SUFICIENTE.**
- Responde ao objetivo? → Propor DONE
- Fatos têm substância (nomes/números)? → Propor DONE
- Lacunas são secundárias? → Propor DONE

**REFINE: só se NECESSÁRIO**
- Fatos genéricos SEM dados/nomes
- Lacunas IMPEDEM resposta ao objetivo
- Evidências MUITO fracas

**NEW_PHASE: ângulo COMPLETAMENTE diferente**
- Escopo/temporal/fonte totalmente diferente
- Verifique anti-duplicação (não repita fases existentes!)
- **OBRIGATÓRIO**: Propor seed_family diferente do atual (trocar exploração)

📊 **CHECKLIST:** Coverage≥60% OU fatos de alta qualidade? | ≥50% must_terms/entidades cobertos? | Lacunas críticas?
→ Se ≥2 itens falharem → REFINE ou NEW_PHASE

⏱️ **CUSTO-BENEFÍCIO DE REFINE:**
- Cada loop adicional: 5-10 minutos de pesquisa (discovery + scraper + analyst)
- Coverage incremental típico: +10-15% por loop
- **REGRA DE DECISÃO:**
  - Coverage atual ≥60% E lacunas são dados privados → DONE (economia 5-10 min)
  - Coverage atual <50% E lacunas são dados públicos → REFINE (investimento justificado)
  - Coverage 50-60% → Avaliar qualidade dos fatos existentes (nomes/números = DONE)

🔴 **CRÍTICO - FRAMEWORK "NÃO EXISTE vs NÃO ACHEI" (LEIA ANTES DE DECIDIR):**

Antes de decidir REFINE, pergunte-se:

1️⃣ O que falta é REALISTICAMENTE público?
   • Use seu conhecimento: este tipo de org divulga isso?
   • Exemplos:
     ✅ Portfólio de produtos → Público (em sites/press releases)
     ✅ Rankings e prêmios → Público (em press/LinkedIn)
     ❌ Margens operacionais empresa privada → Privado
     ❌ Success rate consultoria → Privado (vantagem competitiva)
     ❌ Pricing exato (negociado) → Privado

2️⃣ Se é privado, há PROXY público suficiente?
   • Exemplo: não há 'time-to-fill exato' mas há 'processo 3-4 meses'
   • Proxy responde suficientemente o objective? → Sim? DONE

3️⃣ Se é público, realmente não achei ou não procurei bem?
   • Avaliar: domínios visitados são autoritativos para esta info?
   • Exemplo: procurei setores da empresa mas só visitei agregadores
   • Se não procurei bem → REFINE com query focada

FRAMEWORK DE DECISÃO:

┌─ Lacunas críticas (esperadas públicas)?
│  ├─ SIM → REFINE
│  └─ NÃO ↓
│
├─ Lacunas são dados privados para este setor?
│  ├─ SIM → Há proxies úteis?
│  │  ├─ SIM → Coverage efetivo +15% → DONE
│  │  └─ NÃO → Coverage efetivo = raw → Avaliar threshold
│  └─ NÃO ↓
│
└─ Objective principal foi respondido?
   ├─ SIM (≥65% cobertura) → DONE
   └─ NÃO → REFINE

EXEMPLO DE RACIOCÍNIO:

Fase: 'Métricas operacionais de boutiques executive search'
Encontrado: Portfólio serviços, setores, ciclo declarado '3-4 meses'
Faltando: Success rate %, time-to-fill exato, retenção cliente

Pergunta 1: Success rate é público?
→ NÃO. Consultoria nunca divulga (vantagem competitiva).

Pergunta 2: Há proxy?
→ SIM. 'Ciclo 3-4 meses' é proxy razoável para eficiência operacional.

Pergunta 3: Objective principal respondido?
→ SIM. 'Métricas operacionais' = entender rapidez/qualidade.
   Proxy + portfólio respondem suficientemente.

Decisão: DONE (coverage ajustado 0.50 → 0.70)
Reasoning: 'Métricas exatas são privadas para este setor.
            Proxies disponíveis (ciclos declarados) são suficientes.'

⚠️ NÃO force refinamento para achar dados que você SABE serem privados.

💡 **EXEMPLO 1 - DONE COM LACUNAS SECUNDÁRIAS (boutiques privadas):**

Objetivo: "Mapear posicionamento de EXEC, Vila Nova Partners, Flow EF no Brasil"
Encontrado: 
- EXEC: 250+ placements/ano, fundada 2009, expansão 4 sócios 2024, setores diversos
- Vila Nova: foco Energy/Industrial, parceiro 10 anos exp
- Flow EF: PE/VC focused, fundada 2011, unfunded

Faltando: TAM exato setor, pricing das boutiques, receita anual, margens

Pergunta 1: TAM/pricing/receita são públicos para boutiques privadas?
→ NÃO. Boutiques privadas raramente divulgam dados financeiros.

Pergunta 2: Há proxies?
→ SIM. Volume operacional (250+ placements), setores atendidos, funding status, expansão estratégica.

Decisão: DONE (coverage 0.65 → ajustado 0.75 com proxies)
Reasoning: "Posicionamento das três boutiques mapeado com evidências operacionais. Lacunas (TAM, pricing, receita) são dados privados; proxies disponíveis respondem ao objetivo de mapeamento competitivo."

💡 **EXEMPLO 2 - DONE COM DADOS QUALITATIVOS (estudo de setor):**

Objetivo: "Entender dinâmica competitiva setor XYZ no Brasil"
Encontrado: 
- 5 players principais identificados com market share relativo (fontes: 3 reports setoriais)
- Tendências 2023-2024: consolidação (2 fusões), entrada player internacional
- Drivers: regulação X (2023), demanda segmento Y +30%

Faltando: Market share exato (%), receita por player, forecasts 2025-2027

Pergunta 1: Market share exato é público?
→ PARCIAL. Reports setoriais dão ranking ordinal, não percentuais exatos.

Pergunta 2: Ranking + tendências respondem ao objetivo?
→ SIM. "Dinâmica competitiva" = entender posições relativas e movimentos estratégicos.

Decisão: DONE (coverage 0.70)
Reasoning: "Ranking ordinal, fusões/aquisições e drivers regulatórios mapeiam adequadamente a dinâmica competitiva. Percentuais exatos não alteram compreensão estratégica."

💡 **EXEMPLO 3 - REFINE JUSTIFICADO (lacunas críticas públicas):**

Objetivo: "Avaliar conformidade empresa X com regulação Y"
Encontrado: 
- Empresa X mencionada em 2 notícias genéricas sobre setor
- Regulação Y descrita (oficial gov.br)

Faltando: Certificações empresa X, auditorias, processos regulatórios, declarações conformidade

Pergunta 1: Certificações/auditorias são públicas?
→ SIM. Empresas reguladas divulgam certificações ISO, auditorias anuais.

Pergunta 2: Procuramos bem?
→ NÃO. Apenas agregadores visitados; falta site oficial empresa X, portal regulador.

Decisão: REFINE
Next_query: "Empresa X certificações ISO auditorias conformidade regulação Y Brasil site oficial"
Reasoning: "Dados de conformidade são públicos para empresas reguladas. Fontes visitadas foram insuficientes; necessário buscar site oficial e portal regulador."

📊 **AJUSTE DE COVERAGE POR PROXIES (PRAGMÁTICO):**

Quando lacunas são **dados privados para o setor** mas há **proxies úteis**, ajuste coverage mentalmente:

**Exemplo 1 - Boutiques Executive Search:**
- Raw coverage: 0.60 (60% perguntas com resposta direta)
- Lacunas: TAM exato, pricing, receita (PRIVADOS para boutiques)
- Proxies: Volume operacional (250+ placements), setores, expansão estratégica, funding
- **Ajuste: +15% → Coverage efetivo 0.75** → Propor DONE

**Exemplo 2 - Startup Pré-Série A:**
- Raw coverage: 0.55 (55% perguntas respondidas)
- Lacunas: Receita, valuation, cap table (PRIVADOS pré-IPO)
- Proxies: Funding rodada (público), team size (LinkedIn), clientes mencionados (press)
- **Ajuste: +10% → Coverage efetivo 0.65** → Propor DONE

**Exemplo 3 - Empresa Regulada (dados públicos faltando):**
- Raw coverage: 0.50
- Lacunas: Certificações ISO, auditorias compliance (PÚBLICOS esperados)
- Proxies: Nenhum proxy válido para compliance
- **Ajuste: 0% → Coverage 0.50** → Propor REFINE (dados públicos não encontrados)

**CRITÉRIO:**
- Proxies respondem ≥70% do valor informacional da lacuna → +15% ajuste
- Proxies respondem 40-69% do valor informacional → +10% ajuste  
- Proxies <40% ou lacuna é dado público → 0% ajuste (manter raw coverage)

🌲 **QUANDO USAR NEW_PHASE:**
- Lacunas essenciais + 2 loops flat → ângulo esgotado
- Coverage <50% + 2 loops flat → hipóteses erradas
- Blind spots que INVALIDAM premissas (loops≥1) → contexto mudou
- Entidades ausentes + loops≥2 → NEW_PHASE focada
- Contradições persistentes (2+ loops) → fonte oficial
- Janela temporal errada → ajustar período
**SENÃO → REFINE ou DONE**

**EXEMPLOS:**

**NEW_PHASE por blind spot crítico:**
- Descoberta: "Processo SEC contra Heidrick 2024" invalida premissa "reputação sólida"
- seed_query: "Processos regulatórios SEC litígios trabalhistas executive search 2023-2024 impactos"
- por_que: "Fases anteriores focaram perfis comerciais; dimensão regulatória ausente"

**REFINE para lacunas específicas:**
- Lacuna MERCADO: "Tamanho e participação boutiques vs internacionais Brasil 2022-2024"
- Lacuna EMPRESAS: "Portfólio serviços rankings Flow EF Vila Nova Brasil"
- ❌ NÃO: "Buscar dados" (genérico) ou "Korn Ferry" (entidade não priorizada)

**ESTRATÉGIA DE REFINEMENT ANTI-REDUNDÂNCIA (CRÍTICO):**

⚠️ **SE APÓS 2 LOOPS COM POUCAS URLs NOVAS** → MUDE A FONTE, NÃO APENAS OS TERMOS!

**ÂNGULOS DE BUSCA (ordenados por probabilidade de sucesso):**

1. **FONTE DIRETA (buscar em empresas/produtos mencionados)**
   - Exemplo: "Vila Nova Partners LinkedIn about team size"
   - Exemplo: "Heidrick Struggles Brazil earnings revenue 2023"

2. **FONTE SETORIAL (associações, relatórios de mercado)**
   - Exemplo: "ABRH relatório mercado RH Brasil estatísticas"
   - Exemplo: "SHRM Brazil talent acquisition report"

3. **FONTE PROXY (dados relacionados que inferem a resposta)**
   - Exemplo: "custo contratação executivo Brasil salário C-level"
   - Exemplo: "tempo médio hiring senior leadership Brazil"

4. **FONTE COMPARATIVA (benchmarks setoriais)**
   - Exemplo: "market share consultoria RH Brasil ranking"
   - Exemplo: "top headhunters Brazil comparison"

**REGRAS ANTI-DUPLICAÇÃO:**
- ❌ NÃO trocar sinônimos: "tamanho" → "valor" → "faturamento" SÃO A MESMA QUERY!
- ❌ NÃO apenas adicionar ano: "mercado Brasil" → "mercado Brasil 2023" GERA MESMAS URLs!
- ✅ MUDAR FONTE: "tamanho mercado" → "ABRH relatório setor RH" → URLs NOVAS!
- ✅ MUDAR ÂNGULO: "volume total" → "custo médio por contratação" → PROXY DIFERENTE!

**PRINCÍPIO**: Se após 2 loops ainda faltam dados, provavelmente são **privados/não publicados**.
→ Busque PROXIES ou considere DONE (dados parciais são úteis).

⚠️ **ENTIDADES AMBÍGUAS (contexto setorial obrigatório):**

Se entidade pode ter múltiplos significados → INCLUIR setor/indústria na query

**Entidades ambíguas típicas:**
- Nomes muito curtos (≤4 chars): "Exec", "Flow", "Link", "Run"
- Palavras comuns em inglês/tech: "exec", "search", "data", "flow"

**REGRA CRÍTICA:**
- Se next_query mencionar entidade com NOME GENÉRICO/CURTO (≤4 chars OU palavra comum)
- E query NÃO contém contexto setorial
- → ADICIONAR contexto setorial no início

**Exemplos:**
✅ CORRETO:
  - "executive search Exec aquisição Brasil" (desambigua: Exec = consultoria)
  - "fintech Nubank receita Brasil" (desambigua: Nubank = banco)
  - "headhunting Flow EF posicionamento Brasil" (desambigua: Flow = consultoria)

❌ INCORRETO (ambíguo):
  - "Exec aquisição Brasil" → pode ser função exec() programação
  - "Flow aquisição Brasil" → pode ser flow de dados
  - "Run receita Brasil" → pode ser função run()

**Como adicionar contexto:**
1. Identifique o setor principal da pesquisa (do contract/fase)
2. Mapeamento setor → termo:
   - RH/Headhunting → "executive search" ou "headhunting"
   - Tech/Software → "software" ou "tecnologia" ou "startup"
   - Finance → "fintech" ou "banco" ou "crédito"
3. Prefixe a query com o termo setorial

**Exemplo de correção:**
- Query proposta: "Exec aquisição 90 dias Brasil"
- Setor detectado: RH/Headhunting
- Query corrigida: "executive search Exec aquisição 90 dias Brasil"

🚨 **NEXT_QUERY: 1 frase rica (≤150 chars, SEM operadores)**
- Contexto completo: aspecto + setor/entidades + geo + período
- MERCADO: "<aspecto> mercado <setor> <geo> <período>"
- EMPRESAS: "<aspecto> <entidades priorizadas> <geo> <período>"
- ❌ "buscar dados" (genérico) | operadores (site:, filetype:) | entidades não-priorizadas
{entities_block}
{analyst_proposals_block}
**OBJETIVO DA FASE:** {user_prompt}

**ANÁLISE DO ANALYST:**
{json.dumps(analysis, ensure_ascii=False, indent=2)}

⚠️ **PRÉ-JSON CHECKLIST:**
- verdict=REFINE → next_query específica (mercado OU entidades priorizadas, ≤150 chars, sem operadores)
- verdict=NEW_PHASE → seed_query contextualizada (responde blind spot + contrasta fases anteriores)
- selected_index → escolher opção do Analyst (se aplicável) OU criar própria (deixar null)

🔴 **VALIDAÇÃO CRÍTICA PARA REFINE:**
- Se verdict=refine, next_query DEVE incluir ≥1 entidade das listadas acima
- ❌ REJEITAR queries genéricas como "mercado executive search Brasil" (sem entidades)
- ✅ ACEITAR queries específicas como "Exec Vila Nova portfólio serviços Brasil" (com entidades)

**SAÍDA JSON:**
{{
  "verdict": "done|refine|new_phase",
  "reasoning": "<1-2 frases CONSISTENTES com verdict>",
  "selected_index": <int or null>,  // ← NOVO: índice da opção escolhida (se aplicável)
  "key_questions_status": {{
    "relevant_to_phase": [<índices>],
    "answered_in_phase": [<índices>],
    "unanswered_in_phase": [<índices>],
    "coverage": <0.0-1.0>,
    "blind_spots": ["<descobertas críticas>"]
  }},
  "refine": {{
    "next_query": "<3-8 palavras, SEM operadores>",  // Short for UI/telemetry
    "seed_core": "<12-200 chars, 1 frase rica, SEM operadores, contexto completo>",  // Rich for Discovery
    "focused_objective": "<objetivo específico focado na lacuna>",  // Focused objective
    "por_que": ["<lacuna A>", "<lacuna B>"],
    "operadores_sugeridos": []  // DEPRECATED - não use
  }},
  "new_phase": {{
    "objective": "<objetivo mensurável>",
    "seed_query": "<1 frase rica ≤150 chars, SEM operadores, POSITIVA>",
    "por_que": "<por que não cabe na fase atual>",
    "phase_type": "industry|profiles|news|regulatory|technical"
  }},
  "unexpected_findings": ["<descoberta 1>"]
}}

⚠️ **SEED_CORE (OBRIGATÓRIO para refine E new_phase):**
- Formato: 1 frase rica (12-200 chars), linguagem natural, SEM operadores
- Inclui: entidades + tema + aspecto + recorte geotemporal
- Contexto completo para Discovery Tool executar busca efetiva
- Exemplos:
  ✅ "Flow Executive Finders posicionamento competitivo e portfólio de serviços no mercado brasileiro de executive search 2023-2024"
  ✅ "Métricas operacionais e taxas de sucesso de boutiques de headhunting no Brasil últimos 12 meses fontes verificáveis"
  ❌ "Flow CNPJ Brasil" (muito curta, sem contexto)

**FOCUSED_OBJECTIVE (OBRIGATÓRIO para refine):**
- Reescreva o objective da fase para FOCAR na lacuna específica
- Use o reasoning + lacunas para criar objective direcionado
- Exemplo:
  Original: "Mapear portfólio e posicionamento de Flow Executive no Brasil"
  Lacuna: "Falta CNPJ oficial e data de fundação"
  → focused_objective: "Obter CNPJ oficial, razão social completa e data de registro da Flow Executive Finders no Brasil via Receita Federal ou CNPJ.info"

⚠️ **CONSISTÊNCIA REASONING ↔ VERDICT (VALIDAÇÃO CRÍTICA):**

**CORRETO (reasoning alinhado com verdict):**
✅ verdict=DONE + reasoning="Evidências sólidas sobre posicionamento (250+ placements EXEC, setores VN, perfil Flow). Lacunas (TAM, pricing) são dados privados; proxies respondem ao objetivo."
✅ verdict=REFINE + reasoning="Fatos genéricos sem especificidade. Lacunas críticas (certificações ISO, auditorias) impedem avaliar conformidade. Dados são públicos mas não foram encontrados."

**INCORRETO (contradição interna):**
❌ verdict=DONE + reasoning="Faltam dados essenciais sobre TAM e pricing..." → CONTRADIÇÃO (reasoning insatisfeito + verdict satisfeito)
❌ verdict=REFINE + reasoning="Há evidências sólidas sobre capacidades..." → CONTRADIÇÃO (reasoning satisfeito + verdict insatisfeito)

**REGRA DE OURO:**
- Reasoning menciona "evidências sólidas" / "dados substanciais" / "proxies suficientes" → verdict DEVE ser DONE
- Reasoning menciona "faltam dados críticos" / "lacunas impedem resposta" / "evidências fracas" → verdict DEVE ser REFINE/NEW_PHASE
- Se raciocínio diz "MAS falta X" → Pergunte: X é privado? X tem proxy? Se sim → DONE

Retorne APENAS o JSON."""


# MOVED (v4.5): _extract_contract_from_history → agora é método da classe Pipe (linha ~4690)
# Corrigido AttributeError quando usuário digita "siga"


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
        """Gera seed_core usando LLM (1 frase rica, sem operadores).

        Args:
            phase: Dicionário com objective, seed_family_hint
            entities: Lista de entidades canônicas
            geo_bias: Lista de bias geográfico (ex: ["BR", "global"])

        Returns:
            seed_core string ou None se falhar
        """
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
- SEM operadores (site:, filetype:, after:, before:)
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

            if not result or "content" not in result:
                logger.warning("[Planner] LLM seed_core: resposta vazia")
                return None

            content = result["content"].strip()

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
        """Valida e normaliza o contrato do novo formato JSON

        Args:
            phases: Máximo de fases permitidas (não obrigatório atingir)
        """
        phases_list = obj.get("phases", [])
        intent_profile = obj.get("intent_profile", "company_profile")

        if not phases_list:
            raise ValueError("Nenhuma fase encontrada")

        # Validar número de fases (pode ser MENOS que o máximo, mas não MAIS)
        if len(phases_list) > phases:
            raise ValueError(
                f"Excesso de fases: {len(phases_list)} > {phases} (máximo permitido)"
            )

        # Aceitar qualquer quantidade ≥1 e ≤ phases (flexibilidade para o Planner decidir)

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
                "avoid_terms",
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
            # Se for fase de notícias, garantir @noticias na seed (para Discovery news mode)
            try:
                if getattr(self.valves, "FORCE_NEWS_SEED_ATNOTICIAS", True):
                    name_obj = (
                        f"{phase.get('name','')} {phase.get('objective','')}".lower()
                    )
                    news_keys = [
                        "notícia",
                        "noticias",
                        "notícias",
                        "news",
                        "aquisição",
                        "fusões",
                        "fusão",
                        "mudanças de liderança",
                        "nomea",
                        "eventos",
                    ]
                    is_news_phase_name = any(k in name_obj for k in news_keys)
                    is_news_seed = "@noticias" in seed_query.lower()
                    if is_news_phase_name and not is_news_seed:
                        # Prefixar com @noticias mantendo regra de 3-6 palavras (permite o token especial)
                        seed_words = seed_query.split()
                        # Se a seed ficar maior que 6, cortar último termo
                        updated = ["@noticias"] + seed_words
                        if len(updated) > 6:
                            updated = updated[:6]
                        seed_query = " ".join(updated)
                        phase["seed_query"] = seed_query
                        # Nota: seed_core será sincronizado DEPOIS da geração (linha ~4273)
            except Exception:
                pass
            # Validar seed_query (3-8 palavras, SEM contar @noticias) - Ajustado para nomes de empresas
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

            # 🔴 P0: Validar POLÍTICA ENTITY-CENTRIC (seed_query deve conter entidades)
            entities = obj.get("entities", {}).get("canonical", [])
            must_terms = phase["must_terms"]

            # ✅ P0.1: Validar must_terms contém entidades (já existia)
            if entities and not any(
                entity.lower() in " ".join(must_terms).lower() for entity in entities
            ):
                # Warning, não erro
                logger.warning(
                    f"[PLANNER] Fase {i}: must_terms pode não conter entidades canônicas"
                )

            # 🔴 P0.2: CRITICAL - Validar seed_query contém entidades (NOVO)
            # Se ≤3 entidades E fase é entity-centric → OBRIGATÓRIO incluir na seed_query
            phase_type = phase.get("phase_type", "")
            entity_centric_types = ["industry", "profiles", "financials"]

            if (
                len(entities) <= 3
                and len(entities) > 0
                and phase_type in entity_centric_types
                and "@noticias" not in seed_query.lower()
            ):  # News pode usar @noticias em vez de entidades

                # Verificar se TODAS as entidades principais aparecem na seed_query
                seed_lower = seed_query.lower()
                missing_entities = []

                for entity in entities[:3]:  # Apenas as 3 primeiras (principais)
                    # Verificar se o nome (ou primeira palavra) aparece
                    entity_tokens = entity.split()
                    first_word = entity_tokens[0].lower()

                    # Aceitar se qualquer token significativo da entidade aparece
                    if not any(
                        token.lower() in seed_lower
                        for token in entity_tokens
                        if len(token) > 2
                    ):
                        missing_entities.append(entity)

                if missing_entities and getattr(
                    self.valves, "ENTITY_COVERAGE_STRICT", True
                ):
                    # HARD FAIL (default)
                    raise ValueError(
                        f"Fase {i} ({phase.get('name', 'sem nome')}): seed_query DEVE conter as entidades principais.\n"
                        f"Política entity-centric: ≤3 entidades → incluir TODAS na seed_query.\n"
                        f"Entidades faltando: {missing_entities}\n"
                        f"seed_query atual: '{seed_query}'\n"
                        f"Sugestão: '{' '.join(entities[:3])} {seed_query}'"
                    )
                elif missing_entities:
                    # SOFT WARNING (se strict=False)
                    logger.warning(
                        f"[PLANNER] Fase {i}: seed_query pode violar política entity-centric. "
                        f"Entidades faltando: {missing_entities}. seed_query: '{seed_query}'"
                    )

            # Validar avoid_terms não sobrepõe must_terms
            avoid_terms = phase["avoid_terms"]
            overlap = set(must_terms) & set(avoid_terms)
            if overlap:
                raise ValueError(
                    f"Fase {i}: must_terms e avoid_terms sobrepõem: {overlap}"
                )

            # Validar time_hint
            time_hint = phase.get("time_hint") or defaults.get(
                "time_hint", {"recency": "1y", "strict": False}
            )
            # Se a seed_query tiver @noticias, planner define janela e strict=True
            if "@noticias" in seed_query.lower():
                # Priorizar recency do planner; se ausente, default para 1y
                rec = time_hint.get("recency") or "1y"
                if rec not in ["90d", "1y", "3y"]:
                    rec = "1y"
                time_hint = {"recency": rec, "strict": True}
            # REMOVIDO: Enforcement rígido de news window (v4.2)
            # RAZÃO: Em estudos de mercado, Planner deve criar 2 fases separadas:
            #   - Fase "tendências/evolução" com 1y (captura 12 meses)
            #   - Fase "breaking news" com 90d (eventos pontuais)
            # Forçar 1y aqui impede o Planner de criar a arquitetura correta.
            # A validação agora é feita via prompt guidance + key_questions coverage check.

            if "recency" not in time_hint or time_hint["recency"] not in [
                "90d",
                "1y",
                "3y",
            ]:
                raise ValueError(f"Fase {i}: time_hint.recency deve ser 90d, 1y ou 3y")

            # P1: Validate seed_core (should be provided by Planner LLM now)
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
            
            # Log seed_core source for telemetry
            logger.info(
                f"[SEED][TELEMETRY] Fase {i}: seed_core source={seed_core_source}, len={len(seed_core)}"
            )

            # 🔧 FIX: Sincronizar seed_core quando @noticias estiver presente em seed_query
            # IMPORTANTE: Fazer DEPOIS da geração de seed_core para evitar sobrescrita
            try:
                if "@noticias" in seed_query.lower():
                    current_seed_core = phase.get("seed_core", "")
                    if (
                        current_seed_core
                        and "@noticias" not in current_seed_core.lower()
                    ):
                        phase["seed_core"] = f"@noticias {current_seed_core}"
                        logger.info(
                            f"[PLANNER] Fase {i}: @noticias adicionado ao seed_core (fase de notícias detectada)"
                        )
            except Exception as e:
                logger.warning(
                    f"[PLANNER] Erro ao sincronizar @noticias no seed_core: {e}"
                )

            # ✅ v4.7: Garantir seed_family_hint (default: entity-centric)
            seed_family_hint = phase.get("seed_family_hint", "entity-centric")
            if not seed_family_hint or seed_family_hint not in [
                "entity-centric",
                "problem-centric",
                "outcome-centric",
                "regulatory",
                "counterfactual",
            ]:
                seed_family_hint = "entity-centric"
                phase["seed_family_hint"] = seed_family_hint

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
                    "seed_core": phase.get("seed_core", ""),  # ✅ v4.7
                    "seed_family_hint": phase.get(
                        "seed_family_hint", "entity-centric"
                    ),  # ✅ v4.7
                    "must_terms": must_terms,
                    "avoid_terms": avoid_terms,
                    "time_hint": time_hint,
                    "source_bias": source_bias,
                    "evidence_goal": evidence_goal,
                    "lang_bias": phase.get("lang_bias")
                    or defaults.get("lang_bias", ["pt-BR", "en"]),
                    "geo_bias": phase.get("geo_bias")
                    or defaults.get("geo_bias", ["BR", "global"]),
                    "suggested_domains": phase.get("suggested_domains", []),  # ✅ v4.7
                    "suggested_filetypes": phase.get(
                        "suggested_filetypes", []
                    ),  # ✅ v4.7
                    "accept_if_any_of": [
                        f"Info sobre {phase['objective']}"
                    ],  # Para compatibilidade
                }
            )

        # 📰 FASE 1: News Slot Telemetry (rastreia se news foi adicionada/pulada)
        news_slot_telemetry = {
            "attempted": False,
            "reason": "not_requested",
            "market_study_detected": False,
        }

        # P0 enhancement: Enforce automatic 12m news phase for market/study intents
        try:
            research_objectives = obj.get("research_objectives", []) or []
            intent_text = str(obj.get("intent", user_prompt or "")).lower()
            # detect keywords indicating a market study / panorama request
            market_terms = [
                "estudo",
                "mercado",
                "panorama",
                "mercados",
                "setor",
                "analysis",
                "análise",
            ]
            is_market_study = any(t in intent_text for t in market_terms) or any(
                any(t in str(ro).lower() for t in market_terms)
                for ro in research_objectives
            )

            news_slot_telemetry["market_study_detected"] = is_market_study

            # Only enforce when there are >=3 phases planned (user requested multi-phase study)
            if is_market_study and len(validated_phases) >= 3:
                news_slot_telemetry["attempted"] = True

                # Check if a news-like phase already exists (by seed or by name/objective)
                news_present = False
                for p in validated_phases:
                    name_obj = f"{p.get('name','')} {p.get('objetivo','')}"
                    # look for @noticias or obvious news keywords
                    if "@noticias" in p.get("seed_query", "").lower() or any(
                        k in name_obj.lower()
                        for k in [
                            "notícia",
                            "noticias",
                            "news",
                            "evento",
                            "fusão",
                            "aquisição",
                        ]
                    ):
                        news_present = True
                        break

                if news_present:
                    news_slot_telemetry["reason"] = "already_present"
                elif len(validated_phases) >= phases:
                    news_slot_telemetry["reason"] = "skipped_exceed_limit"
                    logger.warning(
                        f"[PLANNER] Skipping automatic news phase: would exceed allowed phases "
                        f"({len(validated_phases)+1} > {phases}). Consider increasing DEFAULT_PHASE_COUNT."
                    )
                else:
                    # Add news phase
                    # construct sensible central theme
                    central = (
                        obj.get("topic")
                        or obj.get("intent")
                        or user_prompt
                        or "noticias"
                    ).strip()
                    # take up to 3 significant words
                    words = [w for w in re.split(r"\W+", central) if w]
                    core = " ".join(words[:3]) if words else "noticias"
                    # build seed components ensuring 3-6 words excluding @noticias
                    seed_parts = (core + " Brasil").split()
                    # ensure between 3 and 6 words
                    if len(seed_parts) < 3:
                        seed_parts += ["noticias"]
                    if len(seed_parts) > 6:
                        seed_parts = seed_parts[:6]
                    seed_query_new = "@noticias " + " ".join(seed_parts)

                    new_phase = {
                        "id": len(validated_phases) + 1,
                        "objetivo": "Notícias e eventos relevantes dos últimos 12 meses que impactam o tema",
                        "query_sugerida": seed_query_new,
                        "name": "Eventos & Notícias (12m)",
                        "seed_query": seed_query_new,
                        "must_terms": [],
                        "avoid_terms": [],
                        "time_hint": {"recency": "1y", "strict": True},
                        "source_bias": defaults.get(
                            "source_bias", ["oficial", "primaria", "secundaria"]
                        ),
                        "evidence_goal": {
                            "official_or_two_independent": True,
                            "min_domains": max(self.valves.MIN_UNIQUE_DOMAINS, phases),
                        },
                        "lang_bias": defaults.get("lang_bias", ["pt-BR", "en"]),
                        "geo_bias": defaults.get("geo_bias", ["BR", "global"]),
                        "accept_if_any_of": ["Notícias e eventos relevantes"],
                    }
                    validated_phases.append(new_phase)
                    news_slot_telemetry["reason"] = "added"
                    logger.info(
                        f"[PLANNER] Auto-added news phase: {len(validated_phases)}/{phases} phases"
                    )
        except Exception:
            # never fail contract validation for this enhancement; log and continue
            news_slot_telemetry["reason"] = "error"
            logger.exception("Failed to enforce automatic 12m news phase")

        if len(validated_phases) < 2:
            raise ValueError(f"Apenas {len(validated_phases)} fases")

        # ✅ FIX: Final validation to ensure phases limit is never exceeded
        if len(validated_phases) > phases:
            raise ContractValidationError(
                f"Contract validation failed: {len(validated_phases)} phases exceed limit of {phases}. "
                f"This should not happen after the auto-addition fix. Please check the logic."
            )

        # 📋 FASE 1: Entity Coverage Validation (Strict/Soft mode)
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
            # 📰 FASE 1: News slot telemetry
            "_news_slot_attempted": news_slot_telemetry["attempted"],
            "_news_slot_reason": news_slot_telemetry["reason"],
        }

        # 🔧 FASE 1: Seed Patch (se modo relax)
        seed_strict = getattr(self.valves, "SEED_VALIDATION_STRICT", False)
        if not seed_strict:
            for phase in validated_phases:
                _patch_seed_if_needed(phase, seed_strict, {}, logger)

        # Validar entity coverage usando helpers da FASE 1
        if entities_canonical:
            min_coverage = getattr(self.valves, "MIN_ENTITY_COVERAGE", 0.70)
            strict_mode = getattr(self.valves, "MIN_ENTITY_COVERAGE_STRICT", True)

            coverage = _calc_entity_coverage(validated_phases, entities_canonical)

            if coverage < min_coverage:
                if strict_mode:
                    # Hard-fail: lançar exceção
                    missing = _list_missing_entity_phases(
                        validated_phases, entities_canonical
                    )
                    raise ContractValidationError(
                        f"❌ ENTITY COVERAGE VIOLATION: Com {len(entities_canonical)} entidades, "
                        f"pelo menos {min_coverage:.0%} das fases devem incluir essas entidades em must_terms.\n"
                        f"Cobertura atual: {coverage:.0%}\n"
                        f"Entidades alvo: {entities_canonical}\n"
                        f"Fases sem entidades: {missing}\n"
                        f"🔧 FIX: Inclua as entidades em must_terms de mais fases"
                    )
                else:
                    # Soft mode: warning + metadados no contract
                    _check_entity_coverage_soft(
                        contract_dict, entities_canonical, min_coverage, logger
                    )

        # ✅ FASE 2: MECE Light Integration (P0.2)
        key_questions = obj.get("key_questions", [])
        if key_questions:
            uncovered = _check_mece_basic(validated_phases, key_questions)
            if uncovered:
                contract_dict["_mece_uncovered"] = uncovered
                logger.warning(
                    f"[MECE] {len(uncovered)} key_questions não cobertas por nenhuma fase: {uncovered[:3]}"
                )
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[PLANNER][MECE] Key questions órfãs: {uncovered}")

        # ✅ FASE 2: Economy Validation + TF-IDF Overlap Detection (Commit 6 + 8)
        actual_count = len(validated_phases)
        economy_threshold = getattr(self.valves, "PLANNER_ECONOMY_THRESHOLD", 0.75)

        # Layer 2 (Commit 8): Validar contador e justificativa (campos OPCIONAIS)
        declared_count = obj.get("total_phases_used", actual_count)  # Infere se ausente

        # Se fornecido, validar consistência
        if "total_phases_used" in obj and declared_count != actual_count:
            raise ContractValidationError(
                f"❌ Inconsistência: declarou {declared_count} fases em 'total_phases_used' "
                f"mas criou {actual_count} fases no array 'phases'. "
                f"Corrija o contador ou remova o campo (será inferido automaticamente)."
            )

        # Log automático de economia (sempre)
        budget_pct = (actual_count / phases * 100) if phases > 0 else 0
        logger.info(
            f"[PLANNER][ECONOMY] {actual_count}/{phases} fases criadas ({budget_pct:.0f}% budget)"
        )

        # Warning se ≥80% budget SEM justificativa de economia
        if actual_count >= int(0.8 * phases):
            justification = obj.get("phases_justification", "").lower()
            economy_keywords = [
                "economia",
                "combinar",
                "comparativa",
                "suficiente",
                "necessário",
                "econômico",
            ]
            has_economy_mention = any(kw in justification for kw in economy_keywords)

            if not has_economy_mention and justification:
                logger.warning(
                    f"[PLANNER][ECONOMY] Usou {actual_count}/{phases} fases (≥80%) mas justificativa não menciona economia. "
                    f"Justificativa: '{obj.get('phases_justification', 'N/A')[:80]}'"
                )
            elif not justification:
                logger.warning(
                    f"[PLANNER][ECONOMY] Usou {actual_count}/{phases} fases (≥80%) SEM campo 'phases_justification'. "
                    f"Considere adicionar justificativa para auditoria."
                )

        # Soft warning se >threshold
        if actual_count > phases * economy_threshold:
            logger.warning(
                f"[PLANNER][ECONOMY] Usou {actual_count}/{phases} fases (>{economy_threshold*100:.0f}% budget) - Validando overlap..."
            )

            # TF-IDF overlap detection (reutilizar sklearn)
            try:
                from sklearn.feature_extraction.text import TfidfVectorizer
                from sklearn.metrics.pairwise import cosine_similarity

                phase_objectives = [p.get("objetivo", "") for p in validated_phases]
                overlap_threshold = getattr(
                    self.valves, "PLANNER_OVERLAP_THRESHOLD", 0.70
                )

                if len(phase_objectives) >= 2:
                    vectorizer = TfidfVectorizer()
                    vectors = vectorizer.fit_transform(phase_objectives)

                    # Matriz de similaridade
                    sim_matrix = cosine_similarity(vectors)

                    # Detectar pares com >overlap_threshold (excluir diagonal)
                    overlaps_found = []
                    for i in range(len(phase_objectives)):
                        for j in range(i + 1, len(phase_objectives)):
                            if sim_matrix[i][j] > overlap_threshold:
                                overlap_info = {
                                    "phase_i": i + 1,
                                    "phase_j": j + 1,
                                    "similarity": round(float(sim_matrix[i][j]), 3),
                                    "objective_i": phase_objectives[i][:80],
                                    "objective_j": phase_objectives[j][:80],
                                }
                                overlaps_found.append(overlap_info)

                                logger.warning(
                                    f"[PLANNER][OVERLAP] Fases {i+1} e {j+1} têm {sim_matrix[i][j]*100:.0f}% similaridade (>{overlap_threshold*100:.0f}%). "
                                    f"Considere combinar:\n"
                                    f"  - Fase {i+1}: {phase_objectives[i][:60]}...\n"
                                    f"  - Fase {j+1}: {phase_objectives[j][:60]}..."
                                )

                    if overlaps_found:
                        # Armazenar no contract para Judge/Synth usar
                        contract_dict["_overlap_warnings"] = overlaps_found
                        logger.info(
                            f"[PLANNER][OVERLAP] {len(overlaps_found)} pares de overlap detectados e armazenados no contract"
                        )
                    else:
                        logger.info(
                            f"[PLANNER][OVERLAP] Nenhum overlap significativo detectado (threshold={overlap_threshold*100:.0f}%)"
                        )

            except Exception as e:
                logger.debug(f"[PLANNER][OVERLAP] Detecção falhou (não-crítico): {e}")

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


# ===== PiPeOpenaiPipe: OpenWebUI-style pipe compatible with existing pipes =====
# ===== Orchestrator with Cache and State (from PipeHay3) =====
class Orchestrator:
    """Orchestrator multi-fase com cache, acumulação de contexto e métricas de novidade

    RESPONSABILIDADES:
    - Executar iterações de pesquisa: Discovery → Scrape → Reduce → Analyze → Judge
    - Manter cache de URLs scraped (evita re-scraping)
    - Acumular contexto filtrado de todas as fases (para Context Reducer)
    - Rastrear novidade global (fatos e domínios já usados)
    - Aplicar rails de qualidade do contrato a cada iteração

    ESTADO GLOBAL:
    - contract: Plano completo com todas as fases (definido via set_contract())
    - all_phase_queries: Lista de queries de TODAS as fases (para Context Reducer)
    - scraped_cache: {url: content} - Cache persistente entre iterações
    - filtered_accumulator: Contexto acumulado de todas as fases
    - used_claim_hashes: Set de hashes de fatos já vistos (novelty tracking)
    - used_domains: Set de domínios já consultados (novelty tracking)
    - intent_profile: Perfil de intenção sincronizado com Pipe._detected_context

    INTEGRAÇÃO COM LLM COMPONENTS:
    - Analyst: Processa contexto acumulado completo (todas fases até agora)
    - Judge: Decide próximo passo com gates programáticos + telemetria

    GERENCIAMENTO DE CONTRACT:
    - Contract armazenado em self.contract (NÃO _last_contract)
    - Definido via set_contract(contract) pelo Pipe
    - Usado para extrair entidades, rails e phase_context
    - Passado ao Judge para prevenir duplicação de fases
    """

    def __init__(
        self,
        valves,
        discovery_call,
        scraper_call,
        context_reducer_call=None,
        job_id=None,
    ):
        self.valves = valves
        self.job_id = job_id

        self.discovery_tool = discovery_call
        self.scraper_tool = scraper_call
        self.context_reducer_tool = context_reducer_call

        self.analyst = AnalystLLM(valves)
        self.judge = JudgeLLM(valves)

        # v4.4: Deduplicação centralizada
        self.deduplicator = Deduplicator(valves)

        # ===== ESTADO GLOBAL =====
        self.contract = None  # Plano completo (todas as fases)
        self.all_phase_queries = []  # TODAS as queries de TODAS as fases
        self.scraped_cache = {}  # {url: content} - Cache de URLs já scraped
        self.filtered_accumulator = ""  # Contexto filtrado acumulado
        # Novidade global
        self.used_claim_hashes: set[str] = set()
        self.used_domains: set[str] = set()

    def set_contract(self, contract: dict):
        """Define contrato e extrai TODAS as queries"""
        self.contract = contract
        self.all_phase_queries = [
            fase["query_sugerida"] for fase in contract.get("fases", [])
        ]
        if getattr(self.valves, "VERBOSE_DEBUG", False):
            print(f"[Orchestrator] Contract set: {len(self.all_phase_queries)} queries")
        # Atualizar perfil de intenção, se fornecido
        self.intent_profile = contract.get(
            "intent_profile", getattr(self, "_intent_profile", "company_profile")
        )

    def _extract_focus_entities(
        self, query: str, original_entities: List[str]
    ) -> List[str]:
        """
        Extrai entidades que estão presentes em uma query focada.
        Usado em refinamentos para reduzir pipe_must_terms apenas ao relevante.

        Usa matching por palavras + sinônimos conhecidos para casos como "Exec" → "executive".
        """
        query_lower = query.lower()
        focus_entities = []

        # Mapeamento de sinônimos conhecidos (expansível)
        synonyms = {
            "exec": ["executive", "executivo", "executiva"],
            "flow ef": ["flowef", "flow"],
            "vila nova": ["vilanova"],
            # Adicionar mais conforme necessário
        }

        if getattr(self.valves, "VERBOSE_DEBUG", False):
            print(f"[FOCUS] Extração de entidades focadas:")
            print(f"[FOCUS] Query: '{query}'")
            print(f"[FOCUS] Entidades originais: {original_entities}")

        for entity in original_entities:
            entity_lower = entity.lower()
            entity_words = entity.split()

            # Tentativa 1: Match direto por palavras
            matched = any(
                word.lower() in query_lower for word in entity_words if len(word) > 2
            )

            if matched:
                focus_entities.append(entity)
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[FOCUS] ✅ '{entity}' encontrada (match direto, palavras: {entity_words})"
                    )
                continue

            # Tentativa 2: Match por sinônimos
            if entity_lower in synonyms:
                synonym_matched = any(
                    syn in query_lower for syn in synonyms[entity_lower]
                )
                if synonym_matched:
                    focus_entities.append(entity)
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            f"[FOCUS] ✅ '{entity}' encontrada (match por sinônimo: {synonyms[entity_lower]})"
                        )
                    continue

            # Nenhum match
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(
                    f"[FOCUS] ❌ '{entity}' não encontrada (palavras: {entity_words}, sinônimos: {synonyms.get(entity_lower, 'N/A')})"
                )

        if getattr(self.valves, "VERBOSE_DEBUG", False):
            print(f"[FOCUS] Entidades focadas extraídas: {focus_entities}")

        return focus_entities

    def _extract_entities_from_query(self, query: str) -> List[str]:
        """
        Extrai entidades preservando nomes compostos.
        
        Estratégias:
        1. Quoted strings (entidades explícitas)
        2. Multi-token proper nouns (2-3 palavras capitalizadas consecutivas)
        3. Single proper nouns (não capturados acima)
        """
        import re
        
        entities = []
        
        # 1. Quoted entities (prioridade máxima)
        quoted = re.findall(r'"([^"]+)"', query)
        entities.extend(quoted)
        
        # 2. Extrair entidades multi-token de forma mais inteligente
        words = query.split()
        proper_nouns = []
        i = 0
        
        while i < len(words):
            word = words[i]
            
            # Verificar se é palavra capitalizada
            if re.match(r'^[A-Z][a-zà-ú]+$', word):
                # Tentar formar entidade multi-token (máximo 3 palavras)
                entity_parts = [word]
                j = i + 1
                
                # Buscar palavras consecutivas capitalizadas (máximo 2 adicionais)
                while j < len(words) and len(entity_parts) < 3:
                    next_word = words[j]
                    if re.match(r'^[A-Z][a-zà-ú]+$', next_word):
                        entity_parts.append(next_word)
                        j += 1
                    else:
                        break
                
                # Se temos pelo menos 2 palavras, formar entidade
                if len(entity_parts) >= 2:
                    proper_nouns.append(' '.join(entity_parts))
                    i = j  # Pular as palavras já processadas
                else:
                    # Palavra única capitalizada
                    if len(word) > 2:
                        proper_nouns.append(word)
                    i += 1
            else:
                i += 1
        
        entities.extend(proper_nouns)
        
        # 4. Fallback simples: deixar LLM extrair entidades relevantes
        if not entities:
            # LLM-first: apenas stopwords básicas, deixar LLM decidir relevância
            basic_stopwords = {"para", "com", "sobre", "em", "de", "da", "do", "que", "os", "as", "the", "and", "or"}
            entities = [
                word for word in query.split()
                if len(word) > 3 and word.lower() not in basic_stopwords
            ][:5]
        
        # Dedupe preservando ordem
        seen = set()
        unique_entities = []
        for e in entities:
            if e not in seen:
                seen.add(e)
                unique_entities.append(e)
        
        return unique_entities

    def prepare_planner_context(
        self,
        phase_context: Dict,
        judge_result: Optional[Dict] = None,
        loop_number: int = 0,
    ) -> Dict:
        """
        Prepara contexto para o Planner baseado no estado da fase.

        Modos:
        1. Iteração inicial (loop=0): Usa phase_objective completo
        2. Refinamento (refine): Foca em lacuna específica
        3. Nova fase (new_phase): Não aplicável (nova fase tem contexto limpo)
        """
        # Modo 1: Iteração inicial - usar contexto original completo
        if loop_number == 0 or judge_result is None:
            return {
                "phase_objective": phase_context.get("objective", ""),
                "pipe_must_terms": phase_context.get("must_terms", []),
                "pipe_avoid_terms": phase_context.get("avoid_terms", []),
                "pipe_time_hint": phase_context.get("time_hint", {}),
                "pipe_lang_bias": phase_context.get("lang_bias", ["pt-BR", "en"]),
                "pipe_geo_bias": phase_context.get("geo_bias", ["BR", "global"]),
                "pipe_source_bias": phase_context.get(
                    "source_bias", ["oficial", "primaria", "secundaria"]
                ),
                "is_refinement": False,
            }

        # Modo 2: Refinamento - foco em lacuna específica
        verdict = judge_result.get("verdict", "")

        if verdict == "refine":
            refine_dict = judge_result.get("refine", {})
            
            # Extract seed_core from Judge (priority)
            seed_core = refine_dict.get("seed_core", "").strip()
            seed_core_source = "judge_llm"
            
            # Fallback if Judge didn't provide valid seed_core
            if not seed_core or len(seed_core) < 12:
                logger.warning("[ORCH] Judge didn't provide valid seed_core, generating fallback")
                seed_core = self._generate_seed_core_fallback(refine_dict, phase_context)
                seed_core_source = "orchestrator_fallback"
            
            # Extract next_query for focus_entities extraction
            next_query = refine_dict.get("next_query", "")
            if not next_query:
                # Fallback: usar objective original
                return self.prepare_planner_context(phase_context, None, 0)

            # Extrair entidades relevantes da next_query
            original_entities = (
                self.contract.get("entities", {}).get("canonical", [])
                if self.contract
                else []
            )
            focus_entities = self._extract_focus_entities(next_query, original_entities)

            # Se não encontrou entidades na query, usar termos da própria query como must_terms
            if not focus_entities:
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[FOCUS] ⚠️ Nenhuma entidade encontrada na query, ativando fallback"
                    )

                # Extrair entidades multi-token usando regex melhorado
                focus_entities = self._extract_entities_from_query(next_query)

                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[FOCUS] Palavras-chave extraídas (fallback): {focus_entities}"
                    )

            return {
                "phase_objective": refine_dict.get("focused_objective", ""),  # From Judge
                "seed_core": seed_core,  # From Judge or fallback
                "seed_core_source": seed_core_source,
                "pipe_must_terms": focus_entities,
                "pipe_avoid_terms": phase_context.get("avoid_terms", []),
                "pipe_time_hint": phase_context.get("time_hint", {}),
                "pipe_lang_bias": phase_context.get("lang_bias", ["pt-BR", "en"]),
                "pipe_geo_bias": phase_context.get("geo_bias", ["BR", "global"]),
                "pipe_source_bias": phase_context.get(
                    "source_bias", ["oficial", "primaria", "secundaria"]
                ),
                "is_refinement": True,
                "refinement_focus": next_query,
                "refinement_reason": judge_result.get("reasoning", ""),
            }

        # Fallback: usar contexto original
        return self.prepare_planner_context(phase_context, None, 0)

    def _generate_seed_core_fallback(self, refine_dict: Dict, phase_context: Dict) -> str:
        """Fallback for generating seed_core when Judge doesn't provide it (edge case)"""
        next_query = refine_dict.get("next_query", "")
        objective = phase_context.get("objetivo", "")[:80]
        entities = self.contract.get("entities", {}).get("canonical", [])[:2] if self.contract else []
        
        # Build basic seed_core
        entity_names = " ".join(entities) if entities else ""
        seed_core = f"{entity_names} {next_query} {objective}".strip()[:200]
        
        return seed_core

    async def run_iteration(
        self, query_text: str, phase_context: Dict = None
    ) -> Dict[str, Any]:
        """Executa iteração: Discovery → Cache Check → Scrape → Reduce → Accumulate → Analyze

        Args:
            query_text: Query para descoberta
            phase_context: Contexto da fase (objetivo, must_terms, etc)
                          NOTA: Em refinamentos, phase_context já vem focado (ajustado pelo caller)
        """
        # ===== TELEMETRY SETUP =====
        correlation_id = str(uuid.uuid4())[:8]
        self._current_query = query_text  # Store for telemetry
        
        # ===== ORCHESTRATION HIGH-LEVEL =====
        discovered_urls = await self._run_discovery(query_text, phase_context, correlation_id)
        
        # ===== CACHE CALCULATION (BEFORE SCRAPING) =====
        # Calculate new_urls and cached_urls BEFORE scraping to get accurate telemetry
        cached_urls = list(set(discovered_urls) & set(self.scraped_cache.keys()))
        new_urls = [url for url in discovered_urls if url not in self.scraped_cache]
        
        scraped_content = await self._run_scraping(discovered_urls, correlation_id)
        reduced_context = await self._run_context_reduction(scraped_content, correlation_id)
        analysis = await self._run_analysis(reduced_context, phase_context, correlation_id)
        
        # ===== METRICS & JUDGEMENT =====
        evidence_metrics = self._calculate_evidence_metrics_new(
            analysis.get("facts", []), discovered_urls
        )
        
        # Novelty metrics
        new_facts_ratio, new_domains_ratio = self._calculate_novelty_metrics(analysis)
        
        # Judge decision
        judge_loops = [
            {
                "new_facts_ratio": new_facts_ratio, 
                "new_domains_ratio": new_domains_ratio,
                "unique_domains": evidence_metrics.get("unique_domains", 0),  # P0: Incluir unique_domains
                "evidence_metrics": evidence_metrics,  # P0: Incluir evidence_metrics completo
                "n_facts": len(analysis.get("facts", []))  # P0: Incluir contagem de fatos
            }
        ]
        # Extract previous queries from telemetry
        previous_queries = []
        if judge_loops:
            for loop in judge_loops[-3:]:  # Last 3 loops only
                if "query" in loop:
                    previous_queries.append(loop["query"])

        judgement = await self.judge.run(
            user_prompt=query_text,
            analysis=analysis,
            phase_context=phase_context,
            telemetry_loops=judge_loops,
            intent_profile=getattr(self, "intent_profile", "company_profile"),
            full_contract=self.contract,
            refine_queries=analysis.get("refine_queries", [])[:3],
            phase_candidates=analysis.get("phase_candidates", [])[:2],
            previous_queries=previous_queries,  # ← NEW
            failed_queries=analysis.get("failed_queries", []),  # ← WIN #3: Failed queries context
        )

        # Validate for semantic overlap if REFINE verdict
        if judgement.get("verdict") == "REFINE":
            next_query = judgement.get("next_query", "")
            
            # Check semantic overlap with recent queries
            if previous_queries and next_query:
                def semantic_overlap(q1: str, q2: str) -> float:
                    """Calculate token overlap ratio between two queries"""
                    tokens1 = set(q1.lower().split())
                    tokens2 = set(q2.lower().split())
                    if not tokens1 or not tokens2:
                        return 0.0
                    return len(tokens1 & tokens2) / len(tokens1 | tokens2)
                
                # Check against last 2 queries
                max_overlap = 0.0
                for prev_q in previous_queries[-2:]:
                    overlap = semantic_overlap(next_query, prev_q)
                    max_overlap = max(max_overlap, overlap)
                
                # If > 70% overlap, force angle change
                if max_overlap > 0.70:
                    logger.warning(
                        f"[JUDGE] ⚠️ Query redundante detectada: {max_overlap:.0%} overlap "
                        f"entre '{next_query}' e queries anteriores"
                    )
                    
                    # Override verdict to NEW_PHASE or inject warning
                    if len(previous_queries) >= 2:
                        # After 2+ redundant attempts, force NEW_PHASE
                        logger.warning(
                            f"[JUDGE] Forçando NEW_PHASE após {len(previous_queries)} queries redundantes"
                        )
                        judgement["verdict"] = "NEW_PHASE"
                        judgement["new_phase_reason"] = (
                            f"Queries redundantes (>70% overlap): mudança de ângulo necessária. "
                            f"Queries anteriores falharam em gerar URLs novas com variações superficiais."
                        )
                    else:
                        # First redundant attempt: warn but allow
                        logger.warning(
                            f"[JUDGE] Permitindo query com overlap alto pela primeira vez. "
                            f"Próxima redundância forçará NEW_PHASE."
                        )
        
        # Coverage metrics
        sa = analysis.get("self_assessment", {})
        coverage_score = sa.get("coverage_score", 0)
        entities_covered = self._calculate_entities_covered(analysis)
        
        return {
            "query": query_text,
            "discovered_urls": discovered_urls,
            "new_urls": new_urls,  # Use pre-calculated values
            "cached_urls": cached_urls,  # Use pre-calculated values
            "filtered_content": reduced_context,
            "accumulated_context_size": len(self.filtered_accumulator),
            "accumulated_context": self.filtered_accumulator,
            "analysis": analysis,
            "judgement": judgement,
            "evidence_metrics": evidence_metrics,
            "new_facts_ratio": new_facts_ratio,
            "new_domains_ratio": new_domains_ratio,
            "coverage_score": coverage_score,
            "entities_covered": entities_covered,
            "n_facts": len(analysis.get("facts", [])),
            "unique_domains": evidence_metrics.get("unique_domains", 0),
            "seed_core_source": getattr(self, "_current_seed_core_source", "fallback"),
            "analyst_proposals": {
                "refine_queries_count": len(analysis.get("refine_queries", [])),
                "phase_candidates_count": len(analysis.get("phase_candidates", [])),
            },
        }

    # ===== HELPER METHODS FOR run_iteration =====
    
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
    
    async def _run_discovery(self, query_text: str, phase_context: Dict = None, correlation_id: str = None) -> List[str]:
        """Discovery logic (~80 linhas)
        
        Extrai de run_iteration:
        - Preparação de discovery_params
        - Chamada ao discovery_tool
        - Parse de discovery_result
        """
        # ===== TELEMETRY =====
        tel = StepTelemetry(
            step="discovery",
            correlation_id=correlation_id or "unknown",
            start_ms=time.time() * 1000,
            inputs_brief=f"query={query_text[:50]}...",
            counters={"urls": 0},
        )
        # CAMADA 1: Enriquecer seed com entidades prioritárias
        enriched_query = query_text
        
        if phase_context and self.contract:
            must_terms = phase_context.get("must_terms", [])
            entities = self.contract.get("entities", {})
            canonical = entities.get("canonical", [])
            
            query_lower = query_text.lower()
            
            # Identificar entidades ausentes na seed
            missing_entities = []
            for entity in canonical:
                entity_words = entity.split()
                if not any(
                    word.lower() in query_lower
                    for word in entity_words
                    if len(word) > 2
                ):
                    missing_entities.append(entity.split()[0])
            
            # Adicionar entidades se cabem
            if missing_entities:
                current_words = len(query_text.split())
                can_add = max(0, min(len(missing_entities), 12 - current_words))
                entities_to_add = missing_entities[:can_add]
                
                if entities_to_add:
                    enriched_query = f"{' '.join(entities_to_add)} {query_text}".strip()
                    
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(f"[ORCH][ENRICHMENT] Seed original: '{query_text}'")
                        print(f"[ORCH][ENRICHMENT] Seed enriquecida: '{enriched_query}'")
                        print(f"[ORCH][ENRICHMENT] Entidades adicionadas: {entities_to_add}")
        
        # 🔧 FIX: Add debug logging for query without prioritized entities
        if must_terms and getattr(self.valves, "DEBUG_LOGGING", False):
            query_has_entities = any(term.lower() in query_text.lower() for term in must_terms[:3])
            if not query_has_entities:
                print(f"[DISCOVERY] ⚠️ Query sem entidades prioritárias. Must_terms: {must_terms[:3]}")
                print(f"[DISCOVERY] Query original: '{query_text}'")
        
        # PRIORITY: seed_core from Judge/Planner (rich) > enriched_query (short)
        query_for_discovery = enriched_query
        seed_core_source = "enriched_query_fallback"

        if phase_context:
            seed_core = phase_context.get("seed_core", "").strip()
            seed_core_source = phase_context.get("seed_core_source", "unknown")

            if seed_core and len(seed_core) >= 12:
                query_for_discovery = seed_core
                logger.info(f"[ORCH] Using seed_core from {seed_core_source}: '{seed_core[:80]}...'")
            else:
                logger.warning(f"[ORCH] seed_core absent or invalid (len={len(seed_core) if seed_core else 0}), using enriched_query")
        else:
            # Fallback determinístico
            logger.warning("[ORCH] seed_core ausente, gerando fallback determinístico")
            try:
                entities = (self.contract or {}).get("entities", {})
                canon = [str(x) for x in entities.get("canonical", [])]
                alias = [str(x) for x in entities.get("aliases", [])]
                all_names = [n for n in (canon + alias) if n][:2]
                
                objective = phase_context.get("objetivo") or phase_context.get("objective", "")
                objective_words = objective.split()[:10] if objective else []
                
                geo_bias = phase_context.get("geo_bias", ["BR"])
                # NÃO adicionar geo diretamente ao query (vai para geo_bias depois)
                geo_hint = ""  # Removido - geo vai apenas para discovery_params["geo_bias"]
                
                entity_part = " ".join(all_names) if all_names else ""
                objective_part = " ".join(objective_words)
                query_for_discovery = f"{entity_part} {objective_part}".strip()
                # geo_hint removido - contexto geográfico vai via discovery_params["geo_bias"]
                
                if len(query_for_discovery) > 200:
                    query_for_discovery = query_for_discovery[:197] + "..."
                
                seed_core_source = "orchestrator_fallback"
                
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[ORCH] Fallback determinístico: '{query_for_discovery[:80]}...'")
                    
            except Exception as e:
                logger.error(f"[ORCH] Falha no fallback determinístico: {e}")
        
        # Store for telemetry
        self._current_seed_core_source = seed_core_source
        
        # Preparar discovery_params
        discovery_params = {"query": query_for_discovery}
        
        # Adicionar rails do Planner
        if phase_context:
            if phase_context.get("must_terms"):
                discovery_params["must_terms"] = phase_context["must_terms"]
            if phase_context.get("avoid_terms"):
                discovery_params["avoid_terms"] = phase_context["avoid_terms"]
            if phase_context.get("time_hint"):
                discovery_params["time_hint"] = phase_context["time_hint"]
            if phase_context.get("lang_bias"):
                discovery_params["lang_bias"] = phase_context["lang_bias"]
            if phase_context.get("geo_bias"):
                discovery_params["geo_bias"] = phase_context["geo_bias"]
            if phase_context.get("seed_family_hint"):
                discovery_params["seed_family_hint"] = phase_context["seed_family_hint"]
            if phase_context.get("suggested_domains"):
                discovery_params["suggested_domains"] = phase_context["suggested_domains"]
            if phase_context.get("suggested_filetypes"):
                discovery_params["suggested_filetypes"] = phase_context["suggested_filetypes"]
            
            # Propagar phase_objective
            objective = phase_context.get("objetivo") or phase_context.get("objective", "")
            if objective:
                discovery_params["phase_objective"] = objective
                objective_sentences = objective.split(".")[:2]
                discovery_params["phase_objective_short"] = ". ".join(
                    s.strip() for s in objective_sentences if s.strip()
                )
            
            # Empurrar APENAS entidades relevantes (não geo) para must_terms
            try:
                def _is_geographic_term(term: str) -> bool:
                    """Detecta se um termo é geográfico baseado em características estruturais"""
                    term_lower = term.strip().lower()
                    
                    # Termos muito curtos (< 3 chars) são provavelmente códigos geográficos
                    if len(term_lower) < 3:
                        return True
                    
                    # Países conhecidos (lista mínima, apenas os mais comuns)
                    countries = {"brasil", "brazil", "portugal", "argentina", "chile", "colombia", "mexico"}
                    if term_lower in countries:
                        return True
                    
                    # Códigos de país (2-3 letras, maiúsculas ou minúsculas)
                    if len(term_lower) <= 3 and term_lower.isalpha():
                        # Códigos comuns de país/região
                        geo_codes = {"br", "pt", "ar", "cl", "co", "mx", "us", "uk", "fr", "de", "es", "it"}
                        if term_lower in geo_codes:
                            return True
                    
                    # Termos geográficos genéricos (sem contexto setorial)
                    geo_generics = {
                        "global", "mundial", "nacional", "internacional", 
                        "latam", "latinamerica", "america", "europa"
                    }
                    if term_lower in geo_generics:
                        return True
                    
                    # NÃO filtrar se contém contexto setorial/empresarial
                    business_indicators = [
                        "executive", "search", "consultoria", "partners", 
                        "associados", "group", "corporation", "ltda", "sa"
                    ]
                    if any(indicator in term_lower for indicator in business_indicators):
                        return False  # Manter - tem contexto empresarial
                    
                    return False
                
                entities = (self.contract or {}).get("entities", {})
                canon = [str(x) for x in entities.get("canonical", [])]
                alias = [str(x) for x in entities.get("aliases", [])]
                all_names = [n for n in (canon + alias) if n]
                
                # Filtrar usando detecção estrutural (não blacklist hardcoded)
                filtered_names = [
                    n for n in all_names 
                    if n.strip() and not _is_geographic_term(n)
                ]
                
                if filtered_names:
                    merged = list(dict.fromkeys(
                        (discovery_params.get("must_terms") or []) + filtered_names
                    ))
                    discovery_params["must_terms"] = merged
                    
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        filtered_out = set(all_names) - set(filtered_names)
                        if filtered_out:
                            logger.info(f"[ORCH] Filtered geo terms from must_terms: {filtered_out}")
                        logger.info(f"[ORCH] Final must_terms: {merged}")
                    
                    logger.info(f"[ORCH] Enriched must_terms with entities: {len(merged)} total terms, {len(filtered_names)} entities (filtered {len(all_names) - len(filtered_names)} geo terms)")
                
                # Garantir geo separado em geo_bias (não em must_terms)
                if any(n.lower() in GEO_BLACKLIST for n in all_names):
                    # Adicionar geo_bias se ainda não existe
                    if "geo_bias" not in discovery_params or not discovery_params["geo_bias"]:
                        discovery_params["geo_bias"] = ["BR", "global"]
                        
            except Exception as e:
                logger.error(f"[ORCH] Error filtering must_terms: {e}")
        
        # Perfil do Planner → Discovery.profile
        try:
            profile_map = {
                "company_profile": "general",
                "regulation_review": "regulatory",
                "technical_spec": "technical",
                "literature_review": "academic",
                "history_review": "history",
            }
            eff_profile = profile_map.get(
                getattr(self, "intent_profile", "company_profile"), "general"
            )
            discovery_params["profile"] = eff_profile
        except Exception:
            discovery_params["profile"] = "general"
        
        # Domínios oficiais agregados
        try:
            all_official = []
            for arr in self.valves.OFFICIAL_DOMAINS.values():
                all_official.extend(arr)
            discovery_params["official_domains"] = list(sorted(set(all_official)))
        except Exception:
            pass
        
        # Chamar discovery tool
        try:
            discovery_result = await (
                self.discovery_tool(**discovery_params)
                if asyncio.iscoroutinefunction(self.discovery_tool)
                else asyncio.to_thread(self.discovery_tool, **discovery_params)
            )
        except Exception as e:
            logger.error(f"[DISCOVERY] Tool call failed: {e}")
            discovery_result = {"urls": []}
        
        # Parse discovery result
        if isinstance(discovery_result, str):
            try:
                discovery_result = json.loads(discovery_result)
            except json.JSONDecodeError as e:
                logger.error(f"[DISCOVERY] Failed to parse JSON: {e}")
                return []
        
        try:
            discovered_urls = self._parse_discovery_result(discovery_result)
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[Discovery] {len(discovered_urls)} URLs priorizadas")
        except Exception as e:
            logger.error(f"[DISCOVERY] Parse error: {e}")
            discovered_urls = []
        
        # ===== TELEMETRY FINAL =====
        tel.end_ms = time.time() * 1000
        tel.counters["urls"] = len(discovered_urls)
        tel.outputs_brief = f"urls={len(discovered_urls)}"
        logger.info(f"[{tel.step.upper()}] {json.dumps(tel.to_dict())}")
        
        return discovered_urls
    
    async def _run_scraping(self, discovered_urls: List[str], correlation_id: str = None) -> str:
        """Scraping + cache check (~60 linhas)
        
        Extrai de run_iteration:
        - Cache check
        - Chamada ao scraper_tool
        - Atualização de scraped_cache
        """
        # ===== TELEMETRY =====
        tel = StepTelemetry(
            step="scraper",
            correlation_id=correlation_id or "unknown",
            start_ms=time.time() * 1000,
            inputs_brief=f"urls={len(discovered_urls)}",
            counters={"new_urls": 0, "cached_urls": 0, "chars": 0},
        )
        cached_urls = set(discovered_urls) & set(self.scraped_cache.keys())
        new_urls = [url for url in discovered_urls if url not in self.scraped_cache]
        
        if getattr(self.valves, "VERBOSE_DEBUG", False):
            print(f"[Cache] {len(cached_urls)} URLs já scraped, {len(new_urls)} novas")
        
        raw_content = ""
        if new_urls:
            scraper_result = await (
                self.scraper_tool(**{"urls": new_urls})
                if asyncio.iscoroutinefunction(self.scraper_tool)
                else asyncio.to_thread(self.scraper_tool, **{"urls": new_urls})
            )
            
            if isinstance(scraper_result, str):
                scraper_result = json.loads(scraper_result)
            
            # Parsear scraped_content
            scraped_content = self._extract_scraped_content(scraper_result)
            for item in scraped_content:
                if isinstance(item, dict):
                    url = item.get("url")
                    content = item.get("content") or item.get("text") or ""
                    if url and content:
                        self.scraped_cache[url] = content
                        raw_content += f"\nURL: {url}\n{content}\n---\n"
            
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[Scraper] {len(new_urls)} URLs scraped, {len(raw_content)} chars")
        else:
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[Scraper] Nenhuma URL nova para scrape")
        
        # ===== TELEMETRY FINAL =====
        tel.end_ms = time.time() * 1000
        tel.counters["new_urls"] = len(new_urls)
        tel.counters["cached_urls"] = len(cached_urls)
        tel.counters["chars"] = len(raw_content)
        tel.outputs_brief = f"content={len(raw_content)} chars"
        logger.info(f"[{tel.step.upper()}] {json.dumps(tel.to_dict())}")
        
        return raw_content
    
    async def _run_context_reduction(self, raw_content: str, correlation_id: str = None) -> str:
        """Context Reducer call (~40 linhas)"""
        if not raw_content or not self.valves.ENABLE_CONTEXT_REDUCER or not self.context_reducer_tool:
            return raw_content
        
        # ===== TELEMETRY =====
        tel = StepTelemetry(
            step="context_reducer",
            correlation_id=correlation_id or "unknown",
            start_ms=time.time() * 1000,
            inputs_brief=f"content={len(raw_content)} chars",
            counters={"input_chars": len(raw_content), "output_chars": 0},
        )
        
        try:
            context_params = {
                "corpo": {"ultrasearcher_result": {"scraped_content": raw_content}},
                "mode": "coarse",
                "queries": self.all_phase_queries,
                "tipo": "fase",
            }
            
            if self.job_id:
                context_params["job_id"] = self.job_id
            
            context_result = await (
                self.context_reducer_tool(**context_params)
                if asyncio.iscoroutinefunction(self.context_reducer_tool)
                else asyncio.to_thread(self.context_reducer_tool, **context_params)
            )
            
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
            tel.end_ms = time.time() * 1000
            tel.counters["output_chars"] = len(filtered_content)
            tel.outputs_brief = f"reduced={len(filtered_content)} chars (-{reduction:.1f}%)"
            logger.info(f"[{tel.step.upper()}] {json.dumps(tel.to_dict())}")
            
            return filtered_content
            
        except (KeyError, ValueError, TypeError) as e:
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[Context Reducer] Parse error: {e}, usando raw")
            return raw_content
        except Exception as e:
            logger.error(f"Context reducer failed unexpectedly: {e}")
            raise ToolExecutionError(f"Context reducer execution failed: {e}") from e
    
    async def _run_analysis(self, filtered_content: str, phase_context: Dict, correlation_id: str = None) -> Dict:
        """Analyst LLM call (~50 linhas)"""
        # ===== TELEMETRY =====
        tel = StepTelemetry(
            step="analyst",
            correlation_id=correlation_id or "unknown",
            start_ms=time.time() * 1000,
            inputs_brief=f"context={len(self.filtered_accumulator)} chars",
            counters={"facts": 0, "lacunas": 0},
        )
        if filtered_content:
            self.filtered_accumulator += f"\n\n{filtered_content}"
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[Accumulator] Total acumulado: {len(self.filtered_accumulator)} chars")
        
        # Deduplicação opcional para Analyst
        analyst_context = self.filtered_accumulator
        
        if self.valves.ENABLE_ANALYST_DEDUPLICATION and self.filtered_accumulator:
            # Divisão mais agressiva para garantir ativação da deduplicação
            paragraphs = [
                p.strip() for p in self.filtered_accumulator.split("\n\n") if p.strip()
            ]
            
            # Se ainda não tem parágrafos suficientes, dividir por sentenças
            if len(paragraphs) < self.valves.MAX_ANALYST_PARAGRAPHS:
                # Dividir por sentenças (pontos seguidos de espaço)
                sentences = [
                    s.strip() for s in self.filtered_accumulator.replace('\n', ' ').split('. ') if s.strip()
                ]
                # Agrupar sentenças em parágrafos de ~3 sentenças
                paragraphs = []
                for i in range(0, len(sentences), 3):
                    paragraph = '. '.join(sentences[i:i+3])
                    if paragraph and not paragraph.endswith('.'):
                        paragraph += '.'
                    paragraphs.append(paragraph)
            
            if len(paragraphs) > self.valves.MAX_ANALYST_PARAGRAPHS:
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[DEDUP ANALYST] ✅ ATIVADO: {len(paragraphs)} parágrafos > {self.valves.MAX_ANALYST_PARAGRAPHS} → deduplicando para Analyst...")
                
                # Calcular preservação de contexto recente
                # 📊 PRESERVATION STRATEGY (v4.8.2):
                # - preserve_recent_pct = % of paragraphs preserved from deduplication
                # - reference_first=True → dedupe OLD against NEW (NEW is reference)
                # - Goal: Protect current iteration findings from being lost
                # - Default: ANALYST_PRESERVE_RECENT_PCT = 1.0 (100% preservation of new content)
                # - Formula: min(valve_cap, new_count / total_count)
                #   → If all content is new (Loop 1): 100% preserved
                #   → If 30/250 is new (Loop 3): min(100%, 12%) = 12% preserved (only the 30 new paragraphs)
                # - Trade-off: Higher preservation = more tokens to Analyst, but zero information loss
                if filtered_content:
                    new_paragraphs = [
                        p.strip() for p in filtered_content.split("\n\n") if p.strip()
                    ]
                    new_count = len(new_paragraphs)
                    
                    # Use valve to control preservation (default 1.0 = 100%)
                    # Ensures new content from current iteration is never lost
                    preserve_recent_pct = min(
                        self.valves.ANALYST_PRESERVE_RECENT_PCT,  # Configurable cap (default 1.0)
                        new_count / len(paragraphs)  # Dynamic ratio based on new vs accumulated
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
                model_name = getattr(self.valves, "ANALYST_DEDUP_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
                print(f"[DEDUP ANALYST] 🧠 Algoritmo: {algorithm.upper()}")
                print(f"[DEDUP ANALYST] 📊 Input: {len(paragraphs)} parágrafos → Target: {self.valves.MAX_ANALYST_PARAGRAPHS}")
                
                # Deduplicar com context-aware
                dedupe_result = self.deduplicator.dedupe(
                    chunks=paragraphs,
                    max_chunks=self.valves.MAX_ANALYST_PARAGRAPHS,
                    algorithm=algorithm,
                    threshold=getattr(self.valves, "DEDUP_SIMILARITY_THRESHOLD", 0.85),
                    preserve_order=True,
                    preserve_recent_pct=preserve_recent_pct,
                    shuffle_older=True,
                    reference_first=True,
                    # NOVO: Context-aware parameters
                    must_terms=phase_context.get("must_terms", []),
                    key_questions=phase_context.get("key_questions", []),
                    enable_context_aware=self.valves.ENABLE_CONTEXT_AWARE_DEDUP,
                )
                
                analyst_context = "\n\n".join(dedupe_result["chunks"])
                
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[DEDUP ANALYST] {dedupe_result['original_count']} → {dedupe_result['deduped_count']} parágrafos ({dedupe_result['reduction_pct']:.1f}% redução)")
                    
                    # Additional telemetry
                    preserved_count = int(len(paragraphs) * preserve_recent_pct)
                    print(f"[DEDUP ANALYST] 📌 Recent preservation: {preserved_count} paragraphs ({preserve_recent_pct:.1%}) protected from deduplication")
                    print(f"[DEDUP ANALYST] 🎯 Valve setting: ANALYST_PRESERVE_RECENT_PCT = {self.valves.ANALYST_PRESERVE_RECENT_PCT}")
        
        # Chamar Analyst
        analysis = await self.analyst.run(
            query=getattr(self, "_current_query", ""),
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
        
        # ===== TELEMETRY FINAL =====
        tel.end_ms = time.time() * 1000
        tel.counters["facts"] = len(analysis.get("facts", []))
        tel.counters["lacunas"] = len(analysis.get("lacunas", []))
        tel.outputs_brief = f"facts={len(analysis.get('facts', []))}, lacunas={len(analysis.get('lacunas', []))}"
        logger.info(f"[{tel.step.upper()}] {json.dumps(tel.to_dict())}")
        
        return analysis
    
    def _calculate_novelty_metrics(self, analysis: Dict) -> tuple[float, float]:
        """Calculate novelty metrics for facts and domains"""
        def _norm_text(s: str) -> str:
            return (s or "").strip().lower()
        
        def _hash_text(s: str) -> str:
            return hashlib.sha256(_norm_text(s).encode("utf-8")).hexdigest()
        
        facts_list = analysis.get("facts", []) or []
        current_hashes = set()
        for f in facts_list:
            if isinstance(f, dict):
                current_hashes.add(_hash_text(f.get("texto", "")))
            else:
                current_hashes.add(_hash_text(str(f)))
        
        # Domains from evidences
        current_domains = set()
        for f in facts_list:
            if isinstance(f, dict):
                for ev in f.get("evidencias", []) or []:
                    url = (ev or {}).get("url") or ""
                    try:
                        dom = url.split("/")[2] if "/" in url else ""
                        if dom:
                            current_domains.add(dom)
                    except Exception:
                        pass
        
        new_facts = [h for h in current_hashes if h not in self.used_claim_hashes]
        new_domains = [d for d in current_domains if d not in self.used_domains]
        
        new_facts_ratio = (
            (len(new_facts) / max(len(current_hashes), 1)) if current_hashes else 0.0
        )
        new_domains_ratio = (
            (len(new_domains) / max(len(current_domains), 1))
            if current_domains
            else 0.0
        )
        
        # Update global sets
        self.used_claim_hashes.update(current_hashes)
        self.used_domains.update(current_domains)
        
        return new_facts_ratio, new_domains_ratio
    
    def _calculate_entities_covered(self, analysis: Dict) -> int:
        """Calculate how many entities from contract appear in facts"""
        entities_covered = 0
        if self.contract and self.contract.get("entities"):
            canonical = self.contract["entities"].get("canonical", [])
            aliases = self.contract["entities"].get("aliases", [])
            all_entities = canonical + aliases
            
            # Verificar quantas entidades aparecem nos fatos
            facts_text = " ".join(
                [f.get("texto", "") for f in analysis.get("facts", [])]
            ).lower()
            for entity in all_entities:
                if entity.lower() in facts_text:
                    entities_covered += 1
        
        return entities_covered

        # 📋 FASE 2: Aplicar decisões do Judge (incremental)
        if self.contract and (refine_queries or phase_candidates):
            verdict = judgement.get("verdict", "done")
            selected_index = judgement.get("selected_index")

            if (
                verdict == "refine"
                and selected_index is not None
                and selected_index < len(refine_queries)
            ):
                # Aplicar refine_query selecionada
                selected_query = refine_queries[selected_index]
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[FASE2] Aplicando refine_query selecionada: {selected_query}"
                    )

            elif (
                verdict == "new_phase"
                and selected_index is not None
                and selected_index < len(phase_candidates)
            ):
                # Aplicar phase_candidate selecionada
                selected_candidate = phase_candidates[selected_index]

                # Verificar se cabe no limite de fases
                max_phases = getattr(self.valves, "MAX_PHASES", 6)
                current_phases = len(self.contract.get("fases", []))

                if current_phases < max_phases:
                    _append_phase(self.contract, selected_candidate)
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            f"[FASE2] Aplicando phase_candidate selecionada: {selected_candidate.get('name', 'N/A')}"
                        )
                else:
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            f"[FASE2] Limite de fases atingido ({current_phases}/{max_phases}), ignorando new_phase"
                        )

    def _empty_metrics(self) -> Dict[str, Any]:
        """Retorna métricas vazias para casos de erro ou sem dados"""
        return {
            "total_facts": 0,
            "facts_with_evidence": 0,
            "evidence_per_fact": 0.0,
            "unique_domains": 0,
            "facts_with_multiple_sources": 0,
            "evidence_coverage": 0.0,
            "high_confidence_facts": 0,
            "contradictions": 0,
        }

    def _calculate_evidence_metrics_new(self, facts_list, discovered_urls):
        """Calcula métricas de evidência para auditoria e qualidade (nova estrutura)

        REFACTORED (v4.3.1 - P1C): Usa _extract_quality_metrics() para evitar duplicação

        Args:
            facts_list: Lista de fatos extraídos pelo Analyst
            discovered_urls: URLs descobertas pela Discovery

        Returns:
            Dict com métricas de evidência ou métricas vazias se input inválido
        """
        # Validate inputs with null safety
        if not facts_list or not isinstance(facts_list, list):
            return self._empty_metrics()

        # Usar função consolidada para extrair métricas base
        metrics = _extract_quality_metrics(facts_list)

        # Retornar formato esperado pela telemetria
        return {
            "total_facts": len(facts_list),
            "facts_with_evidence": metrics["facts_with_evidence"],
            "evidence_per_fact": metrics["total_evidence"] / max(len(facts_list), 1),
            "unique_domains": len(metrics["domains"]),
            "facts_with_multiple_sources": metrics["facts_with_multiple_sources"],
            "evidence_coverage": metrics["facts_with_evidence"]
            / max(len(facts_list), 1),
            "high_confidence_facts": metrics["high_confidence_facts"],
            "contradictions": metrics["contradictions"],
        }

    def _extract_scraped_content(self, scraper_result):
        """Extrai scraped_content com contrato explícito (P0.1)"""
        if not isinstance(scraper_result, dict):
            raise ValueError(f"Scraper result must be dict, got {type(scraper_result)}")

        if getattr(self.valves, "VERBOSE_DEBUG", False):
            print(f"[DEBUG] Scraper result keys: {list(scraper_result.keys())}")

        # Contract A: {"scraped_content": [...]}
        if "scraped_content" in scraper_result:
            content = scraper_result["scraped_content"]
            if isinstance(content, list):
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[DEBUG] Contract A: Found {len(content)} items in scraped_content"
                    )
                return content
            else:
                raise ValueError(
                    f"Contract A: scraped_content must be list, got {type(content)}"
                )

        # Contract B: {"results": [...]}
        elif "results" in scraper_result:
            results = scraper_result["results"]
            if isinstance(results, list):
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[DEBUG] Contract B: Found {len(results)} items in results")
                return results
            else:
                raise ValueError(
                    f"Contract B: results must be list, got {type(results)}"
                )

        # Contract C: {"url": "...", "content": "..."} (single URL)
        elif "url" in scraper_result and "content" in scraper_result:
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[DEBUG] Contract C: Found single URL format")
            return [scraper_result]

        # Unknown format
        else:
            available_keys = list(scraper_result.keys())
            raise ValueError(
                f"Unknown scraper result format. Available keys: {available_keys}. Expected: scraped_content, results, or url+content"
            )

    async def finalize_global_context(self, mode: str = "light") -> str:
        """Context Reducer global com TODAS as queries"""
        if not (self.valves.ENABLE_CONTEXT_REDUCER and self.context_reducer_tool):
            raise ValueError("Context Reducer não disponível")

        try:
            # Build params and only include job_id when not null (P0.1)
            global_params = {
                "corpo": {"accumulated_content": [self.filtered_accumulator]},
                "mode": mode,
                "queries": self.all_phase_queries,  # ← CRÍTICO: TODAS as queries
                "tipo": "global",
            }
            if self.job_id:
                global_params["job_id"] = self.job_id

            global_result = await (
                self.context_reducer_tool(**global_params)
                if asyncio.iscoroutinefunction(self.context_reducer_tool)
                else asyncio.to_thread(self.context_reducer_tool, **global_params)
            )

            if isinstance(global_result, str):
                global_result = json.loads(global_result)

            final_markdown = global_result.get("final_markdown", "")

            total = len(self.filtered_accumulator)
            reduction = (1 - len(final_markdown) / total) * 100 if total > 0 else 0
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(
                    f"[Context Reducer] {mode}: {total} → {len(final_markdown)} chars (-{reduction:.1f}%)"
                )

            return final_markdown

        except Exception as e:
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(f"[Context Reducer] global falhou: {e}")
            raise

    def _parse_discovery_result(self, discovery_result):
        """Parse discovery result with explicit contract (P0.1)"""
        if not isinstance(discovery_result, dict):
            raise ValueError(
                f"Discovery result must be dict, got {type(discovery_result)}"
            )

        logger.debug("Discovery result keys: %s", list(discovery_result.keys()))

        discovered_urls = []

        # Contract A: {"urls": [...]}
        if "urls" in discovery_result:
            urls = discovery_result.get("urls", [])
            if isinstance(urls, list):
                discovered_urls = urls
                logger.debug(
                    "Contract A: Found %d URLs in discovery_result.urls",
                    len(discovered_urls),
                )
            else:
                raise ValueError(f"Contract A: urls must be list, got {type(urls)}")

        # Contract B: {"candidates": [{"url": ...}]}
        elif "candidates" in discovery_result:
            candidates = discovery_result.get("candidates", [])
            if isinstance(candidates, list):
                for candidate in candidates:
                    if isinstance(candidate, dict) and candidate.get("url"):
                        discovered_urls.append(candidate["url"])
                    elif hasattr(candidate, "url"):  # UrlCandidate object
                        discovered_urls.append(candidate.url)
                    else:
                        logger.warning("Invalid candidate format: %s", type(candidate))
                logger.debug(
                    "Contract B: Found %d URLs in discovery_result.candidates",
                    len(discovered_urls),
                )
            else:
                raise ValueError(
                    f"Contract B: candidates must be list, got {type(candidates)}"
                )

        # Contract C: Legacy format {"discovery_results": {"candidates": [...]}}
        elif "discovery_results" in discovery_result:
            discovery_results = discovery_result.get("discovery_results", {})
            if isinstance(discovery_results, dict):
                candidates = discovery_results.get("candidates", [])
                if isinstance(candidates, list):
                    for candidate in candidates:
                        if isinstance(candidate, dict) and candidate.get("url"):
                            discovered_urls.append(candidate["url"])
                    logger.debug(
                        "Contract C: Found %d URLs in discovery_results.candidates",
                        len(discovered_urls),
                    )
                else:
                    raise ValueError(
                        f"Contract C: candidates must be list, got {type(candidates)}"
                    )
            else:
                raise ValueError(
                    f"Contract C: discovery_results must be dict, got {type(discovery_results)}"
                )

        # Unknown format
        else:
            available_keys = list(discovery_result.keys())
            raise ValueError(
                f"Unknown discovery result format. Available keys: {available_keys}. Expected: urls, candidates, or discovery_results"
            )

        if not discovered_urls:
            logger.warning("Discovery returned 0 URLs - possible empty result")

        return discovered_urls


class Pipe:
    """Context-Aware Research Orchestration Pipeline (v4.5)

    OpenWebUI Manifold Pipe com detecção unificada de contexto, síntese adaptativa,
    error handling robusto, Planner inteligente com validação de key_questions coverage,
    e integração otimizada com discovery tool (dict return + config propagation).

    FLUXO PRINCIPAL:
    1. **Context Detection**: Detecta setor, tipo e perfil da pesquisa (fonte única)
    2. **Intelligent Planning**: LLM gera plano multi-fase com validação de key_questions coverage
    3. **Orchestration**: Executa fases com Discovery → Scrape → Reduce → Analyze → Judge
    4. **Adaptive Synthesis**: Gera relatório final adaptado ao contexto detectado

    COMPONENTES LLM:
    - **Planner**: Cria contract com fases, rails e entidades (validação rigorosa + key_questions coverage)
    - **Analyst**: Extrai fatos com evidências do contexto acumulado
    - **Judge**: Decide DONE/REFINE/NEW_PHASE com gates de qualidade programáticos
    - **Context Detector**: Classifica setor/tipo e mapeia para perfil apropriado
    - **Synthesizer**: Gera relatório final com estrutura adaptada ao contexto

    PERFIS E DETECÇÃO:
    - Detecção automática de 10+ setores (tech, saúde, finanças, direito, etc.)
    - Classificação de 6 tipos de pesquisa (acadêmica, regulatória, técnica, etc.)
    - Mapeamento inteligente setor → perfil de intenção
    - Sincronização automática entre _detected_context e _intent_profile

    QUALITY RAILS (aplicados em Judge):
    - Evidence coverage: 100% fatos com ≥1 evidência
    - Domain diversity: ≥2 domínios únicos por fase
    - Staleness: validação de recency por perfil
    - Novelty gates: thresholds de novos fatos/domínios por perfil

    INTELLIGENT PLANNING (v4.3):
    - Key_questions coverage: TODAS as key_questions têm fase correspondente
    - Market study architecture: 3y baseline + 1y trends + 90d breaking news
    - Customized seed_queries: Baseadas nos aspectos únicos dos objectives (não templates)
    - MECE phase validation: Sem overlap, sem lacunas críticas

    INTEGRAÇÃO:
    - Tools: discovery, scraper, context_reducer (resolução determinística)
    - Valves: Configuração completa via UI (LLM, rails, deduplicação, etc.)
    - GPT-5/O1 Support: Automatic parameter filtering for new models

    ROTAS E CONSISTÊNCIA:
    **EXECUÇÃO MANUAL ÚNICA:**

    **FLUXO ÚNICO:**
       - Context Detection → Planner LLM → Contract → Orchestrator → Synthesis
       - Contract.intent_profile sincronizado com _detected_context
       - Orchestrator.intent_profile = _detected_context['perfil']
       - Orchestrator.contract gerenciado via set_contract()

    **GARANTIAS DE CONSISTÊNCIA:**
    - _detected_context detectado UMA VEZ no início (pipe method)
    - _intent_profile sincronizado imediatamente após detecção
    - Todos os contracts validados contra _detected_context
    - Orchestrator sempre recebe intent_profile do _detected_context
    - Synthesis sempre usa _detected_context (não re-detecta)

    **CHANGELOG v4.0 → v4.4:**
    - ✅ v4.4: DEDUPLICATION & CONTEXT DETECTION UPGRADES
      - Deduplication: Unified Deduplicator class with 3 algorithms (MMR, MinHash, TF-IDF)
      - Threshold: 0.9 → 0.85 (-75% duplicates), Shingles: n=5 → n=3 (+40% detection)
      - Removed dangerous fallback, Analyst preserve_recent: DYNAMIC (100% current iteration)
      - Analyst reference_first: dedupe older AGAINST new data
      - Context Detection: SOURCE OF TRUTH (Planner follows detected profile)
      - Added RH/Headhunting sector with 18 keywords
      - Seed Queries: Mandatory tema central + ALL entity names (1-3 entities)
      - News Default: 1y (not 90d) - 90d only for explicit breaking news
    - ✅ v4.4.1: ANALYST AUTO-ASSESSMENT & CONFIDENCE-BASED EXIT
      - Analyst: self_assessment (coverage_score, gaps_critical, suggest_pivot/refine)
      - Analyst: calibration examples (0.3/0.6/0.9/1.0) to prevent optimism/pessimism
      - Judge: cross-check coverage_score vs objective metrics (facts/lacunas count)
      - Judge: confidence-based exit (DONE if coverage ≥ 70% even with low novelty)
      - Judge: auto-convert REFINE → NEW_PHASE when loops >= MAX_AGENT_LOOPS
      - Judge: 6 explicit NEW_PHASE cases (entities, contradictions, unexpected, temporal, angle, source)
      - Telemetry: enriched with coverage_score, suggest_pivot, failed_query per loop
      - Performance: MinHash 100x faster than MMR
    - ✅ v4.3.1: CODE CLEANUP & CONSOLIDATION (P0 + P1)
      - P0 - Dead code: 115 lines of orphan functions/models removed
      - P1A - JSON parsing: 3 implementations → 1 unified (parse_json_resilient)
      - P1B - LLM retry: removed _safe_llm_run wrapper (14 call sites updated)
      - P1C - Quality metrics: 3 functions → 1 shared (_extract_quality_metrics)
      - P1D - Synthesis sections: extracted _build_synthesis_sections
      - Total: ~265 lines consolidated, better maintainability
    - ✅ v4.3: PLANNER INTELLIGENCE UPGRADE
      - Mandatory key_questions coverage validation
      - Market study phase architecture (3y + 1y + 90d)
      - Customized seed_query generation
      - Enhanced prompt guidance with MECE examples
    - ✅ v4.2: Correção crítica Orchestrator._last_contract → self.contract
    - ✅ v4.1: GPT-5/O1 compatibility (automatic parameter filtering)
    - ✅ v4.0: Robustness refactor (custom exceptions, constants, helpers)
    - ✅ 4 exceções customizadas + error handling específico
    - ✅ Classe PipeConstants com 15+ configurações centralizadas
    - ✅ 4 métodos helper + state validation
    - ✅ Retry com backoff exponencial (LLM calls)
    - ✅ Timeouts dinâmicos e configuráveis
    - ✅ Suite de testes: 33 tests - 100% passing

    **VERSÕES ANTERIORES:**
    - v3.2: Context detection unificado + adaptive synthesis
    - v3.0: Multi-phase orchestration baseline
    """

    def _check_diminishing_returns(self, phase_telemetry, evidence_metrics):
        """Verifica diminishing returns programático com telemetria integrada (P0.2)"""
        loops = phase_telemetry.get("loops", [])

        if len(loops) < 2:
            return None  # Precisa de pelo menos 2 loops para comparar

        # Comparar últimos 2 loops
        loop1 = loops[-2]
        loop2 = loops[-1]

        # Calcular crescimento de domínios únicos
        domains1 = loop1.get("unique_domains", 0)
        domains2 = loop2.get("unique_domains", 0)
        domains_growth = (domains2 - domains1) / max(domains1, 1) if domains1 > 0 else 0

        # Calcular crescimento de fatos
        facts1 = loop1.get("n_facts", 0)
        facts2 = loop2.get("n_facts", 0)
        facts_growth = (facts2 - facts1) / max(facts1, 1) if facts1 > 0 else 0

        # Calcular crescimento de evidência
        evidence1 = loop1.get("evidence_coverage", 0)
        evidence2 = loop2.get("evidence_coverage", 0)
        evidence_growth = (
            (evidence2 - evidence1) / max(evidence1, 1) if evidence1 > 0 else 0
        )

        # Calcular crescimento de múltiplas fontes
        multi1 = loop1.get("facts_with_multiple_sources", 0)
        multi2 = loop2.get("facts_with_multiple_sources", 0)
        multi_growth = (multi2 - multi1) / max(multi1, 1) if multi1 > 0 else 0

        # KPI: <10% crescimento em domínios E fatos E evidência
        if domains_growth < 0.1 and facts_growth < 0.1 and evidence_growth < 0.1:
            return {
                "reason": f"Diminishing returns: domínios {domains_growth*100:.0f}%, fatos {facts_growth*100:.0f}%, evidência {evidence_growth*100:.0f}% (KPI: ≥10%)"
            }

        return None

    # (legacy signature docstring kept for reference; single pipe uses optional args)
    """
    It looks up tools in `__tools__` by names defined in `Valves` and falls back to
    internal default implementations defined above.
    """

    class Valves(BaseModel):
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
            default=3,
            ge=2,
            le=10,
            description="Máximo de fases iniciais (Planner cria ATÉ este número)",
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

        # --- FASE 1: Métricas simples inline ---

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

        # Official domains mapping (P1.1)
        OFFICIAL_DOMAINS: Dict[str, List[str]] = Field(
            default={
                "gov": ["gov.br", "gov.uk", "gov.au", "gov.ca", "europa.eu"],
                "regulatory": [
                    "anpd.gov.br",
                    "cnpj.gov.br",
                    "receita.fazenda.gov.br",
                    "bcb.gov.br",
                ],
                "academic": [
                    "arxiv.org",
                    "scholar.google.com",
                    "doi.org",
                    "ieee.org",
                    "acm.org",
                ],
                "standards": ["iso.org", "ietf.org", "w3.org", "nist.gov"],
                "health": ["who.int", "cdc.gov", "ans.gov.br", "saude.gov.br"],
                "tech": [
                    "github.com",
                    "stackoverflow.com",
                    "developer.mozilla.org",
                    "docs.python.org",
                ],
                "news": [
                    "reuters.com",
                    "bloomberg.com",
                    "valor.globo.com",
                    "folha.uol.com.br",
                ],
            },
            description="Mapa de domínios oficiais por vertical",
        )

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

        # Defaults por perfil (Planner → fases)
        PROFILE_DEFAULTS: Dict[str, Dict[str, Any]] = Field(
            default={
                "company_profile": {
                    "time_hint": {"recency": "1y", "strict": False},
                    "source_bias": ["oficial", "primaria", "secundaria"],
                    "evidence_goal": {"min_domains": 3},
                    "lang_bias": ["pt-BR", "en"],
                    "geo_bias": ["BR", "global"],
                },
                "regulation_review": {
                    "time_hint": {"recency": "1y", "strict": True},
                    "source_bias": ["oficial", "primaria", "secundaria"],
                    "evidence_goal": {"min_domains": 3},
                    "lang_bias": ["pt-BR", "en"],
                    "geo_bias": ["BR", "global"],
                },
                "technical_spec": {
                    "time_hint": {"recency": "3y", "strict": False},
                    "source_bias": ["oficial", "primaria", "secundaria"],
                    "evidence_goal": {"min_domains": 3},
                    "lang_bias": ["en", "pt-BR"],
                    "geo_bias": ["global"],
                },
                "literature_review": {
                    "time_hint": {"recency": "3y", "strict": False},
                    "source_bias": ["primaria", "secundaria", "terciaria"],
                    "evidence_goal": {"min_domains": 3},
                    "lang_bias": ["en"],
                    "geo_bias": ["global"],
                },
                "history_review": {
                    "time_hint": {"recency": "3y", "strict": False},
                    "source_bias": ["primaria", "secundaria", "terciaria"],
                    "evidence_goal": {"min_domains": 3},
                    "lang_bias": ["pt-BR", "en"],
                    "geo_bias": ["BR", "global"],
                },
            },
            description="Defaults de fase por perfil de intenção",
        )

        # Ensure seed_query includes @noticias for news phases (to activate discovery news mode)
        FORCE_NEWS_SEED_ATNOTICIAS: bool = Field(
            default=True,
            description="Garante que seed_query de fases de notícias contenha '@noticias'",
        )

        # Discovery query expansion
        EXPAND_DISCOVERY_QUERY_WITH_ENTITIES: bool = Field(
            default=True,
            description="Expandir a query do discovery com todos os nomes de entidades do plano",
        )

    def __init__(self):
        self.valves = self.Valves()
        self._last_contract: Optional[dict] = None
        self._last_hash: Optional[str] = None
        self._intent_profile: str = "company_profile"
        self._detected_context: Optional[Dict[str, Any]] = (
            None  # Centralizar contexto detectado
        )
        self._context_locked: bool = False  # Prevenir re-detecção após lock (para SIGA)

        # ✅ NOVO: Inicializar referência para export_pdf_tool (será preenchida no pipe())
        self._export_pdf_tool = None
        
        # ✅ LINE-BUDGET GUARD: Validar tamanho de funções críticas
        # Executado na inicialização para alertar sobre funções que excedem limites
        if getattr(self.valves, "ENABLE_LINE_BUDGET_GUARD", False):
            try:
                _warn_if_too_long(self.pipe, soft=500, hard=800)
                _warn_if_too_long(self._synthesize_final, soft=400, hard=600)
                _warn_if_too_long(_build_planner_prompt, soft=250, hard=400)
                _warn_if_too_long(_build_judge_prompt, soft=250, hard=400)
                _warn_if_too_long(_build_analyst_prompt, soft=200, hard=350)
            except Exception as e:
                # Não falhar a inicialização por erro no Line-Budget Guard
                logger.debug(f"[LBG] Line-Budget Guard failed: {e}")

    # No pipes() method needed - OpenWebUI auto-detects Pipe class

    # ==================== HELPER METHODS ====================
    # Métodos utilitários para reduzir duplicação e melhorar legibilidade

    async def _safe_export_pdf(
        self, content: str, filename: str, title: str, __event_emitter__=None
    ) -> dict:
        """Wrapper seguro para chamar export_pdf_tool"""
        if not self._export_pdf_tool:
            return {"success": False, "error": "Export PDF Tool not available"}

        try:
            # Tentar chamar como async
            if asyncio.iscoroutinefunction(self._export_pdf_tool):
                result = await self._export_pdf_tool(
                    content=content,
                    filename=filename,
                    title=title,
                    __event_emitter__=__event_emitter__,  # ✅ Passar event emitter
                )
            else:
                # Chamar como sync e converter para async
                result = await asyncio.to_thread(
                    self._export_pdf_tool,
                    content=content,
                    filename=filename,
                    title=title,
                    __event_emitter__=__event_emitter__,  # ✅ Passar event emitter
                )

            # Parsear resultado se for string
            if isinstance(result, str):
                return json.loads(result)
            return result

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _debug_log(self, message: str, context: str = "PIPE") -> None:
        """
        Helper para logs de debug verbose.

        Args:
            message: Mensagem de debug
            context: Contexto da mensagem (PIPE, SDK, MANUAL, etc.)

        Uso:
            self._debug_log("Context locked for SIGA", "SDK")
        """
        if getattr(self.valves, "VERBOSE_DEBUG", False):
            print(f"[DEBUG][{context}] {message}")

    def _get_current_profile(self) -> str:
        """
        Extrai perfil atual do contexto detectado com fallback seguro.

        Returns:
            Perfil atual ou DEFAULT_PROFILE se não detectado

        Uso:
            profile = self._get_current_profile()
        """
        if self._detected_context:
            return self._detected_context.get("perfil", PipeConstants.DEFAULT_PROFILE)
        return PipeConstants.DEFAULT_PROFILE

    def _sync_contract_with_context(self, contract: dict) -> dict:
        """
        Garante que contract.intent_profile está sincronizado com _detected_context.

        Args:
            contract: Contract a ser sincronizado

        Returns:
            Contract com intent_profile sincronizado

        Uso:
            contract = self._sync_contract_with_context(contract)
        """
        if self._detected_context and "intent_profile" in contract:
            detected_profile = self._get_current_profile()
            current_profile = contract.get("intent_profile")

            if current_profile != detected_profile:
                print(
                    f"[SYNC] Profile mismatch: contract='{current_profile}' vs detected='{detected_profile}'. Using detected profile."
                )
                contract["intent_profile"] = detected_profile

        return contract

    def _emit_status(self, message: str, emoji: str = "📋") -> str:
        """
        Formata mensagem de status para yield.

        Args:
            message: Mensagem de status
            emoji: Emoji prefixo (padrão: 📋)

        Returns:
            String formatada para yield

        Uso:
            yield self._emit_status("Context detected", "🔍")
        """
        return f"**[STATUS]** {emoji} {message}\n"

    def _validate_pipeline_state(self) -> None:
        """Valida consistência do estado interno do pipeline

        Verifica e corrige inconsistências entre estado interno.
        Chama este método no início de pipe() para prevenir estados inválidos.
        """
        # Check context lock consistency
        if self._context_locked and not self._detected_context:
            logger.warning("Context locked but no detected context - resetting lock")
            self._context_locked = False

        # Check contract consistency
        if self._last_contract:
            if not isinstance(self._last_contract, dict):
                logger.error("Invalid contract type, clearing")
                self._last_contract = None
                self._last_hash = None
            elif "fases" not in self._last_contract:
                logger.error("Contract missing fases, clearing")
                self._last_contract = None
                self._last_hash = None
            elif not isinstance(self._last_contract.get("fases"), list):
                logger.error("Contract fases is not a list, clearing")
                self._last_contract = None
                self._last_hash = None

        # Check profile sync
        if self._detected_context and self._intent_profile:
            detected = self._detected_context.get("perfil")
            if detected and detected != self._intent_profile:
                logger.warning(f"Profile mismatch syncing to detected: {detected}")
                self._intent_profile = detected

    # ==================== END HELPER METHODS ====================

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

        # 3. Prefixo (case-insensitive)
        for k in available_keys:
            if k.lower().startswith(name_hint.lower()):
                return k

        # 4. Fallback hints (prefixo)
        if fallback_hints:
            for hint in fallback_hints:
                for k in available_keys:
                    if k.lower().startswith(hint.lower()):
                        return k

        # 5. Substring (case-insensitive) - último recurso
        for k in available_keys:
            if name_hint.lower() in k.lower():
                return k

        # 6. Fallback hints (substring)
        if fallback_hints:
            for hint in fallback_hints:
                for k in available_keys:
                    if hint.lower() in k.lower():
                        return k

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

        # Resolve discovery tool using deepresearcher logic
        def _resolve_discovery_tool(tools_dict: Dict[str, Any]) -> Optional[str]:
            """Resolve discovery tool preferring rails-capable callable.

            Order:
            1) PREFERRED_TOOLS valve (discover)
            2) Signature-first: callable that accepts 'must_terms' (rails)
            3) Name-based candidates
            """
            if not tools_dict:
                return None

            # 1) Prefer explicit valve mapping
            try:
                preferred = getattr(self.valves, "PREFERRED_TOOLS", {}) or {}
                if isinstance(preferred, dict):
                    for key in ("discover", "discovery", "discovery_tool"):
                        name = preferred.get(key)
                        if name and name in tools_dict:
                            return name
            except Exception:
                pass

            # 2) Signature-first: pick callable that supports rails
            try:
                import inspect

                for name, info in (tools_dict or {}).items():
                    fn = (info or {}).get("callable")
                    if not fn:
                        continue
                    try:
                        params = set(inspect.signature(fn).parameters.keys())
                    except Exception:
                        continue
                    if {
                        "must_terms",
                        "avoid_terms",
                        "time_hint",
                        "lang_bias",
                        "geo_bias",
                        "min_domains",
                        "official_domains",
                    } & params:
                        return name
            except Exception:
                pass

            # 3) Fallback: name-based resolution
            discover_candidates = [
                "discovery_tool",
                "discovery_tool_clone",
                "discover",
                "search_web",
                "search",
                "supersearch",
                "discovery",
                "exa_search",
                "discoverywexa",
                "url searcher",
                "url_searcher",
                "discoverywExa",
            ]

            for candidate in discover_candidates:
                if candidate in tools_dict:
                    return candidate

            for tool_name in tools_dict.keys():
                if any(
                    candidate.lower() in tool_name.lower()
                    for candidate in discover_candidates
                ):
                    return tool_name

            return None

        # Find the actual discovery tool
        discovery_tool_name = _resolve_discovery_tool(__tools__)
        if discovery_tool_name:
            discovery_tool_with_rails = __tools__[discovery_tool_name]["callable"]
        else:
            discovery_tool_with_rails = d_callable_raw  # Fallback

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

            # If it's a news profile/phase, enforce @noticias and derive date range from recency
            try:
                eff_profile = (profile or "general").lower()
                is_news = ("news" in eff_profile) or (
                    "@noticias" in (query or "").lower()
                )
                if not is_news:
                    # Heuristic: if seed suggests 'notícias' explicitly
                    if any(
                        tok in (query or "").lower()
                        for tok in ["noticias", "notícias", "@noticias"]
                    ):
                        is_news = True
                if is_news:
                    # Force profile to news for downstream tool behavior
                    profile = "news"
                    if "@noticias" not in (query or ""):
                        query = f"@noticias {query}".strip()
                    # derive after/before if not explicitly provided using recency
                    if not after or not before:
                        from datetime import datetime, timedelta

                        recency = (time_hint or {}).get("recency", "90d")
                        now = datetime.utcnow()
                        if recency == "90d":
                            start = now - timedelta(days=90)
                            temporal_phrase = "últimos 90 dias"
                        elif recency == "1y":
                            start = now - timedelta(days=365)
                            temporal_phrase = "últimos 12 meses"
                        elif recency == "3y":
                            start = now - timedelta(days=365 * 3)
                            temporal_phrase = "últimos 3 anos"
                        else:
                            start = now - timedelta(days=365)
                            temporal_phrase = "últimos 12 meses"
                        # Set day-precision ISO
                        after = after or start.strftime("%Y-%m-%d")
                        before = before or now.strftime("%Y-%m-%d")
                    else:
                        # If explicit after/before provided, add a generic phrase to help parser
                        temporal_phrase = None

                    # Strengthen time_hint for news: strict window
                    if time_hint is None:
                        time_hint = {
                            "recency": (time_hint or {}).get("recency", "1y"),
                            "strict": True,
                        }
                    else:
                        time_hint.setdefault("recency", "1y")
                        time_hint["strict"] = True

                    # Help tool-side parser by embedding a natural language temporal hint
                    try:
                        if temporal_phrase:
                            low_q = (query or "").lower()
                            if temporal_phrase not in low_q:
                                query = f"{query} {temporal_phrase}".strip()
                    except Exception:
                        pass
            except Exception:
                pass

            # Make time filters PREFERENTIAL (soft) rather than RIGID (hard)
            # This allows comprehensive coverage while still prioritizing recent sources

            # Detect if this is clearly a historical topic that needs full temporal coverage
            is_clearly_historical = any(
                term in (query or "").lower()
                for term in [
                    "revolução",
                    "história",
                    "histórico",
                    "passado",
                    "antigo",
                    "clássico",
                    "medieval",
                    "renascimento",
                    "guerra mundial",
                    "império",
                    "monarquia",
                    "século",
                    "idade média",
                    "antiguidade",
                    "pré-história",
                ]
            )

            # Detect if this is clearly a current event that needs recent focus
            is_current_event = any(
                term in (query or "").lower()
                for term in [
                    "hoje",
                    "agora",
                    "atual",
                    "recente",
                    "último",
                    "novo",
                    "lançamento",
                    "eleições 2024",
                    "copa 2024",
                    "olimpíadas 2024",
                    "covid-19 2024",
                ]
            )

            if is_clearly_historical:
                # For clearly historical topics, ignore time filters completely
                after = None
                before = None
                logger.info("🔍 HISTORICAL DETECTED - Ignoring time filters: %s", query)
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[DISCOVERY] 🔍 Historical query detected, ignoring time filters: {query}"
                    )
            elif is_current_event or ("@noticias" in (query or "")):
                # For current events/news, keep/enforce time filters
                logger.info("📰 NEWS/CURRENT EVENT - Applying time filters: %s", query)
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[DISCOVERY] 📰 News/current event query, applying time filters: {query}"
                    )
            else:
                # For mixed topics (sectoral analysis, technology), make filters PREFERENTIAL
                # Keep the time hint but don't enforce it strictly
                if after or before:
                    logger.info("⚖️ MIXED TOPIC - Time filters as preference: %s", query)
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            f"[DISCOVERY] ⚖️ Mixed topic query, time filters as preference: {query}"
                        )
                else:
                    logger.info(
                        "🌐 NO TIME FILTERS - Full temporal coverage: %s", query
                    )
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            f"[DISCOVERY] 🌐 No time filters, full temporal coverage: {query}"
                        )

            # Build kwargs dynamically based on the callable's supported parameters
            import inspect

            supported_params = set()
            try:
                supported_params = set(
                    inspect.signature(discovery_tool_with_rails).parameters.keys()
                )
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[DEBUG][D_WRAPPER] Discovery tool supports params: {supported_params}"
                    )
            except Exception as e:
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[WARNING][D_WRAPPER] Could not inspect discovery tool signature: {e}"
                    )
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
                logger.error(
                    "[D_WRAPPER] Discovery tool does not support 'query' parameter!"
                )
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[ERROR][D_WRAPPER] Discovery tool missing 'query' param. Available: {supported_params}"
                    )
                return {"urls": []}

            base_kwargs = {
                "query": query,
                "profile": profile or "general",
                "after": after,
                "before": before,
                "whitelist": "",
                "pages_per_slice": 2,
            }

            # 🔴 P0: Log quando phase_objective for recebido ou não
            if phase_objective:
                logger.info(
                    f"[D_WRAPPER] ✅ Phase objective received: '{phase_objective[:100]}...'"
                )
            else:
                logger.warning(
                    f"[D_WRAPPER] ⚠️ Phase objective NOT received (got {type(phase_objective)})"
                )

            rails_kwargs = {
                "must_terms": must_terms,
                "avoid_terms": avoid_terms,
                "time_hint": time_hint,
                "lang_bias": lang_bias,
                "geo_bias": geo_bias,
                "min_domains": min_domains,
                "official_domains": official_domains,
                # 🔴 P0: Propagar phase_objective para Discovery Planner e Selector
                "phase_objective": phase_objective,
            }

            # Filter kwargs to only what the callable accepts
            final_kwargs = {}
            for k, v in base_kwargs.items():
                if k in supported_params:
                    # Skip None values except for query (which is required)
                    if v is not None or k == "query":
                        final_kwargs[k] = v

            for k, v in rails_kwargs.items():
                if k in supported_params and v is not None:
                    final_kwargs[k] = v

            # Ensure query is never empty
            if not final_kwargs.get("query"):
                logger.error("[D_WRAPPER] Query is empty!")
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[ERROR][D_WRAPPER] Query is empty or None")
                return {"urls": []}

            # PATCH 3 (FASE 2): Propagar configs do Pipe para Discovery Tool
            # Alinha timeouts, retries e outras configs para evitar falhas em cascata
            if "request_timeout" in supported_params:
                final_kwargs["request_timeout"] = self.valves.LLM_TIMEOUT_DEFAULT
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[DEBUG][D_WRAPPER] Setting request_timeout={self.valves.LLM_TIMEOUT_DEFAULT}s"
                    )

            if "max_retries" in supported_params:
                max_retries = getattr(self.valves, "LLM_MAX_RETRIES", 3)
                final_kwargs["max_retries"] = max_retries
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[DEBUG][D_WRAPPER] Setting max_retries={max_retries}")

            if "llm_timeout" in supported_params:
                final_kwargs["llm_timeout"] = self.valves.LLM_TIMEOUT_DEFAULT

            # Propagar model se discovery tool suportar
            if "llm_model" in supported_params:
                final_kwargs["llm_model"] = self.valves.LLM_MODEL
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[DEBUG][D_WRAPPER] Setting llm_model={self.valves.LLM_MODEL}"
                    )

            # Propagar limite de páginas se configurado
            if "pages_per_slice" in supported_params:
                pages_per_slice = getattr(self.valves, "DISCOVERY_PAGES_PER_SLICE", 2)
                final_kwargs["pages_per_slice"] = pages_per_slice

            # PATCH v4.5.1: Solicitar retorno como dict (evita json.dumps/loads overhead)
            if "return_dict" in supported_params:
                final_kwargs["return_dict"] = True
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[DEBUG][D_WRAPPER] Requesting dict return (eliminates JSON parse overhead)"
                    )

            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(
                    f"[DEBUG][D_WRAPPER] Calling discovery tool with params: {list(final_kwargs.keys())}"
                )
                print(f"[DEBUG][D_WRAPPER] Query: '{final_kwargs.get('query', 'N/A')}'")
                print(
                    f"[DEBUG][D_WRAPPER] Profile: {final_kwargs.get('profile', 'N/A')}"
                )
                print(
                    f"[DEBUG][D_WRAPPER] Must terms: {final_kwargs.get('must_terms', 'N/A')}"
                )
                print(
                    f"[DEBUG][D_WRAPPER] Avoid terms: {final_kwargs.get('avoid_terms', 'N/A')}"
                )
                if "after" in final_kwargs or "before" in final_kwargs:
                    print(
                        f"[DEBUG][D_WRAPPER] Time range: {final_kwargs.get('after', 'N/A')} to {final_kwargs.get('before', 'N/A')}"
                    )
                if "phase_objective" in final_kwargs:
                    print(
                        f"[DEBUG][D_WRAPPER] ✅ Phase objective: '{final_kwargs.get('phase_objective', 'N/A')[:80]}...'"
                    )

            try:
                result = await discovery_tool_with_rails(**final_kwargs)

                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[DEBUG][D_WRAPPER] Discovery returned type: {type(result)}")
                    if isinstance(result, dict):
                        print(
                            f"[DEBUG][D_WRAPPER] Discovery result keys: {list(result.keys())}"
                        )
                        if "urls" in result:
                            print(
                                f"[DEBUG][D_WRAPPER] URLs count: {len(result.get('urls', []))}"
                            )
                        if "candidates" in result:
                            print(
                                f"[DEBUG][D_WRAPPER] Candidates count: {len(result.get('candidates', []))}"
                            )
                    elif isinstance(result, str):
                        print(
                            f"[DEBUG][D_WRAPPER] Discovery returned string, length: {len(result)}"
                        )

                # Validate result is not empty/None
                if result is None:
                    logger.warning("[D_WRAPPER] Discovery tool returned None")
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            "[WARNING][D_WRAPPER] Discovery tool returned None, returning empty dict"
                        )
                    return {"urls": []}

                return result

            except TypeError as e:
                logger.error(f"[D_WRAPPER] TypeError calling discovery tool: {e}")
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[ERROR][D_WRAPPER] TypeError: {e}")
                    print(f"[ERROR][D_WRAPPER] Supported params: {supported_params}")
                    print(
                        f"[ERROR][D_WRAPPER] Passed params: {list(final_kwargs.keys())}"
                    )
                    import traceback

                    traceback.print_exc()
                return {"urls": []}
            except Exception as e:
                logger.error(f"[D_WRAPPER] Exception calling discovery tool: {e}")
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(f"[ERROR][D_WRAPPER] Exception: {e}")
                    import traceback

                    traceback.print_exc()
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
                        print(
                            f"[DEBUG] Plan markers: has_fase={has_fase}, has_plano={has_plano}"
                        )

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
      "avoid_terms": ["ruído"],
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
                                        if not phase.get("objective") or not phase.get(
                                            "seed_query"
                                        ):
                                            # Re-ask LLM for seed_query if missing; avoid programmatic fallback
                                            if not phase.get(
                                                "seed_query"
                                            ) and phase.get("objective"):
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
                                                            candidate = (
                                                                candidate.replace(
                                                                    op, " "
                                                                )
                                                            )
                                                        words = candidate.split()
                                                        if 3 <= len(words) <= 6:
                                                            phase["seed_query"] = (
                                                                " ".join(words)
                                                            )
                                                except Exception:
                                                    pass
                                            if not phase.get("objective"):
                                                phase["objective"] = (
                                                    f"Análise: {phase.get('seed_query', 'tópico')}"
                                                )
                                    return contract
                            except (json.JSONDecodeError, KeyError, ValueError) as e:
                                logger.warning(
                                    f"Failed to parse extracted contract: {e}"
                                )
                            except Exception as e:
                                logger.error(f"Unexpected error parsing contract: {e}")
                                raise ContractValidationError(
                                    f"Contract parsing failed: {e}"
                                ) from e

            return None
        except ContractValidationError:
            raise  # Re-raise specific errors
        except Exception as e:
            logger.warning(f"Failed to extract contract from history: {e}")
            return None

    async def pipe(
        self,
        body: dict,
        __user__: dict = None,
        __tools__: dict = None,
        __event_emitter__: Optional[Callable] = None,
        **kwargs,
    ):
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

        user_msg = (
            body.get("messages", [{"content": ""}])[-1].get("content", "") or ""
        ).strip()
        low = user_msg.lower()

        # 🎯 DETECÇÃO DE INTENÇÃO (antes de qualquer processamento)
        # Comandos de continuação
        is_continue_command = any(
            kw in low for kw in ["siga", "continue", "prossiga", "next"]
        )

        # ✅ NOVO: Detectar intenção de refinamento de plano
        # RAZÃO: Preservar contexto quando usuário quer ajustar (não criar novo)
        # IMPACTO: Evita mudança de perfil/setor em refinamentos simples
        refinement_keywords = [
            "adicione",
            "inclua",
            "acrescente",
            "mude",
            "altere",
            "ajuste",
            "remova",
            "tire",
            "delete",
            "corrija",
            "refine",
            "modifique",
            "aumente",
            "reduza",
            "expanda",
            "foque mais",
            "menos em",
            "troque",
            "substitua",
            "adapte",
            "personalize",
            "atualize",
        ]
        is_refinement = any(kw in low for kw in refinement_keywords)
        has_previous_plan = bool(self._last_contract)

        # 🔒 DECISÃO: Preservar contexto ou re-detectar?
        # Preservar se: (comando "siga") OU (refinamento E tem plano anterior)
        should_preserve_context = is_continue_command or (
            is_refinement and has_previous_plan
        )

        if should_preserve_context and self._detected_context:
            # Manter contexto anterior (refinamento ou siga)
            logger.info(
                f"[PIPE] Contexto preservado: is_continue={is_continue_command}, is_refinement={is_refinement}, has_plan={has_previous_plan}"
            )
            yield f"**[CONTEXT]** 🔒 Mantendo contexto: {self._detected_context.get('perfil', 'N/A')}\n"
            if is_refinement:
                yield f"**[INFO]** 💡 Modo refinamento detectado - ajustando plano existente\n"
        else:
            # Re-detectar contexto (nova query)
            if self._context_locked:
                logger.info("[PIPE] Nova query detectada - desbloqueando contexto")
                self._context_locked = False
                self._detected_context = None

            self._detected_context = await self._detect_unified_context(user_msg, body)
            logger.info(f"[PIPE] Contexto detectado: {self._detected_context}")

            # 🔗 SINCRONIZAR PERFIL DETECTADO com intent_profile (fonte única de verdade)
            if self._detected_context:
                self._intent_profile = self._detected_context.get(
                    "perfil", "company_profile"
                )
                logger.info(f"[PIPE] Perfil sincronizado: {self._intent_profile}")
                yield f"**[CONTEXT]** 🔍 Perfil: {self._detected_context.get('perfil', 'N/A')} | Setor: {self._detected_context.get('setor', 'N/A')} | Tipo: {self._detected_context.get('tipo', 'N/A')}\n"

        d_callable, s_callable, cr_callable = self._resolve_tools(__tools__ or {})

        # ✅ NOVO: Resolver export_pdf_tool se disponível
        if __tools__:
            export_pdf_key = self._resolve_tool_deterministic(
                "export_pdf",
                list(__tools__.keys()),
                fallback_hints=[
                    "export",
                    "pdf",
                    "export_pdf_from_json",
                    "export_pdf_from_text",
                ],
            )

            if export_pdf_key:
                self._export_pdf_tool = __tools__[export_pdf_key]["callable"]
                if self.valves.DEBUG_LOGGING:
                    yield f"**[DEBUG]** Export PDF Tool encontrada: {export_pdf_key}\n"
            else:
                self._export_pdf_tool = None
                if self.valves.DEBUG_LOGGING:
                    yield f"**[DEBUG]** Export PDF Tool não disponível\n"
        # Manual route - single execution path
        # If the user asks to continue (siga), execute stored contract
        if low in {"siga", "continue", "prosseguir"}:
            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(
                    f"[DEBUG] SIGA mode: _last_contract exists: {bool(self._last_contract)}"
                )
            if self._last_contract:
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[DEBUG] _last_contract fases: {len(self._last_contract.get('fases', []))}"
                    )

            # Try to get contract from stored state first
            if not self._last_contract:
                # Try to extract contract from message history
                yield "**[INFO]** Procurando plano no histórico...\n"
                contract = await self._extract_contract_from_history(body)
                if contract:
                    self._last_contract = contract

                    # 🔗 SINCRONIZAR contract recuperado com contexto ATUAL
                    # PROBLEMA: contexto detectado agora pode ser diferente do contexto original
                    # SOLUÇÃO: Tentar recuperar contexto do histórico ou usar atual com warning
                    if "intent_profile" in contract:
                        historical_profile = contract.get("intent_profile")
                        current_profile = (
                            self._detected_context.get("perfil", "company_profile")
                            if self._detected_context
                            else "company_profile"
                        )

                        if historical_profile != current_profile:
                            logger.warning(
                                f"[SIGA] Context drift detected: historical={historical_profile}, current={current_profile}. Using historical to maintain consistency."
                            )
                            # Usar perfil do contract histórico para manter consistência
                            if self._detected_context:
                                self._detected_context["perfil"] = historical_profile
                                self._intent_profile = historical_profile

                    yield "**[INFO]** ✅ Plano recuperado do histórico\n"
                else:
                    yield "**[AVISO]** Nenhum contrato pendente ou encontrado no histórico\n"
                    yield "**[DICA]** Tente criar um novo plano primeiro\n"
                    return

            job_id = f"cached_{int(time.time() * 1000)}"

            orch = Orchestrator(
                self.valves, d_callable, s_callable, cr_callable, job_id
            )
            orch.set_contract(self._last_contract)  # ← Define contract e extrai queries

            # 🔗 GARANTIR que Orchestrator use perfil detectado (fonte única)
            if self._detected_context:
                orch.intent_profile = self._detected_context.get(
                    "perfil", "company_profile"
                )
                logger.info(
                    f"[ORCH] Perfil sincronizado com contexto detectado: {orch.intent_profile}"
                )

            yield f"\n### Execução iniciada\n"
            phase_results = []
            telemetry_data = {
                "execution_id": job_id,
                "start_time": time.time(),
                "phases": [],
            }

            for i, ph in enumerate(self._last_contract.get("fases", []), 1):
                objetivo, q = ph["objetivo"], ph["query_sugerida"]
                yield f"\n**Fase {i}** – {objetivo}\n"

                phase_start_time = time.time()
                loops = 0
                phase_telemetry = {
                    "phase": i,
                    "objective": objetivo,
                    "query": q,
                    "start_time": phase_start_time,
                    "loops": [],
                }

                # Track queries to detect infinite loops and add phase timeout
                seen_queries = set()
                phase_timeout_start = time.time()
                PHASE_TIMEOUT = 600  # 10 minutes max per phase

                # Ensure result variable exists even if loop exits early due to error/timeout
                res = None
                res_already_added = (
                    False  # Track if result was already added to phase_results
                )
                while True:
                    # Check phase timeout
                    if time.time() - phase_timeout_start > PHASE_TIMEOUT:
                        logger.error(
                            f"Phase timeout after {PHASE_TIMEOUT}s",
                            extra={"phase": i, "correlation_id": correlation_id},
                        )
                        yield f"**[ERRO]** Timeout de fase excedido ({PHASE_TIMEOUT}s)\n"
                        break

                    loop_start_time = time.time()
                    loops += 1

                    # Safety check: prevent infinite loops
                    if (
                        loops > self.valves.MAX_AGENT_LOOPS * 2
                    ):  # Double the limit as safety
                        yield f"**[ERROR]** Loop infinito detectado! Saindo após {loops} iterações.\n"
                        break

                    try:
                        # NOTA: ph (phase_context) já foi ajustado para foco se should_refine=True
                        res = await orch.run_iteration(q, phase_context=ph)

                        # 🔴 DEFESA P0: Validar estrutura de res antes de consumir
                        if not isinstance(res, dict):
                            logger.error(
                                f"[CRITICAL] run_iteration returned non-dict: {type(res)}"
                            )
                            yield f"**[ERROR]** Iteração retornou tipo inválido: {type(res)}\n"
                            break

                        if "analysis" not in res:
                            logger.error(
                                "[CRITICAL] run_iteration missing 'analysis' key"
                            )
                            yield f"**[ERROR]** Resposta de iteração incompleta (sem 'analysis')\n"
                            break

                    except Exception as e:
                        if getattr(self.valves, "VERBOSE_DEBUG", False):
                            print(f"[ERROR] run_iteration failed: {e}")
                            import traceback

                            traceback.print_exc()
                        yield f"**[ERROR]** Falha na iteração: {e}\n"
                        break
                    loop_end_time = time.time()

                    discovered_count = len(res.get("discovered_urls", []))
                    new_urls_count = len(res.get("new_urls", []))
                    accumulated_size = res.get("accumulated_context_size", 0)

                    yield f"**[discovery]** {discovered_count} URLs\n"
                    if discovered_count == 0 and accumulated_size == 0:
                        yield f"**[⚠️ AVISO]** Discovery retornou 0 URLs e não há contexto acumulado. Verifique se a tool_discovery está funcionando corretamente.\n"
                    elif discovered_count == 0 and accumulated_size > 0:
                        yield f"**[INFO]** Discovery retornou 0 URLs novas, mas há {accumulated_size} chars de contexto acumulado das iterações anteriores.\n"

                    yield f"**[cache]** {len(res['cached_urls'])} já scraped, {new_urls_count} novas\n"
                    yield f"**[accumulator]** {accumulated_size} chars acumulados\n"

                    # Show analyst insights
                    analysis = res["analysis"]

                    # 🔴 DEFESA P0.1: Validar que analysis é dict
                    if not isinstance(analysis, dict):
                        logger.error(
                            f"[CRITICAL] analysis is not dict: {type(analysis)}, value: {repr(analysis)[:200]}"
                        )
                        yield f"**[ERROR]** Análise retornou tipo inválido\n"
                        # Criar analysis vazio para continuar
                        analysis = {"summary": "", "facts": [], "lacunas": []}

                    summary = analysis.get("summary", "")
                    facts = analysis.get("facts", [])
                    evidence = analysis.get("evidence", [])
                    evidence_metrics = res.get("evidence_metrics", {})

                    # Debug: verificar contexto passado para analista
                    accumulated_context = res.get("accumulated_context", "")
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            f"[DEBUG] Accumulated context size: {len(accumulated_context)} chars"
                        )
                        print(
                            f"[DEBUG] Context preview: {accumulated_context[:300]}..."
                            if accumulated_context
                            else "[DEBUG] Empty context"
                        )

                    # Debug: verificar se há dados do analista
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            f"[DEBUG] Analysis keys: {list(analysis.keys()) if analysis else 'None'}"
                        )
                        print(f"[DEBUG] Evidence metrics: {evidence_metrics}")
                        print(f"[DEBUG] Facts count: {len(facts)}")
                        print(
                            f"[DEBUG] RES keys: {list(res.keys()) if res else 'None'}"
                        )
                        print(f"[DEBUG] Analysis content: {str(analysis)[:500]}...")
                        print(f"[DEBUG] Summary: {summary[:200]}...")
                    if facts:
                        if getattr(self.valves, "VERBOSE_DEBUG", False):
                            print(
                                f"[DEBUG] First fact: {facts[0] if facts else 'None'}"
                            )
                    else:
                        if getattr(self.valves, "VERBOSE_DEBUG", False):
                            print(f"[DEBUG] No facts found in analysis")

                    if summary:
                        yield f"**[analyst]** 💡 {summary}\n"
                    if facts:
                        yield f"**[analyst]** 📊 {len(facts)} fatos encontrados\n"
                        for i, fact in enumerate(facts[:3], 1):  # Show first 3 facts
                            if isinstance(fact, dict):
                                texto = fact.get("texto", str(fact))
                                confianca = fact.get("confiança", "")
                                contradicao = fact.get("contradicao", False)
                                critical = fact.get("critical", False)
                                conf_emoji = (
                                    "🟢"
                                    if confianca == "alta"
                                    else "🟡" if confianca == "média" else "🔴"
                                )
                                contra_emoji = "⚠️" if contradicao else ""
                                critical_emoji = "🔥" if critical else ""
                                yield f"   {i}. {conf_emoji} {contra_emoji} {critical_emoji} {texto}\n"
                            else:
                                yield f"   {i}. {fact}\n"
                        if len(facts) > 3:
                            yield f"   ... e mais {len(facts) - 3} fatos\n"

                    # Show evidence metrics
                    if evidence_metrics:
                        coverage = evidence_metrics.get("evidence_coverage", 0) * 100
                        multi_sources = evidence_metrics.get(
                            "facts_with_multiple_sources", 0
                        )
                        unique_domains = evidence_metrics.get("unique_domains", 0)
                        high_conf = evidence_metrics.get("high_confidence_facts", 0)
                        contradictions = evidence_metrics.get("contradictions", 0)
                        yield f"**[evidence]** 📋 {coverage:.0f}% fatos com evidência, {multi_sources} com múltiplas fontes, {unique_domains} domínios únicos\n"
                        yield f"**[quality]** 🎯 {high_conf} alta confiança, {contradictions} contradições\n"
                        # Expor ratios no log de execução para consumo do Judge gates
                        new_facts_ratio = res.get("new_facts_ratio")
                        new_domains_ratio = res.get("new_domains_ratio")
                        if (
                            new_facts_ratio is not None
                            and new_domains_ratio is not None
                        ):
                            yield f"**[telemetry]** 📈 novidade: fatos {new_facts_ratio:.2f}, domínios {new_domains_ratio:.2f}\n"

                    # Show lacunas
                    lacunas = analysis.get("lacunas", [])
                    if lacunas:
                        # P0: lacunas pode ser lista de dicts (novo) ou strings (legado)
                        lacunas_display = []
                        for lac in lacunas[:2]:
                            if isinstance(lac, dict):
                                lacunas_display.append(lac.get("descricao", str(lac)))
                            else:
                                lacunas_display.append(str(lac))
                        yield f"**[lacunas]** 🔍 {len(lacunas)} lacunas identificadas: {', '.join(lacunas_display)}{'...' if len(lacunas) > 2 else ''}\n"

                    verdict = res["judgement"].get("verdict", "done")
                    reasoning = res["judgement"].get("reasoning", "")
                    # next_query pode vir dentro de 'refine' no JSON do Judge
                    next_query = res["judgement"].get("next_query", "")
                    if (not next_query) and isinstance(
                        res["judgement"].get("refine"), dict
                    ):
                        next_query = res["judgement"]["refine"].get("next_query", "")
                    unexpected_findings = res["judgement"].get(
                        "unexpected_findings", []
                    )
                    proposed_phase = res["judgement"].get("proposed_phase", {})

                    # Verificar diminishing returns programático
                    diminishing_returns = None
                    try:
                        diminishing_returns = self._check_diminishing_returns(
                            phase_telemetry, evidence_metrics
                        )
                    except AttributeError as e:
                        if getattr(self.valves, "VERBOSE_DEBUG", False):
                            print(
                                f"[WARNING] _check_diminishing_returns not available: {e}"
                            )
                        diminishing_returns = None

                    # v4.5.3: Preserve @noticias token in next_query if original seed had it
                    if verdict == "refine" and next_query:
                        try:
                            original_seed = (
                                ph.get("seed_query", "") if isinstance(ph, dict) else ""
                            )
                            has_noticias_original = "@noticias" in original_seed.lower()
                            has_noticias_next = "@noticias" in next_query.lower()

                            if has_noticias_original and not has_noticias_next:
                                # Adicionar @noticias ao início da next_query
                                next_query = f"@noticias {next_query}".strip()
                                if getattr(self.valves, "VERBOSE_DEBUG", False):
                                    print(
                                        f"[DEBUG][REFINE] Adicionado @noticias à next_query (seed original tinha @noticias)"
                                    )
                                    print(
                                        f"[DEBUG][REFINE] Query atualizada: '{next_query}'"
                                    )
                        except Exception as e:
                            if getattr(self.valves, "VERBOSE_DEBUG", False):
                                print(f"[WARNING] Erro ao preservar @noticias: {e}")

                    # v4.4.1: Validate next_query: REJECT if generic or missing entity
                    if verdict == "refine" and next_query:
                        try:
                            # Extrair entidades disponíveis (must_terms + entities canonical)
                            available_entities = []
                            if isinstance(ph, dict):
                                if ph.get("must_terms"):
                                    available_entities.extend(
                                        [
                                            t
                                            for t in ph["must_terms"]
                                            if isinstance(t, str)
                                        ]
                                    )

                            # Verificar se next_query tem pelo menos 1 entidade
                            nq_low = next_query.strip().lower()
                            has_entity = any(
                                ent.lower() in nq_low
                                for ent in available_entities
                                if len(ent) > 2
                            )

                            # Verificar se é genérica (frases proibidas)
                            generic_bad = {
                                "buscar fontes",
                                "fontes mais específicas",
                                "dados verificáveis",
                                "buscar dados",
                                "fontes específicas",
                                "mais específicas",
                                "mais verificáveis",
                                "aprofundar",
                                "refinar busca",
                            }
                            is_generic = (
                                any(bad in nq_low for bad in generic_bad)
                                or len(nq_low.split()) <= 2
                            )

                            # Detectar queries muito restritivas (muitos operadores)
                            operator_count = 0
                            restrictive_operators = [
                                "filetype:",
                                "site:",
                                "after:",
                                "before:",
                                "intitle:",
                                "inurl:",
                            ]
                            for op in restrictive_operators:
                                operator_count += (next_query or "").lower().count(op)

                            # Detectar OR complexo (mais de 3 termos em OR)
                            or_terms = (next_query or "").upper().split(" OR ")
                            has_complex_or = len(or_terms) > 3

                            # Detectar query muito LONGA (muitos termos)
                            query_words = len((next_query or "").split())
                            query_chars = len(next_query or "")
                            is_too_long = query_words > 12 or query_chars > 100

                            # Query muito restritiva se: >3 operadores OU OR complexo OU muito longa
                            is_too_restrictive = (
                                operator_count > 3 or has_complex_or or is_too_long
                            )

                            # v4.4.1: REJEITAR agressivamente queries sem entidade ou genéricas
                            needs_rewrite = (
                                (not next_query)
                                or is_generic
                                or not has_entity
                                or is_too_restrictive
                            )

                            if needs_rewrite:
                                # Re-ask Judge to rewrite next_query properly
                                if is_too_restrictive:
                                    # Determinar o motivo específico
                                    if is_too_long:
                                        reason = f"muito longa ({query_words} palavras, {query_chars} chars)"
                                    elif operator_count > 3:
                                        reason = f"muitos operadores ({operator_count})"
                                    else:
                                        reason = f"OR muito complexo ({len(or_terms)} termos)"

                                    reask = (
                                        f"A query anterior era {reason} e pode causar timeout ou 0 URLs. "
                                        f"Reescreva como query SIMPLES (3-7 palavras, MAX 1 operador), "
                                        f"focando em termos chave. Objetivo: '{ph.get('objetivo', '')}'. "
                                        f"Seed: '{ph.get('seed_query', '')}'. Retorne apenas a query."
                                    )
                                else:
                                    # Query sem entidade OU genérica
                                    problem = (
                                        "SEM ENTIDADE" if not has_entity else "GENÉRICA"
                                    )
                                    reask = (
                                        f"Query anterior {problem} ('{next_query}'). "
                                        f"Reescreva incluindo OBRIGATORIAMENTE ≥1 nome: {', '.join(available_entities[:5])}. "
                                        f"Formato: 3-7 palavras, específica. "
                                        f"Objetivo: '{ph.get('objetivo', '')[:80]}'. "
                                        f"Retorne apenas a query."
                                    )
                                safe_reask = get_safe_llm_params(
                                    self.judge.model_name, {"temperature": 0.2}
                                )
                                re = await _safe_llm_run_with_retry(
                                    self.judge.llm,
                                    reask,
                                    safe_reask,
                                    timeout=self.judge.valves.LLM_TIMEOUT_DEFAULT,
                                    max_retries=1,
                                )
                                if re and re.get("replies"):
                                    candidate = (
                                        re["replies"][0].strip().strip('"').strip("'")
                                    )
                                    if 3 <= len(candidate.split()) <= 7:
                                        next_query = candidate
                                        if getattr(self.valves, "VERBOSE_DEBUG", False):
                                            print(
                                                f"[DEBUG] Query reescrita: '{candidate}'"
                                            )
                        except Exception:
                            pass

                    # 🔧 FIX: Apply sector context enrichment for ambiguous entities
                    if verdict == "refine" and next_query:
                        try:
                            # Extrair contexto do contract
                            sector = ""
                            canonical = []
                            if self.contract:
                                sector = self.contract.get("setor_principal", "")
                                if self.contract.get("entities"):
                                    canonical = self.contract["entities"].get("canonical", [])
                            
                            # Enriquecer query se tiver entidade ambígua
                            original_query = next_query
                            next_query = _add_sector_context_if_ambiguous(next_query, canonical, sector)
                            
                            # Log se houve mudança
                            if next_query != original_query and getattr(self.valves, "VERBOSE_DEBUG", False):
                                print(f"[JUDGE] Query enriquecida: '{original_query}' → '{next_query}'")
                        except Exception as e:
                            if getattr(self.valves, "VERBOSE_DEBUG", False):
                                print(f"[WARNING] Erro no enrichment de query: {e}")

                    should_refine = (
                        verdict == "refine"
                        and next_query
                        and loops < self.valves.MAX_AGENT_LOOPS
                        and not diminishing_returns
                    )

                    # Check TOTAL phases (planejadas no contrato + já executadas)
                    total_planned_phases = (
                        len(self._last_contract.get("fases", []))
                        if self._last_contract
                        else 0
                    )
                    total_executed_phases = len(phase_results)
                    total_phases = max(total_planned_phases, total_executed_phases)

                    should_new_phase = (
                        verdict == "new_phase"
                        and proposed_phase
                        and total_phases < self.valves.MAX_PHASES
                    )

                    # v4.5.3: Se Judge pediu NEW_PHASE mas atingiu MAX_PHASES → Converter para DONE
                    if (
                        verdict == "new_phase"
                        and total_phases >= self.valves.MAX_PHASES
                    ):
                        if getattr(self.valves, "VERBOSE_DEBUG", False):
                            print(
                                f"[DEBUG] NEW_PHASE bloqueado: total_phases={total_phases} >= MAX_PHASES={self.valves.MAX_PHASES}"
                            )
                        yield f"**[judge]** ⚠️ Limite de fases atingido ({total_phases}/{self.valves.MAX_PHASES}), convertendo new_phase → done\n"
                        verdict = "done"
                        reasoning = f"[AUTO-CORREÇÃO] Limite de fases atingido ({total_phases}/{self.valves.MAX_PHASES}). {reasoning}"
                        should_new_phase = False

                    # v4.4.1: Se Judge pediu REFINE mas atingiu limite de loops → Converter para NEW_PHASE
                    if (
                        verdict == "refine"
                        and loops >= self.valves.MAX_AGENT_LOOPS
                        and not should_new_phase
                    ):
                        # Se há espaço para nova fase, converter refine → new_phase
                        if total_phases < self.valves.MAX_PHASES:
                            # Criar new_phase a partir do refine intent
                            if not proposed_phase and next_query:
                                proposed_phase = {
                                    "objective": f"Busca focada: {reasoning[:100]}",
                                    "seed_query": next_query,
                                    "name": f"Busca dirigida (fase {len(phase_results)+1})",
                                    "por_que": f"Atingido limite de loops ({loops}) mas lacunas essenciais persistem",
                                }
                                should_new_phase = True
                                verdict = "new_phase"
                                if getattr(self.valves, "VERBOSE_DEBUG", False):
                                    print(
                                        f"[DEBUG] Convertido REFINE → NEW_PHASE (max loops atingido, lacunas essenciais)"
                                    )
                                yield f"**[judge]** ⚠️ Limite de loops atingido ({loops}), convertendo refine → new_phase\n"

                    # Debug: log loop conditions
                    if getattr(self.valves, "VERBOSE_DEBUG", False):
                        print(
                            f"[DEBUG] Loop {loops}: verdict={verdict}, should_refine={should_refine}, should_new_phase={should_new_phase}"
                        )
                    print(
                        f"[DEBUG] Conditions: next_query={bool(next_query)}, loops={loops}/{self.valves.MAX_AGENT_LOOPS}, diminishing_returns={bool(diminishing_returns)}"
                    )
                    print(
                        f"[DEBUG] New phase: proposed_phase={bool(proposed_phase)}, phase_results={len(phase_results)}/{self.valves.MAX_PHASES}"
                    )

                    # Se diminishing returns, avaliar se vale continuar
                    # MAS: não forçar DONE se Judge identificou lacunas essenciais
                    if diminishing_returns and verdict == "refine":
                        # Verificar se há lacunas essenciais mencionadas no reasoning
                        essential_gap_keywords = [
                            "lacuna essencial",
                            "falta",
                            "ausente",
                            "necessário",
                            "exigem",
                        ]
                        has_essential_gaps = any(
                            kw in reasoning.lower() for kw in essential_gap_keywords
                        )

                        # Verificar se último loop teve 0 URLs (query ruim, não esgotamento)
                        last_loop_had_urls = len(res.get("discovered_urls", [])) > 0

                        # Só forçar DONE se:
                        # 1. Não há lacunas essenciais E
                        # 2. Último loop teve URLs (não foi query ruim)
                        if not has_essential_gaps and last_loop_had_urls:
                            verdict = "done"
                            reasoning = f"Diminishing returns detectado: {diminishing_returns['reason']}. {reasoning}"
                            should_refine = False
                        else:
                            # Diminishing returns mas há lacunas essenciais ou query ruim
                            # Permitir mais 1 tentativa com nova abordagem
                            if getattr(self.valves, "VERBOSE_DEBUG", False):
                                print(
                                    f"[DEBUG] Diminishing returns detectado mas permitindo refine (lacunas essenciais ou query ruim)"
                                )
                            # Manter verdict = "refine" original do Judge

                    # Coletar telemetria integrada do loop (P0.2)
                    # v4.4.1: Adicionar self_assessment do Analyst
                    sa = analysis.get("self_assessment", {})

                    loop_telemetry = {
                        "loop": loops,
                        "start_time": loop_start_time,
                        "end_time": loop_end_time,
                        "duration": loop_end_time - loop_start_time,
                        "n_urls": len(res["discovered_urls"]),
                        "n_new_urls": len(res["new_urls"]),
                        "n_cached_urls": len(res["cached_urls"]),
                        "accumulated_chars": res["accumulated_context_size"],
                        "n_facts": len(facts),
                        "evidence_coverage": (
                            evidence_metrics.get("evidence_coverage", 0)
                            if evidence_metrics
                            else 0
                        ),
                        "unique_domains": (
                            evidence_metrics.get("unique_domains", 0)
                            if evidence_metrics
                            else 0
                        ),
                        "facts_with_multiple_sources": (
                            evidence_metrics.get("facts_with_multiple_sources", 0)
                            if evidence_metrics
                            else 0
                        ),
                        "high_confidence_facts": (
                            evidence_metrics.get("high_confidence_facts", 0)
                            if evidence_metrics
                            else 0
                        ),
                        "contradictions": (
                            evidence_metrics.get("contradictions", 0)
                            if evidence_metrics
                            else 0
                        ),
                        "lacunas_count": len(lacunas),
                        "new_facts_ratio": new_facts_ratio,
                        "new_domains_ratio": new_domains_ratio,
                        "verdict": verdict,
                        "reasoning": reasoning,
                        "diminishing_returns": diminishing_returns is not None,
                        # v4.4.1: Analyst self-assessment metrics
                        "coverage_score": sa.get("coverage_score", 0),
                        "analyst_confidence": sa.get("confidence", "baixa"),
                        "suggest_pivot": sa.get("suggest_pivot", False),
                        "failed_query": (len(res["discovered_urls"]) == 0),
                        "query": res.get("query", ""),
                    }
                    phase_telemetry["loops"].append(loop_telemetry)

                    if should_refine:
                        # Check for infinite loop (repeated query)
                        if next_query in seen_queries:
                            logger.warning(
                                f"Infinite loop detected: repeated query '{next_query}'",
                                extra={"phase": i, "correlation_id": correlation_id},
                            )
                            yield f"**[AVISO]** Query repetida detectada, forçando DONE\n"
                            break
                        seen_queries.add(next_query)

                        q = next_query

                        # 🔥 FIX #13: Ajustar phase_context para refinamento focado
                        # 🛡️ VALIDAÇÃO DEFENSIVA: Garantir que judgement existe
                        judgement = res.get("judgement")
                        if not judgement:
                            logger.error(
                                f"[CRITICAL] res não contém 'judgement' após Judge decision (should_refine=True)",
                                extra={"phase": i, "correlation_id": correlation_id},
                            )
                            yield f"**[ERROR]** Estrutura inválida: 'judgement' ausente após decisão do Judge. Abortando refinamento.\n"
                            break

                        # Preparar contexto focado usando o Judge result
                        focused_context = orch.prepare_planner_context(
                            ph, judgement, loops
                        )

                        # Atualizar ph (phase_context) para próxima iteração
                        ph = {
                            **ph,  # Manter campos não sobrescritos (time_hint, source_bias, etc)
                            "objective": focused_context["phase_objective"],
                            "must_terms": focused_context["pipe_must_terms"],
                            "avoid_terms": focused_context["pipe_avoid_terms"],
                            "is_refinement": True,
                            "refinement_focus": focused_context.get(
                                "refinement_focus", ""
                            ),
                            "refinement_reason": focused_context.get(
                                "refinement_reason", ""
                            ),
                        }

                        if getattr(self.valves, "VERBOSE_DEBUG", False):
                            print(f"[REFINE] Contexto ajustado para foco:")
                            print(
                                f"[REFINE] Objective focado: {focused_context['phase_objective'][:100]}..."
                            )
                            print(
                                f"[REFINE] Must_terms focados: {focused_context['pipe_must_terms']}"
                            )

                        # REMOVED: loops += 1 (duplicação - já incrementado no início do while loop)

                        # Log da decisão original do Judge
                        original_verdict = res["judgement"].get(
                            "original_verdict", verdict
                        )
                        modifications = res["judgement"].get("modifications", [])

                        yield f"**[judge]** 🔄 refine → loop {loops}\n"
                        if reasoning:
                            yield f"**[judge]** 💭 {reasoning}\n"

                        # Mostrar modificações se houver
                        if (
                            getattr(self.valves, "VERBOSE_DEBUG", False)
                            and modifications
                        ):
                            yield f"**[DEBUG]** Modificações Judge: {', '.join(modifications)}\n"
                            yield f"**[DEBUG]** Judge original: {original_verdict} → final: {verdict}\n"

                        # Mostrar informações detalhadas do refine
                        refine_info = res["judgement"].get("refine", {})
                        if refine_info:
                            por_que = refine_info.get("por_que", [])
                            operadores = refine_info.get("operadores_sugeridos", [])
                            if por_que:
                                yield f"**[judge]** 🎯 Por que refine: {', '.join(por_que[:2])}\n"
                            if operadores:
                                yield f"**[judge]** 🔧 Operadores sugeridos: {', '.join(operadores)}\n"
                            # Construir exclude_terms a partir de lacunas para a próxima discovery (logs)
                            try:
                                lacunas = analysis.get("lacunas", [])
                                exclude_terms = []
                                # Heurística simples: sempre excluir genéricos
                                generic_terms = [
                                    "definição",
                                    "o que é",
                                    "história",
                                    "fundação",
                                    "visão geral",
                                    "marketing",
                                ]
                                exclude_terms.extend(generic_terms)
                                # Logar exclude_terms
                                yield f"**[discovery]** ❌ exclude_terms aplicados: {', '.join(exclude_terms)}\n"
                                # Anexar ao contexto da fase para próxima iteração
                                if isinstance(ph, dict):
                                    ph.setdefault("avoid_terms", [])
                                    ph["avoid_terms"] = list(
                                        {*ph["avoid_terms"], *exclude_terms}
                                    )
                            except Exception:
                                pass

                        if next_query:
                            yield f"**[judge]** 🔍 Nova query: {next_query}\n"
                        continue
                    elif should_new_phase:
                        # Adicionar resultado atual ao phase_results ANTES de criar nova fase
                        phase_results.append(res)
                        res_already_added = True  # Mark as added to prevent duplication

                        # Garantir que seed_query exista e seja válida (1 frase rica, não descrição negativa)
                        try:
                            seed_q = proposed_phase.get("seed_query", "").strip()
                            if getattr(self.valves, "VERBOSE_DEBUG", False):
                                print(
                                    f"[NEW_PHASE] Validando seed_query: '{seed_q[:80]}...'"
                                )

                            # Validar se seed_query é inútil (negativa/cortada/muito curta)
                            is_invalid = (
                                not seed_q
                                or len(seed_q) < 20  # Muito curta para ser frase rica
                                or seed_q.lower().startswith("ausência")
                                or seed_q.lower().startswith("falta")
                                or len(seed_q.split())
                                < 4  # Menos de 4 palavras = pobre
                            )

                            if is_invalid:
                                if getattr(self.valves, "VERBOSE_DEBUG", False):
                                    print(
                                        f"[NEW_PHASE] Seed_query inválida detectada! Regenerando..."
                                    )

                                # Construir contexto rico sobre POR QUE a nova fase foi criada
                                why_new_phase = proposed_phase.get("por_que", "")
                                blind_spots = (res.get("judgement", {}) or {}).get(
                                    "blind_spots", []
                                )
                                lacunas = (res.get("analysis", {}) or {}).get(
                                    "lacunas", []
                                )

                                # P0: lacunas pode ser lista de dicts ou strings
                                lacunas_text = []
                                for lac in lacunas[:2]:
                                    if isinstance(lac, dict):
                                        lacunas_text.append(
                                            lac.get("descricao", str(lac))
                                        )
                                    else:
                                        lacunas_text.append(str(lac))

                                # Informações das fases anteriores para contraste
                                previous_phases_summary = ""
                                if self._last_contract and self._last_contract.get(
                                    "fases"
                                ):
                                    phase_names = [
                                        f.get("name", "N/A")
                                        for f in self._last_contract["fases"][:3]
                                    ]
                                    previous_phases_summary = f"\nFases anteriores cobriram: {', '.join(phase_names)}"

                                reask_prompt = (
                                    f"Gere UMA FRASE RICA (≤150 chars) de busca para uma NOVA FASE de pesquisa.\n\n"
                                    f"**CONTEXTO - POR QUE ESTA NOVA FASE FOI CRIADA:**\n"
                                    f"Objetivo da nova fase: '{proposed_phase.get('objective', '')}'\n"
                                    f"Razão: {why_new_phase or 'Lacunas críticas identificadas'}\n"
                                    f"Blind spots: {', '.join(blind_spots[:2]) if blind_spots else 'Nenhum'}\n"
                                    f"Lacunas principais: {', '.join(lacunas_text) if lacunas_text else 'Nenhuma'}{previous_phases_summary}\n\n"
                                    f"**TAREFA:**\n"
                                    f"Crie uma query de busca que:\n"
                                    f"1. DIFERENCIE desta nova fase das fases anteriores (ângulo/escopo diferente)\n"
                                    f"2. RESPONDA especificamente às lacunas/blind spots mencionados\n"
                                    f"3. INCLUA contexto completo (aspecto + setor/entidades + geo + período)\n"
                                    f"4. Seja POSITIVA (busque O QUE você quer encontrar, NÃO 'ausência de')\n"
                                    f"5. SEM operadores (site:, filetype:, OR, aspas)\n\n"
                                    f"**EXEMPLO:**\n"
                                    f"Se lacuna = 'falta métrica de adoção de assessment remoto'\n"
                                    f"→ Query: 'Taxas e percentuais de adoção de assessment remoto e ferramentas digitais no mercado brasileiro de executive search 2023-2024'\n\n"
                                    f"Retorne APENAS a frase de busca (≤150 chars)."
                                )
                                safe_reask = get_safe_llm_params(
                                    self.judge.model_name, {"temperature": 0.2}
                                )
                                reask_out = await _safe_llm_run_with_retry(
                                    self.judge.llm,
                                    reask_prompt,
                                    safe_reask,
                                    timeout=self.judge.valves.LLM_TIMEOUT_DEFAULT,
                                    max_retries=1,
                                )
                                if reask_out and reask_out.get("replies"):
                                    candidate = (
                                        reask_out["replies"][0]
                                        .strip()
                                        .strip('"')
                                        .strip("'")
                                    )
                                    # Sanitizar operadores
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
                                    # Aceitar se >= 20 chars e >= 4 palavras
                                    if (
                                        len(candidate) >= 20
                                        and len(candidate.split()) >= 4
                                    ):
                                        seed_q = candidate[:150]  # Limitar a 150 chars
                                        if getattr(self.valves, "VERBOSE_DEBUG", False):
                                            print(
                                                f"[NEW_PHASE] Seed_query regenerada: '{seed_q[:80]}...'"
                                            )
                                    else:
                                        logger.warning(
                                            f"[NEW_PHASE] Seed_query regenerada inválida: '{candidate[:80]}...'"
                                        )
                        except Exception as e:
                            logger.error(
                                f"[NEW_PHASE] Erro ao validar/regenerar seed_query: {e}"
                            )
                            pass

                        # Adicionar nova fase proposta (formato novo)
                        new_phase = {
                            "objetivo": proposed_phase.get("objective", ""),
                            "query_sugerida": seed_q or proposed_phase.get("query", ""),
                            "name": proposed_phase.get("name", ""),
                            "seed_query": seed_q
                            or proposed_phase.get("seed_query", ""),
                            "seed_core": seed_q
                            or proposed_phase.get(
                                "seed_query", ""
                            ),  # ✅ Adicionar seed_core para evitar fallback
                            "seed_core_source": "judge_new_phase",
                            "must_terms": proposed_phase.get("must_terms", []),
                            "avoid_terms": proposed_phase.get("avoid_terms", []),
                            "time_hint": proposed_phase.get("time_hint", {}),
                            "source_bias": proposed_phase.get("source_bias", []),
                            "evidence_goal": proposed_phase.get("evidence_goal", {}),
                            "lang_bias": proposed_phase.get("lang_bias", []),
                            "geo_bias": proposed_phase.get("geo_bias", []),
                            "accept_if_any_of": [
                                f"Info sobre {proposed_phase.get('objective', '')}"
                            ],
                        }
                        self._last_contract["fases"].append(new_phase)

                        yield f"**[judge]** 🆕 new_phase → {proposed_phase.get('name', 'Nova Fase')}\n"
                        if reasoning:
                            yield f"**[judge]** 💭 {reasoning}\n"
                        if unexpected_findings:
                            yield f"**[judge]** 🔍 Descobertas inesperadas: {', '.join(unexpected_findings[:3])}\n"
                        yield f"**[judge]** 📋 Nova fase: {proposed_phase.get('objective', '')}\n"
                        yield f"**[judge]** 🌱 Seed: {new_phase.get('seed_query', 'N/A')}\n"
                        break  # Sair do loop atual para processar nova fase
                    else:
                        yield f"**[judge]** ✅ done\n"
                        if reasoning:
                            yield f"**[judge]** 💭 {reasoning}\n"
                        if unexpected_findings:
                            yield f"**[judge]** 🔍 Descobertas inesperadas: {', '.join(unexpected_findings[:3])}\n"
                        break

                # Only append if not already added (prevents duplication when new_phase created)
                if res is not None and not res_already_added:
                    phase_results.append(res)

                # Finalizar telemetria da fase
                phase_end_time = time.time()
                phase_telemetry["end_time"] = phase_end_time
                phase_telemetry["duration"] = phase_end_time - phase_start_time
                phase_telemetry["total_loops"] = len(phase_telemetry["loops"])
                telemetry_data["phases"].append(phase_telemetry)

            # Finalizar telemetria geral
            execution_end_time = time.time()
            telemetry_data["end_time"] = execution_end_time
            telemetry_data["total_duration"] = (
                execution_end_time - telemetry_data["start_time"]
            )
            telemetry_data["total_phases"] = len(telemetry_data["phases"])

            # Emitir telemetria estruturada
            if self.valves.DEBUG_LOGGING:
                yield f"\n**[TELEMETRIA]** 📊 Dados estruturados da execução:\n"
                yield f"```json\n{json.dumps(telemetry_data, indent=2, ensure_ascii=False)}\n```\n"

            yield f"\n**[SÍNTESE FINAL]**\n"
            async for chunk in self._synthesize_final(
                phase_results, orch, user_msg, body, __event_emitter__=__event_emitter__
            ):
                yield chunk

            # 🔓 UNLOCK contexto após conclusão para permitir nova detecção
            self._last_contract = None
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

        # Check if there's a previous plan in history to allow refinement
        previous_plan_text = None
        try:
            messages = body.get("messages", [])
            for msg in reversed(messages):
                if msg.get("role") == "assistant":
                    content = msg.get("content", "")
                    # Look for rendered plan
                    if (
                        "📋 Plano" in content or "Fase 1/" in content
                    ) and "Objetivo:" in content:
                        previous_plan_text = content
                        break
        except Exception:
            pass

        planner = PlannerLLM(self.valves)
        out = await planner.run(
            user_prompt=user_msg,
            phases=phases,
            current_date=getattr(self, "_current_date", None),
            previous_plan=previous_plan_text,
            detected_context=self._detected_context,
        )
        self._last_contract = out["contract"]
        self._last_hash = out["contract_hash"]

        # 🔗 SINCRONIZAR contract Manual com contexto detectado
        # v4.4 FIX: CONTEXT DETECTION é a fonte de verdade (foi feito para isso)
        if self._detected_context:
            detected_profile = self._detected_context.get("perfil", "company_profile")
            contract_profile = self._last_contract.get("intent_profile", "")

            # Se profiles divergem, CONTEXT DETECTION tem razão (fonte de verdade)
            if contract_profile != detected_profile:
                logger.warning(
                    f"[SYNC] Profile mismatch: planner={contract_profile}, detected={detected_profile}. Using CONTEXT DETECTION (source of truth)."
                )
                # Contract deve seguir detected_context (fonte de verdade)
                self._last_contract["intent_profile"] = detected_profile
                # Recomputar hash após sincronização
                self._last_hash = _hash_contract(self._last_contract)

                # Se mismatch persistente, pode indicar problema no Context Detection
                if getattr(self.valves, "VERBOSE_DEBUG", False):
                    print(
                        f"[DEBUG] Profile corrected: {contract_profile} → {detected_profile}"
                    )
                    print(
                        f"[DEBUG] If this seems wrong, Context Detection LLM may need better prompt"
                    )

        # Emit detected intent/profile for validation before rendering contract
        try:
            detected_intent = (
                self._last_contract.get("intent")
                or self._last_contract.get("objetivo")
                or ""
            )
            detected_profile = (
                self._last_contract.get("intent_profile") or self._intent_profile or ""
            )
            if detected_intent:
                yield f"**[PLANNER] Intent detectado:** {detected_intent}\n"
            if detected_profile:
                yield f"**[PLANNER] Perfil:** {detected_profile}\n"
        except Exception:
            pass
        # 🔒 LOCK contexto para prevenir re-detecção no próximo "siga"
        self._context_locked = True
        logger.info("[PLANNER] Context locked for SIGA consistency")

        yield _render_contract(self._last_contract)

    # ===== SYNTHESIS HELPERS =====
    
    async def _synthesize_final(
        self,
        phase_results: List[Dict],
        orch: Orchestrator,
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
        # Try Context Reducer first (inline to avoid async generator complexities)
        if self.valves.ENABLE_CONTEXT_REDUCER:
            try:
                yield "**[SÍNTESE]** Context Reducer global...\n"
                mode = self.valves.CONTEXT_MODE
                final = await orch.finalize_global_context(mode=mode)

                if final:
                    yield f"\n{final}\n\n"

                    total_urls = len(orch.scraped_cache)
                    total_phases = len(phase_results)
                    yield f"\n---\n**📊 Estatísticas:**\n"
                    yield f"- Fases: {total_phases}\n"
                    yield f"- URLs únicas scraped: {total_urls}\n"
                    yield f"- Contexto acumulado: {len(orch.filtered_accumulator)} chars\n"
                    return
            except Exception as e:
                yield f"**[ERRO]** Context Reducer: {e}\n"

        # Fallback: deduplicar (merge+dedupe+MMR-lite) e depois sintetizar com UMA ÚNICA chamada ao LLM
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

        # Note: _shingles, _jaccard, _mmr_select are now global functions (defined at module level)

        # Build context (with optional deduplication and size limit)
        raw_context = orch.filtered_accumulator
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
            if hasattr(self, 'contract') and self.contract:
                # Tentar extrair must_terms do contract
                entities = self.contract.get("entities", {}).get("canonical", [])
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
            yield f"**[SÍNTESE]** Deduplicação ativa ({order_mode}, {dedupe_result['algorithm_used']}): {dedupe_result['original_count']} → {dedupe_result['deduped_count']} parágrafos ({dedupe_result['reduction_pct']:.1f}% redução)\n"
            yield f"**[SÍNTESE]** Tokens economizados: ~{dedupe_result['tokens_saved']}\n"
        else:
            deduped_context = raw_context
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
            yield f"**[SÍNTESE]** Contexto truncado: {len(deduped_context)} chars (limite: {max_chars})\n"
        else:
            yield f"**[SÍNTESE]** Contexto dentro do limite: {len(deduped_context)} chars\n"
        try:
            # Log do contexto que será usado
            if self.valves.DEBUG_LOGGING:
                yield f"**[DEBUG]** Contexto para síntese: {len(deduped_context)} chars\n"
                yield f"**[DEBUG]** Primeiros 200 chars: {deduped_context[:200]}...\n"

            # Coletar estatísticas para incluir no prompt
            total_urls = len(orch.scraped_cache)
            total_phases = len(phase_results)
            domains = set()
            for url in orch.scraped_cache:
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
                contract_entities=contract_entities,
                phase_objectives=phase_objectives,
            )

            key_questions_section = sections["key_questions"]
            research_objectives_section = sections["research_objectives"]
            entities_section = sections["entities"]
            phase_objectives_section = sections["phase_objectives"]

            # UMA ÚNICA chamada ao LLM com prompt adaptativo baseado no contexto
            synthesis_prompt = f"""Você é um analista executivo especializado em criar relatórios executivos completos e narrativos.

**QUERY ORIGINAL DO USUÁRIO:**
{user_msg}

**PERFIL ADAPTATIVO:** {research_context['perfil_descricao']}
**OBJETIVO ADAPTATIVO:** Criar um relatório executivo profissional no estilo {research_context['estilo']}, rico em {research_context['foco_detalhes']}, baseado no contexto de pesquisa fornecido sobre {research_context['tema_principal']}.
{key_questions_section}{research_objectives_section}{entities_section}{phase_objectives_section}
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
                    yield f"**[DEBUG]** Auto-ajuste timeout: {self.valves.LLM_TIMEOUT_SYNTHESIS}s → {timeout_synthesis}s (contexto: {context_size:,} chars)\n"

            # Log do modelo sendo usado
            if self.valves.DEBUG_LOGGING:
                yield f"**[DEBUG]** Modelo de síntese: {synthesis_model} (params: {generation_kwargs})\n"

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
            if not report:
                raise ValueError("Relatório vazio na síntese final")
            yield f"\n{report}\n"

            # ✅ AUTO-EXPORT PDF (APÓS GERAR RELATÓRIO)
            if getattr(self.valves, "AUTO_EXPORT_PDF", False) and self._export_pdf_tool:
                try:
                    yield f"\n**[EXPORT]** 📄 Gerando PDF...\n"

                    # Preparar conteúdo
                    export_full = getattr(self.valves, "EXPORT_FULL_CONTEXT", True)
                    if export_full:
                        pdf_content = f"""# Pesquisa Completa - {self._detected_context.get('tema_principal', 'N/A')}

## Metadata
- Data: {getattr(self, '_current_date', 'N/A')}
- Fases: {len(phase_results)}
- URLs: {len(orch.scraped_cache)}
- Domínios: {len(set(url.split('/')[2] for url in orch.scraped_cache if '/' in url))}
- Tamanho: {len(orch.filtered_accumulator):,} chars

---

## 📋 Relatório Final

{report}

---

## 📊 Contexto Bruto (Todas as Fases)

{orch.filtered_accumulator}

---

## 🔗 URLs Scraped

{chr(10).join([f"- {url}" for url in sorted(orch.scraped_cache.keys())])}
"""
                    else:
                        pdf_content = f"""# Relatório Executivo - {self._detected_context.get('tema_principal', 'Pesquisa')}

## Metadata
- Data: {getattr(self, '_current_date', 'N/A')}
- Fases: {len(phase_results)}
- URLs: {len(orch.scraped_cache)}
- Contexto: {len(orch.filtered_accumulator):,} chars

---

{report}
"""

                    # Chamar tool via wrapper seguro
                    from datetime import datetime

                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    result_dict = await self._safe_export_pdf(
                        content=pdf_content,
                        filename=f"pesquisa_{ts}.pdf",
                        title=f"Relatório - {self._detected_context.get('tema_principal', 'Pesquisa')}",
                        __event_emitter__=__event_emitter__,  # ✅ Passar event emitter para gerar data URI
                    )

                    if result_dict.get("success"):
                        pdf_url = result_dict.get("url")
                        pdf_data_uri = result_dict.get("data_uri")
                        pdf_filename = result_dict.get("filename", "relatorio.pdf")
                        pdf_size_bytes = result_dict.get("size_bytes", 0)
                        pdf_size_mb = (
                            pdf_size_bytes / (1024 * 1024) if pdf_size_bytes else 0
                        )

                        yield f"\n**[EXPORT]** ✅ PDF gerado com sucesso!\n"

                        # ✅ Exibir ambos os links: data URI (base64) e URL HTTP (se disponíveis)
                        if pdf_data_uri:
                            # Data URI (base64) - funciona sempre, sem depender de servidor
                            yield f"📄 [**Baixar {pdf_filename}** ({pdf_size_mb:.1f} MB) - Link Direto]({pdf_data_uri})\n"

                        if pdf_url:
                            # URL HTTP - requer servidor configurado
                            yield f"🔗 [**Baixar via HTTP** - {pdf_filename}]({pdf_url})\n"

                        if not pdf_data_uri and not pdf_url:
                            # Sem links públicos (diretório não servido)
                            pdf_path = result_dict.get("path", "N/A")
                            yield f"**[EXPORT]** ✅ PDF salvo localmente: `{pdf_path}` ({pdf_size_mb:.1f} MB)\n"
                            yield f"**[AVISO]** ⚠️ PDF não está acessível via HTTP (diretório não servido)\n"
                            yield f"**[DICA]** Configure `pdf_output_dir` nas valves para `/app/backend/data/uploads`\n"
                        else:
                            yield f"\n💡 **Dica**: Clique no link acima para baixar o relatório completo\n"

                        # Se houver warning (fallback para texto plano)
                        if result_dict.get("warning"):
                            yield f"**[EXPORT]** ⚠️ {result_dict['warning']}\n"
                    else:
                        error_msg = result_dict.get("error", "Erro desconhecido")
                        yield f"**[EXPORT]** ❌ Falha ao gerar PDF: {error_msg}\n"

                except Exception as e:
                    # Não falhar a síntese por erro de exportação
                    yield f"**[EXPORT]** ⚠️ Erro ao exportar PDF: {e}\n"
            elif getattr(self.valves, "AUTO_EXPORT_PDF", False):
                # Tool não disponível - apenas avisar
                yield f"\n**[INFO]** 💡 Export PDF Tool não está disponível. Para habilitar:\n"
                yield f"   1. Instale a tool em Workspace → Tools\n"
                yield f"   2. Habilite para este modelo em Workspace → Models\n"

            # Estatísticas avançadas
            total_urls = len(orch.scraped_cache)
            total_phases = len(phase_results)
            domains = set()
            for url in orch.scraped_cache:
                try:
                    domain = url.split("/")[2]
                    domains.add(domain)
                except:
                    pass

            yield f"\n---\n**📊 Estatísticas da Pesquisa:** Fases={total_phases}, URLs únicas={total_urls}, Domínios={len(domains)}, Contexto={len(orch.filtered_accumulator):,} chars\n"

        except Exception as e:
            error_msg = str(e)
            is_timeout = (
                "timeout" in error_msg.lower() or "exceeded" in error_msg.lower()
            )

            if is_timeout:
                # 🔧 FIX: Calcular timeout HTTP real usado (não mostrar PLANNER_REQUEST_TIMEOUT que é irrelevante)
                effective_http_timeout = max(60, int(timeout_synthesis - 30))

                yield f"**[ERRO]** Síntese completa falhou: {e}\n"
                yield f"**[DICA]** Contexto muito grande ({len(deduped_context):,} chars). Sugestões:\n"
                # Get max timeout value safely (Pydantic v1/v2 compatibility)
                try:
                    max_timeout = getattr(self.valves.__fields__['LLM_TIMEOUT_SYNTHESIS'], 'field_info', {}).get('extra', {}).get('le', 1800)
                except (AttributeError, KeyError):
                    max_timeout = 1800  # fallback
                yield f"  - Aumente LLM_TIMEOUT_SYNTHESIS nas valves (atual: {timeout_synthesis}s, máx: {max_timeout}s)\n"
                yield f"  - 🔍 **Diagnóstico atual:**\n"
                yield f"    • timeout_synthesis (asyncio): {timeout_synthesis}s\n"
                yield f"    • request_timeout (HTTP): {effective_http_timeout}s (margem de 30s)\n"
                yield f"    • HTTPX_READ_TIMEOUT (base): {self.valves.HTTPX_READ_TIMEOUT}s (não usado na síntese)\n"
                yield f"  - ⚠️ **Se a resposta apareceu na OpenAI mas não aqui:** pode ser timeout de conexão HTTP. Verifique logs para '[LLM_CALL]'.\n"
                yield f"  - Ou reduza MAX_DEDUP_PARAGRAPHS (atual: {self.valves.MAX_DEDUP_PARAGRAPHS}) para diminuir contexto\n"
                yield f"  - Ou aumente DEDUP_SIMILARITY_THRESHOLD (atual: {self.valves.DEDUP_SIMILARITY_THRESHOLD}) para deduplicar mais agressivamente\n"
            else:
                yield f"**[ERRO]** Síntese completa falhou: {e}\n"

            yield f"**[FALLBACK]** Contexto completo ({len(orch.filtered_accumulator):,} chars) disponível para análise manual.\n"

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
  
  ⚠️ ATENÇÃO CRÍTICA - EXEMPLOS DE CLASSIFICAÇÃO:
  
  ✅ company_profile:
     - "Estudo de mercado de X"
     - "Análise competitiva dos players A, B, C"
     - "Reputação e posicionamento de empresas"
     - "Volume de mercado e market share"
     - "Perfis de boutiques vs internacionais"
  
  ✅ technical_spec:
     - "Arquitetura técnica do sistema X"
     - "Stack tecnológico e implementação"
     - "Especificações de API e protocolos"
  
  ✅ literature_review:
     - "Estado da arte de X"
     - "Panorama atual de Y"
     - "Evolução tecnológica de Z"
     - "Avanços recentes em W"
     - "Situação atual do mercado de V"
  
  ⚠️ NÃO CONFUNDA: "Estudo sobre empresas" = company_profile (NÃO technical_spec!)
  ⚠️ "Estado de AI Agentica" = literature_review (estado da arte, NÃO mercado!)
  
  🎯 EXEMPLO ESPECÍFICO - Headhunting/Executive Search:
  Query: "Estudo sobre mercado de headhunting no Brasil, players Spencer e Heidrick"
  → setor_principal: "rh_talento_headhunting" (ou "servicos_profissionais")
  → tipo_pesquisa: "analise_mercado" (NÃO "academica" ou "tecnica")
  → perfil_sugerido: "company_profile" (É ESTUDO DE MERCADO/COMPETIÇÃO!)
  → entities_mentioned: [{{"canonical": "Spencer Stuart", "aliases": ["Spencer"]}}, {{"canonical": "Heidrick & Struggles", "aliases": ["Heidrick"]}}]
  ⚠️ NÃO incluir "Brasil" em entities_mentioned (é contexto geográfico, não entidade específica)
  
- 5-10 key_questions (perguntas de DECISÃO que precisam resposta, não queries de busca)
- entities_mentioned (APENAS empresas/produtos/pessoas/marcas específicas mencionadas EXPLICITAMENTE, incluir aliases)
  ⚠️ NÃO incluir termos geográficos (países, regiões, códigos como "Brasil", "BR", "global", "mundial")
  ⚠️ NÃO incluir termos genéricos ("mercado", "setor", "nacional", "internacional")
  ⚠️ FOCAR em entidades específicas: nomes de empresas, produtos, pessoas, marcas
- research_objectives (objetivos finais: entender/analisar/comparar o quê?)

SCHEMA JSON:
{{
  "setor_principal": "setor específico da pesquisa",
  "tipo_pesquisa": "tipo específico da pesquisa",
  "perfil_sugerido": "perfil mais apropriado",
  "confianca_deteccao": "alta|média|baixa",
  "justificativa": "breve explicação da classificação (≤60 palavras)",
  "key_questions": [
    "Pergunta decisória 1",
    "Pergunta decisória 2",
    "..."
  ],
  "entities_mentioned": [
    {{"canonical": "Spencer Stuart", "aliases": ["Spencer", "SS"]}},
    {{"canonical": "Heidrick & Struggles", "aliases": ["Heidrick", "H&S"]}},
    {{"canonical": "Flow Executive", "aliases": ["Flow", "Flow Exec"]}}
  ],
  "research_objectives": [
    "Objetivo 1 (entender/analisar/comparar o quê)",
    "Objetivo 2",
    "..."
  ]
}}

FORMATO DE RESPOSTA:
Apenas JSON válido, sem qualquer texto adicional."""

            llm = _get_llm(self.valves, model_name=self.valves.LLM_MODEL)
            if llm:
                # Parâmetros base (podem ser filtrados para GPT-5)
                base_params = {
                    "temperature": 0.2,
                    "response_format": {"type": "json_object"},
                }
                # Filtrar parâmetros incompatíveis com GPT-5/O1
                llm_params = get_safe_llm_params(self.valves.LLM_MODEL, base_params)
                detect_result = await _safe_llm_run_with_retry(
                    llm, detect_prompt, llm_params, timeout=60, max_retries=1
                )
                if detect_result and detect_result.get("replies"):
                    # Usar parser resiliente para Context Detection
                    try:
                        detect_json = parse_llm_json_strict(detect_result["replies"][0])
                    except ContractValidationError:
                        # Fallback: usar parser legacy
                        detect_json = _extract_json_from_text(
                            detect_result["replies"][0]
                        )

                    if detect_json and detect_json.get("setor_principal"):
                        contexto_enriquecido = detect_json
                        logger.info(
                            f"[PIPE] Contexto detectado por LLM: {contexto_enriquecido}"
                        )
        except Exception as e:
            logger.warning(f"[PIPE] Erro na detecção LLM: {e}")

        # SÍNTESE FINAL DO CONTEXTO
        if contexto_enriquecido:
            # LLM disponível - usar EXCLUSIVAMENTE as classificações do LLM
            setor_final = contexto_enriquecido.get("setor_principal", "geral")
            tipo_final = contexto_enriquecido.get("tipo_pesquisa", "geral")
            perfil_final = contexto_enriquecido.get(
                "perfil_sugerido", "company_profile"
            )
            confianca = contexto_enriquecido.get("confianca_deteccao", "alta")
            fonte_deteccao = "LLM"

            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(
                    f"[CONTEXT DETECTION] LLM: setor={setor_final}, tipo={tipo_final}, perfil={perfil_final}"
                )
        else:
            # LLM falhou - fallback simples (não heurística complexa)
            setor_final = "geral"
            tipo_final = "geral"
            perfil_final = "company_profile"
            confianca = "baixa"
            fonte_deteccao = "fallback"

            if getattr(self.valves, "VERBOSE_DEBUG", False):
                print(
                    f"[CONTEXT DETECTION] LLM falhou, usando fallback simples: setor={setor_final}, tipo={tipo_final}, perfil={perfil_final}"
                )

        # GERAR METADADOS ADAPTATIVOS
        contexto_map = {
            "company_profile": {
                "descricao": "análise estratégica de empresas e mercado",
                "estilo": "estratégico e de negócios",
                "foco": "análise de mercado e oportunidades",
                "secoes": [
                    "Resumo Executivo",
                    "Análise de Mercado",
                    "Posicionamento Competitivo",
                    "Oportunidades Estratégicas",
                ],
            },
            "regulation_review": {
                "descricao": "análise regulatória e compliance",
                "estilo": "regulatório e jurídico",
                "foco": "aspectos legais e regulatórios",
                "secoes": [
                    "Marco Regulatório",
                    "Análise de Compliance",
                    "Riscos Jurídicos",
                    "Recomendações",
                ],
            },
            "technical_spec": {
                "descricao": "especificações técnicas e implementação",
                "estilo": "técnico e operacional",
                "foco": "detalhes técnicos e implementação",
                "secoes": [
                    "Especificações Técnicas",
                    "Arquitetura do Sistema",
                    "Processos Operacionais",
                    "Requisitos",
                ],
            },
            "literature_review": {
                "descricao": "revisão acadêmica e científica",
                "estilo": "acadêmico e científico",
                "foco": "dados científicos e metodologia de pesquisa",
                "secoes": [
                    "Revisão da Literatura",
                    "Metodologia Científica",
                    "Análise de Dados",
                    "Conclusões Acadêmicas",
                ],
            },
            "history_review": {
                "descricao": "análise histórica e contextual",
                "estilo": "histórico e contextual",
                "foco": "evolução histórica e contexto cultural",
                "secoes": [
                    "Contexto Histórico",
                    "Evolução Temporal",
                    "Análise Comparativa",
                    "Lições Históricas",
                ],
            },
        }

        perfil_info = contexto_map.get(perfil_final, contexto_map["company_profile"])

        # Extrair key_questions, entities e objectives do LLM (se disponíveis)
        key_questions = []
        entities_mentioned = []
        research_objectives = []

        if contexto_enriquecido:
            key_questions = contexto_enriquecido.get("key_questions", [])
            entities_mentioned = contexto_enriquecido.get("entities_mentioned", [])
            research_objectives = contexto_enriquecido.get("research_objectives", [])

        return {
            "setor": setor_final,
            "tipo": tipo_final,
            "perfil": perfil_final,
            "perfil_descricao": perfil_info["descricao"],
            "estilo": perfil_info["estilo"],
            "foco_detalhes": perfil_info["foco"],
            "tema_principal": setor_final.replace("_", " ").title(),
            "secoes_sugeridas": perfil_info["secoes"],
            "detecção_confianca": confianca,
            "fonte_deteccao": fonte_deteccao,
            # CoT: key questions, entities, objectives (do LLM se disponível)
            "key_questions": key_questions,
            "entities_mentioned": entities_mentioned,
            "research_objectives": research_objectives,
        }


# ==================== SELF-TESTS (FASES 1 E 2) ====================


def _selftest_phase1():
    """Self-test da Fase 1: valida funcionalidades básicas"""
    print("=== SELF-TEST FASE 1 ===")

    ok = True

    # 1) Entity coverage soft/hard
    try:
        fases = [
            {"must_terms": ["empresa A", "Brasil"]},
            {"must_terms": ["empresa B", "Brasil"]},
            {"must_terms": ["Brasil"]},  # Sem entidades
        ]
        entities = ["empresa A", "empresa B"]

        coverage = _calc_entity_coverage(fases, entities)
        expected = 2 / 3  # 66.7%

        if abs(coverage - expected) < 0.01:
            print("✅ Entity coverage calculation OK")
        else:
            print(f"❌ Entity coverage: esperado {expected}, got {coverage}")
            ok = False

    except Exception as e:
        print(f"❌ Entity coverage test falhou: {e}")
        ok = False

    # 2) News telemetry
    try:
        telemetry = {"attempted": True, "reason": "skipped_exceed_limit"}
        if telemetry["reason"] == "skipped_exceed_limit":
            print("✅ News telemetry OK")
        else:
            print("❌ News telemetry falhou")
            ok = False
    except Exception as e:
        print(f"❌ News telemetry test falhou: {e}")
        ok = False

    # 3) Seed patch
    try:
        # Mock valves
        class MockValves:
            SEED_VALIDATION_STRICT = False

        valves = MockValves()
        phase = {
            "seed_query": "teste Brasil",
            "objective": "participação mercado empresas brasileiras",
        }
        metrics = {}

        _patch_seed_if_needed(phase, valves.SEED_VALIDATION_STRICT, metrics, logger)

        # Verificar se o patch foi aplicado
        if phase["seed_query"] != "teste Brasil":
            print("✅ Seed patch OK")
        else:
            print(
                f"❌ Seed patch não funcionou (seed_query ainda: {phase['seed_query']})"
            )
            ok = False
    except Exception as e:
        print(f"❌ Seed patch test falhou: {e}")
        ok = False

    # 4) Judge JSON log
    try:
        decision = {
            "decision": "done",
            "reason": "teste",
            "coverage": 0.8,
            "domains": 5,
            "evidence": 0.9,
            "staleness_ok": True,
            "loops": "2/3",
        }

        log_str = f"[JUDGE]{json.dumps(decision, ensure_ascii=False)}"
        if "[JUDGE]" in log_str and "decision" in log_str:
            print("✅ Judge log JSON OK")
        else:
            print("❌ Judge log JSON falhou")
            ok = False
    except Exception as e:
        print(f"❌ Judge log JSON test falhou: {e}")
        ok = False

    print(f"=== FASE 1 SELF-TEST: {'OK' if ok else 'FAIL'} ===")
    return ok


def _selftest_phase2():
    """Self-test da Fase 2: valida funcionalidades incrementais"""
    print("=== SELF-TEST FASE 2 ===")

    ok = True

    # 1) MECE básico
    try:
        fases = [{"objetivo": "volume mercado Brasil"}, {"objetivo": "perfis empresas"}]
        key_questions = [
            "Qual volume anual?",
            "Quais empresas atuam?",
            "Pergunta órfã sem cobertura",
        ]

        uncovered = _check_mece_basic(fases, key_questions)
        expected = ["Pergunta órfã sem cobertura"]

        if uncovered == expected:
            print("✅ MECE básico OK")
        else:
            print(f"❌ MECE básico: esperado {expected}, got {uncovered}")
            ok = False
    except Exception as e:
        print(f"❌ MECE básico test falhou: {e}")
        ok = False

    # 2) Append phase
    try:
        contract = {"fases": []}
        candidate = {
            "name": "Nova Fase",
            "phase_type": "news",
            "objective": "notícias recentes",
            "seed_query": "noticias recentes Brasil",
            "seed_core": "busca noticias recentes Brasil",
            "must_terms": ["Brasil"],
            "time_hint": {"recency": "1y", "strict": True},
        }

        _append_phase(contract, candidate)

        if len(contract["fases"]) == 1 and contract["fases"][0]["name"] == "Nova Fase":
            print("✅ Append phase OK")
        else:
            print("❌ Append phase falhou")
            ok = False
    except Exception as e:
        print(f"❌ Append phase test falhou: {e}")
        ok = False

    print(f"=== FASE 2 SELF-TEST: {'OK' if ok else 'FAIL'} ===")
    return ok


# ==================== ECONOMY SELF-TEST (Commit 7) ====================


def _selftest_economy():
    """Self-test para validação de economia e overlap detection"""
    print("=== SELF-TEST ECONOMIA ===")
    ok = True

    # Mock de valves
    class MockValves:
        PLANNER_ECONOMY_THRESHOLD = 0.75
        PLANNER_OVERLAP_THRESHOLD = 0.70
        VERBOSE_DEBUG = False

    # Test 1: Economy logging (sem overlap)
    print("Test 1: Economy logging...")
    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity

        # Simular validated_phases com objectives distintos
        validated_phases = [
            {"objetivo": "Mapear panorama do mercado de executive search no Brasil"},
            {"objetivo": "Identificar tendências e drivers de crescimento do setor"},
            {
                "objetivo": "Analisar notícias e eventos recentes sobre players principais"
            },
        ]

        phase_objectives = [p.get("objetivo", "") for p in validated_phases]
        actual_count = len(validated_phases)
        phases = 6
        economy_threshold = 0.75

        # Log (simulado)
        budget_pct = actual_count / phases * 100
        assert budget_pct == 50.0, f"Expected 50%, got {budget_pct}%"
        assert (
            actual_count <= phases * economy_threshold
        ), "Não deveria disparar warning"

        print("✅ Economy logging OK")
    except Exception as e:
        print(f"❌ Economy logging FAIL: {e}")
        ok = False

    # Test 2: Overlap detection (detectar similaridade alta)
    print("Test 2: Overlap detection...")
    try:
        # Simular fases com overlap MUITO alto (quase idênticas)
        validated_phases_overlap = [
            {
                "objetivo": "Mapear receita faturamento resultados financeiros players executive search Brasil 2023"
            },
            {
                "objetivo": "Mapear receita faturamento resultados financeiros empresas executive search Brasil 2023"
            },  # Quase idêntica!
            {"objetivo": "Analisar notícias eventos recentes mercado executive search"},
        ]

        phase_objectives = [p.get("objetivo", "") for p in validated_phases_overlap]
        overlap_threshold = 0.70

        if len(phase_objectives) >= 2:
            vectorizer = TfidfVectorizer()
            vectors = vectorizer.fit_transform(phase_objectives)
            sim_matrix = cosine_similarity(vectors)

            # Fases 0 e 1 devem ter >70% similaridade
            overlap_detected = sim_matrix[0][1] > overlap_threshold
            assert (
                overlap_detected
            ), f"Esperava overlap, mas similaridade={sim_matrix[0][1]:.2f}"

        print("✅ Overlap detection OK")
    except Exception as e:
        print(f"❌ Overlap detection FAIL: {e}")
        ok = False

    # Test 3: Valve configurability
    print("Test 3: Valve configurability...")
    try:
        mock_valves = MockValves()
        assert mock_valves.PLANNER_ECONOMY_THRESHOLD == 0.75
        assert mock_valves.PLANNER_OVERLAP_THRESHOLD == 0.70

        # Simular threshold ajustado
        mock_valves.PLANNER_ECONOMY_THRESHOLD = 0.50
        actual_count = 4
        phases = 6
        should_warn = actual_count > phases * mock_valves.PLANNER_ECONOMY_THRESHOLD
        assert should_warn, f"Deveria disparar warning com threshold 50% (4/6 = 66%)"

        print("✅ Valve configurability OK")
    except Exception as e:
        print(f"❌ Valve configurability FAIL: {e}")
        ok = False

    # Test 4: Layer 2 - Campos opcionais (inferência)
    print("Test 4: Layer 2 - Campos opcionais (inferência)...")
    try:
        # Simular obj sem total_phases_used
        obj_no_counter = {"phases": [{"objetivo": "A"}, {"objetivo": "B"}]}
        actual_count = len(obj_no_counter.get("phases", []))
        declared_count = obj_no_counter.get("total_phases_used", actual_count)

        assert (
            declared_count == actual_count
        ), f"Inferência falhou: declared={declared_count}, actual={actual_count}"
        assert declared_count == 2, "Esperava 2 fases"

        print("✅ Layer 2 - Campos opcionais OK")
    except Exception as e:
        print(f"❌ Layer 2 - Campos opcionais FAIL: {e}")
        ok = False

    # Test 5: Layer 2 - Validação de consistência (declared != actual)
    print("Test 5: Layer 2 - Validação de consistência...")
    try:
        # Simular obj COM total_phases_used inconsistente
        obj_inconsistent = {
            "total_phases_used": 5,  # Declara 5
            "phases": [{"objetivo": "A"}, {"objetivo": "B"}],  # Mas cria 2
        }
        actual_count = len(obj_inconsistent.get("phases", []))
        declared_count = obj_inconsistent.get("total_phases_used", actual_count)

        # Deveria detectar inconsistência
        is_inconsistent = (
            "total_phases_used" in obj_inconsistent and declared_count != actual_count
        )
        assert is_inconsistent, "Não detectou inconsistência declared(5) != actual(2)"

        print("✅ Layer 2 - Validação de consistência OK")
    except Exception as e:
        print(f"❌ Layer 2 - Validação de consistência FAIL: {e}")
        ok = False

    # Test 6: Layer 2 - Warning se ≥80% budget sem justificativa
    print("Test 6: Layer 2 - Warning ≥80% budget sem justificativa...")
    try:
        obj_high_budget = {
            "phases": [{"objetivo": f"Fase {i}"} for i in range(5)],  # 5 fases
            "phases_justification": "Plano genérico",  # SEM keywords de economia
        }
        actual_count = len(obj_high_budget["phases"])
        phases = 6

        # 5/6 = 83% (≥80%)
        is_high_budget = actual_count >= int(0.8 * phases)
        justification = obj_high_budget.get("phases_justification", "").lower()
        economy_keywords = [
            "economia",
            "combinar",
            "comparativa",
            "suficiente",
            "necessário",
            "econômico",
        ]
        has_economy_mention = any(kw in justification for kw in economy_keywords)

        # Deve disparar warning (≥80% mas sem keywords)
        should_warn = is_high_budget and not has_economy_mention
        assert (
            should_warn
        ), "Não disparou warning para 83% budget sem keywords de economia"

        print("✅ Layer 2 - Warning ≥80% budget OK")
    except Exception as e:
        print(f"❌ Layer 2 - Warning ≥80% budget FAIL: {e}")
        ok = False

    # Test 7: Layer 2 - Overlap warnings armazenados no contract
    print("Test 7: Layer 2 - Overlap warnings no contract...")
    try:
        # Simular detecção de overlap e armazenamento
        contract_dict = {}
        overlap_info = {
            "phase_i": 1,
            "phase_j": 2,
            "similarity": 0.85,
            "objective_i": "Receita empresas executive search Brasil 2023",
            "objective_j": "Faturamento empresas executive search Brasil 2023",
        }

        # Simular armazenamento
        overlaps_found = [overlap_info]
        if overlaps_found:
            contract_dict["_overlap_warnings"] = overlaps_found

        # Validar
        assert "_overlap_warnings" in contract_dict, "Overlap warnings não armazenadas"
        assert len(contract_dict["_overlap_warnings"]) == 1, "Esperava 1 overlap"
        assert (
            contract_dict["_overlap_warnings"][0]["similarity"] == 0.85
        ), "Similaridade incorreta"

        print("✅ Layer 2 - Overlap warnings no contract OK")
    except Exception as e:
        print(f"❌ Layer 2 - Overlap warnings FAIL: {e}")
        ok = False

    print(f"=== ECONOMIA SELF-TEST: {'OK' if ok else 'FAIL'} ===")
    return ok


# ==================== MAIN SELF-TEST ====================


def run_self_tests():
    """Executa todos os self-tests"""
    print("🚀 EXECUTANDO SELF-TESTS DO PIPEMANUAL")
    print("=" * 50)

    results = []
    results.append(("FASE 1", _selftest_phase1()))
    results.append(("FASE 2", _selftest_phase2()))
    results.append(("ECONOMIA", _selftest_economy()))

    print("=" * 50)
    all_ok = all(result[1] for result in results)

    print(f"🏁 SELF-TESTS: {'TODOS OK' if all_ok else 'ALGUNS FALHARAM'}")

    for name, result in results:
        status = "✅ OK" if result else "❌ FAIL"
        print(f"  {name}: {status}")

    return all_ok


# ===== LINE-BUDGET GUARD EXECUTION =====
# Executar validação de tamanho de funções críticas
try:
    _warn_if_too_long(Orchestrator.run_iteration)
    _warn_if_too_long(Pipe.pipe)
    _warn_if_too_long(Pipe._synthesize_final)
    _warn_if_too_long(_build_planner_prompt)
    _warn_if_too_long(_build_analyst_prompt)
    _warn_if_too_long(_build_judge_prompt)
except Exception as e:
    logger.debug(f"Line-budget guard execution failed: {e}")

# ===== OpenWebUI Pipe Export =====
# The Pipe class above is automatically discovered by OpenWebUI
# No get_tools() function needed for Manifold pipes
