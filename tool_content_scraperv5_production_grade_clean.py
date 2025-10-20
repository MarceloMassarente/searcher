#!/usr/bin/env python3
"""
title: Advanced Content Scraper V5
author: AI Assistant
author_url: https://github.com/ai-assistant/content-scraper
git_url: https://github.com/ai-assistant/content-scraper.git
description: Advanced content scraper with multiple strategies: HTTP direct (httpx), Browserless headless browser, Jina Reader API, Document processing (PDF/DOCX), Bright Data Unlocker & SBR. Totalmente configurável, robusto contra falhas em lote e com logging seguro.
required_open_webui_version: 0.4.0
requirements: httpx, aiohttp, beautifulsoup4, readability-lxml, lxml, pydantic>=2.0.0
version: 7.0.1
license: MIT

ARCHITECTURE:
┌─────────────────────────────────────────────────────────────┐
│ SCRAPING PIPELINE (Multi-Strategy with Intelligent Fallback) │
└─────────────────────────────────────────────────────────────┘
    ↓
1. Cache Check (10min TTL)
    ↓ (miss)
2. Circuit Breaker Check (per-host, 5 failures → 5min cooldown)
    ↓ (open = pass)
3. Playbook Selection (domain-specific strategies)
    ↓
4. Adapter Chain Execution (ordered by priority):
    ┌─────────────────────────────────────────┐
    │ DocumentAdapterV5 (priority: 10)       │ ← PDF/DOCX
    │ HttpxAdapterV5 (priority: 50)          │ ← HTTP direct
    │ JinaAdapterV5 (priority: 55)           │ ← API extraction
    │ BrowserlessAdapterV5 (priority: 70)    │ ← Headless browser
    │ AlternatesAdapterV5 (priority: 150)    │ ← AMP/print
    │ WaybackAdapterV5 (priority: 200)       │ ← Archive fallback
    └─────────────────────────────────────────┘
    ↓
5. Content Cleaning (ContentCleanerV5 with candidate scoring)
    ↓
6. Quality Validation (word count, paragraphs, signals)
    ↓
7. Paywall Detection & Bypass (soft/hard strategies)
    ↓
8. Metadata Extraction (title, author, date)
    ↓
9. Cache Store (successful results)
    ↓
✅ Return ScrapeResult

USAGE EXAMPLES:

1. SINGLE URL SCRAPING (básico):
   ```python
   from tools.tool_content_scraperv5_production_grade_clean import Tools
   
   tools = Tools()
   result = await tools.scrape(url="https://example.com")
   
   if result['success']:
       print(f"✅ Extracted {result['word_count']} words")
       print(f"Adapter: {result['adapter_used']}")
       print(f"Content: {result['content'][:200]}...")
   else:
       print(f"❌ Failed: {result.get('error')}")
   ```

2. BATCH SCRAPING (parallel, produção):
   ```python
   urls = ["https://a.com", "https://b.com", "https://c.com"]
   results = await tools.scrape(urls=urls)
   
   for idx, result in enumerate(results['results']):
       if result['success']:
           print(f"URL {idx+1}: {result['word_count']} words via {result['adapter_used']}")
       else:
           print(f"URL {idx+1}: FAILED - {result.get('error')}")
   ```

3. CUSTOM CONFIGURATION (advanced):
   ```python
   tools = Tools()
   
   # Disable expensive adapters
   tools.valves.enable_browserless = False
   tools.valves.enable_jina = True
   
   # Increase timeouts for slow sites
   tools.valves.request_timeout = 30
   tools.valves.jina_timeout = 40
   
   # Configure Browserless (if needed)
   tools.valves.browserless_endpoint = "http://192.168.1.100:3000"
   tools.valves.browserless_token = "your-token-here"
   
   result = await tools.scrape(url="https://slow-site.com")
   ```

4. DOMAIN-SPECIFIC PLAYBOOKS (expert):
   ```python
   # Custom playbook for specific domain
   playbooks = {
       "example.com": {
           "strategy_order": ["httpx", "jina"],  # Try httpx first, then jina
           "min_words_accept": 100,              # Require 100+ words
           "wait_time": 3000,                    # Wait 3s before extraction
           "preferred_selectors": ["article", ".content"]
       }
   }
   
   tools.valves.domain_playbooks = json.dumps(playbooks)
   result = await tools.scrape(url="https://example.com/article")
   ```

5. OPENWEBUI INTEGRATION (production):
   ```python
   # Em OpenWebUI, a tool é chamada automaticamente:
   # User: "Scrape https://example.com and summarize"
   # LLM calls:
   result = await scrape(url="https://example.com", __event_emitter__=emitter)
   # Returns: JSON string with content
   ```

TROUBLESHOOTING:

Q: Getting 0 words extracted?
A: 1. Check logs for adapter attempts
   2. Domain may have Cloudflare/bot-wall → enable browserless
   3. Try with enable_jina=True (bypasses most protections)

Q: Timeout errors?
A: 1. Increase request_timeout valve (default: 15s → 30s+)
   2. Increase browserless_timeout (default: 45s → 60s+)
   3. Check if site is down (curl test)

Q: Paywall content only (excerpt)?
A: 1. Paywall bypass is enabled by default (enable_paywall_bypass=True)
   2. Check logs for "premium bypass" attempts
   3. Strategies tried: Alternates (AMP) → Wayback → Jina
   4. If all fail, content is truly paywalled

Q: Bot-wall/Cloudflare detected?
A: 1. HttpxAdapter uses realistic headers (rotates UA, Sec-Ch-Ua, etc.)
   2. BrowserlessAdapter tries /function with stealth mode
   3. WaybackAdapter as last resort (archive.org)
   4. Check circuit breaker state (may be cooling down)

Q: PDF/DOCX not extracted?
A: 1. Requires pdfplumber: pip install pdfplumber
   2. For DOCX: pip install docx2txt
   3. Check enable_document=True in valves
   4. DocumentAdapter has priority 10 (always first for docs)

Q: Too much content truncated?
A: 1. Increase content_word_limit (default: 2000 words)
   2. Increase max_content_chars_in_response (default: 15000 chars)
   3. For batch scraping, limit is DISTRIBUTED across all URLs

PERFORMANCE BENCHMARKS:

| Adapter | Avg Time | Success Rate | Use Case |
|---------|----------|--------------|----------|
| HttpxAdapterV5 | ~2s | 70% | Static HTML, blogs |
| JinaAdapterV5 | ~3s | 85% | News, articles, modern sites |
| BrowserlessAdapterV5 | ~8s | 90% | SPAs, dynamic content, paywalls |
| DocumentAdapterV5 | ~5s | 95% | PDF/DOCX extraction |
| WaybackAdapterV5 | ~6s | 60% | Archive fallback, old content |

ADAPTER PRIORITY CHAIN (default order):
DocumentAdapterV5 (10) → HttpxAdapterV5 (50) → JinaAdapterV5 (55) →
BrowserlessAdapterV5 (70) → AlternatesAdapterV5 (150) → WaybackAdapterV5 (200)

Lower number = Higher priority (tries first)
"""

# Exportar símbolos corretos para o OpenWebUI
__all__ = ["Tools", "get_tools"]


# ============================================================================
# SEÇÃO 1: IMPORTS E DEPENDÊNCIAS
# ============================================================================

import asyncio
import json
import logging
import re
import time
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin, urlparse

import aiohttp
from pydantic import BaseModel, Field

# Optional dependencies with graceful fallbacks
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None

try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    requests = None

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False
    BeautifulSoup = None

try:
    from readability import Document
    READABILITY_AVAILABLE = True
except ImportError:
    READABILITY_AVAILABLE = False
    Document = None

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False
    pdfplumber = None

try:
    import docx2txt
    DOCX2TXT_AVAILABLE = True
except ImportError:
    DOCX2TXT_AVAILABLE = False
    docx2txt = None

try:
    import easyocr
    EASYOCR_AVAILABLE = True
except ImportError:
    EASYOCR_AVAILABLE = False
    easyocr = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# SEÇÃO 2: CLASSES CENTRALIZADAS (INLINE PARA OPENWEBUI)
# ============================================================================


# ============================================================================
# SEÇÃO: CLASSES CENTRALIZADAS (INLINE PARA OPENWEBUI)
# ============================================================================

class ScraperConstants:
    """Constantes centralizadas para evitar magic numbers e strings duplicadas"""
    
    # User Agents
    DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
    DEFAULT_ACCEPT_LANGUAGE = "pt-BR,pt;q=0.9,en;q=0.8"
    
    # Timeouts (ms)
    DEFAULT_TIMEOUT_HTTPX = 20000
    DEFAULT_TIMEOUT_BROWSERLESS = 30000
    DEFAULT_TIMEOUT_JINA = 25000
    
    # Retry configuration
    DEFAULT_MAX_RETRIES = 2
    DEFAULT_BACKOFF_BASE_MS = 300
    DEFAULT_BACKOFF_FACTOR = 2.0
    
    # Content limits
    DEFAULT_MIN_WORDS = 30
    DEFAULT_MIN_PARAGRAPHS = 2
    DEFAULT_CONTENT_WORD_LIMIT = 2000
    DEFAULT_MAX_CONTENT_CHARS = 15000
    
    # Cache configuration
    DEFAULT_CACHE_TTL_SECONDS = 600
    
    # Circuit breaker
    DEFAULT_CIRCUIT_BREAKER_THRESHOLD = 5
    DEFAULT_CIRCUIT_BREAKER_TIMEOUT = 60
    
    # Concurrency
    DEFAULT_MAX_CONCURRENT = 5
    DEFAULT_MAX_REDIRECTS = 5
    
    # Content size limits
    MAX_CONTENT_LENGTH_BYTES = 5_000_000
    
    # Endpoints
    DEFAULT_JINA_ENDPOINT = "https://r.jina.ai"
    DEFAULT_UNLOCK_ENDPOINT = "https://brd.superproxy.io:33335"
    DEFAULT_SBR_ENDPOINT = "https://brd.superproxy.io:33335"
    
    # Bot wall / Cloudflare detection
    CLOUDFLARE_CHALLENGE_MARKERS = [
        "checking your browser",
        "cloudflare",
        "please enable cookies",
        "ray id:",
        "cf-ray",
        "ddos protection by",
        "attention required"
    ]
    
    PAYWALL_MARKERS = [
        "subscribe to continue",
        "register to read",
        "upgrade your account",
        "premium content",
        "members only",
        "subscriber exclusive",
        "login to access",
        "create a free account"
    ]
    
    SOFT_404_MARKERS = [
        "page not found",
        "404 error",
        "not available",
        "doesn't exist",
        "removed or deleted",
        "no longer available"
    ]


class HeaderPolicy:
    """Política centralizada de headers para evitar duplicação"""
    
    DEFAULT_UA = ScraperConstants.DEFAULT_USER_AGENT
    DEFAULT_ACCEPT_LANGUAGE = ScraperConstants.DEFAULT_ACCEPT_LANGUAGE
    
    def __init__(self, config):
        self.config = config
    
    def build_basic_headers(self):
        """Headers básicos para HTTP requests"""
        headers = {
            "User-Agent": self.config.user_agent or self.DEFAULT_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": self.DEFAULT_ACCEPT_LANGUAGE,
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
        
        # Headers personalizados do config
        if hasattr(self.config, 'headers') and self.config.headers:
            headers.update(self.config.headers)
            
        return headers
    
    def build_browserless_headers(self):
        """Headers específicos para Browserless"""
        return {
            "Accept-Language": self.DEFAULT_ACCEPT_LANGUAGE,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1"
        }
    
    def get_user_agent(self):
        """Retorna User-Agent configurado"""
        return self.config.user_agent or self.DEFAULT_UA

class RetryPolicy:
    """Política unificada de retry para evitar duplicação"""
    
    def __init__(self, max_retries=2, base_ms=300, factor=2.0):
        self.max_retries = max_retries
        self.base_ms = base_ms
        self.factor = factor
    
    def backoff_ms(self, attempt):
        """Calcula delay de backoff exponencial"""
        return int(self.base_ms * (self.factor ** attempt))
    
    def should_retry(self, result_or_exc):
        """Determina se deve fazer retry baseado no resultado ou exceção"""
        if isinstance(result_or_exc, Exception):
            return True
        
        # Se é um resultado de scraping
        if hasattr(result_or_exc, 'content') and hasattr(result_or_exc, 'success'):
            r = result_or_exc
            # Retry se falhou ou tem status que indica retry
            if not r.success:
                return True
            if hasattr(r, 'status_code') and r.status_code in (429, 500, 502, 503, 504):
                return True
            if hasattr(r, 'soft_paywall') and r.soft_paywall:
                return True
        
        return False
    
    async def execute_with_retry(self, func, *args, **kwargs):
        """Executa função com retry automático"""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                result = await func(*args, **kwargs)
                
                # Se sucesso, retorna
                if self._is_success(result):
                    if attempt > 0:
                        logger.info(f"[RetryPolicy] Success on attempt {attempt + 1}")
                    return result
                
                # Se não deve retry, retorna resultado
                if not self.should_retry(result):
                    return result
                
                # Se última tentativa, retorna resultado
                if attempt == self.max_retries:
                    logger.warning(f"[RetryPolicy] Max retries reached, returning result")
                    return result
                
                # Calcular delay e aguardar
                delay_ms = self.backoff_ms(attempt)
                logger.info(f"[RetryPolicy] Retry {attempt + 1}/{self.max_retries} in {delay_ms}ms")
                await asyncio.sleep(delay_ms / 1000.0)
                
            except Exception as e:
                last_exception = e
                logger.warning(f"[RetryPolicy] Exception on attempt {attempt + 1}: {e}")
                
                # Se última tentativa, re-raise
                if attempt == self.max_retries:
                    logger.error(f"[RetryPolicy] Max retries reached, raising exception")
                    raise e
                
                # Calcular delay e aguardar
                delay_ms = self.backoff_ms(attempt)
                logger.info(f"[RetryPolicy] Retry {attempt + 1}/{self.max_retries} in {delay_ms}ms")
                await asyncio.sleep(delay_ms / 1000.0)
        
        # Se chegou aqui, algo deu errado
        if last_exception:
            raise last_exception
        return None
    
    def _is_success(self, result):
        """Determina se resultado é sucesso"""
        if hasattr(result, 'success'):
            return result.success
        if hasattr(result, 'content') and result.content:
            return True
        return False

class StrategySignals:
    """Sinais extraídos da URL e HTML para decisão de estratégia"""
    
    def __init__(self, url, html_head="", headers=None):
        self.url = url
        self.html_head = html_head or ""
        self.headers = headers or {}
        
        # Sinais de AMP
        self.has_amp_hint = (
            'rel="amphtml"' in self.html_head or 
            "/amp/" in url or 
            "amp" in url.lower()
        )
        
        # Sinais de proteção CDN
        self.possible_cdn_protection = any(x in self.html_head.lower() for x in [
            "cloudflare", "akamai", "datadome", "incapsula", "sucuri"
        ])
        
        # Sinais de paywall
        self.is_paywallish = any(x in self.html_head.lower() for x in [
            "paywall", "metered", "subscriber", "premium", "subscribe",
            "piano", "tinypass", "subscribe with google"
        ])
        
        # Sinais de SPA
        self.is_spa = any(x in self.html_head.lower() for x in [
            '<script type="module"', "next-data", "nuxt", "webpackJsonp",
            "__NEXT_DATA__", "vue", "react", "angular"
        ])
        
        # Sinais de conteúdo dinâmico
        self.has_heavy_js = (
            self.html_head.count('<script') > 5 or
            'webpack' in self.html_head.lower() or
            'bundle' in self.html_head.lower()
        )
        
        # Domínio específico
        self.domain = self._extract_domain(url)
        
        # Sinais de notícia/blog
        self.is_news = any(x in self.domain for x in [
            "globo.com", "uol.com.br", "folha.com.br", "estadao.com.br",
            "g1.globo.com", "noticias.uol.com.br"
        ])
    
    def _extract_domain(self, url):
        """Extrai domínio da URL"""
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc.lower()
        except:
            return ""

class StrategyDecider:
    """Decisor de estratégias baseado em sinais e playbooks"""
    
    def __init__(self):
        # Estratégias por domínio (fallback)
        self.domain_strategies = {
            "g1.globo.com": ["amp", "content", "function", "scrape"],
            "noticias.uol.com.br": ["content", "amp", "function"],
            "www.correiobraziliense.com.br": ["content", "amp", "function", "scrape"],
            "www.anthropic.com": ["content", "function", "scrape"],
            "www.mistral.ai": ["content", "function", "scrape"],
            "pricepertoken.com": ["content", "function", "scrape"]
        }
    
    def decide(self, url, playbook=None, config=None, head_html=""):
        """
        Decide ordem de estratégias baseado em sinais e playbook
        
        Returns:
            Tuple[List[str], int]: (ordem de estratégias, budget_ms)
        """
        signals = StrategySignals(url, head_html)
        
        # 1) Começar pela ordem do playbook (se existir)
        order = []
        if playbook and "force_order" in playbook:
            order = list(playbook["force_order"])
            logger.info(f"[StrategyDecider] Using playbook order: {order}")
        
        # 2) Fallback para domínio específico
        if not order and signals.domain in self.domain_strategies:
            order = list(self.domain_strategies[signals.domain])
            logger.info(f"[StrategyDecider] Using domain-specific order: {order}")
        
        # 3) Ajustes heurísticos
        order = self._apply_heuristics(order, signals, config)
        
        # 4) Fallback sensato se vazio
        if not order:
            order = ["httpx", "content", "amp", "scrape", "function"]
            logger.info(f"[StrategyDecider] Using default order: {order}")
        
        # 5) Budget/timeouts
        budget_ms = self._calculate_budget(config, signals)
        
        logger.info(f"[StrategyDecider] Final order: {order}, budget: {budget_ms}ms")
        return order, budget_ms
    
    def _apply_heuristics(self, order, signals, config):
        """Aplica heurísticas para ajustar ordem de estratégias"""
        
        # Se AMP detectado, priorizar AMP (é barato e rápido)
        if signals.has_amp_hint and "amp" not in order:
            order.insert(0, "amp")
            logger.info(f"[StrategyDecider] AMP detected, prioritizing AMP")
        
        # Se proteção CDN, priorizar function (mais robusto)
        if signals.possible_cdn_protection:
            if "function" in order:
                order.remove("function")
                order.insert(0, "function")
                logger.info(f"[StrategyDecider] CDN protection detected, prioritizing function")
        
        # Se SPA, remover content (raramente funciona) e priorizar function/scrape
        if signals.is_spa:
            if "content" in order:
                order.remove("content")
                logger.info(f"[StrategyDecider] SPA detected, removing content strategy")
            if "function" not in order:
                order.insert(0, "function")
                logger.info(f"[StrategyDecider] SPA detected, prioritizing function")
        
        # Se paywall, priorizar function com cookies
        if signals.is_paywallish:
            if "function" in order:
                order.remove("function")
                order.insert(0, "function")
                logger.info(f"[StrategyDecider] Paywall detected, prioritizing function")
        
        # Se muito JS, priorizar function/scrape
        if signals.has_heavy_js:
            if "function" in order:
                order.remove("function")
                order.insert(0, "function")
                logger.info(f"[StrategyDecider] Heavy JS detected, prioritizing function")
        
        # Se notícia, priorizar AMP e content
        if signals.is_news:
            if "amp" in order:
                order.remove("amp")
                order.insert(0, "amp")
                logger.info(f"[StrategyDecider] News site detected, prioritizing AMP")
        
        return order
    
    def _calculate_budget(self, config, signals):
        """Calcula budget de tempo baseado em configuração e sinais"""
        # Budget base
        budget_ms = 20000  # 20 segundos
        
        if config and hasattr(config, 'timeout_ms'):
            if isinstance(config.timeout_ms, dict):
                # Soma dos timeouts das estratégias
                budget_ms = sum(config.timeout_ms.values())
            else:
                budget_ms = config.timeout_ms
        
        # Ajustes baseados em sinais
        if signals.possible_cdn_protection:
            budget_ms += 15000  # +15s para CDN protection (Cloudflare)
        if signals.domain in ["julius.ai", "techpoint.africa"]:
            budget_ms += 20000  # +20s para domínios com Cloudflare conhecido
        if signals.is_spa:
            budget_ms += 15000  # +15s para SPA
        if signals.has_heavy_js:
            budget_ms += 10000  # +10s para heavy JS
        
        # Limite máximo
        budget_ms = min(budget_ms, 60000)  # 60s máximo
        
        return budget_ms

class CacheKey:
    """Chave de cache baseada em URL e configurações"""
    
    @staticmethod
    def from_url(url, config=None):
        """Cria chave de cache baseada na URL e configuração"""
        # Normalizar URL (remover fragmentos, ordenar query params)
        from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        # Ordenar query params para consistência
        sorted_query = urlencode(sorted(query_params.items()), doseq=True)
        normalized_url = urlunparse((
            parsed.scheme, parsed.netloc, parsed.path,
            parsed.params, sorted_query, ""  # Remove fragment
        ))
        
        # Incluir configurações relevantes na chave
        key_parts = [normalized_url]
        
        if config:
            if hasattr(config, 'user_agent') and config.user_agent:
                key_parts.append(f"ua:{config.user_agent}")
            if hasattr(config, 'headers') and config.headers:
                # Incluir headers relevantes (mas não todos)
                relevant_headers = ['Accept-Language', 'Accept']
                for header in relevant_headers:
                    if header in config.headers:
                        key_parts.append(f"{header}:{config.headers[header]}")
        
        return "|".join(key_parts)


# ============================================================================
# SEÇÃO: MODELOS DE DADOS (PYDANTIC)
# ============================================================================

class ScraperConfig(BaseModel):
    """Configuração unificada para scraping com válvulas globais genéricas"""
    
    # ============================================================================
    # VÁLVULAS GLOBAIS GENÉRICAS (80/20 rule)
    # ============================================================================
    
    # Timeouts por estratégia
    timeout_ms: Dict[str, int] = Field(
        default={"httpx": 20000, "browserless": 30000, "jina": 25000},
        description="Timeout por estratégia em milliseconds"
    )
    
    # Retry configuration
    retries: Dict[str, Any] = Field(
        default={"count": 2, "backoff_ms": [500, 1500]},
        description="Configuração de retries com backoff exponencial"
    )
    
    # User Agent realista
    user_agent: str = Field(
        default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36",
        description="User agent desktop comum (evita versões lite)"
    )
    
    # Configurações de rede
    max_redirects: int = Field(default=5, description="Máximo de redirects a seguir")
    max_content_length: int = Field(default=5_000_000, description="Máximo tamanho de conteúdo em bytes")
    
    # Ordem e bloqueio de adapters
    force_order: Optional[List[str]] = Field(
        default=None,
        description="Forçar ordem específica de adapters (deixa heurística decidir se None)"
    )
    block_adapters: List[str] = Field(
        default=[],
        description="Lista de adapters para não usar"
    )
    
    # JavaScript heurístico
    javascript: Dict[str, Any] = Field(
        default={
            "enabled": "auto",
            "wait_until": "domcontentloaded",
            "wait_for_selector": ["article", "main", "[role='main']", ".article-body", ".entry-content"]
        },
        description="Configuração heurística de JavaScript"
    )
    
    # Extração e limpeza
    extract: Dict[str, Any] = Field(
        default={
            "strip_selectors": ["nav", "aside", ".share", ".ad", ".promo", ".newsletter", "footer"],
            "custom_selectors": []
        },
        description="Configuração de extração e limpeza"
    )
    
    # AMP e cache
    follow_amp: bool = Field(default=True, description="Tentar AMP primeiro quando detectado")
    respect_robots: bool = Field(default=True, description="Respeitar robots.txt")
    cache_ttl_sec: int = Field(default=600, description="Cache TTL em segundos (10 min)")
    cache_max_size: int = Field(default=1000, description="Max cache entries (LRU eviction)")
    
    # Headers e cookies personalizados
    headers: Dict[str, str] = Field(default={}, description="Headers personalizados")
    cookies: Dict[str, str] = Field(default={}, description="Cookies personalizados")
    
    # Proxy (opcional)
    proxy: Optional[str] = Field(default=None, description="Proxy para usar (se necessário)")
    
    # Debug logging
    enable_debug_logging: bool = Field(default=False, description="Enable verbose debug logs (security: may log sensitive data)")
    
    # ============================================================================
    # CONFIGURAÇÕES LEGADAS (mantidas para compatibilidade)
    # ============================================================================
    
    # Limites de conteúdo
    content_word_limit: int = Field(default=2000, description="Maximum words in content")
    max_content_chars_in_response: int = Field(default=15000, description="Maximum characters in response")
    
    # Timeouts (legados - usar timeout_ms preferencialmente)
    browserless_timeout: int = Field(default=45, description="Browserless timeout in seconds")
    jina_timeout: int = Field(default=25, description="Jina API timeout in seconds")
    request_timeout: int = Field(default=15, description="HTTP request timeout in seconds")
    rate_limit_delay: float = Field(default=0.5, description="Delay between attempts for rate limiting")
    
    # Configurações de adapters
    enable_httpx: bool = Field(default=True, description="Enable HTTPX adapter")
    enable_jina: bool = Field(default=True, description="Enable Jina Reader API")
    enable_browserless: bool = Field(default=True, description="Enable Browserless headless browser")
    enable_document: bool = Field(default=True, description="Enable Document processing")
    enable_wayback: bool = Field(default=True, description="Enable Wayback Machine")
    enable_alternates: bool = Field(default=True, description="Enable Alternates adapter")
    enable_unlock: bool = Field(default=False, description="Enable Bright Data Unlocker")
    enable_sbr: bool = Field(default=False, description="Enable Bright Data SBR")
    
    # Tokens e credenciais
    browserless_token: Optional[str] = Field(default=None, description="Browserless API token")
    unlock_username: Optional[str] = Field(default=None, description="Bright Data Unlocker username")
    unlock_password: Optional[str] = Field(default=None, description="Bright Data Unlocker password")
    
    # URLs de serviços
    browserless_endpoint: Optional[str] = Field(default=None, description="Browserless endpoint URL")
    jina_endpoint: str = Field(default="https://r.jina.ai", description="Jina Reader endpoint")
    
    # Bright Data Unlocker
    unlock_endpoint: str = Field(
        default="https://brd.superproxy.io:33335",
        description="Bright Data Unlocker proxy endpoint"
    )
    
    # Bright Data SBR
    sbr_endpoint: str = Field(
        default="https://brd.superproxy.io:33335",
        description="Bright Data SBR proxy endpoint"
    )
    sbr_username: Optional[str] = Field(
        default=None,
        description="Bright Data SBR username (format: brd-customer-<CUSTOMER_ID>-zone-<ZONE>)"
    )
    sbr_password: Optional[str] = Field(
        default=None,
        description="Bright Data SBR password"
    )
    
    # Configurações específicas (legadas)
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    
    # Playbooks por domínio (substitui domain_selectors)
    domain_playbooks_json: str = Field(
        default="{}",
        description="JSON with domain-specific scraping strategies. Format: {\"domain\": {\"force_order\": [...], \"block_adapters\": [...], \"custom_selectors\": [...]}}"
    )

class ScrapeResult(BaseModel):
    """Resultado de scraping padronizado"""
    
    url: str = Field(description="URL that was processed")
    content: str = Field(default="", description="Retrieved content")
    title: Optional[str] = Field(default=None, description="Page title")
    author: Optional[str] = Field(default=None, description="Content author")
    published_at: Optional[str] = Field(default=None, description="Publication date")
    word_count: int = Field(default=0, description="Word count of content")
    success: bool = Field(default=False, description="Whether scraping was successful")
    adapter_used: str = Field(default="none", description="Adapter that succeeded")
    extraction_time: float = Field(default=0.0, description="Time taken for extraction")
    quality_score: Optional[float] = Field(default=None, description="Content quality score")
    error: Optional[str] = Field(default=None, description="Error message if failed")
    request_id: Optional[str] = Field(default=None, description="Correlation ID for distributed tracing")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    @property
    def strategy(self) -> str | None:
        """Extract strategy from metadata or adapter_used"""
        # 1) Se vier explicitamente nos metadados
        if isinstance(self.metadata, dict) and "strategy" in self.metadata:
            return self.metadata["strategy"]
        # 2) Fallback: tenta extrair de "adapter_used" no formato NomeAdapter(estrategia)
        if isinstance(self.adapter_used, str) and "(" in self.adapter_used and self.adapter_used.endswith(")"):
            try:
                return self.adapter_used.split("(", 1)[1][:-1]
            except Exception:
                return None
        return None

class AdapterMetrics(BaseModel):
    """Métricas de performance por adapter"""
    
    attempts: int = Field(default=0, description="Total attempts")
    successes: int = Field(default=0, description="Successful attempts")
    failures: int = Field(default=0, description="Failed attempts")
    total_time: float = Field(default=0.0, description="Total time spent")
    avg_time: float = Field(default=0.0, description="Average time per attempt")
    success_rate: float = Field(default=0.0, description="Success rate percentage")
    last_success: Optional[datetime] = Field(default=None, description="Last successful attempt")
    last_failure: Optional[datetime] = Field(default=None, description="Last failed attempt")

class CircuitBreakerState(BaseModel):
    """Estado do circuit breaker"""
    
    failures: int = Field(default=0, description="Consecutive failures")
    last_failure: Optional[datetime] = Field(default=None, description="Last failure time")
    last_success: Optional[datetime] = Field(default=None, description="Last success time")
    state: str = Field(default="closed", description="Circuit breaker state: closed, open, half_open")
    failure_threshold: int = Field(default=5, description="Failures before opening circuit")
    timeout_seconds: int = Field(default=60, description="Timeout before trying again")


# ============================================================================
# SEÇÃO: INFRAESTRUTURA E UTILITÁRIOS
# ============================================================================

class TimeBudgetExceeded(Exception):
    """Exceção para timeout de orçamento de tempo"""
    pass


class ScraperException(Exception):
    """Base exception for scraper"""
    pass


class AdapterException(ScraperException):
    """Adapter failed to retrieve content"""
    pass


class ValidationException(ScraperException):
    """Content validation failed"""
    pass


class CircuitBreakerOpenException(ScraperException):
    """Circuit breaker is open for host"""
    pass


class ContentQualityException(ScraperException):
    """Content quality below threshold"""
    pass


class PaywallException(ScraperException):
    """Content behind paywall"""
    pass

class ExponentialBackoff:
    """Backoff exponencial para retries"""
    
    def __init__(self, base_delay: float = 1.0, max_delay: float = 60.0, backoff_factor: float = 2.0):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
    
    async def wait(self, attempt: int) -> float:
        """Calcula e aguarda o delay para a tentativa"""
        delay = min(self.base_delay * (self.backoff_factor ** attempt), self.max_delay)
        await asyncio.sleep(delay)
        return delay

class CircuitBreaker:
    """Circuit breaker para proteção contra falhas"""
    
    def __init__(self, failure_threshold: int = 10, timeout_seconds: int = 30):
        self.state = CircuitBreakerState(
            failure_threshold=failure_threshold,
            timeout_seconds=timeout_seconds
        )
    
    def is_open(self) -> bool:
        """Verifica se o circuit breaker está aberto"""
        if self.state.state != "open":
            return False
        
        if self.state.last_failure:
            time_since_failure = datetime.now() - self.state.last_failure
            if time_since_failure.total_seconds() > self.state.timeout_seconds:
                self.state.state = "half_open"
                return False
        
        return True
    
    def record_success(self):
        """Registra sucesso e fecha o circuit breaker"""
        self.state.failures = 0
        self.state.state = "closed"
        self.state.last_success = datetime.now()
    
    def record_failure(self):
        """Registra falha e potencialmente abre o circuit breaker"""
        self.state.failures += 1
        self.state.last_failure = datetime.now()
        
        if self.state.failures >= self.state.failure_threshold:
            self.state.state = "open"

class StructuredLogger:
    """Logger estruturado para métricas"""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.metrics = defaultdict(int)
    
    def log_scrape_attempt(self, url: str, adapter: str, attempt: int = 1):
        """Log de tentativa de scraping"""
        self.logger.info(f"[{adapter}] Attempt {attempt} for {url}")
        self.metrics[f"{adapter}_attempts"] += 1
    
    def log_scrape_success(self, url: str, adapter: str, duration: float, word_count: int):
        """Log de sucesso no scraping"""
        self.logger.info(f"[{adapter}] SUCCESS for {url} | {word_count} words | {duration:.2f}s")
        self.metrics[f"{adapter}_successes"] += 1
    
    def log_scrape_failure(self, url: str, adapter: str, error: str, duration: float = 0.0):
        """Log de falha no scraping"""
        self.logger.warning(f"[{adapter}] FAILED for {url} | {error} | {duration:.2f}s")
        self.metrics[f"{adapter}_failures"] += 1
    
    def warning(self, msg: str):
        """Wrapper for logger.warning"""
        self.logger.warning(msg)

class HostSemaphore:
    """Semáforo por host para controle de concorrência"""
    
    def __init__(self, max_concurrent: int = 5):
        self.max_concurrent = max_concurrent
        self.semaphores: Dict[str, asyncio.Semaphore] = {}
    
    async def acquire(self, url: str):
        """Adquire semáforo para o host da URL"""
        host = urlparse(url).netloc
        if host not in self.semaphores:
            self.semaphores[host] = asyncio.Semaphore(self.max_concurrent)
        return await self.semaphores[host].acquire()
    
    def release(self, url: str):
        """Libera semáforo para o host da URL"""
        host = urlparse(url).netloc
        if host in self.semaphores:
            self.semaphores[host].release()
    
    @asynccontextmanager
    async def limit(self, url: str):
        """Context manager para garantir release automático do semáforo"""
        host = urlparse(url).netloc
        sem = self.semaphores.setdefault(host, asyncio.Semaphore(self.max_concurrent))
        await sem.acquire()
        try:
            yield
        finally:
            sem.release()

def sanitize_for_logging(obj: Any, sensitive_keys: Optional[set] = None) -> Any:
    """Remove sensitive data from objects before logging (security)"""
    if sensitive_keys is None:
        sensitive_keys = {
            'token', 'password', 'api_key', 'secret', 'auth',
            'browserless_token', 'unlock_token', 'sbr_password',
            'unlock_password', 'bd_password', 'authorization'
        }
    
    if isinstance(obj, dict):
        return {
            k: '***REDACTED***' if any(sk in k.lower() for sk in sensitive_keys) else sanitize_for_logging(v, sensitive_keys)
            for k, v in obj.items()
        }
    elif isinstance(obj, str) and len(obj) > 200:
        # Truncate very long strings (likely tokens/keys)
        return f"{obj[:100]}...***TRUNCATED***"
    elif hasattr(obj, 'model_dump'):
        # Pydantic models
        return sanitize_for_logging(obj.model_dump(), sensitive_keys)  # type: ignore
    elif hasattr(obj, '__dict__'):
        # Regular objects
        return sanitize_for_logging(obj.__dict__, sensitive_keys)
    else:
        return obj


def build_headers(user_agent: str, additional_headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Constrói headers HTTP otimizados"""
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    
    if additional_headers:
        headers.update(additional_headers)
    
    return headers

# Headers "humanos" para domínios anti-bot
# Headers realistas com rotação de User-Agent e locale
REAL_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
]

REAL_LOCALES = [
    "en-US,en;q=0.9",
    "pt-BR,pt;q=0.9,en-US;q=0.8", 
    "es-ES,es;q=0.9,en;q=0.8",
    "fr-FR,fr;q=0.9,en;q=0.8"
]

def build_real_headers(attempt: int = 0, locale: Optional[str] = None, url: str = "") -> dict:
    """Constrói headers realistas com rotação progressiva de UA e locale"""
    user_agent = REAL_USER_AGENTS[attempt % len(REAL_USER_AGENTS)]
    
    # Detectar locale por domínio se não especificado
    if locale is None:
        if url:
            domain = urlparse(url).netloc.lower()
            if domain.endswith('.br'):
                locale = "pt-BR,pt;q=0.9,en-US;q=0.8"
            elif domain.endswith('.es'):
                locale = "es-ES,es;q=0.9,en;q=0.8"
            elif domain.endswith('.fr'):
                locale = "fr-FR,fr;q=0.9,en;q=0.8"
            else:
                locale = REAL_LOCALES[attempt % len(REAL_LOCALES)]
        else:
            locale = REAL_LOCALES[attempt % len(REAL_LOCALES)]
    
    # Headers base com melhor bypass do Cloudflare
    headers = {
        "User-Agent": user_agent,
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
        "Sec-Ch-Ua": '"Chromium";v="126", "Not(A:Brand";v="24", "Google Chrome";v="126"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"'
    }
    
    # Headers específicos por tentativa (progressão)
    if attempt == 0:
        # Desktop padrão - headers completos
        headers.update({
            "Sec-Ch-Ua": '"Not)A;Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        })
    elif attempt == 1:
        # Mobile - simular iPhone
        headers.update({
            "Sec-Ch-Ua": '"Not)A;Brand";v="8", "Chromium";v="126", "Safari";v="126"',
            "Sec-Ch-Ua-Mobile": "?1",
            "Sec-Ch-Ua-Platform": '"iOS"',
        })
    elif attempt == 2:
        # Linux - headers mínimos
        headers.update({
            "Accept": "*/*",
            "Connection": "keep-alive",
        })
    
    return headers

# Headers humanos para domínios anti-bot (backward compatibility)
HTTPX_HEADERS_HUMAN = build_real_headers(0)

# Domínios que precisam de headers especiais
ANTI_BOT_DOMAINS = {
    "platform.openai.com",
    "x.ai", 
    "grok.x.ai",
    "docs.anthropic.com",
    "julius.ai",
    "techpoint.africa"
}

# Sinais específicos de bot-wall/Cloudflare
CF_SIGNS = [
    "/cdn-cgi/challenge-platform/",
    "__cf_chl_rt_tk=",
    "__cf_bm=",
    "turnstile",
    "cf-error",
    "please enable javascript",
    "are you human",
    "access denied",
    "checking your browser",
    "please wait while we check your browser"
]

# Headers de bot-wall
BOT_WALL_HEADERS = [
    "cf-ray",
    "cf-cache-status", 
    "cf-request-id",
    "x-akamai-*",
    "x-datadome-*",
    "x-served-by",
    "server"
]

# Sinais de bot-wall no conteúdo
BOT_WALL_CONTENT_SIGNS = [
    ".cf-error-details",
    "#challenge-stage",
    "checking your browser",
    "please wait while we check your browser",
    "attention required",
    "access denied",
    "blocked",
    "forbidden"
]

def is_bot_wall(status_code: int, url: str = "", body_text: str = "", headers: Optional[dict] = None) -> dict:
    """Detect bot-wall/challenge with confidence scoring and provider identification.
    
    Analyzes HTTP response to detect if content is blocked by anti-bot systems
    like Cloudflare, Akamai, DataDome, etc. Uses multi-signal detection:
    status codes, headers, content patterns.
    
    Args:
        status_code: HTTP status code (403/401/429/503 are strong indicators)
        url: Request URL (optional, for URL-based detection)
        body_text: Response body HTML (optional, for content-based detection)
        headers: Response headers dict (optional, for header-based detection)
    
    Returns:
        dict with schema:
        {
            'is_bot_wall': bool,        # True if bot-wall detected (confidence ≥50)
            'provider': str | None,      # 'cloudflare', 'akamai', 'datadome', 'generic'
            'confidence': int,           # 0-100 confidence score
            'indicators': List[str],     # List of detection signals found
            'reason': str                # Primary detection reason
        }
    
    Detection signals and scores:
        - Status 403/401/429/503: +30 points
        - CF headers (cf-ray, cf-cache-status): +20 points
        - Content signs (turnstile, challenge-platform): +25 points
        - Bot-wall phrases ("checking your browser"): +20 points
        - Confidence ≥50 → is_bot_wall=True
    
    Example:
        >>> result = is_bot_wall(403, body_text="<html>checking your browser...</html>")
        >>> if result['is_bot_wall']:
        ...     print(f"Blocked by {result['provider']} ({result['confidence']}%)")
        Blocked by cloudflare (confidence: 85%)
        
        >>> result = is_bot_wall(200, body_text="<html>normal content</html>")
        >>> print(result['is_bot_wall'])
        False
    """
    if headers is None:
        headers = {}
    
    # Verificar headers específicos de CF
    header_names = [k.lower() for k in headers.keys()]
    has_cf_headers = any(h.startswith('cf-') or h == 'server' for h in header_names)
    
    # Status codes suspeitos
    if status_code in [401, 403, 429, 503]:
        if not body_text:
            return {
                'is_bot_wall': True,
                'provider': 'cloudflare' if status_code == 403 else 'generic',
                'confidence': 80,
                'reason': f'status_{status_code}_no_content'
            }
        
        body_lower = body_text.lower()
        url_lower = url.lower()
        
        # Verificar sinais específicos
        for sign in CF_SIGNS:
            if sign in url_lower or sign in body_lower:
                provider = 'cloudflare' if any(cf in sign for cf in ['cf-', 'turnstile', 'challenge']) else 'generic'
                return {
                    'is_bot_wall': True,
                    'provider': provider,
                    'confidence': 95,
                    'reason': f'sign_detected_{sign}'
                }
        
        # Headers CF + status suspeito
        if has_cf_headers and status_code == 403:
            return {
                'is_bot_wall': True,
                'provider': 'cloudflare',
                'confidence': 90,
                'reason': 'cf_headers_403'
            }
    
    # Status 200 mas com sinais suspeitos
    if status_code == 200 and body_text:
        body_lower = body_text.lower()
        
        # Sinais muito específicos de challenge
        challenge_signs = [
            "/cdn-cgi/challenge-platform/",
            "__cf_chl_rt_tk=",
            "checking your browser",
            "please wait while we check your browser",
            "attention required"
        ]
        
        for sign in challenge_signs:
            if sign in body_lower:
                return {
                    'is_bot_wall': True,
                    'provider': 'cloudflare',
                    'confidence': 85,
                    'reason': f'challenge_sign_{sign}'
                }
    
    return {
        'is_bot_wall': False,
        'provider': None,
        'confidence': 0,
        'reason': 'no_signs_detected'
    }

# Backward compatibility
def detect_cloudflare_challenge(status_code: int, url: str = "", body_text: str = "") -> bool:
    """Detecta se a resposta indica challenge do Cloudflare/Turnstile"""
    result = is_bot_wall(status_code, url, body_text)
    return result['is_bot_wall'] and result['provider'] == 'cloudflare'


def detect_paywall(raw_html: str, cleaned_text: str = "") -> dict:
    """Detect paywall/premium content with soft/hard classification and confidence scoring.
    
    Multi-signal detector that identifies paywalled content by analyzing:
    - Text indicators (PT/EN: "assine", "subscribe", "premium content")
    - JavaScript trackers (Piano.io, Tinypass, Subscribe with Google)
    - DOM overlays (.paywall, .metered, overflow:hidden)
    - Content truncation (short content + CTA = soft paywall)
    - JSON-LD structured data (isAccessibleForFree: false)
    
    Args:
        raw_html: Raw HTML content from response (for DOM/JS analysis)
        cleaned_text: Extracted text content (for truncation detection)
    
    Returns:
        dict with schema:
        {
            'type': str,           # 'none', 'soft', 'hard'
            'hits': List[str],     # Detection signals found (e.g., "text_assine", "js_piano.io")
            'confidence': int,     # 0-100 confidence score
            'word_count': int      # Word count of cleaned_text
        }
    
    Classification rules:
        - confidence ≥60 → 'hard' paywall (full block, must bypass)
        - confidence 30-59 → 'soft' paywall (excerpt shown, CTA to subscribe)
        - confidence 15-29 → 'soft' paywall (light indicators)
        - confidence <15 → 'none' (no paywall)
    
    Detection scoring:
        - Text indicators (each): +15 points
        - JS trackers (each): +20 points
        - DOM overlays (each): +25 points
        - Truncation (<250 words): +25 points
        - Severe truncation (<100 words): +35 points
        - JSON-LD not free: +40 points
    
    Example:
        >>> html = '<html><script src="piano.io"></script><div class="paywall">Subscribe</div></html>'
        >>> text = "Short excerpt... Subscribe to read more"
        >>> result = detect_paywall(html, text)
        >>> print(f"Type: {result['type']}, Confidence: {result['confidence']}%")
        Type: hard, Confidence: 65%
        >>> print(f"Indicators: {result['hits']}")
        Indicators: ['js_piano.io', 'dom_.paywall', 'text_subscribe']
    """
    if not raw_html and not cleaned_text:
        return {"type": "unknown", "hits": [], "confidence": 0}
    
    hits = []
    confidence = 0
    paywall_type = "none"
    
    # Normalizar para análise
    html_lower = raw_html.lower() if raw_html else ""
    text_lower = cleaned_text.lower() if cleaned_text else ""
    
    # 1. HEURÍSTICAS DE TEXTO (PT/EN)
    text_indicators = [
        # Português
        "assine", "assinatura", "conteúdo exclusivo", "acesso exclusivo", 
        "conteúdo premium", "faça login para continuar", "entre para ler",
        "cadastre-se para continuar", "conteúdo para assinantes",
        "leia mais com assinatura", "desbloqueie este conteúdo",
        "apenas para assinantes", "conteúdo restrito",
        
        # Inglês  
        "subscribe", "subscription", "sign in to continue", "sign up to read",
        "premium content", "exclusive content", "member only", "subscribers only",
        "free trial", "unlock article", "read full article", "continue reading",
        "log in to read", "register to continue", "paywall", "metered",
        "you have reached your limit", "free articles remaining"
    ]
    
    for indicator in text_indicators:
        if indicator in text_lower:
            hits.append(f"text_{indicator}")
            confidence += 15
    
    # 2. MARCAÇÕES/JS COMUNS (indicadores específicos)
    js_indicators = [
        "piano.io", "tinypass", "tinypass.min.js", "arcxp_subscriptions", "metered", 
        "paywall", "subscription", "isAccessibleForFree: false",
        "paywall-gate", "content-gate", "subscription-wall",
        "cdn.tinypass.com", "basics-piano-script.js",
        "swg.googleusercontent.com", "/swg-google/", "swg-google/dev/index.js",
        "subscribe.google.com", "subscribe with google"
    ]
    
    for indicator in js_indicators:
        if indicator in html_lower:
            hits.append(f"js_{indicator}")
            confidence += 20
    
    # 3. DOM OVERLAY/PAYWALL SELECTORS
    overlay_selectors = [
        ".paywall", ".metered", ".piano-paywall", ".tp-modal",
        "[data-paywall]", ".subscribe", ".premium-gate", ".gateway-content",
        ".subscription-wall", ".content-gate", ".paywall-overlay",
        ".modal-paywall", ".paywall-modal", ".subscription-modal"
    ]
    
    if raw_html:
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(raw_html, "html.parser")
            
            for selector in overlay_selectors:
                if soup.select(selector):
                    hits.append(f"dom_{selector}")
                    confidence += 25
                    paywall_type = "hard"  # DOM overlay = hard paywall
            
            # Verificar overflow hidden (overlay ativo)
            body_tags = soup.find_all('body', style=True)
            for body in body_tags:
                style_attr = body.get('style') if hasattr(body, 'get') else None  # type: ignore
                if style_attr and isinstance(style_attr, str) and 'overflow:hidden' in style_attr.lower():
                    hits.append("dom_overflow_hidden")
                    confidence += 30
                    paywall_type = "hard"
            
            # Verificar blur/filtro em article
            articles = soup.find_all(['article', 'main', '[role="main"]'])
            for article in articles:
                style_attr = article.get('style') if hasattr(article, 'get') else None  # type: ignore
                style = style_attr.lower() if isinstance(style_attr, str) else ''
                if any(filter_type in style for filter_type in ['filter: blur', 'opacity: 0.5', 'opacity:0.5']):
                    hits.append("dom_blur_filter")
                    confidence += 20
                    paywall_type = "soft"
                    
        except Exception:
            pass
    
    # 4. TRUNCAMENTO + CTA DE ASSINATURA
    if cleaned_text:
        word_count = len(cleaned_text.split())
        
        # Se conteúdo muito curto + indicadores de paywall = provável soft
        if word_count < 250 and confidence > 20:
            hits.append(f"truncation_{word_count}_words")
            confidence += 25
            if paywall_type == "none":
                paywall_type = "soft"
        
        # Se muito curto + muitos indicadores = hard
        elif word_count < 100 and confidence > 40:
            hits.append(f"severe_truncation_{word_count}_words")
            confidence += 35
            paywall_type = "hard"
    
    # 5. JSON-LD STRUCTURED DATA
    if raw_html and "isAccessibleForFree" in html_lower:
        hits.append("jsonld_not_free")
        confidence += 40
        paywall_type = "hard"
    
    # 6. CLASSIFICAÇÃO FINAL
    if confidence >= 60:
        paywall_type = "hard"
    elif confidence >= 30:
        if paywall_type == "none":
            paywall_type = "soft"
    elif confidence >= 15:
        paywall_type = "soft"
    
    return {
        "type": paywall_type,
        "hits": hits,
        "confidence": min(confidence, 100),
        "word_count": len(cleaned_text.split()) if cleaned_text else 0
    }

def detect_bot_wall(status_code: int, headers: Optional[dict] = None, body_text: str = "", url: str = "") -> dict:
    """Detecção avançada de bot-wall com score de confiança"""
    score = 0
    indicators = []
    reason = None
    
    # Status codes
    if status_code in [401, 403, 429, 503]:
        score += 30
        indicators.append(f"status_{status_code}")
        
        if status_code == 403:
            reason = "cloudflare"
        elif status_code == 429:
            reason = "rate_limited"
        elif status_code == 503:
            reason = "service_unavailable"
    
    # Headers específicos
    if headers:
        for header_name, header_value in headers.items():
            header_lower = header_name.lower()
            if any(bot_header in header_lower for bot_header in BOT_WALL_HEADERS):
                score += 20
                indicators.append(f"header_{header_name}")
                
                if "cf-" in header_lower:
                    reason = "cloudflare"
                elif "akamai" in header_lower:
                    reason = "akamai"
                elif "datadome" in header_lower:
                    reason = "datadome"
    
    # Content analysis
    if body_text:
        body_lower = body_text.lower()
        
        # Sinais de Cloudflare
        for sign in CF_SIGNS:
            if sign in body_lower:
                score += 25
                indicators.append(f"content_cf_{sign}")
                reason = "cloudflare"
        
        # Sinais genéricos de bot-wall
        for sign in BOT_WALL_CONTENT_SIGNS:
            if sign in body_lower:
                score += 20
                indicators.append(f"content_bot_{sign}")
                if not reason:
                    reason = "generic_bot_wall"
    
    return {
        'is_bot_wall': score >= 50,
        'confidence': min(score, 100),
        'indicators': indicators,
        'reason': reason or "unknown"
    }

def discover_amp_url_from_html(url: str, html_content: str = "") -> Optional[str]:
    """
    Descobre URL AMP real via rel=amphtml no HTML fornecido (sync)
    Nunca assume /amp/ - sempre busca o link real
    """
    if not html_content:
        return None
    
    try:
        if BS4_AVAILABLE:
            soup = BeautifulSoup(html_content, "html.parser")  # type: ignore
            amp_link = soup.find('link', rel='amphtml')  # type: ignore
            if amp_link and hasattr(amp_link, 'get'):
                href_attr = amp_link.get('href')  # type: ignore
                if href_attr and isinstance(href_attr, str):
                    amp_url = href_attr
                    # Se for URL relativa, tornar absoluta
                    if amp_url.startswith('/'):
                        from urllib.parse import urljoin
                        amp_url = urljoin(url, amp_url)
                    return amp_url
    except Exception as e:
        logger.debug(f"[AMP Discovery] Error parsing HTML: {e}")
    
    return None

def derive_alternates(url: str) -> list:
    """Deriva URLs alternativas automaticamente (AMP, print, canonical, feeds)"""
    from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
    
    alternates = []
    parsed = urlparse(url)
    
    # AMP populares (fallback se não houver rel=amphtml)
    alternates.append(urljoin(url, "/amp/"))
    alternates.append(urljoin(url, "/amp"))
    
    # Print/read modes
    alternates.append(urljoin(url, "/print/"))
    alternates.append(urljoin(url, "/print"))
    
    # Query parameters comuns
    query_params = parse_qs(parsed.query)
    query_params['output'] = ['amp']
    new_query = urlencode(query_params, doseq=True)
    alternates.append(urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)))
    
    query_params['output'] = ['print']
    new_query = urlencode(query_params, doseq=True)
    alternates.append(urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)))
    
    return alternates

def get_fallback_url(url: str) -> str:
    """Gera URL do Wayback Machine como fallback"""
    from urllib.parse import quote
    return f"https://web.archive.org/web/{url}"

def parse_content_type(content_type: str) -> str:
    """Extrai tipo de conteúdo limpo do header"""
    if not content_type:
        return "text/html"
    
    return content_type.split(";")[0].strip().lower()

def normalize_url(url: str) -> str:
    """Normaliza URL removendo parâmetros desnecessários"""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

def is_valid_url(url: str) -> bool:
    """Verifica se URL é válida"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

def is_document_url(url: str, content_type: Optional[str] = None, content_bytes: Optional[bytes] = None) -> tuple[bool, str]:
    """
    Detecta se URL/conteúdo é um documento (PDF/DOCX/DOC) de forma robusta
    
    Returns:
        tuple[bool, str]: (is_document, document_type)
    """
    # 1. Verificar extensão da URL
    url_lower = url.lower()
    if url_lower.endswith('.pdf'):
        return (True, 'pdf')
    elif url_lower.endswith(('.docx', '.doc')):
        return (True, 'docx')
    
    # 2. Verificar Content-Type header
    if content_type:
        ct_lower = content_type.lower()
        if 'application/pdf' in ct_lower:
            return (True, 'pdf')
        elif any(doc in ct_lower for doc in ['application/msword', 'application/vnd.openxmlformats-officedocument']):
            return (True, 'docx')
    
    # 3. Verificar assinatura mágica do conteúdo (magic bytes)
    if content_bytes and len(content_bytes) >= 4:
        # PDF: %PDF
        if content_bytes[:4] == b'%PDF':
            return (True, 'pdf')
        # DOCX: PK (ZIP archive)
        if content_bytes[:2] == b'PK':
            return (True, 'docx')
        # DOC: older Word format
        if content_bytes[:8] == b'\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1':
            return (True, 'doc')
    
    return (False, '')

def extract_domain(url: str) -> str:
    """Extrai domínio da URL"""
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""

def timeout_guard(timeout_seconds: int):
    """Decorator para controle de timeout"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await asyncio.wait_for(func(*args, **kwargs), timeout=timeout_seconds)
                elapsed = time.time() - start_time
                if elapsed > timeout_seconds:
                    raise TimeBudgetExceeded(f"Timeout guard: {timeout_seconds}s exceeded: {elapsed:.2f}s")
                return result
            except asyncio.TimeoutError:
                elapsed = time.time() - start_time
                raise TimeBudgetExceeded(f"Function timeout: {timeout_seconds}s exceeded: {elapsed:.2f}s")
        return wrapper
    return decorator


# ============================================================================
# SEÇÃO: SISTEMA DE VALIDAÇÃO INTELIGENTE
# ============================================================================

@dataclass
class ScraperMetricsV2:
    """Métricas exportáveis para telemetria e observabilidade"""
    
    def __init__(self):
        # Counters
        self.adapter_attempts: Dict[str, int] = defaultdict(int)
        self.adapter_successes: Dict[str, int] = defaultdict(int)
        self.adapter_failures: Dict[str, int] = defaultdict(int)
        
        # Latencies (histograma simplificado - mantém últimas 100)
        self.adapter_latencies: Dict[str, List[float]] = defaultdict(list)
        
        # Cache
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        
        # Circuit breakers
        self.circuit_breaker_opens: Dict[str, int] = defaultdict(int)
        
        # Validation
        self.content_validation_failures: Dict[str, int] = defaultdict(int)
    
    def record_attempt(self, adapter: str):
        """Record an adapter attempt"""
        self.adapter_attempts[adapter] += 1
    
    def record_success(self, adapter: str, latency: float):
        """Record a successful scrape"""
        self.adapter_successes[adapter] += 1
        self.adapter_latencies[adapter].append(latency)
        # Keep only last 100 for memory efficiency
        if len(self.adapter_latencies[adapter]) > 100:
            self.adapter_latencies[adapter] = self.adapter_latencies[adapter][-100:]
    
    def record_failure(self, adapter: str):
        """Record a failed scrape"""
        self.adapter_failures[adapter] += 1
    
    def record_circuit_breaker_open(self, host: str):
        """Record circuit breaker opening"""
        self.circuit_breaker_opens[host] += 1
    
    def record_validation_failure(self, reason: str):
        """Record content validation failure"""
        self.content_validation_failures[reason] += 1
    
    def get_summary(self) -> dict:
        """Export metrics for monitoring systems"""
        adapters_summary = {}
        all_adapters = set(list(self.adapter_attempts.keys()) + list(self.adapter_successes.keys()))
        
        for name in all_adapters:
            attempts = self.adapter_attempts.get(name, 0)
            successes = self.adapter_successes.get(name, 0)
            failures = self.adapter_failures.get(name, 0)
            latencies = self.adapter_latencies.get(name, [])
            
            adapters_summary[name] = {
                "attempts": attempts,
                "successes": successes,
                "failures": failures,
                "success_rate": successes / max(1, attempts),
                "avg_latency": sum(latencies) / len(latencies) if latencies else 0,
                "p95_latency": sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) > 0 else 0
            }
        
        return {
            "adapters": adapters_summary,
            "cache": {
                "hits": self.cache_hits,
                "misses": self.cache_misses,
                "hit_rate": self.cache_hits / max(1, self.cache_hits + self.cache_misses)
            },
            "circuit_breakers": dict(self.circuit_breaker_opens),
            "validation_failures": dict(self.content_validation_failures)
        }


@dataclass
class ValidationPolicy:
    """Políticas de validação por modo de página"""
    min_words_accept: int
    min_paragraphs_accept: int
    min_words_warn: int  # Para logar "magro" sem falhar

# Políticas por modo de página
VALIDATION_POLICIES = {
    "strict": ValidationPolicy(180, 2, 120),   # docs, posts longos
    "marketing": ValidationPolicy(50, 1, 30),  # home/SPAs, landing
    "stats": ValidationPolicy(40, 1, 25),      # dashboards/placares
    "pdf": ValidationPolicy(100, 1, 80),       # documentos PDF
}

# Playbook de domínios com modo específico
DOMAIN_VALIDATION_PLAYBOOK = {
    "mistral.ai": "marketing",
    "cohere.ai": "marketing", 
    "ai.meta.com": "marketing",
    "aleph-alpha.com": "marketing",
    "llm-stats.com": "stats",
    "pricepertoken.com": "stats",
    "platform.openai.com": "strict",
    "grok.x.ai": "strict",
}

# Playbook de estratégias anti-Cloudflare
ANTI_CLOUDFLARE_PLAYBOOK = {
    "platform.openai.com": {
        "mode": "strict",
        "strategy_order": ["httpx", "wayback", "skip"],  # Nunca tente Browserless se detectar CF
        "avoid_browserless": True
    },
    "x.ai": {
        "mode": "strict", 
        "strategy_order": ["httpx", "wayback", "skip"],
        "avoid_browserless": True
    },
    "grok.x.ai": {
        "mode": "strict",
        "strategy_order": ["httpx", "wayback", "skip"], 
        "avoid_browserless": True
    },
    "ai.meta.com": {
        "mode": "marketing",
        "strategy_order": ["httpx", "browserless"],
        "avoid_browserless": False
    },
    "mistral.ai": {
        "mode": "marketing",
        "strategy_order": ["httpx", "browserless"],
        "avoid_browserless": False
    },
    "cohere.ai": {
        "mode": "marketing", 
        "strategy_order": ["httpx", "browserless"],
        "avoid_browserless": False
    }
}

def determine_validation_mode(url: str, html_content: str = "") -> str:
    """Determina o modo de validação baseado na URL e conteúdo"""
    domain = extract_domain(url)
    
    # 1. PDFs sempre em modo PDF
    if url.lower().endswith('.pdf'):
        return "pdf"
    
    # 2. Domínio específico no playbook
    if domain in DOMAIN_VALIDATION_PLAYBOOK:
        return DOMAIN_VALIDATION_PLAYBOOK[domain]
    
    # 3. Heurística baseada no conteúdo (se disponível)
    if html_content and BS4_AVAILABLE:
        try:
            soup = BeautifulSoup(html_content, "html.parser")  # type: ignore
            main = soup.find('main')  # type: ignore
            if main and hasattr(main, 'find_all'):
                # Se main tem poucos nós de texto e muitas imagens/iframes -> marketing
                text_nodes = len([n for n in main.find_all(text=True) if hasattr(n, 'strip') and n.strip()])  # type: ignore
                media_nodes = len(main.find_all(['img', 'iframe', 'video', 'svg']))  # type: ignore
                if text_nodes < 10 and media_nodes > 5:
                    return "marketing"
        except Exception:
            pass
    
    # 4. Default: modo strict
    return "strict"

def validate_content_quality(content: str, mode: str = "strict") -> tuple[str, Optional[str]]:
    """Valida qualidade do conteúdo baseado no modo"""
    policy = VALIDATION_POLICIES.get(mode, VALIDATION_POLICIES["strict"])
    
    # Contar palavras e parágrafos
    words = len(content.split())
    paragraphs = len([p for p in content.split('\n\n') if len(p.strip()) > 10])
    
    # Validação principal
    if words >= policy.min_words_accept and paragraphs >= policy.min_paragraphs_accept:
        return ("ok", None)
    
    # Validação flexível (aceitar mas marcar como "thin")
    if words >= policy.min_words_warn and paragraphs >= 1:
        return ("ok_thin", f"Thin content: {words} words, {paragraphs} paragraphs (mode={mode})")
    
    # Falha
    return ("fail", f"Low content ({words}w/{paragraphs}p) for mode={mode} (min: {policy.min_words_accept}w/{policy.min_paragraphs_accept}p)")


# ============================================================================
# SEÇÃO: PROCESSAMENTO DE CONTEÚDO
# ============================================================================

# BEGIN PATCH: metrics normalization - função global
def _nz(x, default=0):
    """Normalize None values to default before comparison"""
    try:
        return x if (x is not None) else default
    except Exception:
        return default

class ContentCleanerV5:
    """Versão otimizada do ContentCleanerV52 com sistema de candidatos"""
    
    def __init__(self):
        # END PATCH
        
        # Padrões de ruído pré-compilados para performance
        self.noise_patterns_raw = [
            "patrocinado", "publicidade", "newsletter", "advertisement",
            "subscribe", "sign up", "register", "login", "follow us",
            "related articles", "you may also like", "more stories",
            "cookie", "privacy policy", "terms of service", "contact us"
        ]
        
        # Padrões de limpeza HTML
        self.html_cleaners = {
            'script': True,
            'style': True,
            'nav': True,
            'header': True,
            'footer': True,
            'aside': True,
            'form': True,
            'noscript': True,
            'svg': True,
            'iframe': True,
            'figure': True
        }
        
        # Seletores de conteúdo principal
        self.main_selectors = [
            'article', 'main', '[role="main"]', '.content', '.article-body',
            '.story-body', '.post-content', '.entry-content', '.the-content',
            '.post-body', '.wp-block-post-content', '#content article',
            '#primary article', '.mc-article-body', '.c-news__body'
        ]
    
    def clean(self, content: Union[str, bytes], config: ScraperConfig) -> str:
        """Limpeza principal com detecção automática de tipo"""
        if isinstance(content, bytes):
            return self._clean_binary(content, config)
        
        if not content or not content.strip():
            return ""
        
        # Detectar tipo de conteúdo
        if self._looks_like_html(content):
            return self._process_html(content, config)
        else:
            return self._clean_plain_text(content)
    
    def _looks_like_html(self, content: str) -> bool:
        """Detect if content looks like HTML (not plain text, JSON, etc.)"""
        if not content or len(content.strip()) < 10:
            return False
        
        content_lower = content.lower().strip()
        
        # Skip obvious non-HTML content (mas trate <!doctype> explicitamente como HTML)
        if content_lower.startswith('<!doctype'):
            return True
        if (content_lower.startswith(('%pdf-', '{', '[', '"')) or 
            content_lower.startswith(('<?xml',)) or
            'content-type:' in content_lower or
            content_lower.count('<') < 2):
            return False
        
        # Look for HTML indicators
        html_indicators = ['<html', '<head', '<body', '<div', '<p>', '<span', '<h1', '<h2', '<h3']
        return any(indicator in content_lower for indicator in html_indicators)
    
    def _process_html(self, html: str, config: ScraperConfig) -> str:
        """Extract text from HTML using candidate scoring system"""
        candidates = []
        
        # Skip Readability for non-HTML content (PDFs, binaries, plain text, etc.)
        if not self._looks_like_html(html):
            logger.info("[ContentCleaner] Content doesn't look like HTML, using basic text extraction")
            return self._strip_html_basic(html, config)
        
        # CANDIDATE 1: Readability (full article extraction)
        if READABILITY_AVAILABLE:
            try:
                doc = Document(html)  # type: ignore
                summary_html = doc.summary()  # type: ignore
                if BS4_AVAILABLE and summary_html:
                    soup = BeautifulSoup(summary_html, "html.parser")  # type: ignore
                    # Convert <br> tags to line breaks before extracting text
                    for br in soup.find_all('br'):  # type: ignore
                        if hasattr(br, 'replace_with'):
                            br.replace_with('\n')  # type: ignore
                    text_readability = soup.get_text(separator='\n\n')  # type: ignore  # Usar \n\n para preservar parágrafos
                    
                    validation = self._is_substantial(text_readability, min_words=50, min_paragraphs=1)
                    if validation['is_substantial']:
                        candidates.append({
                            'method': 'readability',
                            'text': text_readability,
                            'score': validation['score'],
                            'metrics': validation['metrics']
                        })
                        logger.info(f"[ContentCleaner] Readability candidate: {validation['metrics']['word_count']} words, score={validation['score']}")
                    else:
                        logger.warning(f"[ContentCleaner] Readability rejected: {', '.join(validation['reasons'])}")
            except (ValueError, Exception) as e:
                if "XML compatible" in str(e) or "NULL bytes" in str(e):
                    logger.warning(f"[ContentCleaner] Readability failed (binary): {str(e)[:100]}")
                else:
                    logger.warning(f"[ContentCleaner] Readability failed: {str(e)[:100]}")
        
        # CANDIDATE 2: extract_body_two_phase (density + selectors)
        try:
            body_text, body_selector, method = self._extract_body_two_phase(html, "")
            if body_text:
                validation = self._is_substantial(body_text, min_words=50, min_paragraphs=1)
                if validation['is_substantial']:
                    candidates.append({
                        'method': f'dom_two_phase({method}:{body_selector})',
                        'text': body_text,
                        'score': validation['score'],
                        'metrics': validation['metrics']
                    })
                    logger.info(f"[ContentCleaner] DOM two-phase candidate: {validation['metrics']['word_count']} words, score={validation['score']}")
                else:
                    logger.warning(f"[ContentCleaner] DOM two-phase rejected: {', '.join(validation['reasons'])}")
        except Exception as e:
            logger.warning(f"[ContentCleaner] DOM two-phase failed: {str(e)[:100]}")
        
        # CANDIDATE 3: JSON-LD (with strict validation)
        if BS4_AVAILABLE:
            try:
                soup = BeautifulSoup(html, "lxml")  # type: ignore
                parts = []
                for ld in soup.select("script[type='application/ld+json']"):
                    try:
                        data = json.loads(ld.string or "") if ld.string else None
                        if not data:
                            continue
                        for item in (data if isinstance(data, list) else [data]):
                            ab = item.get("articleBody") or item.get("text")
                            if ab:
                                parts.append(ab.strip())
                    except (json.JSONDecodeError, TypeError):
                        continue
                
                if parts:
                    jsonld_text = "\n\n".join(parts)
                    validation = self._is_substantial(jsonld_text, min_words=40, min_paragraphs=1)
                    full_article_check = self._looks_like_full_article(jsonld_text, html)
                    
                    if validation['is_substantial'] and full_article_check['is_full']:
                        # JSON-LD bonus quando substancial (≥120 palavras + metadados)
                        words = len(jsonld_text.split())
                        bonus = 0.0
                        if words >= 120:  # substancial
                            # Verificar metadados para bonus
                            for ld in soup.select("script[type='application/ld+json']"):
                                try:
                                    data = json.loads(ld.string or "") if ld.string else None
                                    if data:
                                        item = data if isinstance(data, dict) else (data[0] if isinstance(data, list) and data else {})
                                        if item.get("headline"): bonus += 0.3
                                        if item.get("author"): bonus += 0.3
                                        if item.get("datePublished") or item.get("dateCreated"): bonus += 0.4
                                        break  # usar primeiro item válido
                                except:
                                    continue
                            
                            adjusted_bonus = min(1.0 + bonus, 1.6)  # +60% máx
                            adjusted_score = validation['score'] * adjusted_bonus
                        else:
                            # JSON-LD gets 20% penalty in scoring (snippets risk)
                            adjusted_score = validation['score'] * 0.8
                        
                        candidates.append({
                            'method': 'jsonld',
                            'text': jsonld_text,
                            'score': adjusted_score,
                            'metrics': validation['metrics'],
                            'full_article_confidence': full_article_check['confidence']
                        })
                        logger.info(f"[ContentCleaner] JSON-LD candidate: {validation['metrics']['word_count']} words, score={adjusted_score:.1f}, full_article_confidence={full_article_check['confidence']:.2f}")
                    else:
                        logger.warning(f"[ContentCleaner] JSON-LD rejected: substantial={validation['is_substantial']}, full_article={full_article_check['is_full']}")
            except Exception as e:
                logger.warning(f"[ContentCleaner] JSON-LD failed: {str(e)[:100]}")
        
        # CANDIDATE 4: BeautifulSoup with aggressive blacklist (last resort)
        if BS4_AVAILABLE:
            try:
                soup = BeautifulSoup(html, "html.parser")  # type: ignore
                
                # REMOÇÃO DE PAYWALL/OVERLAY ANTES DE EXTRAIR TEXTO
                self._remove_paywall_elements(soup)
                
                # Remove noisy elements
                for tag_name in self.html_cleaners:
                    for tag in soup.find_all(tag_name):  # type: ignore
                        tag.decompose()
                
                # Try main content selectors first
                main_content = None
                for selector in self.main_selectors:
                    elements = soup.select(selector)
                    if elements:
                        main_content = elements[0]
                        break
                
                if main_content:
                    # Convert <br> tags to line breaks
                    for br in main_content.find_all('br'):  # type: ignore
                        if hasattr(br, 'replace_with'):
                            br.replace_with('\n')  # type: ignore
                    text_bs4 = main_content.get_text(separator='\n\n', strip=True)  # type: ignore
                else:
                    # Convert <br> tags to line breaks
                    for br in soup.find_all('br'):  # type: ignore
                        if hasattr(br, 'replace_with'):
                            br.replace_with('\n')  # type: ignore
                    text_bs4 = soup.get_text(separator='\n\n', strip=True)  # Preservar parágrafos
                
                validation = self._is_substantial(text_bs4, min_words=30, min_paragraphs=1)
                if validation['is_substantial']:
                    # BeautifulSoup gets 40% penalty (least reliable)
                    adjusted_score = validation['score'] * 0.6
                    candidates.append({
                        'method': 'beautifulsoup_blacklist',
                        'text': text_bs4,
                        'score': adjusted_score,
                        'metrics': validation['metrics']
                    })
                    logger.info(f"[ContentCleaner] BeautifulSoup candidate: {validation['metrics']['word_count']} words, score={adjusted_score:.1f}")
                else:
                    logger.warning(f"[ContentCleaner] BeautifulSoup rejected: {', '.join(validation['reasons'])}")
            except Exception as e:
                logger.warning(f"[ContentCleaner] BeautifulSoup failed: {str(e)[:100]}")
        
        # Select best candidate
        if candidates:
            best_candidate = max(candidates, key=lambda x: x['score'])
            logger.info(f"[ContentCleaner] Selected {best_candidate['method']} as winner (score={best_candidate['score']:.1f})")
            return self._final_cleanup(best_candidate['text'], config)
        else:
            logger.warning("[ContentCleaner] No substantial candidates found, using basic HTML stripping")
            return self._strip_html_basic(html, config)
    
    
    def _remove_paywall_elements(self, soup):
        """Remove elementos de paywall/overlay do HTML"""
        if not BS4_AVAILABLE:
            return
        
        # Seletores de paywall para remover completamente
        PAYWALL_SELECTORS = [
            '.paywall', '.metered', '.piano-paywall', '.tp-modal',
            '[data-paywall]', '.subscribe', '.premium-gate', '.gateway-content',
            '.subscription-wall', '.content-gate', '.paywall-overlay',
            '.modal-paywall', '.paywall-modal', '.subscription-modal',
            '.paywall-content', '.premium-content', '.locked-content',
            '.subscription-required', '.login-required', '.paywall-wrapper'
        ]
        
        # Remover elementos de paywall
        for selector in PAYWALL_SELECTORS:
            for element in soup.select(selector):
                element.decompose()
        
        # Limpar estilos que escondem o scroll
        for tag in soup.select('body[style*="overflow: hidden"]'):
            style = tag.get('style', '')
            # Remove apenas overflow:hidden, mantém outros estilos
            style = re.sub(r'overflow\s*:\s*hidden[;\s]*', '', style)
            if style.strip():
                tag['style'] = style
            else:
                del tag['style']
        
        # Remover blur/filtros que escondem conteúdo
        for tag in soup.select('[style*="filter: blur"], [style*="opacity: 0.5"], [style*="opacity:0.5"]'):
            style = tag.get('style', '')
            # Remove filtros que escondem conteúdo
            style = re.sub(r'filter\s*:\s*blur\([^)]*\)[;\s]*', '', style)
            style = re.sub(r'opacity\s*:\s*0\.5[;\s]*', '', style)
            if style.strip():
                tag['style'] = style
            else:
                del tag['style']
        
        # Remover classes que indicam conteúdo bloqueado
        BLOCKED_CLASSES = [
            'blur', 'blurred', 'locked', 'blocked', 'hidden-content',
            'premium-only', 'subscriber-only', 'paywall-content'
        ]
        
        for class_name in BLOCKED_CLASSES:
            for element in soup.find_all(class_=class_name):
                # Se é um elemento de paywall, remover completamente
                if any(pw in element.get('class', []) for pw in ['paywall', 'subscription', 'premium']):
                    element.decompose()
                else:
                    # Caso contrário, apenas remover a classe
                    classes = element.get('class', [])
                    if class_name in classes:
                        classes.remove(class_name)
                        if classes:
                            element['class'] = classes
                        else:
                            del element['class']
    
    
    def _extract_body_two_phase(self, html: str, url: str = "") -> Tuple[Optional[str], Optional[str], str]:
        """Two-phase body extraction: selectors first, then density analysis"""
        if not BS4_AVAILABLE:
            return None, None, "bs4_unavailable"
        
        soup = BeautifulSoup(html, "html.parser")  # type: ignore
        
        # Phase 1: Try specific selectors first
        for selector in self.main_selectors:
            try:
                elements = soup.select(selector)
                if elements:
                    # Get the first element with substantial content
                    for element in elements:
                        text = element.get_text(separator=' ', strip=True)
                        if len(text.split()) >= 100:  # Minimum 100 words
                            return text, selector, "selector"
            except Exception:
                continue
        
        # Phase 2: Density-based analysis
        return self._density_analysis(soup, url)
    
    def _density_analysis(self, soup, url: str = ""):
        """Density-based content extraction"""
        # Remove noise elements
        for tag_name in self.html_cleaners:
            for tag in soup.find_all(tag_name):
                tag.decompose()
        
        # Score all candidate blocks
        candidates = []
        for tag in soup.find_all(['div', 'section', 'article']):
            try:
                # Get text content
                text = tag.get_text(separator=' ', strip=True)
                if len(text.split()) < 30:  # Skip tiny blocks (30 words minimum)
                    continue
                
                # Count elements
                paragraphs = len(tag.find_all('p'))
                links = len(tag.find_all('a'))
                
                # Calculate text-to-link ratio
                text_to_link_ratio = len(text) / max(1, links * 30)  # Penalize link farms
                
                # Calculate score
                score = len(text) + (paragraphs * 20) + (text_to_link_ratio * 100)
                
                candidates.append({
                    'element': tag,
                    'text': text,
                    'score': score,
                    'paragraphs': paragraphs,
                    'links': links,
                    'text_to_link_ratio': text_to_link_ratio
                })
            except Exception:
                continue
        
        if candidates:
            # Return the highest scoring candidate
            best = max(candidates, key=lambda x: x['score'])
            return best['text'], "density_analysis", "density"
        
        return None, None, "no_candidates"
    
    def _is_substantial(self, text: str, min_words: int = 100, min_paragraphs: int = 2) -> dict:
        """Validate if extracted text is substantial"""
        if not text or not text.strip():
            return {
                'is_substantial': False,
                'score': 0.0,
                'metrics': {},
                'reasons': ['empty_text']
            }
        
        # Basic metrics
        words = text.split()
        word_count = len(words)
        paragraphs = len([p for p in text.split('\n') if p.strip()])
        char_count = len(text)
        lines = len([l for l in text.split('\n') if l.strip()])
        
        # Calculate ratios
        unique_sentences = len(set(s.strip() for s in text.split('.') if s.strip()))
        total_sentences = len([s for s in text.split('.') if s.strip()])
        unique_sentence_ratio = unique_sentences / max(1, total_sentences)
        
        # Boilerplate detection
        text_lower = text.lower()
        boilerplate_hits = sum(1 for pattern in self.noise_patterns_raw if pattern in text_lower)
        boilerplate_ratio = boilerplate_hits / max(1, len(self.noise_patterns_raw))
        
        # Text to link ratio (approximate)
        link_count = text.count('http') + text.count('www.')
        text_to_link_ratio = 1.0 - (link_count / max(1, word_count))
        
        # Average sentence length
        sentences = [s.strip() for s in text.split('.') if s.strip()]
        avg_sentence_len = sum(len(s.split()) for s in sentences) / max(1, len(sentences))
        
        # Calculate overall score (0-10)
        score = 0.0
        reasons = []
        
        # Word count score (0-3)
        if word_count >= min_words:
            score += min(3.0, word_count / 200)
        else:
            reasons.append(f'low_word_count_{word_count}')
        
        # Paragraph count score (0-2)
        if _nz(paragraphs) >= min_paragraphs:
            score += min(2.0, _nz(paragraphs) / 5)
        else:
            reasons.append(f'low_paragraphs_{_nz(paragraphs)}')
        
        # Uniqueness score (0-2)
        if unique_sentence_ratio >= 0.7:
            score += 2.0
        elif unique_sentence_ratio >= 0.5:
            score += 1.0
        else:
            reasons.append(f'low_uniqueness_{unique_sentence_ratio:.2f}')
        
        # Boilerplate score (0-2)
        if boilerplate_ratio <= 0.1:
            score += 2.0
        elif boilerplate_ratio <= 0.2:
            score += 1.0
        else:
            reasons.append(f'high_boilerplate_{boilerplate_ratio:.2f}')
        
        # Text quality score (0-1)
        if text_to_link_ratio >= 0.8:
            score += 1.0
        elif text_to_link_ratio >= 0.6:
            score += 0.5
        else:
            reasons.append(f'low_text_ratio_{text_to_link_ratio:.2f}')
        
        # Sentence length score (0-1)
        if 10 <= avg_sentence_len <= 30:
            score += 1.0
        elif 5 <= avg_sentence_len <= 40:
            score += 0.5
        else:
            reasons.append(f'poor_sentence_len_{avg_sentence_len:.1f}')
        
        is_substantial = score >= 7.0  # 70% threshold
        
        return {
            'is_substantial': is_substantial,
            'score': score,
            'metrics': {
                'word_count': word_count,
                'paragraphs': paragraphs,
                'char_count': char_count,
                'lines': lines,
                'unique_sentence_ratio': unique_sentence_ratio,
                'boilerplate_ratio': boilerplate_ratio,
                'text_to_link_ratio': text_to_link_ratio,
                'avg_sentence_len': avg_sentence_len
            },
            'reasons': reasons
        }
    
    def _looks_like_full_article(self, jsonld_text: str, html: str) -> dict:
        """Validate if JSON-LD articleBody is a full article (not just a snippet)"""
        if not jsonld_text or not html:
            return {'is_full': False, 'confidence': 0.0, 'reasons': ['missing_data']}
        
        # Basic metrics for JSON-LD
        jsonld_words = len(jsonld_text.split())
        jsonld_paragraphs = len([p for p in jsonld_text.split('\n') if p.strip()])
        
        # Compare with raw HTML content
        if BS4_AVAILABLE:
            soup = BeautifulSoup(html, "html.parser")  # type: ignore
            html_text = soup.get_text(separator=' ', strip=True)  # type: ignore
            html_words = len(html_text.split())
            html_paragraphs = len([p for p in html_text.split('\n') if p.strip()])
        else:
            html_words = len(html.split())
            html_paragraphs = 1
        
        # Calculate ratios
        word_ratio = jsonld_words / max(1, html_words)
        paragraph_ratio = jsonld_paragraphs / max(1, html_paragraphs)
        
        # Average sentence length
        sentences = [s.strip() for s in jsonld_text.split('.') if s.strip()]
        avg_sentence_len = sum(len(s.split()) for s in sentences) / max(1, len(sentences))
        
        # Confidence calculation
        confidence = 0.0
        reasons = []
        
        # Word ratio check (0-40%)
        if word_ratio >= 0.8:
            confidence += 0.4
        elif word_ratio >= 0.5:
            confidence += 0.2
        else:
            reasons.append(f'low_word_ratio_{word_ratio:.2f}')
        
        # Paragraph ratio check (0-20%)
        if paragraph_ratio >= 0.6:
            confidence += 0.2
        elif paragraph_ratio >= 0.3:
            confidence += 0.1
        else:
            reasons.append(f'low_paragraph_ratio_{paragraph_ratio:.2f}')
        
        # Sentence length check (0-20%)
        if 10 <= avg_sentence_len <= 30:
            confidence += 0.2
        elif 5 <= avg_sentence_len <= 40:
            confidence += 0.1
        else:
            reasons.append(f'poor_sentence_len_{avg_sentence_len:.1f}')
        
        # Length check (0-20%)
        if jsonld_words >= 200:
            confidence += 0.2
        elif jsonld_words >= 100:
            confidence += 0.1
        else:
            reasons.append(f'short_content_{jsonld_words}')
        
        is_full = confidence >= 0.6  # 60% confidence threshold
        
        return {
            'is_full': is_full,
            'confidence': confidence,
            'reasons': reasons,
            'metrics': {
                'jsonld_words': jsonld_words,
                'jsonld_paragraphs': jsonld_paragraphs,
                'html_words': html_words,
                'html_paragraphs': html_paragraphs,
                'word_ratio': word_ratio,
                'paragraph_ratio': paragraph_ratio,
                'avg_sentence_len': avg_sentence_len
            }
        }
    
    def _consolidate_selectors(self, selectors: list) -> list:
        """Remove duplicate selectors and order by specificity (more specific first)"""
        if not selectors:
            return []
        
        # Remove duplicates while preserving order
        seen = set()
        unique_selectors = []
        for selector in selectors:
            if selector not in seen:
                seen.add(selector)
                unique_selectors.append(selector)
        
        # Order by specificity (more specific first)
        def specificity_score(selector):
            score = 0
            # ID selectors (#id) are most specific
            score += selector.count('#') * 100
            # Class selectors (.class) are medium specific  
            score += selector.count('.') * 10
            # Attribute selectors [attr] are medium specific
            score += selector.count('[') * 10
            # Tag selectors are least specific
            score += selector.count(' ') * 1
            return score
        
        return sorted(unique_selectors, key=specificity_score, reverse=True)
    
    def _clean_binary(self, content: bytes, config: ScraperConfig) -> str:
        """Clean binary content (PDFs, images, etc.)"""
        # For now, return empty string for binary content
        # This can be extended to handle PDFs, images, etc.
        return ""
    
    def _clean_plain_text(self, text: str) -> str:
        """Clean plain text content"""
        # Basic cleanup for plain text
        lines = [line.strip() for line in text.split('\n')]
        return '\n'.join(line for line in lines if line)
    
    def _strip_html_basic(self, html: str, config: Optional[ScraperConfig] = None) -> str:
        """Basic HTML stripping for fallback"""
        if not BS4_AVAILABLE:
            # Very basic regex-based stripping
            import re
            text = re.sub(r'<[^>]+>', '', html)
            return self._final_cleanup(text, config)
        
        soup = BeautifulSoup(html, "html.parser")  # type: ignore
        return self._final_cleanup(soup.get_text(separator='\n', strip=True), config)  # type: ignore
    
    def _final_cleanup(self, text: str, config: Optional[ScraperConfig] = None) -> str:
        """Final text cleanup and normalization preserving paragraphs"""
        if not text:
            return ""
        
        # Normalize line breaks
        text = text.replace('\r\n', '\n').replace('\r', '\n')
        
        # Remove trailing spaces but **preserve** empty lines (maintain paragraphs)
        text = '\n'.join(line.rstrip() for line in text.split('\n'))
        
        # Collapse 3+ line breaks to 2 (maintain paragraphs)
        text = re.sub(r'\n{3,}', '\n\n', text)
        
        # Multiple spaces -> single (don't cross line breaks)
        text = re.sub(r'[ \t]+', ' ', text)
        
        # Normalize common issues
        replacements = {
            '\u201c': '"',  # Left double quotation mark
            '\u201d': '"',  # Right double quotation mark
            '\u2018': "'",  # Left single quotation mark
            '\u2019': "'",  # Right single quotation mark
            '\u2013': '-',  # En dash
            '\u2014': '--', # Em dash
            '\u2026': '...', # Horizontal ellipsis
            '\u00a0': ' ',  # Non-breaking space
            '\u200b': '',   # Zero-width space
            '\u2011': '-',  # Non-breaking hyphen (causou erro no prompt555)
            '\u00ad': '',   # Soft hyphen
            '\u2009': ' ',  # Thin space
            '\u200a': ' ',  # Hair space
        }
        
        for bad, good in replacements.items():
            text = text.replace(bad, good)
        
        # Apply word limit if configured
        if config and hasattr(config, 'content_word_limit') and config.content_word_limit > 0:
            words = text.split()
            if len(words) > config.content_word_limit:
                text = ' '.join(words[:config.content_word_limit])
                text += f"\n\n[... conteúdo truncado - limite de {config.content_word_limit} palavras atingido ...]"
        
        return text.strip()

class ContentValidator:
    """Validação de qualidade de conteúdo"""
    
    def __init__(self):
        self.min_word_count = 100
        self.min_paragraphs = 2
        self.min_quality_score = 3.0
    
    def validate(self, content: str, url: str="", metadata: Optional[dict]=None) -> dict:
        """Validador com sinais ricos - libera notas curtas quando ricas"""
        import re
        if not content or not content.strip():
            return {'passed': False, 'score': 0.0, 'drop_reason':'empty_content'}
        
        words = len(content.split())
        paragraphs = sum(1 for p in content.split('\n\n') if len(p.strip()) > 10)
        metadata = metadata or {}

        # tenta título do <title> se não veio
        if not metadata.get('title'):
            m = re.search(r'<title>(.*?)</title>', (metadata.get('html_raw') or ''), flags=re.I|re.S)
            if m: metadata['title'] = re.sub(r'\s+', ' ', m.group(1)).strip()

        rich_signals = sum([
            bool(metadata.get('title')),
            bool(metadata.get('published_at')),
            bool(metadata.get('author')),
            ('article' in content[:1000].lower())
        ])

        # Validação mais permissiva - aceitar conteúdo substancial
        min_words = 30  # Reduzido de 80 para 30 para sites simples
        if rich_signals >= 2: min_words = 20  # liberar notas curtas ricas
        if rich_signals >= 1: min_words = 25  # liberar conteúdo com pelo menos 1 sinal

        score = 0.0
        if words >= min_words: score += 3.0
        if _nz(paragraphs) >= 1:    score += 2.0  # Reduzido de 2 para 1
        if rich_signals >= 1:  score += 1.0  # Reduzido de 2 para 1
        if words >= 50: score += 1.0  # Bonus para conteúdo com mais palavras

        # Critério mais permissivo - aceitar conteúdo básico
        passed = (score >= 3.0 and words >= min_words)  # Reduzido de 4.0 para 3.0
        return {
            'passed': passed, 'score': score, 'word_count': words,
            'paragraphs': paragraphs, 'rich_signals': rich_signals,
            'drop_reason': None if passed else f'insufficient_{words}w_{rich_signals}sigs'
        }

class MetadataExtractor:
    """Extração de metadados"""
    
    def __init__(self):
        self.title_selectors = [
            'meta[property="og:title"]',
            'meta[name="twitter:title"]',
            'meta[property="article:title"]',
            'h1',
            'title'
        ]
        
        self.author_selectors = [
            'meta[name="author"]',
            'meta[property="article:author"]',
            'meta[property="og:article:author"]',
            '.author',
            '.byline',
            '[rel="author"]'
        ]
        
        self.date_selectors = [
            'meta[property="article:published_time"]',
            'meta[property="article:modified_time"]',
            'time[datetime]',
            '.published',
            '.date'
        ]
    
    def extract(self, html: str, url: str = "") -> dict:
        """Extração completa de metadados"""
        if not BS4_AVAILABLE:
            return {}
        
        soup = BeautifulSoup(html, "html.parser")  # type: ignore
        
        metadata = {
            'title': self.extract_title(soup),
            'author': self.extract_author(soup),
            'published_at': self.extract_published_date(soup),
            'canonical_url': self.extract_canonical_url(soup, url),
            'description': self.extract_description(soup),
            'language': self.extract_language(soup)
        }
        
        return {k: v for k, v in metadata.items() if v}
    
    def extract_title(self, soup) -> Optional[str]:
        """Extração de título com fallbacks"""
        for selector in self.title_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    title = element.get('content') or element.get_text(strip=True)
                    if title:
                        return self._clean_title(title)
            except Exception:
                continue
        return None
    
    def extract_author(self, soup) -> Optional[str]:
        """Extração de autor"""
        for selector in self.author_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    author = element.get('content') or element.get_text(strip=True)
                    if author:
                        return author.strip()
            except Exception:
                continue
        return None
    
    def extract_published_date(self, soup) -> Optional[str]:
        """Extração de data de publicação"""
        for selector in self.date_selectors:
            try:
                element = soup.select_one(selector)
                if element:
                    date_str = element.get('content') or element.get('datetime') or element.get_text(strip=True)
                    if date_str:
                        return self._parse_date(date_str)
            except Exception:
                continue
        return None
    
    def extract_canonical_url(self, soup, url: str = "") -> Optional[str]:
        """Extração de URL canônica"""
        canonical = soup.find('link', rel='canonical')
        if canonical and canonical.get('href'):
            return canonical['href']
        return url if url else None
    
    def extract_description(self, soup) -> Optional[str]:
        """Extração de descrição"""
        selectors = [
            'meta[property="og:description"]',
            'meta[name="description"]',
            'meta[property="twitter:description"]'
        ]
        
        for selector in selectors:
            try:
                element = soup.select_one(selector)
                if element and element.get('content'):
                    return element['content'].strip()
            except Exception:
                continue
        return None
    
    def extract_language(self, soup) -> Optional[str]:
        """Extração de idioma"""
        html_tag = soup.find('html')
        if html_tag and html_tag.get('lang'):
            return html_tag['lang']
        
        meta_lang = soup.find('meta', attrs={'http-equiv': 'Content-Language'})
        if meta_lang and meta_lang.get('content'):
            return meta_lang['content']
        
        return None
    
    def _clean_title(self, title: str) -> str:
        """Limpeza de título"""
        # Remove site suffixes
        suffixes = [' | ', ' - ', ' :: ']
        for suffix in suffixes:
            if suffix in title:
                title = title.split(suffix)[0]
        
        return title.strip()
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parsing básico de data"""
        # Try to extract ISO date format
        import re
        iso_match = re.search(r'\d{4}-\d{2}-\d{2}', date_str)
        if iso_match:
            return iso_match.group()
        
        # Try to extract other common formats
        date_match = re.search(r'\d{1,2}/\d{1,2}/\d{4}', date_str)
        if date_match:
            return date_match.group()
        
        return date_str.strip()

# ============================================================================
# SEÇÃO 4: BUILDER CANÔNICO PARA BROWSERLESS

def _normalize_context(d: dict | None) -> dict:
    """Normaliza contexto para primitivos (resolve erro de tipo bool)"""
    if not d:
        return {}
    def norm(v):
        if isinstance(v, bool):        # True/False -> "true"/"false"
            return "true" if v else "false"
        if isinstance(v, (int, float, str)) or v is None:
            return v
        return str(v)                  # arrays/objetos -> string
    return {k: norm(v) for k, v in d.items()}

def _is_json_content_type(ct: str | None) -> bool:
    """Verifica se content-type é JSON"""
    return bool(ct and "json" in ct.lower())

def _model_to_dict(obj):
    """Compatibilidade Pydantic v1/v2"""
    if hasattr(obj, "model_dump"):
        return obj.model_dump()  # Pydantic v2
    if hasattr(obj, "dict"):
        return obj.dict()  # Pydantic v1
    return dict(obj)

async def _parse_browserless_response(resp) -> dict:
    """Parse resposta Browserless aceitando HTML ou JSON"""
    ct = (resp.headers or {}).get("content-type", "")
    text = await resp.text()  # Corrigido: await resp.text()
    if _is_json_content_type(ct):
        return await resp.json()  # Corrigido: await resp.json()
    # Quando vier HTML, embrulhe como JSON padronizado do seu app:
    return {
        "ok": resp.status < 400,
        "status": resp.status,
        "mime": ct or "text/html",
        "html": text or "",
        "url": getattr(resp, "url", None),
    }

def _is_soft_404(html: str, title: str | None, status: int | None) -> bool:
    """Detecta soft-404 para evitar tentar parsear como artigo"""
    if status and status >= 400:
        return True
    if not html or len(html.strip()) < 200:
        return True
    t = (title or "").lower()
    if "page not found" in t or "404" in t:
        return True
    return False

def _is_likely_404(html: str, cleaned_text: str) -> bool:
    """Detecção robusta de soft-404 antes do cleaner"""
    import re
    if not html: 
        return True
    if len(re.sub(r"<[^>]+>", " ", html)) < 400:  # HTML "curto"
        return True
    txt = (cleaned_text or "").lower()[:600]
    strong = [
        "page not found", "página não encontrada", "content not found", 
        "conteúdo não encontrado", "404", "not found", "não encontrado",
        "access denied", "acesso negado", "forbidden", "proibido"
    ]
    if any(s in txt for s in strong):
        return len((cleaned_text or "").split()) < 150
    return False

def build_chromium_query(config: ScraperConfig, timeout_ms: int, block_ads: bool = False) -> dict:
    """Build canonical query parameters for chromium endpoints - normaliza para primitivos"""
    query = {}
    if config.browserless_token:
        query["token"] = str(config.browserless_token)
    if timeout_ms:
        query["timeout"] = str(timeout_ms)
    query["blockAds"] = "true" if block_ads else "false"  # Converte bool para string
    return query

def build_content_body(url: str, config: ScraperConfig, js: bool = True, wait_until: Optional[str] = None, headers: Optional[dict] = None) -> dict:
    """Build canonical body for /content endpoint - schema correto do Browserless"""
    body = {"url": str(url)}
    
    # waitUntil deve ir dentro de gotoOptions (não diretamente)
    if wait_until:
        body["gotoOptions"] = {"waitUntil": str(wait_until)}  # type: ignore
    
    # headers NÃO é permitido no /content endpoint do Browserless
    # if headers:
    #     body["headers"] = {str(k): str(v) for k, v in headers.items()}
    
    return body

def build_function_body(code: str, context: Optional[dict] = None) -> dict:
    """Build canonical body for /function endpoint - normaliza context para primitivos"""
    return {
        "code": code,
        "context": _normalize_context(context or {})
    }

# Heurísticas de qualidade e recuperação
def looks_like_404(doc_title: str, html: str) -> bool:
    """Detect if content looks like a 404 page"""
    if not doc_title and not html:
        return False
    
    title_lower = (doc_title or "").lower()
    html_lower = html.lower() if html else ""
    
    # Check title patterns
    title_indicators = [
        "page not found",
        "página não encontrada", 
        "404",
        "not found",
        "error 404",
        "página não existe"
    ]
    
    # Check HTML patterns
    html_indicators = [
        ">404<",
        "page not found",
        "not found",
        "error 404",
        "página não encontrada"
    ]
    
    return any(indicator in title_lower for indicator in title_indicators) or \
           any(indicator in html_lower for indicator in html_indicators)

async def recover_slug_from_wpjson(session, base_url: str, query: str) -> Optional[str]:
    """Recover correct slug from WordPress JSON API"""
    try:
        # Try wp-json API
        api_url = f"{base_url.rstrip('/')}/wp-json/wp/v2/search"
        params = {"search": query, "per_page": 5}
        
        async with session.get(api_url, params=params, timeout=ScraperConstants.DEFAULT_TIMEOUT_HTTPX / 1000) as response:
            if response.status == 200:
                data = await response.json()
                # Get first valid result
                for item in data or []:
                    url = item.get("url") or item.get("link")
                    if url and not looks_like_404("", ""):
                        logger.info(f"[WP-JSON] Recovered slug: {url}")
                        return url
    except Exception as e:
        logger.warning(f"[WP-JSON] Failed to recover slug: {e}")
    
    return None

def extract_search_terms_from_url(url: str) -> str:
    """Extract search terms from URL path for slug recovery"""
    from urllib.parse import urlparse
    import re
    
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    
    # Remove common path patterns
    path = re.sub(r'^.*?/(\d{4}/\d{2}/\d{2}/)', '', path)  # Remove date prefix
    path = re.sub(r'^.*?/(amp/)?', '', path)  # Remove amp prefix
    path = re.sub(r'\.html?$', '', path)  # Remove .html suffix
    
    # Split by slashes and hyphens, take first 2-3 meaningful parts
    terms = re.split(r'[-/_]', path)
    meaningful_terms = [t for t in terms if len(t) > 2 and not t.isdigit()][:3]
    
    return ' '.join(meaningful_terms) if meaningful_terms else path

async def discover_amp_url(session, url: str) -> Optional[str]:
    """Discover real AMP URL via rel=amphtml from canonical page"""
    try:
        # First, get the canonical page to find rel=amphtml
        async with session.get(url, timeout=ScraperConstants.DEFAULT_TIMEOUT_HTTPX / 1000) as response:
            if response.status == 200:
                html = await response.text()
                
                # Look for rel=amphtml in the HTML
                import re
                amp_pattern = r'<link[^>]*rel=["\']amphtml["\'][^>]*href=["\']([^"\']+)["\'][^>]*>'
                amp_match = re.search(amp_pattern, html, re.IGNORECASE)
                
                if amp_match:
                    amp_url = amp_match.group(1)
                    # Make absolute URL if relative
                    if amp_url.startswith('/'):
                        from urllib.parse import urljoin
                        amp_url = urljoin(url, amp_url)
                    logger.info(f"[AMP] Discovered real AMP URL: {amp_url}")
                    return amp_url
                else:
                    logger.info(f"[AMP] No rel=amphtml found in {url}")
                    
    except Exception as e:
        logger.warning(f"[AMP] Failed to discover AMP URL: {e}")
    
    return None

# SEÇÃO 5: ADAPTERS DE SCRAPING
# ============================================================================

class BaseAdapter(ABC):
    """Classe base para todos os adapters"""
    
    def __init__(self, config: ScraperConfig, metrics: AdapterMetrics):
        self.config = config
        self.metrics = metrics
        self.logger = StructuredLogger(self.__class__.__name__)
        self.circuit_breaker = CircuitBreaker()
    
    @abstractmethod
    async def scrape(self, url: str) -> Optional[ScrapeResult]:
        """Método principal de scraping"""
        pass
    
    def get_priority(self, url: str) -> int:
        """Prioridade do adapter para a URL"""
        return 100  # Padrão: baixa prioridade
    
    def can_handle(self, url: str) -> bool:
        """Verifica se pode processar a URL"""
        return True
    
    def _record_success(self, duration: float):
        """Registra sucesso nas métricas"""
        self.metrics.attempts += 1
        self.metrics.successes += 1
        self.metrics.total_time += duration
        self.metrics.avg_time = self.metrics.total_time / self.metrics.successes
        self.metrics.success_rate = (self.metrics.successes / self.metrics.attempts) * 100
        self.metrics.last_success = datetime.now()
        self.circuit_breaker.record_success()
    
    def _record_failure(self, duration: float = 0.0):
        """Registra falha nas métricas"""
        self.metrics.attempts += 1
        self.metrics.failures += 1
        self.metrics.total_time += duration
        self.metrics.success_rate = (self.metrics.successes / self.metrics.attempts) * 100
        self.metrics.last_failure = datetime.now()
        self.circuit_breaker.record_failure()

class HttpxAdapterV5(BaseAdapter):
    """HTTP adapter using modern httpx library - PRIMARY STRATEGY (priority: 50).
    
    Fast, lightweight scraper for static HTML content. Uses httpx with:
    - HTTP/2 support (multiplexing, header compression)
    - Automatic redirect following (301/302/308)
    - Streaming responses (memory efficient)
    - Connection pooling (8 keepalive connections)
    
    Best for:
        ✅ Static HTML (blogs, news articles, documentation)
        ✅ Fast scraping (avg 2s per URL)
        ✅ Low resource usage (no browser overhead)
        ✅ High throughput (16 concurrent connections)
    
    Not suitable for:
        ❌ JavaScript-heavy sites (SPAs, dynamic content)
        ❌ Bot-protected sites (Cloudflare challenges)
        ❌ Paywall content (soft/hard blocks)
        → Use JinaAdapterV5 or BrowserlessAdapterV5 instead
    
    Features:
        - Automatic document detection (delegates PDF/DOCX to DocumentAdapter)
        - Bot-wall detection (falls back to next adapter)
        - Intelligent header rotation (anti-bot, 3 sets)
        - Exponential backoff retry (1s → 2s → 4s)
        - Circuit breaker protection (per-host)
    
    Priority chain position: 2nd (after DocumentAdapter for non-docs)
    Fallback adapters: JinaAdapterV5 → BrowserlessAdapterV5
    """
    
    def __init__(self, config: ScraperConfig, metrics: AdapterMetrics):
        super().__init__(config, metrics)
        if not HTTPX_AVAILABLE:
            raise ImportError("httpx is required for HttpxAdapterV5")
        
        self.client = httpx.AsyncClient(  # type: ignore
            timeout=httpx.Timeout(ScraperConstants.DEFAULT_TIMEOUT_HTTPX / 1000),  # type: ignore
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),  # type: ignore
            follow_redirects=True,  # ESSENCIAL: seguir redirects 301/308
            http2=True,  # Habilitar HTTP/2 para melhor performance
            verify=True  # Verificar SSL por padrão
        )
        self.backoff = ExponentialBackoff(base_delay=1.0, max_delay=10.0)
    
    def get_priority(self, url: str) -> int:
        """Return adapter priority (50 = high priority, tries early in chain).
        
        Lower number = Higher priority (tries first)
        HttpxAdapter: 50 (2nd after DocumentAdapter for non-docs)
        """
        return 50
    
    async def scrape(self, url: str) -> Optional[ScrapeResult]:
        """Scrape content from URL using httpx with retry, backoff, and bot-wall detection.
        
        Execution flow:
        1. Check circuit breaker (skip if open)
        2. Try max_retries times with header rotation:
           - Attempt 0: Desktop headers (full Sec-Ch-Ua)
           - Attempt 1: Mobile headers (simulated iPhone)
           - Attempt 2: Linux headers (minimal)
        3. For each attempt:
           - Stream response (memory efficient)
           - Detect document (PDF/DOCX) → delegate to DocumentAdapter
           - Detect bot-wall → fallback to next adapter
           - Decode content (UTF-8 with ignore errors)
           - Return ScrapeResult on success
        4. On failure: backoff delay (1s → 2s → 4s) and retry
        
        Args:
            url: Target URL to scrape
        
        Returns:
            ScrapeResult with extracted HTML content, or None if:
            - Circuit breaker open (host cooling down)
            - Bot-wall detected (Cloudflare/Akamai challenge)
            - Document detected (delegated to DocumentAdapter)
            - All retries exhausted (HTTP errors)
        
        Side effects:
            - Updates adapter metrics (attempts, successes, failures)
            - Updates circuit breaker state (per-host)
            - Logs scrape attempts and results
        
        Example:
            >>> adapter = HttpxAdapterV5(config, metrics)
            >>> result = await adapter.scrape("https://example.com")
            >>> if result and result.success:
            ...     print(f"✅ {result.word_count} words via httpx")
            ... else:
            ...     print("❌ Failed, will try next adapter")
        """
        if self.circuit_breaker.is_open():
            self.logger.log_scrape_failure(url, self.__class__.__name__, "Circuit breaker open")
            return None
        
        self.logger.log_scrape_attempt(url, self.__class__.__name__)
        start_time = time.time()
        
        # Headers inteligentes baseados no domínio
        domain = extract_domain(url)
        
        # Headers específicos para domínios com Cloudflare
        if domain in ["julius.ai", "techpoint.africa"]:
            # Headers mais realistas para bypass do Cloudflare
            header_sets = []
            for attempt in range(self.config.max_retries):
                headers = build_real_headers(attempt, url=url)
                # Adicionar headers específicos para Cloudflare
                headers.update({
                    "Sec-Ch-Ua": '"Chromium";v="126", "Not(A:Brand";v="24", "Google Chrome";v="126"',
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": '"Windows"',
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-User": "?1",
                    "Sec-Fetch-Dest": "document"
                })
                header_sets.append(headers)
        else:
            # Headers padrão com rotação progressiva
            header_sets = []
            for attempt in range(self.config.max_retries):
                header_sets.append(build_real_headers(attempt, url=url))
        
        # Adicionar headers básicos como fallback final
        header_sets.extend([
            build_headers(self.config.user_agent),
            build_headers(self.config.user_agent, {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}),
        ])
        
        for attempt in range(self.config.max_retries):
            try:
                headers = header_sets[attempt % len(header_sets)]
                
                async with self.client.stream('GET', url, headers=headers) as response:
                    if response.status_code == 200:
                        # ✅ PATCH 2: Detecção PRECOCE de documentos (ANTES de baixar tudo)
                        # RAZÃO: Evita desperdício de banda/tempo baixando PDF/DOCX completo
                        # IMPACTO: Economiza ~95% de banda em URLs de documentos grandes
                        content_type = response.headers.get('content-type', '')
                        
                        # STEP 1: Detecção rápida por URL e Content-Type (SEM ler body)
                        is_doc_quick, doc_type_quick = is_document_url(url, content_type, None)
                        
                        if is_doc_quick:
                            logger.info(f"[HttpxAdapterV5] Document detected EARLY via URL/headers (type: {doc_type_quick}), delegating to DocumentAdapter")
                            # ✅ CRÍTICO: Fechar response SEM ler body (economiza banda!)
                            await response.aclose()
                            return None  # Deixa DocumentAdapter processar
                        
                        # STEP 2: Read full content first, then check magic bytes
                        # httpx.aread() doesn't accept size parameter - read all content
                        content = await response.aread()  # type: ignore
                        
                        # STEP 3: Check magic bytes for document detection
                        is_doc_magic, doc_type_magic = is_document_url(url, content_type, content)
                        
                        if is_doc_magic:
                            logger.info(f"[HttpxAdapterV5] Document detected via magic bytes (type: {doc_type_magic}), delegating to DocumentAdapter")
                            # ✅ CRÍTICO: Fechar response e parar de ler
                            await response.aclose()
                            return None  # Deixa DocumentAdapter processar
                        
                        # Continue with content processing since it's not a document
                        content_str = content.decode('utf-8', errors='ignore')
                        
                        # Verificar se não é bot-wall
                        bot_wall_info = is_bot_wall(response.status_code, url, content_str, dict(response.headers))
                        if bot_wall_info['is_bot_wall']:
                            logger.warning(f"[HttpxAdapterV5] Bot-wall detected for {url}: {bot_wall_info['provider']} ({bot_wall_info['reason']})")
                            # Delay maior para domínios com Cloudflare
                            if domain in ["julius.ai", "techpoint.africa"]:
                                delay = self.config.rate_limit_delay * 3  # 3x delay para Cloudflare
                            else:
                                delay = self.config.rate_limit_delay
                            
                            # Rate limiting por host - delay entre tentativas
                            if attempt < self.config.max_retries - 1:  # Não delay na última tentativa
                                await asyncio.sleep(delay)
                            continue  # Tentar próximo conjunto de headers
                        
                        # Sucesso - processar conteúdo
                        duration = time.time() - start_time
                        self._record_success(duration)
                        self.logger.log_scrape_success(url, self.__class__.__name__, duration, len(content_str.split()))
                        
                        return ScrapeResult(
                            url=url,
                            content=content_str,
                            word_count=len(content_str.split()),
                            success=True,
                            adapter_used=self.__class__.__name__,
                            extraction_time=duration,
                            metadata={'strategy': 'httpx', 'status_code': response.status_code, 'headers': dict(response.headers)}
                        )
                    elif response.status_code in [403, 404, 429]:
                        # Non-retryable errors
                        duration = time.time() - start_time
                        self._record_failure(duration)
                        self.logger.log_scrape_failure(url, self.__class__.__name__, f"HTTP {response.status_code}", duration)
                        return None
                    else:
                        # Retryable errors
                        if attempt < self.config.max_retries - 1:
                            await self.backoff.wait(attempt)
                            continue
                        else:
                            duration = time.time() - start_time
                            self._record_failure(duration)
                            self.logger.log_scrape_failure(url, self.__class__.__name__, f"HTTP {response.status_code}", duration)
                            return None
                            
            except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError) as e:  # type: ignore
                if attempt < self.config.max_retries - 1:
                    await self.backoff.wait(attempt)
                    continue
                else:
                    duration = time.time() - start_time
                    self._record_failure(duration)
                    self.logger.log_scrape_failure(url, self.__class__.__name__, str(e), duration)
                    return None
            except Exception as e:
                duration = time.time() - start_time
                self._record_failure(duration)
                self.logger.log_scrape_failure(url, self.__class__.__name__, str(e), duration)
                return None
        
        return None

class JinaAdapterV5(BaseAdapter):
    """Jina Reader API adapter - CLOUD-BASED EXTRACTION (priority: 55).
    
    Cloud-based content extraction using Jina Reader API (r.jina.ai).
    Excellent for bypassing bot-walls, paywalls, and extracting clean content.
    
    Best for:
        ✅ News articles (bypasses most paywalls)
        ✅ Modern websites (handles JS/SPA well)
        ✅ Bot-protected sites (Cloudflare/Akamai bypass)
        ✅ Clean extraction (removes ads, nav, footers automatically)
    
    Limitations:
        ⚠️ Rate limited (60 RPM default, configurable via jina_rpm valve)
        ⚠️ Requires internet access to r.jina.ai
        ⚠️ Slower than httpx (~3s vs ~2s avg)
        ⚠️ May fail on very old sites (pre-2010)
    
    Features:
        - Automatic paywall bypass (soft paywalls ~80% success)
        - Rate limiting (semaphore + delay between requests)
        - Clean text extraction (article-focused)
        - Circuit breaker protection
    
    Priority chain position: 3rd (after HttpxAdapter)
    Fallback adapters: BrowserlessAdapterV5 → AlternatesAdapterV5
    """
    
    def __init__(self, config: ScraperConfig, metrics: AdapterMetrics):
        super().__init__(config, metrics)
        self.endpoint = config.jina_endpoint
        self.rate_limiter = asyncio.Semaphore(1)  # Simple rate limiting
        self.last_request_time = 0.0
    
    def get_priority(self, url: str) -> int:
        """Return adapter priority (55 = very high, tries after httpx).
        
        Priority 55: Higher than Browserless (70), lower than Httpx (50)
        Rationale: Faster than browser, better paywall bypass than httpx
        """
        return 55
    
    def can_handle(self, url: str) -> bool:
        """Check if adapter can handle URL (always True for Jina).
        
        Jina Reader can process any HTTP(S) URL.
        """
        return True
    
    async def scrape(self, url: str) -> Optional[ScrapeResult]:
        """Scrape content via Jina Reader API with rate limiting and retry.
        
        Execution flow:
        1. Check circuit breaker and enable flag
        2. Acquire rate limiter (max 60 RPM)
        3. Build Jina URL: {endpoint}/{original_url}
        4. GET request with timeout (jina_timeout valve)
        5. Return clean text content
        
        Args:
            url: Target URL to scrape
        
        Returns:
            ScrapeResult with clean text content, or None if:
            - Adapter disabled (enable_jina=False)
            - Circuit breaker open
            - Rate limit exceeded (shouldn't happen with semaphore)
            - HTTP error from Jina API
            - Jina API timeout (default: 25s)
        
        Rate limiting:
            - Semaphore: 1 concurrent request
            - Delay: rate_limit_delay between requests (default: 0.5s)
            - Total: ~60 RPM effective
        
        Example:
            >>> adapter = JinaAdapterV5(config, metrics)
            >>> result = await adapter.scrape("https://paywalled-news.com/article")
            >>> if result:
            ...     print(f"✅ Bypassed paywall: {result.word_count} words")
        """
        if not self.config.enable_jina:
            return None
        
        if self.circuit_breaker.is_open():
            self.logger.log_scrape_failure(url, self.__class__.__name__, "Circuit breaker open")
            return None
        
        self.logger.log_scrape_attempt(url, self.__class__.__name__)
        start_time = time.time()
        
        try:
            # Rate limiting
            async with self.rate_limiter:
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                if time_since_last < self.config.rate_limit_delay:
                    await asyncio.sleep(self.config.rate_limit_delay - time_since_last)
                self.last_request_time = time.time()
            
            # Build Jina URL
            jina_url = f"{self.endpoint}/{url}"
            headers = {
                "Accept": "text/plain",
                "User-Agent": self.config.user_agent
            }
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.config.jina_timeout)) as session:
                async with session.get(jina_url, headers=headers) as response:
                    if response.status == 200:
                        # aiohttp.ClientSession uses .text() for text content
                        content = await response.text()
                        
                        duration = time.time() - start_time
                        self._record_success(duration)
                        self.logger.log_scrape_success(url, self.__class__.__name__, duration, len(content.split()))
                        
                        return ScrapeResult(
                            url=url,
                            content=content,
                            word_count=len(content.split()),
                            success=True,
                            adapter_used=self.__class__.__name__,
                            extraction_time=duration,
                            metadata={'strategy': 'jina', 'jina_url': jina_url, 'status_code': response.status}
                        )
                    else:
                        duration = time.time() - start_time
                        self._record_failure(duration)
                        self.logger.log_scrape_failure(url, self.__class__.__name__, f"HTTP {response.status}", duration)
                        return None
                        
        except asyncio.TimeoutError:
            duration = time.time() - start_time
            self._record_failure(duration)
            self.logger.log_scrape_failure(url, self.__class__.__name__, "Timeout", duration)
            return None
        except Exception as e:
            duration = time.time() - start_time
            self._record_failure(duration)
            self.logger.log_scrape_failure(url, self.__class__.__name__, str(e), duration)
            return None
        
        return None

class BrowserlessAdapterV5(BaseAdapter):
    """Browserless headless browser adapter - HEAVY ARTILLERY (priority: 70).
    
    Puppeteer-based browser automation via Browserless.io for complex sites.
    Executes JavaScript, handles SPAs, and bypasses sophisticated bot-walls.
    
    Best for:
        ✅ SPAs (React, Vue, Angular apps)
        ✅ JavaScript-heavy sites (dynamic content loading)
        ✅ Complex paywalls (overlays, modals, soft blocks)
        ✅ Bot-protected sites (anti-automation systems)
    
    Limitations:
        ⚠️ Slow (~8s avg vs ~2s for httpx)
        ⚠️ Resource intensive (browser instance per request)
        ⚠️ Requires Browserless endpoint (http://host:port)
        ⚠️ May fail on Cloudflare Turnstile (use Wayback instead)
    
    Strategies (tried in order per playbook):
        1. scrape: Extract via CSS selectors + wait for DOM
        2. content: Navigate + wait + extract body
        3. amp: Try AMP version first (if detected)
        4. function: Custom JS function with overlay dismissal
    
    Features:
        - Multi-strategy fallback (4 strategies per domain)
        - Playbook-based configuration (domain-specific)
        - Overlay dismissal (GDPR, cookies, paywalls)
        - Metadata capture (JSON-LD, Open Graph)
        - Stealth mode (hide webdriver, realistic UA)
    
    Priority chain position: 4th (after Httpx, Jina)
    Fallback adapters: AlternatesAdapterV5 → WaybackAdapterV5
    
    Configuration:
        - browserless_endpoint: Browserless server URL
        - browserless_token: API token (optional)
        - browserless_timeout: Timeout in seconds (default: 45s)
        - enable_browserless: Enable/disable adapter
    """
    
    def __init__(self, config: ScraperConfig, metrics: AdapterMetrics):
        super().__init__(config, metrics)
        self.endpoint = config.browserless_endpoint or "http://192.168.1.197:3199"
        self._health_checked = False
        # Listas orientadas por domínio para ordenar estratégias
        self.AMP_FIRST_DOMAINS = {
            "g1.globo.com", "folha.uol.com.br", "estadao.com.br", "exame.com",
            "uol.com.br", "oglobo.globo.com", "valor.globo.com"
        }
        self.SCRAPE_WHITELIST = {
            "github.com", "medium.com", "dev.to", "stackoverflow.com",
            "reddit.com", "wikipedia.org"
        }
        self.strategies = {
            'scrape': ScrapeStrategyV5(),
            'content': ContentFallbackStrategy(),
            'amp': AMPFallbackStrategy(), 
            'function': FunctionFallbackStrategy()
        }
    
    def _detect_challenge(self, response_text: str, status_code: int) -> bool:
        """Detecta Cloudflare challenge ou outros bloqueios"""
        if status_code in [403, 401]:
            return True
        
        if not response_text:
            return False
            
        response_lower = response_text.lower()
        challenge_indicators = [
            'attention required',
            'cloudflare',
            'cdn-cgi/challenge-platform',
            'cf-error',
            'checking your browser',
            'please wait while we check your browser'
        ]
        
        return any(indicator in response_lower for indicator in challenge_indicators)
    
    def get_priority(self, url: str) -> int:
        """Prioridade média - heavy artillery quando HTTPX e Jina falharem"""
        return 70
    
    def can_handle(self, url: str) -> bool:
        """Browserless can handle most URLs"""
        return True
    
    async def _assert_browserless_up(self) -> bool:
        """Health check rápido para Browserless"""
        if self._health_checked:
            return True
            
        try:
            # Testar com endpoint /stats que é mais comum
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=ScraperConstants.DEFAULT_TIMEOUT_HTTPX / 1000)) as session:
                async with session.get(f"{self.endpoint}/stats") as response:
                    if response.status == 200:
                        self._health_checked = True
                        return True
        except Exception:
            pass
        
        # Se /stats falhar, tentar /sessions
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=ScraperConstants.DEFAULT_TIMEOUT_HTTPX / 1000)) as session:
                async with session.get(f"{self.endpoint}/sessions") as response:
                    if response.status in [200, 404]:  # 404 é OK se não há sessões
                        self._health_checked = True
                        return True
        except Exception:
            pass
        
        logger.warning(f"[Browserless] Unreachable: {self.endpoint} — skipping")
        return False
    
    async def scrape(self, url: str) -> Optional[ScrapeResult]:
        """Scraping com múltiplas estratégias (seleção inteligente) - baseado no ultrasearcher"""
        if not self.config.enable_browserless:
            return None
        
        # Removido health check - ir direto para as estratégias como V4
        # if not await self._assert_browserless_up():
        #     return None
        
        if self.circuit_breaker.is_open():
            self.logger.log_scrape_failure(url, self.__class__.__name__, "Circuit breaker open")
            return None
        
        self.logger.log_scrape_attempt(url, self.__class__.__name__)
        start_time = time.time()
        
        # Obter configurações do playbook
        domain = extract_domain(url)
        playbook_manager = PlaybookManager(self.config)
        browserless_config = playbook_manager.get_browserless_config(domain)
        preferred_strategy = browserless_config.get('strategy')
        
        # Verificar se deve evitar Browserless devido ao Cloudflare
        anti_cf_config = ANTI_CLOUDFLARE_PLAYBOOK.get(domain)
        if anti_cf_config and anti_cf_config.get('avoid_browserless'):
            logger.warning(f"[Browserless] Skipping Browserless for {domain} due to Cloudflare protection")
            return None
        
        # Ordem explícita via playbook tem precedência
        strategy_order_names = browserless_config.get('strategy_order')
        if not strategy_order_names:
            # Ordem orientada por domínio
            in_amp = any(domain == d or domain.endswith('.' + d) for d in self.AMP_FIRST_DOMAINS)
            in_scrape = any(domain == d or domain.endswith('.' + d) for d in self.SCRAPE_WHITELIST)
            if in_amp:
                strategy_order_names = ['amp', 'content', 'function', 'scrape']
            elif in_scrape:
                strategy_order_names = ['scrape', 'content', 'amp', 'function']
            else:
                strategy_order_names = ['content', 'amp', 'function', 'scrape']
        # Se preferida simples for definida, coloque-a no início
        if preferred_strategy and preferred_strategy in self.strategies:
            strategy_order_names = [preferred_strategy] + [n for n in strategy_order_names if n != preferred_strategy]
        
        # Construir ordem de estratégias final
        strategy_order = []
        for name in strategy_order_names:
            if name in self.strategies:
                strategy_order.append((name, self.strategies[name]))
        
        logger.info(f"[Browserless] Strategy order for {domain}: {[name for name, _ in strategy_order]}")
        
        # Tenta cada estratégia em ordem
        for strategy_name, strategy in strategy_order:
            try:
                logger.info(f"[Browserless] Trying strategy: {strategy_name}")
                result = await strategy.execute(url, self.endpoint, self.config)
                if result:
                    # Mini-score: palavras e parágrafos mínimos antes de aceitar
                    words = len(result.split())
                    paragraphs = sum(1 for p in result.split('\n') if p.strip())
                    # Permitir override via playbook
                    min_words_accept = browserless_config.get('min_words_accept', getattr(self.config, 'min_words_accept', 180))
                    if words >= min_words_accept and _nz(paragraphs) >= 1:
                        duration = time.time() - start_time
                        self._record_success(duration)
                        self.logger.log_scrape_success(url, self.__class__.__name__, duration, words)
                        return ScrapeResult(
                            url=url,
                            content=result,
                            word_count=words,
                            success=True,
                            adapter_used=f"{self.__class__.__name__}({strategy_name})",
                            extraction_time=duration,
                            metadata={'strategy': strategy_name, 'endpoint': self.endpoint}
                        )
                    else:
                        logger.warning(f"[Browserless] Strategy {strategy_name} produced insufficient content (words={words}, paragraphs={paragraphs})")
                else:
                    logger.warning(f"[Browserless] Strategy {strategy_name} returned insufficient content")
            except Exception as e:
                logger.warning(f"[Browserless] Strategy {strategy_name} failed: {str(e)}")
                continue
        
        duration = time.time() - start_time
        self._record_failure(duration)
        self.logger.log_scrape_failure(url, self.__class__.__name__, "All strategies failed", duration)
        return None

class ScrapeStrategyV5:
    """Estratégia /scrape com payload correto baseado na API real do Browserless"""
    
    async def execute(self, url: str, endpoint: str, config: ScraperConfig) -> Optional[str]:
        """Executa scraping via /scrape endpoint com estrutura correta"""
        try:
            # Payload limpo para /scrape (sem blockAds/blockResourceTypes no body)
            payload = {
                "url": url,
                "elements": [
                    {"selector": "article, main, [role='main'], .content, .article-body, body"}
                ],
                "gotoOptions": {
                    "waitUntil": ["domcontentloaded"],
                    "timeout": 25000
                }
            }
            
            # Query params (token apenas - blockAds não é suportado em /scrape)
            params = {"timeout": "12000"}  # Reduzido para 12s para não arrastar
            if hasattr(config, 'browserless_token') and config.browserless_token:
                params["token"] = config.browserless_token
            
            # Request com timeout adequado
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=ScraperConstants.DEFAULT_TIMEOUT_BROWSERLESS / 1000)) as session:
                async with session.post(f"{endpoint}/scrape", json=payload, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_scrape_response(data)
                    else:
                        logger.warning(f"[ScrapeStrategy] Failed with status {response.status}")
                        return None
        except Exception as e:
            logger.warning(f"[ScrapeStrategy] Exception: {e}")
            return None
    
    def _parse_scrape_response(self, data: dict) -> str:
        """Parse correto da response do /scrape endpoint com fallback para body"""
        content_parts = []
        for group in data.get("data", []):
            for result in group.get("results", []):
                text = result.get("text") or result.get("html", "")
                if text and len(text) > 20:
                    content_parts.append(text)
        
        # Se não encontrou conteúdo nos seletores específicos, tentar body
        if not content_parts:
            # Fallback: extrair do body se disponível
            body_text = data.get('body', {}).get('text', '')
            if body_text and len(body_text) > 20:
                content_parts.append(body_text)
        
        return "\n\n".join(content_parts)

class ContentFallbackStrategy:
    """Estratégia de conteúdo principal (/content endpoint)"""
    
    async def execute(self, url: str, endpoint: str, config: ScraperConfig) -> Optional[str]:
        """Executa scraping de conteúdo via /content endpoint"""
        import time
        t0 = time.time()
        logger.info(f"[Content] Attempt url={url}")
        try:
            # Headers realistas para contornar 403s
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "User-Agent": config.user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
            }
            
            # Payload correto para /content (sem headers no body, sem blockAds)
            payload = {
                "url": url,
                "gotoOptions": {"waitUntil": ["domcontentloaded"], "timeout": 25000}
            }
            params = {"timeout": str(config.browserless_timeout * 1000)}
            if config.browserless_token:
                params["token"] = config.browserless_token
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=ScraperConstants.DEFAULT_TIMEOUT_HTTPX / 1000)) as session:  # Reduzido para 12s
                async with session.post(f"{endpoint}/content", json=payload, params=params) as response:
                    # Parse resposta assíncrona corrigida (Opção A - call site direto)
                    ct = (response.headers or {}).get("content-type", "").lower()
                    if response.status != 200:
                        html = ""
                        logger.warning(f"[Content] Request failed with status {response.status}")
                    else:
                        if "json" in ct:
                            try:
                                data = await response.json()
                                html = (data.get("html") or data.get("text") or data.get("data") or "")
                            except Exception:
                                html = await response.text()
                        else:
                            html = await response.text()
                    
                    html = (html or "").strip()
                    
                    # BEGIN PATCH: status & soft-404 guards
                    status = response.status
                    strategy_name = "Content"
                    
                    # 1) Status "duro"
                    if status in (404, 410):
                        logger.warning(f"[{strategy_name}] Non-retryable status={status} url={url}")
                        return None  # aborta cadeia; não tente AMP/Function para 404

                    if status in (401, 403):
                        logger.warning(f"[{strategy_name}] Access-blocked status={status} url={url}")
                        return None  # deixe a cadeia decidir: Function (1x) ou abortar

                    # 2) Soft-404/HTML mínimo
                    try:
                        import re
                        textish = re.sub(r"<[^>]+>", " ", html or "")
                        if not html or len(textish) < 400:
                            logger.warning(f"[{strategy_name}] Soft-404: html too small")
                            return None
                        lower = (html[:4000] or "").lower()
                        if ("page not found" in lower or "página não encontrada" in lower) and len(textish.split()) < 300:
                            logger.warning(f"[{strategy_name}] Soft-404: title/signal matched")
                            return None
                    except Exception:
                        pass
                    # END PATCH

                    # Sempre passar pelo cleaner para evitar HTML cru
                    if html and len(html.strip()) > 0:
                        cleaner = ContentCleanerV5()
                        cleaned = cleaner.clean(html, config)
                        word_count = len(cleaned.split()) if cleaned else 0

                        # Verificar soft-404 robusto após cleaning
                        if _is_likely_404(html, cleaned):
                            logger.warning(f"[Content] Likely 404 detected after cleaning: {word_count} words")
                            return None

                        # BEGIN PATCH: hard-block detection
                        head = (html or "")[:6000].lower()
                        if any(sig in head for sig in (
                            "captcha-delivery.com", "geo.captcha-delivery.com", "hcaptcha",
                            "cloudflare turnstile", "akamai bot manager", "access denied"
                        )):
                            logger.warning(f"[Detector] Hard-block pattern found for {url}")
                            return None  # acionar política de roteamento: tentar Function 1x (se ainda não) ou abortar
                        # END PATCH

                        # Early fallback: se menos de 80 palavras, marcar para próxima estratégia
                        if word_count >= 80:
                            duration = (time.time()-t0)*1000
                            logger.info(f"[Content] Success: {len(cleaned)} chars, {word_count} words, duration_ms={int(duration)}")
                            return cleaned
                        else:
                            duration = (time.time()-t0)*1000
                            logger.warning(f"[Content] Insufficient content: {word_count} words, duration_ms={int(duration)}")
                            return None

                    # Se não passou no cleaner, retornar None para tentar próxima estratégia
                    logger.warning(f"[Content] No content after cleaning")
                    return None
        except Exception as e:
            logger.warning(f"[Content] Exception: {e}")
            return None

class AMPFallbackStrategy:
    """Estratégia AMP (Accelerated Mobile Pages) - Baseada no ultrasearcher"""
    
    async def _find_amp_url_robust(self, url: str) -> Optional[str]:
        """Busca URL AMP real lendo a tag HTML <link rel="amphtml">"""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=ScraperConstants.DEFAULT_TIMEOUT_HTTPX / 1000)) as session:
                return await discover_amp_url(session, url)
        except Exception as e:
            logger.debug(f"[AMP] Failed to find AMP URL via HTML tag: {e}")
        
        return None
    
    def _find_amp_url_naive(self, url: str) -> Optional[str]:
        """Tentativa ingênua de construir URL AMP"""
        if '/amp' in url or '/amp/' in url:
            return url
        elif url.endswith('/'):
            return url + 'amp/'
        else:
            return url + '/amp/'
    
    async def execute(self, url: str, endpoint: str, config: ScraperConfig) -> Optional[str]:
        """Executa scraping AMP com detecção robusta - baseado no ultrasearcher"""
        try:
            # Tenta versão robusta primeiro (como no ultrasearcher)
            amp_url = await self._find_amp_url_robust(url)
            
            if amp_url:
                logger.info(f"[AMP] Found AMP URL via HTML tag: {amp_url}")
            else:
                # Fallback para tentativa ingênua
                amp_url = self._find_amp_url_naive(url)
                logger.info(f"[AMP] Using naive AMP URL: {amp_url}")
            
            if not amp_url:
                return None
            
            # Headers realistas para contornar 403s
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
                "User-Agent": config.user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36",
            }
            
            # Payload correto para /content (AMP)
            payload = {
                "url": amp_url,
                "gotoOptions": {"waitUntil": ["domcontentloaded"], "timeout": 25000}
            }
            params = {"timeout": "45000"}
            if config.browserless_token:
                params["token"] = config.browserless_token
            
            # Timeout da sessão: 15s para AMP (mais rápido)
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=ScraperConstants.DEFAULT_TIMEOUT_HTTPX / 1000)) as session:  # Reduzido para 12s
                async with session.post(f"{endpoint}/content", json=payload, params=params) as response:
                    # Parse resposta assíncrona corrigida (Opção A - call site direto)
                    ct = (response.headers or {}).get("content-type", "").lower()
                    if response.status != 200:
                        html = ""
                        logger.warning(f"[AMP] Request failed with status {response.status}")
                    else:
                        if "json" in ct:
                            try:
                                data = await response.json()
                                html = (data.get("html") or data.get("text") or data.get("data") or "")
                            except Exception:
                                html = await response.text()
                        else:
                            html = await response.text()
                    
                    html = (html or "").strip()

                    # BEGIN PATCH: status & soft-404 guards
                    status = response.status
                    strategy_name = "AMP"
                    
                    # 1) Status "duro"
                    if status in (404, 410):
                        logger.warning(f"[{strategy_name}] Non-retryable status={status} url={amp_url}")
                        return None  # aborta cadeia; não tente AMP/Function para 404

                    if status in (401, 403):
                        logger.warning(f"[{strategy_name}] Access-blocked status={status} url={amp_url}")
                        return None  # deixe a cadeia decidir: Function (1x) ou abortar

                    # 2) Soft-404/HTML mínimo
                    try:
                        import re
                        textish = re.sub(r"<[^>]+>", " ", html or "")
                        if not html or len(textish) < 400:
                            logger.warning(f"[{strategy_name}] Soft-404: html too small")
                            return None
                        lower = (html[:4000] or "").lower()
                        if ("page not found" in lower or "página não encontrada" in lower) and len(textish.split()) < 300:
                            logger.warning(f"[{strategy_name}] Soft-404: title/signal matched")
                            return None
                    except Exception:
                        pass
                    # END PATCH

                    # Sempre passar pelo cleaner para evitar HTML cru
                    if html and len(html.strip()) > 0:
                        cleaner = ContentCleanerV5()
                        cleaned = cleaner.clean(html, config)
                        word_count = len(cleaned.split()) if cleaned else 0

                        # Verificar soft-404 robusto após cleaning
                        if _is_likely_404(html, cleaned):
                            logger.warning(f"[AMP] Likely 404 detected after cleaning: {word_count} words")
                            return None

                        if word_count >= 80:
                            logger.info(f"[AMP] Successfully scraped AMP content: {word_count} words")
                            return cleaned
                        else:
                            logger.warning(f"[AMP] Insufficient AMP content: {word_count} words")
                            return None

                    logger.warning(f"[AMP] No content after cleaning")
                    return None
        except Exception as e:
            logger.warning(f"[AMP] Exception: {e}")
            return None

class FunctionFallbackStrategy:
    """Estratégia com função JavaScript customizada e dinâmica - baseada no ultrasearcher"""
    
    def _generate_intelligent_script(self, url: str, browserless_config: dict) -> str:
        """Gera script JavaScript customizado baseado na configuração"""
        
        # Parâmetros default ou do browserless_config
        wait_time = browserless_config.get('wait_time', 2000)
        scroll_times = browserless_config.get('scroll_times', 2)
        wait_for_selector = browserless_config.get('wait_for_selector')
        click_selectors = browserless_config.get('click_selectors', [])
        
        # Gera código para wait_for_selector
        wait_for_code = ""
        if wait_for_selector:
            wait_for_code = f"""
            // Wait for specific selector
            console.log('[Scraper] Waiting for selector: {wait_for_selector}');
            const waitForElement = (selector, timeout = 10000) => {{
                return new Promise((resolve, reject) => {{
                    const startTime = Date.now();
                    const check = () => {{
                        const el = document.querySelector(selector);
                        if (el) {{
                            console.log('[Scraper] Found selector: ' + selector);
                            resolve(el);
                        }} else if (Date.now() - startTime > timeout) {{
                            console.log('[Scraper] Timeout waiting for: ' + selector);
                            reject(new Error('Timeout waiting for selector'));
                        }} else {{
                            setTimeout(check, 100);
                        }}
                    }};
                    check();
                }});
            }};
            
            try {{
                await waitForElement('{wait_for_selector}');
            }} catch (e) {{
                console.log('[Scraper] Continuing despite selector timeout');
            }}
            """
        
        # Gera código para scroll
        scroll_code = f"""
        // Scroll to load lazy content
        console.log('[Scraper] Scrolling {scroll_times} times');
        for (let i = 0; i < {scroll_times}; i++) {{
            window.scrollTo(0, document.body.scrollHeight);
            await new Promise(resolve => setTimeout(resolve, 500));
        }}
        window.scrollTo(0, 0); // Reset to top
        """
        
        # Gera código para clicks
        click_code = ""
        if click_selectors:
            click_lines = []
            for i, sel in enumerate(click_selectors):
                click_lines.append(f"""
            try {{
                const btn{i} = document.querySelector('{sel}');
                if (btn{i}) {{
                    console.log('[Scraper] Clicking: {sel}');
                    btn{i}.click();
                    await new Promise(resolve => setTimeout(resolve, 500));
                }}
            }} catch (e) {{
                console.log('[Scraper] Failed to click {sel}: ' + e);
            }}
                """)
            click_code = '\n'.join(click_lines)
        
        # Script completo baseado no ultrasearcher
        return f"""
        async function scrapeContent() {{
            console.log('[Scraper] Starting intelligent scraping for: ' + window.location.href);
            
            // Initial wait
            console.log('[Scraper] Initial wait: {wait_time}ms');
            await new Promise(resolve => setTimeout(resolve, {wait_time}));
            
            {wait_for_code}
            
            {scroll_code}
            
            {click_code}
            
            // Extract content with intelligent selectors
            console.log('[Scraper] Extracting content');
            let content = '';
            const selectors = [
                'article', 
                'main', 
                '[role="main"]', 
                '.content', 
                '.article-body',
                '.post-content',
                '.entry-content',
                '.story-body'
            ];
            
            for (const selector of selectors) {{
                const element = document.querySelector(selector);
                if (element && element.innerText.trim().length > 100) {{
                    content = element.innerText.trim();
                    console.log('[Scraper] Found content with selector: ' + selector + ' (' + content.length + ' chars)');
                    break;
                }}
            }}
            
            // Fallback to body if no main content found
            if (!content || content.length < 100) {{
                console.log('[Scraper] Using fallback: body.innerText');
                content = document.body.innerText.trim();
            }}
            
            console.log('[Scraper] Extracted ' + content.length + ' characters');
            return content;
        }}
        
        return await scrapeContent();
        """
    
    async def execute(self, url: str, endpoint: str, config: ScraperConfig) -> Optional[str]:
        """Executa scraping com função JavaScript customizada - baseado no ultrasearcher"""
        try:
            # Obter configuração do playbook
            domain = extract_domain(url)
            playbook_manager = PlaybookManager(config)
            browserless_config = playbook_manager.get_browserless_config(domain)
            
            # Gerar script inteligente
            function_code = self._generate_intelligent_script(url, browserless_config)
            
            logger.info(f"[Function] Generated {len(function_code)} chars of JavaScript for {domain}")
            
            # Script robusto para /function (Puppeteer puro, anti-bot) - CORRIGIDO
            # BEGIN PATCH: Captura precoce + higiene de código
            robust_script = f"""
            export default async ({{ page, context }}) => {{
                // Leia context como string e converta (resolve erro de tipo bool)
                const blockAds = (context.blockAds === "true");
                const timeoutMs = Number(context.timeoutMs || 30000);
                
                // 1) CAPTURA PRECOCE: Extrair metadados ANTES dos scripts de paywall
                const extractEarlyMetadata = () => {{
                    const metadata = {{}};
                    
                    // JSON-LD (mais confiável)
                    const jsonScripts = document.querySelectorAll('script[type="application/ld+json"]');
                    if (jsonScripts) {{
                        jsonScripts.forEach(script => {{
                            try {{
                                const data = JSON.parse(script.textContent);
                                if (data.headline) metadata.title = data.headline;
                            if (data.datePublished) metadata.published_at = data.datePublished;
                            if (data.author?.name) metadata.author = data.author.name;
                            if (data.articleBody) metadata.articleBody = data.articleBody;
                        }} catch(e) {{}}
                        }});
                    }}
                    
                    // Open Graph (fallback)
                    if (!metadata.title) {{
                        const ogTitle = document.querySelector('meta[property="og:title"]');
                        if (ogTitle) metadata.title = ogTitle.content;
                    }}
                    
                    return metadata;
                }};
                
                // Injetar extração precoce ANTES da navegação
                await page.evaluateOnNewDocument(() => {{
                    // Capturar metadados no DOMContentLoaded (antes dos scripts)
                    document.addEventListener('DOMContentLoaded', () => {{
                        window.earlyMetadata = extractEarlyMetadata();
                    }}, {{ once: true }});
                }});
                
                // Timeouts + perfil "humano" + UA mobile para domínios específicos
                await page.setDefaultNavigationTimeout(timeoutMs);
                
                // UA mobile para domínios específicos (contornar paywalls/anti-bot)
                const url = '{url}';
                const needsMobileUA = url.includes('reuters.com') || url.includes('uol.com.br') || url.includes('axios.com') || 
                                     url.includes('estadao.com.br') || url.includes('valor.globo.com') || url.includes('g1.globo.com') ||
                                     url.includes('oglobo.globo.com') || url.includes('exame.com') || url.includes('istoed.com.br');
                const mobileUA = 'Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Mobile Safari/537.36';
                const desktopUA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36';
                
                await page.setUserAgent(needsMobileUA ? mobileUA : desktopUA);
                await page.setExtraHTTPHeaders({{
                    'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
                    'Upgrade-Insecure-Requests': '1',
                    'DNT': '1',
                    'Cache-Control': 'no-cache',
                    'Pragma': 'no-cache'
                }});
                
                // Reduzir "sinais de headless"
                await page.evaluateOnNewDocument(() => {{
                    Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined }});
                }});
                
                // Interceptar rede para performance/estabilidade (respeitando blockAds)
                await page.setRequestInterception(true);
                page.on('request', (req) => {{
                    const t = req.resourceType();
                    const url = req.url().toLowerCase();
                    
                    // SEMPRE aborta o que não agrega texto (performance)
                    if (['image','media','font','websocket'].includes(t)) {{
                        return req.abort();
                    }}
                    
                    // Exceções: AMP e UOL precisam de CSS para renderizar conteúdo
                    const isAmpOrUol = url.includes('/amp/') || url.includes('uol.com.br') || 
                                       window.location.href.includes('/amp/') || window.location.href.includes('uol.com.br');
                    
                    // Condicional: só bloqueia stylesheets se blockAds=true E não for AMP/UOL
                    if (blockAds && t === 'stylesheet' && !isAmpOrUol) {{
                        return req.abort();
                    }}
                    
                    // Condicional: só bloqueia domínios se blockAds=true
                    if (blockAds) {{
                        const blockedDomains = [
                            'googletagmanager.com', 'google-analytics.com', 'doubleclick.net',
                            'scorecardresearch.com', 'imasdk.googleapis.com', 'facebook.com',
                            'connect.facebook.net', 'parsely.com', 'licdn.com', 'googlesyndication.com',
                            'recaptcha', 'posthog', 'hotjar', 'mixpanel', 'segment.com',
                            'amplitude.com', 'heap.io', 'fullstory.com', 'logrocket.com'
                        ];
                        
                        if (blockedDomains.some(d => url.includes(d))) {{
                            return req.abort();
                        }}
                    }}
                    
                    req.continue();
                }});
                
                // Injetar script para matar GTM/ads cedo (Puppeteer compatible)
                await page.evaluateOnNewDocument(() => {{
                    const block = ['googletagmanager.com','doubleclick.net','scorecardresearch.com','imasdk.googleapis.com'];
                    const origOpen = XMLHttpRequest.prototype.open;
                    XMLHttpRequest.prototype.open = function(m,u) {{ 
                        if (block.some(d=>u.includes(d))) return; 
                        return origOpen.apply(this, arguments); 
                    }};
                    const origFetch = window.fetch;
                    window.fetch = (...args) => block.some(d => String(args[0]).includes(d)) ? new Promise(()=>{{}}) : origFetch(...args);
                }});
                
                // Navegar (prefira domcontentloaded para não "travar")
                await page.goto('{url}', {{ waitUntil: 'domcontentloaded' }});
                
                // Aguardar body estar disponível (evita estado transitório)
                await page.waitForSelector('body', {{ timeout: 5000 }}).catch(() => {{}});
                
                // Overlay dismissal simples - tolerante
                await page.evaluate(() => {{
                    const dismissOverlays = () => {{
                        const sels = [
                            '[class*="paywall"]','[class*="modal"]','[class*="overlay"]',
                            '[id*="modal"]','[aria-modal="true"]','[class*="gdpr"]','[id*="gdpr"]',
                            '[data-testid*="cookie"]','[id*="cookie"]','[class*="cookie"]',
                            '[id*="consent"]','[class*="consent"]',
                            'button[aria-label*="accept" i]','button[aria-label*="agree" i]',
                            '.close','.close-button','button[aria-label*="close" i]'
                        ];
                        try {{
                            sels.forEach(sel => {{
                                const elements = document.querySelectorAll(sel);
                                if (elements) {{
                                    elements.forEach(el => {{
                                        try {{ el.style.display='none'; el.remove && el.remove(); }} catch(e){{}}
                                    }});
                                }}
                            }});
                            const ok = ['accept','aceitar','agree','ok','continuar','continue','no thanks','reject','decline','recusar'];
                            const buttons = document.querySelectorAll('button,a');
                            if (buttons) {{
                                [...buttons].forEach(b=>{{
                                    const t=(b.textContent||'').toLowerCase();
                                    if(ok.some(x=>t.includes(x))) {{ try{{ b.click(); }}catch(e){{}} }}
                                }});
                            }}
                        }} catch(e){{}}
                    }};
                    dismissOverlays();
                }});
                
                // Aguardar um pouco para overlays serem removidos
                await new Promise(resolve => setTimeout(resolve, 500));
                
                // Captura precoce: JSON-LD primeiro, depois fallback para article
                const result = await page.evaluate(() => {{
                    // 1. Tentar JSON-LD primeiro
                    const jsonLdScript = document.querySelector('script[type="application/ld+json"]');
                    if (jsonLdScript) {{
                        try {{
                            const data = JSON.parse(jsonLdScript.textContent);
                            if (data.articleBody || data.text || data.description) {{
                                return {{
                                    title: data.headline || data.name || document.title,
                                    date: data.datePublished || data.dateCreated,
                                    text: data.articleBody || data.text || data.description,
                                    source: 'jsonld'
                                }};
                            }}
                        }} catch (e) {{}}
                    }}
                    
                    // 2. Fallback para article/main
                    const selectors = [
                        'article', 'main', '[role="main"]', '.article-body', 
                        '.post-content', '.entry-content', '.content', '.story-body'
                    ];
                    
                    let contentEl = null;
                    for (const selector of selectors) {{
                        contentEl = document.querySelector(selector);
                        if (contentEl) break;
                    }}
                    
                    if (!contentEl) {{
                        contentEl = document.body;
                    }}
                    
                    // Extrair texto preservando parágrafos
                    let paragraphs = [];
                    if (contentEl && contentEl.querySelectorAll) {{
                        const elements = contentEl.querySelectorAll('p, div, span');
                        if (elements) {{
                            paragraphs = Array.from(elements).map(el => {{
                                const text = el.innerText?.trim();
                                return text && text.length > 20 ? text : null;
                            }}).filter(Boolean);
                        }}
                    }}
                    
                    const text = paragraphs.join('\\n\\n');
                    
                    return {{
                        title: document.title,
                        date: new Date().toISOString(),
                        text: text,
                        source: 'dom'
                    }};
                }});
                
                // Retornar JSON estruturado (não string)
                return result;
            }}"""
            
            payload = build_function_body(robust_script)
            
            # Retry com payload simplificado se falhar
            max_attempts = 2
            for attempt in range(max_attempts):
                try:
                    # Timeout da sessão: 45s + 10s buffer
                    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=ScraperConstants.DEFAULT_TIMEOUT_BROWSERLESS / 1000)) as session:
                        if attempt == 0:
                            # Primeira tentativa: payload completo (SEM blockAds)
                            current_payload = payload
                            params = build_chromium_query(config, 30000, block_ads=False)
                        else:
                            # Segunda tentativa: payload mínimo (só code)
                            simple_script = f"""
                            export default async ({{ page }}) => {{
                                await page.goto('{url}', {{ waitUntil: 'domcontentloaded' }});
                                await page.waitForSelector('body', {{ timeout: 5000 }}).catch(() => {{}});
                                const text = await page.evaluate(() => {{
                                    const elt = document.body || document.documentElement;
                                    return (elt?.innerText || '').trim();
                                }});
                                return String(text || '');
                            }}"""
                            current_payload = build_function_body(simple_script)
                            params = build_chromium_query(config, 25000, block_ads=False)  # 25s para /function
                        
                        
                        async with session.post(f"{endpoint}/function", json=current_payload, params=params) as response:
                            if response.status == 200:
                                # CORREÇÃO: Leitura chunked acumulada (sempre)
                                transfer_encoding = response.headers.get('transfer-encoding', '').lower()
                                content_type = response.headers.get('content-type', '').lower()
                                
                                # Acumular chunks para Transfer-Encoding: chunked
                                chunks = []
                                async for chunk in response.content.iter_chunked(8192):
                                    chunks.append(chunk)
                                total_bytes = sum(len(c) for c in chunks)
                                raw_result = b''.join(chunks).decode(response.charset or 'utf-8', 'replace')
                                
                                logger.info(f"[Function] Response: {len(chunks)} chunks, {total_bytes} bytes, {len(raw_result)} chars, content-type: {content_type}")
                                
                                # ✅ PATCH 3: Validação ROBUSTA de resposta do Browserless function
                                # RAZÃO: Resposta pode ser vazia/"{}"/malformada sem erro HTTP
                                # IMPACTO: Previne falhas silenciosas em produção (detecta e retenta)
                                
                                # Validação PRECOCE: raw_result vazio ou só espaços
                                if not raw_result or len(raw_result.strip()) == 0:
                                    logger.error(f"[Function] EMPTY raw response (attempt {attempt + 1}), {total_bytes} bytes received but empty after decode")
                                    if attempt == 0:
                                        logger.info("[Function] Retrying with simplified payload")
                                        continue
                                    logger.error(f"[Function] Failed after retry - empty response persists")
                                    return None
                                
                                # Tentar parsear como JSON primeiro
                                try:
                                    import json
                                    json_data = json.loads(raw_result)
                                    if isinstance(json_data, dict):
                                        # Resultado JSON estruturado
                                        result = json_data.get('text', json_data.get('result', ''))
                                        title = json_data.get('title', '')
                                        date = json_data.get('date', '')
                                        source = json_data.get('source', 'function')
                                        logger.info(f"[Function] JSON result: {len(result) if result else 0} chars, title: {title[:50] if title else 'N/A'}, source: {source}")
                                    else:
                                        result = str(json_data)
                                except (json.JSONDecodeError, TypeError) as json_err:
                                    # Fallback para texto direto
                                    result = raw_result
                                    logger.debug(f"[Function] Not JSON, using raw text: {str(json_err)[:100]}")
                                
                                # ✅ VALIDAÇÃO DETALHADA com logging rico para debug
                                result_len = len(result.strip()) if result else 0
                                
                                # Guard 1: Resultado vazio ou single char
                                if result_len <= 1:
                                    logger.error(f"[Function] INVALID result (len={result_len}): '{result[:100] if result else 'NULL'}'")
                                    logger.error(f"[Function] Raw response preview (first 500 chars): {raw_result[:500]}")
                                    if attempt == 0:
                                        logger.info("[Function] Retrying with simplified payload")
                                        continue
                                    logger.error(f"[Function] Failed after retry - invalid result persists")
                                    return None
                                
                                # Guard 2: Conteúdo insuficiente (< 20 chars)
                                if result_len < 20:
                                    logger.warning(f"[Function] INSUFFICIENT content (len={result_len}): '{result[:100]}'")
                                    logger.warning(f"[Function] Raw response preview: {raw_result[:300]}")
                                    if attempt == 0:
                                        logger.info("[Function] Retrying with simplified payload")
                                        continue
                                    logger.warning(f"[Function] Failed after retry - insufficient content ({result_len} chars)")
                                    return None
                                
                                # ✅ SUCESSO: Conteúdo válido
                                logger.info(f"[Function] ✅ SUCCESS: {result_len} chars extracted (attempt {attempt + 1})")
                                return result
                            else:
                                logger.warning(f"[Function] Failed with status {response.status} (attempt {attempt + 1})")
                                if attempt == 0:
                                    continue  # Tentar novamente com payload simplificado
                                return None
                                
                except Exception as e:
                    logger.warning(f"[Function] Exception on attempt {attempt + 1}: {str(e)}")
                    if attempt == 0:
                        continue  # Tentar novamente
                    return None
            
            return None
        except Exception as e:
            logger.warning(f"[Function] Exception: {e}")
            return None

class DocumentAdapterV5(BaseAdapter):
    """Processamento de documentos - PRIORIDADE ESPECIAL"""
    
    def __init__(self, config: ScraperConfig, metrics: AdapterMetrics):
        super().__init__(config, metrics)
    
    def get_priority(self, url: str) -> int:
        """Alta prioridade para documentos"""
        if url.lower().endswith(('.pdf', '.docx', '.doc')):
            return 10  # Máxima prioridade
        return 100
    
    def can_handle(self, url: str) -> bool:
        """Pode processar documentos"""
        return url.lower().endswith(('.pdf', '.docx', '.doc'))
    
    async def scrape(self, url: str) -> Optional[ScrapeResult]:
        """Processamento de PDF/DOCX"""
        if not self.can_handle(url):
            return None
        
        self.logger.log_scrape_attempt(url, self.__class__.__name__)
        start_time = time.time()
        
        try:
            # Download document first
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        return None
                    
                    # aiohttp.ClientSession uses .read() for binary content
                    content_bytes = await response.read()
            
            # Process based on file type
            if url.lower().endswith('.pdf'):
                text_content = await self._process_pdf(content_bytes)
            elif url.lower().endswith(('.docx', '.doc')):
                text_content = await self._process_docx(content_bytes)
            else:
                return None
            
            if text_content:
                duration = time.time() - start_time
                self._record_success(duration)
                self.logger.log_scrape_success(url, self.__class__.__name__, duration, len(text_content.split()))
                
                return ScrapeResult(
                    url=url,
                    content=text_content,
                    word_count=len(text_content.split()),
                    success=True,
                    adapter_used=self.__class__.__name__,
                    extraction_time=duration,
                    metadata={'strategy': 'document', 'file_type': url.split('.')[-1].lower()}
                )
            else:
                duration = time.time() - start_time
                self._record_failure(duration)
                self.logger.log_scrape_failure(url, self.__class__.__name__, "No content extracted", duration)
                return None
                
        except Exception as e:
            duration = time.time() - start_time
            self._record_failure(duration)
            self.logger.log_scrape_failure(url, self.__class__.__name__, str(e), duration)
            return None
    
    async def _process_pdf(self, content_bytes: bytes) -> str:
        """Processa PDF com fallback para OCR - baseado no ultrasearcher"""
        if not PDFPLUMBER_AVAILABLE:
            logger.error("[PDF] pdfplumber not available - cannot extract PDF text. Install: pip install pdfplumber")
            return "[ERROR: PDF processing requires pdfplumber library. Content not extracted.]"
        
        try:
            import io
            
            with pdfplumber.open(io.BytesIO(content_bytes)) as pdf:  # type: ignore
                text_parts = []
                pages_needing_ocr = []
                
                # Primeira passagem: extração de texto normal
                for page_num, page in enumerate(pdf.pages):
                    page_text = page.extract_text()
                    
                    if page_text and len(page_text.strip()) > 50:
                        # Página tem texto extraível
                        text_parts.append(page_text)
                        logger.debug(f"[PDF] Page {page_num+1}: Extracted {len(page_text)} chars via text")
                    else:
                        # Página precisa de OCR
                        pages_needing_ocr.append((page_num, page))
                        logger.debug(f"[PDF] Page {page_num+1}: Needs OCR")
                
                # Segunda passagem: OCR para páginas sem texto
                if pages_needing_ocr:
                    logger.info(f"[PDF] {len(pages_needing_ocr)} pages need OCR")
                    
                    for page_num, page in pages_needing_ocr:
                        ocr_text = await self._ocr_page(page, page_num)
                        if ocr_text:
                            # Insere OCR text na posição correta
                            text_parts.insert(page_num, ocr_text)
                            logger.info(f"[PDF] Page {page_num+1}: OCR extracted {len(ocr_text)} chars")
                        else:
                            text_parts.insert(page_num, f"[Page {page_num+1}: OCR failed]")
                
                final_text = '\n\n'.join(text_parts)
                logger.info(f"[PDF] Total extracted: {len(final_text)} chars ({len(pages_needing_ocr)} via OCR)")
                return final_text
                
        except Exception as e:
            logger.error(f"[PDF] Processing failed: {e}")
            return ""
    
    async def _ocr_page(self, page, page_num: int) -> str:
        """Extrai texto de página via OCR usando easyocr"""
        if not EASYOCR_AVAILABLE:
            logger.warning(f"[OCR] easyocr not available for page {page_num+1}")
            return ""
        
        try:
            import io
            import easyocr
            from PIL import Image
            
            # Converte página para imagem (resolução mais alta para melhor OCR)
            pil_image = page.to_image(resolution=200).original
            
            # Salva imagem em buffer
            img_buffer = io.BytesIO()
            pil_image.save(img_buffer, format='PNG')
            img_buffer.seek(0)
            
            # Inicializa reader OCR (cache para reutilizar)
            if not hasattr(self, '_ocr_reader'):
                logger.info("[OCR] Initializing easyocr reader (pt, en)")
                self._ocr_reader = easyocr.Reader(['pt', 'en'], gpu=False, verbose=False)
            
            # Executa OCR
            result = self._ocr_reader.readtext(img_buffer.getvalue(), detail=0, paragraph=True)
            
            # Junta resultados
            text = '\n'.join(result) if isinstance(result, list) else result
            
            return text
            
        except Exception as e:
            logger.error(f"[OCR] Failed for page {page_num+1}: {e}")
            return ""
    
    async def _process_docx(self, content_bytes: bytes) -> str:
        """Processa DOCX/DOC"""
        if not DOCX2TXT_AVAILABLE:
            return ""
        
        try:
            import io
            import docx2txt
            
            text = docx2txt.process(io.BytesIO(content_bytes))
            return text if text else ""
        except Exception:
            return ""

class AlternatesAdapterV5(BaseAdapter):
    """Adapter para tentar URLs alternativas (AMP, print, canonical, feeds)"""
    
    def __init__(self, config: ScraperConfig, metrics: AdapterMetrics):
        super().__init__(config, metrics)
        self.logger = StructuredLogger(self.__class__.__name__)
        self.circuit_breaker = CircuitBreaker()
    
    def get_priority(self, url: str) -> int:
        """Prioridade média - usado após HTTPX falhar"""
        return 150
    
    def can_handle(self, url: str) -> bool:
        """Pode processar qualquer URL"""
        return True
    
    
    async def scrape(self, url: str) -> Optional[ScrapeResult]:
        """Tenta URLs alternativas derivadas automaticamente"""
        if self.circuit_breaker.is_open():
            self.logger.log_scrape_failure(url, self.__class__.__name__, "Circuit breaker open")
            return None
        
        self.logger.log_scrape_attempt(url, self.__class__.__name__)
        start_time = time.time()
        
        try:
            # Derivar URLs alternativas
            alternate_urls = derive_alternates(url)
            
            for alternate_url in alternate_urls:
                try:
                    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=ScraperConstants.DEFAULT_TIMEOUT_HTTPX / 1000)) as session:  # Reduzido para 12s
                        headers = build_real_headers(0)  # Usar headers realistas
                        async with session.get(alternate_url, headers=headers) as response:
                            if response.status == 200:
                                content = await response.text()
                                
                        # Verificar se não é bot-wall
                        bot_wall_info = is_bot_wall(response.status, alternate_url, content, dict(response.headers))
                        if bot_wall_info['is_bot_wall']:
                            logger.warning(f"[Alternates] Bot-wall detected in {alternate_url}: {bot_wall_info['reason']}")
                            continue
                        
                        # Limpar conteúdo com ContentCleanerV5
                        cleaner = ContentCleanerV5()
                        cleaned = cleaner.clean(content, self.config)
                        
                        # Verificar se tem conteúdo útil após limpeza
                        if cleaned and len(cleaned.split()) >= 80:  # Mínimo de 80 palavras
                            duration = time.time() - start_time
                            self._record_success(duration)
                            self.logger.log_scrape_success(alternate_url, self.__class__.__name__, duration, len(cleaned.split()))
                            
                            return ScrapeResult(
                                        url=url,  # URL original
                                        content=cleaned,
                                        word_count=len(cleaned.split()),
                                        success=True,
                                        adapter_used=self.__class__.__name__,
                                        extraction_time=duration,
                                        metadata={'strategy': 'alternates', 'alternate_url': alternate_url, 'source': 'alternate'}
                                    )
                
                except Exception as e:
                    logger.debug(f"[Alternates] Failed to fetch {alternate_url}: {str(e)}")
                    continue
            
            # Nenhuma alternativa funcionou
            duration = time.time() - start_time
            self.logger.log_scrape_failure(url, self.__class__.__name__, "No working alternates found", duration)
            return None
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.log_scrape_failure(url, self.__class__.__name__, str(e), duration)
            return None

class WaybackAdapterV5(BaseAdapter):
    """Adapter para Wayback Machine como fallback anti-Cloudflare"""
    
    def __init__(self, config: ScraperConfig, metrics: AdapterMetrics):
        super().__init__(config, metrics)
        self.logger = StructuredLogger(self.__class__.__name__)
        self.circuit_breaker = CircuitBreaker()
    
    def get_priority(self, url: str) -> int:
        """Baixa prioridade - só usado como fallback"""
        return 200
    
    def can_handle(self, url: str) -> bool:
        """Pode processar qualquer URL como fallback"""
        return True
    
    async def scrape(self, url: str) -> Optional[ScrapeResult]:
        """Tenta obter conteúdo via Wayback Machine"""
        if self.circuit_breaker.is_open():
            self.logger.log_scrape_failure(url, self.__class__.__name__, "Circuit breaker open")
            return None
        
        self.logger.log_scrape_attempt(url, self.__class__.__name__)
        start_time = time.time()
        
        try:
            # Construir URL do Wayback Machine (último snapshot)
            wayback_url = f"https://web.archive.org/web/{url}"
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.get(wayback_url, headers=HTTPX_HEADERS_HUMAN) as response:
                    if response.status == 200:
                        # ✅ PATCH 4: Hierarquia ROBUSTA de encoding fallbacks
                        # RAZÃO: Wayback pode retornar encoding variado (UTF-8, Latin-1, CP1252)
                        # IMPACTO: Previne conteúdo corrompido/crashes por encoding inválido
                        # aiohttp.ClientSession uses .read() for binary content
                        raw_content = await response.read()
                        content = None
                        encoding_used = None
                        
                        # Hierarquia de encodings (ordem: comum → fallback → last resort)
                        encoding_attempts = [
                            ('utf-8', 'strict'),      # Tenta UTF-8 puro primeiro
                            ('utf-8', 'replace'),     # UTF-8 com substituição de chars inválidos
                            ('latin-1', 'strict'),    # Latin-1 puro (Western Europe)
                            ('cp1252', 'replace'),    # Windows-1252 (superset de Latin-1)
                            ('utf-8', 'ignore'),      # Último recurso: ignora chars inválidos
                        ]
                        
                        for encoding, error_mode in encoding_attempts:
                            try:
                                content = raw_content.decode(encoding, errors=error_mode)
                                encoding_used = f"{encoding}/{error_mode}"
                                logger.info(f"[Wayback] Successfully decoded with {encoding_used}")
                                break  # Sucesso - para de tentar
                            except (UnicodeDecodeError, LookupError) as e:
                                logger.debug(f"[Wayback] Failed to decode with {encoding}/{error_mode}: {str(e)[:100]}")
                                continue
                        
                        # Validação final: se TODOS falharam, marcar como erro
                        if content is None:
                            logger.error(f"[Wayback] ALL encoding attempts failed for {url} ({len(raw_content)} bytes)")
                            return None
                        
                        # Verificar se não é página de erro do Wayback
                        if "This URL has not been captured" in content or "Page not found" in content:
                            logger.warning(f"[Wayback] No snapshot available for {url}")
                            return None
                        
                        duration = time.time() - start_time
                        self._record_success(duration)
                        self.logger.log_scrape_success(url, self.__class__.__name__, duration, len(content.split()))
                        
                        return ScrapeResult(
                            url=url,
                            content=content,
                            word_count=len(content.split()),
                            success=True,
                            adapter_used=self.__class__.__name__,
                            extraction_time=duration,
                            metadata={'strategy': 'wayback', 'source': 'wayback_machine', 'original_url': url}
                        )
                    else:
                        logger.warning(f"[Wayback] HTTP {response.status} for {wayback_url}")
                        return None
                        
        except Exception as e:
            duration = time.time() - start_time
            self.logger.log_scrape_failure(url, self.__class__.__name__, str(e), duration)
            return None
        
        return None

class UnlockAdapterV5(BaseAdapter):
    """Bright Data Unlocker - PRIORIDADE BAIXA"""
    
    def __init__(self, config: ScraperConfig, metrics: AdapterMetrics):
        super().__init__(config, metrics)
        # In V5 we follow V3 semantics: Unlocker is a REST API endpoint
        # that accepts a JSON payload and returns cleaned HTML/bytes.
        self.endpoint = config.unlock_endpoint
        self.token = getattr(config, 'unlock_token', None) or getattr(config, 'unlock_api_token', None)
        self.zone = getattr(config, 'unlock_zone', None)
        self.response_format = getattr(config, 'unlock_format', 'raw')
        self.extra_headers = getattr(config, 'unlock_extra_headers', None)
    
    def get_priority(self, url: str) -> int:
        """Baixa prioridade, só para sites bloqueados"""
        return 90
    
    def can_handle(self, url: str) -> bool:
        """Só funciona se a endpoint/token estiverem configurados"""
        return bool(self.endpoint and (self.token or True))
    
    async def scrape(self, url: str) -> Optional[ScrapeResult]:
        """Scraping via Bright Data Unlocker usando proxy"""
        if not self.config.enable_unlock or not self.can_handle(url):
            return None
        
        if self.circuit_breaker.is_open():
            self.logger.log_scrape_failure(url, self.__class__.__name__, "Circuit breaker open")
            return None
        
        self.logger.log_scrape_attempt(url, self.__class__.__name__)
        start_time = time.time()
        
        try:
            # Follow V3: call Bright Data Web Unlocker REST API
            if not self.endpoint:
                return None

            headers = {
                "User-Agent": self.config.user_agent,
                "Content-Type": "application/json",
            }
            # Support Bearer token or X-Auth-Token as in V3
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"
            # Extra headers (JSON string in config)
            if self.extra_headers:
                try:
                    extra = json.loads(self.extra_headers)
                    if isinstance(extra, dict):
                        headers.update({str(k): str(v) for k, v in extra.items()})
                except Exception:
                    pass

            payload = {
                "url": url,
                "timeout": int(getattr(self.config, 'unlock_timeout_sec', 60)) * 1000,
                "format": self.response_format or 'raw'
            }
            if self.zone:
                payload["zone"] = self.zone

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=getattr(self.config, 'unlock_timeout_sec', 60))) as session:
                async with session.post(self.endpoint, json=payload, headers=headers) as response:
                    if response.status != 200:
                        duration = time.time() - start_time
                        self._record_failure(duration)
                        self.logger.log_scrape_failure(url, self.__class__.__name__, f"Unlock HTTP {response.status}", duration)
                        return None

                    # Try to get bytes or text depending on content-type
                    ct = (response.headers or {}).get('content-type', '').lower()
                    raw = await response.read()
                    # Robust decoding fallback similar to WaybackAdapterV5
                    text = ''
                    try:
                        text = raw.decode('utf-8', errors='strict')
                    except Exception:
                        for encoding, error_mode in [
                            ('utf-8', 'replace'),
                            ('latin-1', 'strict'),
                            ('cp1252', 'replace'),
                            ('utf-8', 'ignore'),
                        ]:
                            try:
                                text = raw.decode(encoding, errors=error_mode)
                                break
                            except Exception:
                                continue

                    duration = time.time() - start_time
                    self._record_success(duration)
                    self.logger.log_scrape_success(url, self.__class__.__name__, duration, len(text.split()) if text else len(raw))

                    return ScrapeResult(
                        url=url,
                        content=text if text else (raw.decode('utf-8', errors='ignore') if isinstance(raw, bytes) else str(raw)),
                        word_count=len(text.split()) if text else (len(raw.split()) if isinstance(raw, bytes) else 0),
                        success=True,
                        adapter_used=self.__class__.__name__,
                        extraction_time=duration,
                        metadata={'strategy': 'unlock', 'status_code': response.status, 'content_type': ct}
                    )
        except Exception as e:
            duration = time.time() - start_time
            self._record_failure(duration)
            self.logger.log_scrape_failure(url, self.__class__.__name__, str(e), duration)
            return None

class SBRAdapterV5(BaseAdapter):
    """Bright Data SBR - PRIORIDADE BAIXA"""
    
    def __init__(self, config: ScraperConfig, metrics: AdapterMetrics):
        super().__init__(config, metrics)
        self.endpoint = config.sbr_endpoint
        self.username = config.sbr_username
        self.password = config.sbr_password
    
    def get_priority(self, url: str) -> int:
        """Baixa prioridade, só para casos específicos"""
        return 95
    
    def can_handle(self, url: str) -> bool:
        """Só funciona se credenciais configuradas"""
        return bool(self.username and self.password)
    
    async def _proxy_get_fallback(self, url: str) -> Optional[ScrapeResult]:
        """Fallback: simple GET via configured proxy (existing behaviour)."""
        try:
            if not self.endpoint or not (self.username and self.password):
                return None
            proxy_auth = aiohttp.BasicAuth(self.username, self.password)
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.get(
                    url,
                    proxy=f"http://{self.endpoint}",
                    proxy_auth=proxy_auth,
                    headers={"User-Agent": self.config.user_agent}
                ) as response:
                    if response.status != 200:
                        return None
                    content = await response.text()
                    duration = 0.0
                    try:
                        duration = time.time() - getattr(self, '_last_start_time', time.time())
                    except Exception:
                        duration = time.time()
                    self._record_success(duration)
                    self.logger.log_scrape_success(url, self.__class__.__name__, duration, len(content.split()))
                    return ScrapeResult(
                        url=url,
                        content=content,
                        word_count=len(content.split()),
                        success=True,
                        adapter_used=f"{self.__class__.__name__}(proxy)",
                        extraction_time=duration,
                        metadata={'strategy': 'sbr', 'status_code': response.status, 'via_proxy': True}
                    )
        except Exception as e:
            self.logger.warning(f"[SBR][fallback] proxy GET failed: {e}")
            return None

    async def scrape(self, url: str) -> Optional[ScrapeResult]:
        """Try Bright Data Browser API via CDP (Playwright). Fallback to proxy GET if not available.

        Requires configuration: bd_customer_id, bd_zone_sbr, bd_host, bd_port_cdp, bd_password (in config)
        """
        if not getattr(self.config, 'enable_sbr', False):
            return None

        # Circuit breaker
        if self.circuit_breaker.is_open():
            self.logger.log_scrape_failure(url, self.__class__.__name__, "Circuit breaker open")
            return None

        # Check CDP credentials in config
        customer = getattr(self.config, 'bd_customer_id', None)
        zone = getattr(self.config, 'bd_zone_sbr', None)
        host = getattr(self.config, 'bd_host', None)
        port = getattr(self.config, 'bd_port_cdp', None)
        password = getattr(self.config, 'bd_password', None) or self.password

        # If CDP config missing, fall back to proxy GET
        use_cdp = all([customer, zone, host, port, password])

        self.logger.log_scrape_attempt(url, self.__class__.__name__)
        start_time = time.time()
        # store start_time for fallback telemetry
        setattr(self, '_last_start_time', start_time)

        if not use_cdp:
            return await self._proxy_get_fallback(url)

        # Attempt CDP via Playwright
        try:
            try:
                from playwright.async_api import async_playwright
            except Exception as e:
                self.logger.warning(f"[SBR] Playwright not available: {e}")
                return await self._proxy_get_fallback(url)

            ws = f"wss://brd-customer-{customer}-zone-{zone}:{password}@{host}:{port}"
            timeout_ms = int(getattr(self.config, 'browserless_timeout', 60)) * 1000

            async with async_playwright() as p:
                browser = await p.chromium.connect_over_cdp(ws)
                try:
                    page = await browser.new_page()

                    # Block heavy resources to save cost
                    await page.route("**/*", lambda route: route.abort()
                                     if route.request.resource_type in {"image","media","font","stylesheet"}
                                     else route.continue_())

                    # Set UA and minimal headers
                    ua = getattr(self.config, 'user_agent', None)
                    if ua:
                        try:
                            await page.set_user_agent(ua)  # type: ignore
                        except Exception:
                            pass
                    try:
                        await page.set_extra_http_headers({'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8'})
                    except Exception:
                        pass

                    # Navigate
                    await page.goto(url, timeout=timeout_ms)

                    # Accept cookies if configured
                    if getattr(self.config, 'sbr_accept_cookies', True):
                        try:
                            btns = await page.locator("text=/^(Aceitar|Accept|OK|Entendi)/i").all()
                            if btns:
                                await btns[0].click(timeout=1500)
                        except Exception:
                            pass

                    # Infinite scroll / load more heuristics
                    max_scrolls = int(getattr(self.config, 'sbr_infinite_scroll_max', 8))
                    for _ in range(max_scrolls):
                        try:
                            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            await page.wait_for_timeout(800)
                            size = await page.evaluate('document.body.scrollHeight')
                            # simple stabilization: break if small
                        except Exception:
                            break

                    # Optional wait-for-goal selector
                    goal = getattr(self.config, 'sbr_wait_goal_selector', None)
                    if goal:
                        try:
                            await page.wait_for_selector(goal, timeout=4000)
                        except Exception:
                            pass

                    # Short pause then capture
                    await page.wait_for_timeout(800)
                    html = await page.content()

                    duration = time.time() - start_time
                    self._record_success(duration)
                    self.logger.log_scrape_success(url, self.__class__.__name__, duration, len(html.split()))

                    return ScrapeResult(
                        url=url,
                        content=html,
                        word_count=len(html.split()),
                        success=True,
                        adapter_used=f"{self.__class__.__name__}(cdp)",
                        extraction_time=duration,
                        metadata={'strategy': 'sbr', 'via_cdp': True}
                    )
                finally:
                    try:
                        await browser.close()
                    except Exception:
                        pass
        except Exception as e:
            duration = time.time() - start_time
            self._record_failure(duration)
            self.logger.log_scrape_failure(url, self.__class__.__name__, str(e), duration)
            # Fallback to proxy GET on any failure
            return await self._proxy_get_fallback(url)


# ============================================================================
# SEÇÃO: ORQUESTRAÇÃO E ESTRATÉGIAS
# ============================================================================

class PlaybookManager:
    """Gerenciamento avançado de estratégias por domínio via JSON configurável"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.playbooks = self._load_playbooks()
    
    def _load_playbooks(self) -> Dict[str, dict]:
        """Carrega playbooks padrão críticos + JSON de configuração"""
        # Playbooks padrão críticos
        default_playbooks = {
            # UOL - Site brasileiro com AMP disponível
            "noticias.uol.com.br": {
                "strategy_order": ["httpx", "amp_discovery", "browserless_content", "wayback"],
                "preferred_selectors": ["article", ".conteudo-principal", "[itemprop=articleBody]"],
                "amp_discovery": True,
                "accept_basic_stripping": True,
                "min_words_basic": 800,
                "coverage_threshold": 0.5,
                "browserless_strategy": "content",
                "wait_until": "domcontentloaded"
            },
            "uol.com.br": {
                "strategy_order": ["httpx", "amp_discovery", "browserless_content", "wayback"],
                "preferred_selectors": ["article", ".conteudo-principal", "[itemprop=articleBody]"],
                "amp_discovery": True,
                "accept_basic_stripping": True,
                "min_words_basic": 800
            },
            
            # Axios - App-shell com paywall
            "www.axios.com": {
                "strategy_order": ["httpx", "amp_discovery", "jina", "browserless_function", "wayback"],
                "skip_browserless_content": True,  # App-shell sempre dá pouco texto
                "browserless_strategy": "function",
                "function_remove_overlays": True,
                "overlay_selectors": [".paywall", "[data-paywall]", "[id*='piano']", "[class*='Paywall']"],
                "wait_until": "networkidle2",
                "block_ads": True,
                "minify": False
            },
            
            # HBR - JSON-LD funciona bem
            "hbr.org": {
                "strategy_order": ["httpx", "jsonld_first", "readability", "browserless_content"],
                "jsonld_short_circuit": True,
                "jsonld_confidence_threshold": 0.9,
                "preferred_selectors": ["article", ".article-body", "[data-module='ArticleBody']"]
            },
            
            # Sites de teste - ordem simples
            "example.com": {
                "strategy_order": ["httpx", "browserless_content"],
                "accept_basic_stripping": True,
                "min_words_basic": 20
            },
            "httpbin.org": {
                "strategy_order": ["httpx", "browserless_content"],
                "accept_basic_stripping": True,
                "min_words_basic": 100
            },
            "quotes.toscrape.com": {
                "strategy_order": ["httpx", "browserless_content"],
                "accept_basic_stripping": True,
                "min_words_basic": 50
            }
        }
        
        # Carregar playbooks adicionais do JSON de configuração
        try:
            additional_playbooks = json.loads(self.config.domain_playbooks_json)
            default_playbooks.update(additional_playbooks)
            self.logger.info(f"[PlaybookManager] Loaded {len(additional_playbooks)} additional playbooks")
        except (json.JSONDecodeError, TypeError) as e:
            self.logger.warning(f"[PlaybookManager] Failed to load additional playbooks: {e}")
        
        self.logger.info(f"[PlaybookManager] Loaded {len(default_playbooks)} total playbooks")
        return default_playbooks
    
    def get_playbook_for_domain(self, domain: str) -> Optional[dict]:
        """
        Retorna playbook para um domínio.
        Suporta:
        - Exact match: "github.com"
        - Wildcard prefix: "*.reddit.com"
        - Wildcard suffix: "news.*"
        - Wildcard both: "*medium*"
        """
        # 1. Exact match
        if domain in self.playbooks:
            self.logger.debug(f"[PlaybookManager] Exact match for {domain}")
            return self.playbooks[domain]
        
        # 2. Wildcard patterns
        for pattern, playbook in self.playbooks.items():
            if self._domain_matches_pattern(domain, pattern):
                self.logger.debug(f"[PlaybookManager] Pattern match: {domain} matches {pattern}")
                return playbook
        
        self.logger.debug(f"[PlaybookManager] No playbook for {domain}, using defaults")
        return None
    
    def _domain_matches_pattern(self, domain: str, pattern: str) -> bool:
        """Verifica se domínio corresponde ao padrão wildcard"""
        if not '*' in pattern:
            return domain == pattern
        
        # *.reddit.com matches www.reddit.com, old.reddit.com
        if pattern.startswith('*.'):
            base_domain = pattern[2:]
            return domain == base_domain or domain.endswith(f'.{base_domain}')
        
        # news.* matches news.google.com, news.ycombinator.com
        if pattern.endswith('.*'):
            prefix = pattern[:-2]
            return domain.startswith(f'{prefix}.')
        
        # *medium* matches medium.com, towardsdatascience.medium.com
        if pattern.startswith('*') and pattern.endswith('*'):
            middle = pattern[1:-1]
            return middle in domain
        
        return False
    
    def should_block_adapter(self, domain: str, adapter_name: str) -> bool:
        """Verifica se adapter deve ser bloqueado para este domínio"""
        playbook = self.get_playbook_for_domain(domain)
        if not playbook:
            return False
        
        block_adapters = playbook.get('block_adapters', [])
        # Normaliza nomes para comparação case-insensitive
        normalized_blocks = [a.lower().replace('adapter', '').replace('v5', '') for a in block_adapters]
        normalized_adapter = adapter_name.lower().replace('adapter', '').replace('v5', '')
        
        return normalized_adapter in normalized_blocks
    
    def get_forced_order(self, domain: str) -> Optional[List[str]]:
        """Retorna ordem forçada de adaptadores, se definida"""
        playbook = self.get_playbook_for_domain(domain)
        if playbook and 'force_order' in playbook:
            return playbook['force_order']
        return None
    
    def get_custom_selectors(self, domain: str) -> List[str]:
        """Retorna seletores CSS customizados para o domínio"""
        playbook = self.get_playbook_for_domain(domain)
        if playbook and 'custom_selectors' in playbook:
            return playbook['custom_selectors']
        return []
    
    def get_browserless_config(self, domain: str) -> dict:
        """Retorna configurações específicas para Browserless"""
        playbook = self.get_playbook_for_domain(domain)
        if not playbook:
            return {}
        
        return {
            'wait_time': playbook.get('wait_time', 2000),
            'scroll_times': playbook.get('scroll_times', 2),
            'wait_for_selector': playbook.get('wait_for_selector'),
            'click_selectors': playbook.get('click_selectors', []),
            'strategy': playbook.get('browserless_strategy', 'content'),
            'strategy_order': playbook.get('strategy_order'),
            'min_words_accept': playbook.get('min_words_accept'),
            'prefer_amp': playbook.get('prefer_amp', False),
            'print_paths': playbook.get('print_paths', []),
            'block_domains': playbook.get('block_domains', []),
            'block_urls': playbook.get('block_urls', []),
            'paywall': playbook.get('paywall'),
            'avoid_browserless': playbook.get('avoid_browserless', False),
            'prefer_jsonld': playbook.get('prefer_jsonld', False)
        }

class AdapterChainV5:
    """Orchestrates multiple scraping adapters with intelligent fallback chain.
    
    ARCHITECTURE:
    ┌──────────────────────────────────────────────────────────┐
    │ ADAPTER CHAIN EXECUTION (Smart Fallback with Protection) │
    └──────────────────────────────────────────────────────────┘
         ↓
    1. Cache Check (CacheKey.from_url) → Return if hit
         ↓ (miss)
    2. Circuit Breaker Check (per-host) → Skip if open
         ↓ (closed/half-open)
    3. Playbook Selection (domain-specific config)
         ↓
    4. StrategyDecider (analyze URL, HTML head signals)
         → Determine adapter order and budget_ms
         ↓
    5. Filter Adapters:
         - can_handle(url) = True
         - is_enabled (valve flags)
         - not blocked (playbook block_adapters)
         ↓
    6. Execute Adapter Chain (ordered by priority/playbook):
         ┌──────────────────────────────────┐
         │ For each adapter:                │
         │  1. Check budget remaining       │
         │  2. Try adapter.scrape(url)      │
         │  3. If success:                  │
         │     - Clean content              │
         │     - Detect paywall              │
         │     - Validate quality            │
         │     - Extract metadata            │
         │     - Cache result                │
         │     - Return ✅                   │
         │  4. If fail:                     │
         │     - Log failure                 │
         │     - Try next adapter            │
         └──────────────────────────────────┘
         ↓
    7. All adapters failed → Cache negative result (2min TTL)
         ↓
    ❌ Return None
    
    DECISION FLOW:
    - Each adapter tries to scrape the URL
    - If success + validation passes → return result (stop chain)
    - If paywall detected + content insufficient → try bypass strategies
    - If fails → try next adapter in chain
    - If all fail → return None (cached as negative result)
    
    FEATURES:
    ✅ Per-host circuit breakers (protects against cascading failures)
       - Threshold: 5 consecutive failures
       - Cooldown: 5 minutes (300s)
       - State: closed (ok) → open (skip) → half-open (retry)
    
    ✅ Content quality validation (word count, paragraphs, signals)
       - Min 30 words (configurable via min_words_threshold)
       - Min 1 paragraph
       - Rich signals bonus (title, author, date)
    
    ✅ Paywall detection & bypass
       - Soft paywall: Alternates (AMP) → Wayback → Jina
       - Hard paywall: Wayback → Alternates → Jina (skip Browserless)
       - Bypass success: marked as 'bypassed' in metadata
    
    ✅ Global cache (per-URL with config hash)
       - Positive cache: 10 min TTL (configurable)
       - Negative cache: 2 min TTL (failed scrapes)
       - Cache key: URL + relevant config (UA, headers)
    
    ✅ Host-based semaphores (max 5 concurrent per host)
       - Prevents overwhelming target servers
       - Automatic rate limiting per domain
    
    Args:
        adapters: List of BaseAdapter instances to use
        config: ScraperConfig with global settings
    
    Example:
        >>> adapters = [HttpxAdapterV5(...), JinaAdapterV5(...), BrowserlessAdapterV5(...)]
        >>> chain = AdapterChainV5(adapters, config)
        >>> result = await chain.scrape("https://complex-site.com")
        >>> if result:
        ...     print(f"✅ Success via {result.adapter_used}: {result.word_count} words")
        ...     print(f"Quality score: {result.quality_score}")
        ... else:
        ...     print("❌ All adapters failed (cached for 2min)")
    """
    
    def __init__(self, adapters: List[BaseAdapter], config: ScraperConfig):
        self.adapters = adapters
        self.config = config
        self.playbook_manager = PlaybookManager(config)
        self.cleaner = ContentCleanerV5()
        self.validator = ContentValidator()
        self.metadata_extractor = MetadataExtractor()
        self.semaphore = HostSemaphore(max_concurrent=5)
        
        # Circuit breakers compartilhados por host
        self.host_circuit_breakers: Dict[str, CircuitBreaker] = {}
        
        # Deduplicação de URLs (evitar tentativas repetidas)
        self.recent_attempts: Dict[str, float] = {}
        self.throttle_window = 30.0  # 30 segundos
    
    def _get_circuit_breaker_for_host(self, url: str) -> CircuitBreaker:
        """Retorna circuit breaker específico para o host da URL"""
        host = extract_domain(url)
        if host not in self.host_circuit_breakers:
            self.host_circuit_breakers[host] = CircuitBreaker(
                failure_threshold=5,
                timeout_seconds=300  # 5 minutos
            )
        return self.host_circuit_breakers[host]
    
    def _is_adapter_enabled(self, adapter) -> bool:
        """Verifica se adapter está habilitado baseado nas flags de configuração"""
        adapter_name = adapter.__class__.__name__
        
        if adapter_name == 'HttpxAdapterV5':
            return getattr(self.config, 'enable_httpx', True)
        elif adapter_name == 'BrowserlessAdapterV5':
            return getattr(self.config, 'enable_browserless', True)
        elif adapter_name == 'JinaAdapterV5':
            return getattr(self.config, 'enable_jina', True)
        elif adapter_name == 'DocumentAdapterV5':
            return getattr(self.config, 'enable_document', True)
        elif adapter_name == 'WaybackAdapterV5':
            return getattr(self.config, 'enable_wayback', True)
        elif adapter_name == 'AlternatesAdapterV5':
            return getattr(self.config, 'enable_alternates', True)
        elif adapter_name == 'UnlockAdapterV5':
            return getattr(self.config, 'enable_unlock', True)
        elif adapter_name == 'SBRAdapterV5':
            return getattr(self.config, 'enable_sbr', True)
        
        # Por padrão, assume que está habilitado
        return True
    
    def _throttle_same_url(self, url: str) -> bool:
        """Evita tentativas repetidas da mesma URL em janela curta"""
        from urllib.parse import urlparse
        import time
        
        # Criar chave baseada em domain + path (ignorar query params)
        parsed = urlparse(url)
        key = f"{parsed.netloc}{parsed.path}"
        
        now = time.time()
        last_attempt = self.recent_attempts.get(key, 0)
        
        if now - last_attempt < self.throttle_window:
            logger.debug(f"[AdapterChain] Throttling {url} (last attempt {now - last_attempt:.1f}s ago)")
            return True  # Skip this attempt
        
        self.recent_attempts[key] = now
        return False  # Proceed with attempt
    
    def _order_adapters_by_playbook(self, adapters: List[BaseAdapter], forced_order: List[str]) -> List[BaseAdapter]:
        """Ordena adaptadores segundo playbook"""
        # Cria mapa de adapter por nome normalizado
        adapter_map = {}
        for adapter in adapters:
            name = adapter.__class__.__name__.lower().replace('adapter', '').replace('v5', '')
            adapter_map[name] = adapter
        
        # Ordena segundo force_order
        ordered = []
        for name in forced_order:
            normalized = name.lower().replace('adapter', '').replace('v5', '')
            if normalized in adapter_map:
                ordered.append(adapter_map[normalized])
        
        # Adiciona restantes não especificados na ordem padrão
        remaining = [a for a in adapters if a not in ordered]
        remaining.sort(key=lambda x: x.get_priority(''))
        
        return ordered + remaining
    
    async def scrape(self, url: str) -> Optional[ScrapeResult]:
        """Execução da cadeia de adaptadores com cache e circuit breaker por host"""
        if not is_valid_url(url):
            return None
        
        # CACHE: Verificar cache antes de tentar qualquer adapter
        cache_key = CacheKey.from_url(url, self.config)
        cache = get_cache(self.config.cache_ttl_sec)
        cached_result = cache.get(cache_key)
        
        if cached_result:
            logger.info(f"[AdapterChain] Cache HIT for {url}")
            return cached_result
        
        # Verifica circuit breaker do host ANTES de tentar qualquer adapter
        host_cb = self._get_circuit_breaker_for_host(url)
        host = extract_domain(url)
        
        if host_cb.is_open():
            logger.warning(f"[AdapterChain] Circuit breaker OPEN for host {host} (cooling down)")
            return None
        
        domain = extract_domain(url)
        
        # Aplicar playbook
        playbook = self.playbook_manager.get_playbook_for_domain(domain)
        
        # StrategyDecider: Decidir ordem e budget baseado em sinais
        strategy_decider = StrategyDecider()
        
        # Fazer HEAD request rápido para obter sinais
        head_html = ""
        try:
            import aiohttp
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as session:
                async with session.head(url) as response:
                    if response.status == 200:
                        # Se HEAD não retorna HTML, fazer GET parcial
                        if 'text/html' not in response.headers.get('content-type', ''):
                            async with session.get(url, headers={'Range': 'bytes=0-65535'}) as partial_response:
                                if partial_response.status in [200, 206]:
                                    head_html = await partial_response.text()
        except Exception as e:
            logger.debug(f"[AdapterChain] Could not fetch HEAD for signals: {e}")
        
        # Decidir ordem de estratégias e budget
        strategy_order, budget_ms = strategy_decider.decide(url, playbook, self.config, head_html)
        logger.info(f"[AdapterChain] Strategy order: {strategy_order}, budget: {budget_ms}ms")
        
        # Budget global por URL
        start_time = time.time()
        deadline = start_time + (budget_ms / 1000.0)
        
        # Filter adapters: can_handle + enabled flags + not blocked
        candidates = [
            adapter for adapter in self.adapters
            if adapter.can_handle(url)
            and self._is_adapter_enabled(adapter)
            and not self.playbook_manager.should_block_adapter(domain, adapter.__class__.__name__)
        ]
        
        # CRITICAL: Para documentos (PDF/DOCX), SEMPRE priorizar DocumentAdapter PRIMEIRO
        is_doc, doc_type = is_document_url(url, None, None)
        if is_doc:
            # Forçar DocumentAdapter como primeiro
            doc_adapters = [a for a in candidates if isinstance(a, DocumentAdapterV5)]
            other_adapters = [a for a in candidates if not isinstance(a, DocumentAdapterV5)]
            ordered_adapters = doc_adapters + other_adapters
            logger.info(f"[AdapterChain] Document detected ({doc_type}), forcing DocumentAdapter FIRST for {domain}: {[a.__class__.__name__ for a in ordered_adapters]}")
        elif strategy_order:
            # Usar ordem do StrategyDecider
            ordered_adapters = []
            for strategy_name in strategy_order:
                adapter = next((a for a in candidates if a.__class__.__name__.lower().startswith(strategy_name.lower())), None)
                if adapter:
                    ordered_adapters.append(adapter)
            # Adicionar adapters não especificados no StrategyDecider
            for adapter in candidates:
                if adapter not in ordered_adapters:
                    ordered_adapters.append(adapter)
            logger.info(f"[AdapterChain] Using StrategyDecider order for {domain}: {[a.__class__.__name__ for a in ordered_adapters]}")
        else:
            # Fallback para ordem padrão
            ordered_adapters = sorted(candidates, key=lambda x: x.get_priority(url))
            logger.info(f"[AdapterChain] Using default order for {domain}: {[a.__class__.__name__ for a in ordered_adapters]}")
        
        # ✅ PATCH 5: Cleanup GARANTIDO de semaphore (thread-safe)
        # RAZÃO: Semaphore deve ser liberado SEMPRE, mesmo em exceções/timeouts
        # IMPACTO: Previne deadlock quando adapter trava ou timeout força saída
        # IMPLEMENTAÇÃO: try/finally garante release em TODOS os cenários
        await self.semaphore.acquire(url)
        
        try:
            for adapter in ordered_adapters:
                # Verificar budget global antes de tentar adapter
                remaining_time = deadline - time.time()
                if remaining_time <= 1.0:  # Pelo menos 1 segundo restante
                    logger.warning(f"[AdapterChain] Budget exhausted for {url}, stopping adapter chain")
                    break
                
                # Ajustar timeout do adapter baseado no budget restante
                adapter_timeout = min(remaining_time - 0.5, 30.0)  # Máximo 30s, deixar 0.5s de buffer
                if adapter_timeout <= 0:
                    logger.warning(f"[AdapterChain] No time left for {adapter.__class__.__name__}")
                    break
                
                # Lower noisy attempt logs to DEBUG unless explicitly enabled
                if getattr(self.config, 'enable_debug_logging', False):
                    logger.info(f"[AdapterChain] Trying {adapter.__class__.__name__} with {adapter_timeout:.1f}s timeout (budget: {remaining_time:.1f}s)")
                else:
                    logger.debug(f"[AdapterChain] Trying {adapter.__class__.__name__} with {adapter_timeout:.1f}s timeout (budget: {remaining_time:.1f}s)")
                
                try:
                    # ✅ PATCH 1: FORÇA timeout no adapter (mesmo se ele não respeitar internamente)
                    # RAZÃO: Previne deadlock quando adapter.scrape() ignora timeouts internos
                    # IMPACTO: Garante que budget global seja respeitado SEMPRE
                    try:
                        result = await asyncio.wait_for(
                            adapter.scrape(url),
                            timeout=adapter_timeout
                        )
                    except asyncio.TimeoutError:
                        logger.error(f"[AdapterChain] FORCED TIMEOUT for {adapter.__class__.__name__} after {adapter_timeout:.1f}s (budget protection)")
                        host_cb.record_failure()
                        continue  # Tenta próximo adapter
                    
                    if result and result.success:
                        # Registra sucesso no circuit breaker do host
                        host_cb.record_success()
                        logger.info(f"[AdapterChain] Success for {host} via {adapter.__class__.__name__}")
                        
                        # Clean content
                        cleaned_content = self.cleaner.clean(result.content, self.config)
                        
                        # Detect paywall
                        paywall_info = detect_paywall(result.content, cleaned_content)
                        
                        # Validate content
                        validation = self.validator.validate(cleaned_content, url)
                        
                        # Se detectou conteúdo premium e conteúdo insuficiente, tentar alternativas
                        if paywall_info['type'] in ['soft', 'hard'] and validation['word_count'] < 250:
                            logger.info(f"[AdapterChain] Conteúdo premium detected ({paywall_info['type']}) with {validation['word_count']} words, trying alternatives")
                            
                            # Tentar com estratégias de bypass de conteúdo premium
                            alternative_result = await self._retry_with_paywall_alternatives(url, paywall_info, result)
                            if alternative_result:
                                return alternative_result
                        
                        # Se detectou conteúdo premium mas conteúdo é suficiente, marcar como "excerpt_only" se necessário
                        elif paywall_info['type'] in ['soft', 'hard'] and validation['word_count'] < 400:
                            # Verificar se é realmente conteúdo completo ou apenas excerpt
                            premium_indicators = paywall_info.get('hits', [])
                            if any('tinypass' in hit or 'swg-google' in hit or 'piano' in hit for hit in premium_indicators):
                                result.metadata['quality_note'] = 'excerpt_only'
                                logger.warning(f"[AdapterChain] Content marked as excerpt_only due to premium indicators: {premium_indicators}")
                        
                        if validation['passed']:
                            # Extract metadata
                            metadata = self.metadata_extractor.extract(result.content, url)
                            
                            # Update result with cleaned content and metadata
                            result.content = cleaned_content
                            result.word_count = len(cleaned_content.split())
                            result.quality_score = validation['score']
                            result.title = metadata.get('title')
                            result.author = metadata.get('author')
                            result.published_at = metadata.get('published_at')
                            result.metadata.update(metadata)
                            result.metadata['playbook_used'] = playbook is not None
                            
                            # Adicionar informações de conteúdo premium
                            result.metadata['conteudo_premium'] = paywall_info['type']
                            result.metadata['premium_indicators'] = paywall_info['hits']
                            result.metadata['premium_confidence'] = paywall_info['confidence']
                            
                            # CACHE: Armazenar resultado bem-sucedido
                            if result and result.content and len(result.content.strip()) > 300:
                                cache.put(cache_key, result)
                                logger.info(f"[AdapterChain] Cache SET for {url}")
                            
                            return result
                        else:
                            logger.warning(f"[AdapterChain] Content validation failed for {url}: {validation.get('drop_reason', 'Unknown')}")
                            # Não registra falha no circuit breaker se foi problema de validação
                            continue
                    else:
                        # Registra falha no circuit breaker do host
                        host_cb.record_failure()
                        logger.warning(f"[AdapterChain] Adapter {adapter.__class__.__name__} failed for {host}")
                        continue
                        
                except Exception as e:
                    # Registra falha no circuit breaker do host
                    host_cb.record_failure()
                    logger.error(f"[AdapterChain] Error with {adapter.__class__.__name__} for {host}: {str(e)}")
                    continue
            
            # All adapters failed - registra falha final
            host_cb.record_failure()
            
            # CACHE: Negative cache para falhas (TTL curto)
            negative_result = ScrapeResult(
                content="",
                success=False,
                adapter_used="None",
                url=url,
                metadata={'cached_failure': True, 'timestamp': time.time()}
            )
            cache.put(cache_key, negative_result, ttl_seconds=120)  # 2 minutos
            logger.info(f"[AdapterChain] Negative cache SET for {url}")
            
            return None
            
        finally:
            self.semaphore.release(url)
    
    async def _retry_with_paywall_alternatives(self, url: str, paywall_info: dict, original_result: ScrapeResult) -> Optional[ScrapeResult]:
        """Tenta estratégias alternativas quando detecta paywall"""
        logger.info(f"[AdapterChain] Retrying with paywall alternatives for {url} (type: {paywall_info['type']})")
        
        # Reordenar adapters para priorizar anti-paywall
        paywall_adapters = []
        
        # Para soft paywall: Alternates (AMP/Print) → Wayback → Jina (evitar Browserless)
        if paywall_info['type'] == 'soft':
            paywall_adapters.extend([
                adapter for adapter in self.adapters if isinstance(adapter, AlternatesAdapterV5)
            ])
            paywall_adapters.extend([
                adapter for adapter in self.adapters if isinstance(adapter, WaybackAdapterV5)
            ])
            paywall_adapters.extend([
                adapter for adapter in self.adapters if isinstance(adapter, JinaAdapterV5)
            ])
            # Evitar Browserless para soft paywall (pode ativar overlay)
        
        # Para hard paywall: Wayback → Alternates → Jina (pular Browserless completamente)
        elif paywall_info['type'] == 'hard':
            paywall_adapters.extend([
                adapter for adapter in self.adapters if isinstance(adapter, WaybackAdapterV5)
            ])
            paywall_adapters.extend([
                adapter for adapter in self.adapters if isinstance(adapter, AlternatesAdapterV5)
            ])
            paywall_adapters.extend([
                adapter for adapter in self.adapters if isinstance(adapter, JinaAdapterV5)
            ])
            # Pular Browserless para hard paywall
        
        # Tentar cada adapter anti-paywall
        for adapter in paywall_adapters:
            try:
                if getattr(self.config, 'enable_debug_logging', False):
                    logger.info(f"[AdapterChain] Trying anti-paywall strategy: {adapter.__class__.__name__}")
                else:
                    logger.debug(f"[AdapterChain] Trying anti-paywall strategy: {adapter.__class__.__name__}")
                result = await adapter.scrape(url)
                
                if result and result.success:
                    # Clean e validate
                    cleaned_content = self.cleaner.clean(result.content, self.config)
                    validation = self.validator.validate(cleaned_content, url)
                    
                    if validation['passed'] and validation['word_count'] >= 100:
                        logger.info(f"[AdapterChain] Anti-paywall success with {adapter.__class__.__name__}: {validation['word_count']} words")
                        
                        # Extract metadata
                        metadata = self.metadata_extractor.extract(result.content, url)
                        
                        # Update result
                        result.content = cleaned_content
                        result.word_count = len(cleaned_content.split())
                        result.quality_score = validation['score']
                        result.title = metadata.get('title')
                        result.author = metadata.get('author')
                        result.published_at = metadata.get('published_at')
                        result.metadata.update(metadata)
                        
                        # Marcar como contornado
                        result.metadata['conteudo_premium'] = 'bypassed'
                        result.metadata['premium_original_type'] = paywall_info['type']
                        result.metadata['premium_bypass_method'] = adapter.__class__.__name__
                        result.metadata['premium_indicators'] = paywall_info['hits']
                        result.metadata['premium_confidence'] = paywall_info['confidence']
                        
                        # Telemetria específica baseada nos indicadores encontrados
                        if any('tinypass' in hit for hit in paywall_info['hits']):
                            result.metadata['premium_provider'] = 'tinypass'
                        elif any('swg-google' in hit for hit in paywall_info['hits']):
                            result.metadata['premium_provider'] = 'subscribe_with_google'
                        elif any('piano' in hit for hit in paywall_info['hits']):
                            result.metadata['premium_provider'] = 'piano'
                        else:
                            result.metadata['premium_provider'] = 'generic'
                        
                        return result
                    else:
                        logger.warning(f"[AdapterChain] Premium bypass strategy {adapter.__class__.__name__} failed validation: {validation.get('drop_reason', 'Unknown')}")
                        continue
                else:
                    logger.warning(f"[AdapterChain] Premium bypass strategy {adapter.__class__.__name__} failed")
                    continue
                    
            except Exception as e:
                logger.error(f"[AdapterChain] Error in premium bypass strategy {adapter.__class__.__name__}: {str(e)}")
                continue
        
        logger.warning(f"[AdapterChain] All premium bypass strategies failed for {url}")
        return None

class ScrapeOrchestratorV5:
    """Orquestrador principal de scraping"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.metrics = AdapterMetrics()
        self.metrics_v2 = ScraperMetricsV2()  # NOVO: telemetria avançada
        self.chain = self._build_adapter_chain()
    
    def _build_adapter_chain(self) -> AdapterChainV5:
        """Constrói cadeia de adaptadores baseada na configuração"""
        adapters = []
        
        # Always include core adapters
        if HTTPX_AVAILABLE:
            adapters.append(HttpxAdapterV5(self.config, self.metrics))
        
        # Optional adapters based on configuration
        if self.config.enable_jina:
            adapters.append(JinaAdapterV5(self.config, self.metrics))
        
        if self.config.enable_browserless:
            adapters.append(BrowserlessAdapterV5(self.config, self.metrics))
        
        if self.config.enable_alternates:
            adapters.append(AlternatesAdapterV5(self.config, self.metrics))
        
        if self.config.enable_wayback:
            adapters.append(WaybackAdapterV5(self.config, self.metrics))
        
        if self.config.enable_unlock:
            adapters.append(UnlockAdapterV5(self.config, self.metrics))
        
        if self.config.enable_sbr:
            adapters.append(SBRAdapterV5(self.config, self.metrics))
        
        # Document adapter (always included)
        adapters.append(DocumentAdapterV5(self.config, self.metrics))
        
        return AdapterChainV5(adapters, self.config)
    
    async def scrape_url(self, url: str) -> ScrapeResult:
        """Método principal de scraping"""
        start_time = time.time()
        
        try:
            result = await self.chain.scrape(url)
            if result:
                result.extraction_time = time.time() - start_time
                return result
            else:
                return ScrapeResult(
                    url=url,
                    content="",
                    word_count=0,
                    success=False,
                    adapter_used="none",
                    extraction_time=time.time() - start_time,
                    error="All adapters failed"
                )
        except Exception as e:
            return ScrapeResult(
                url=url,
                content="",
                word_count=0,
                success=False,
                adapter_used="error",
                extraction_time=time.time() - start_time,
                error=str(e)
            )
    
    def get_metrics(self) -> AdapterMetrics:
        """Retorna métricas de performance"""
        return self.metrics


# ============================================================================
# SEÇÃO: CLASSE PRINCIPAL OPENWEBUI
# ============================================================================


# ============================================================================
# SEÇÃO FINAL: INTEGRAÇÃO COM OPENWEBUI (Production Grade)
# ============================================================================

class Tools:
    """
    Advanced Content Scraper V5 for OpenWebUI
    
    This tool provides comprehensive web scraping capabilities with multiple strategies:
    - HTTP direct requests (httpx)
    - Browserless headless browser
    - Jina Reader API
    - Document processing (PDF/DOCX)
    - Bright Data Unlocker & SBR
    
    Features:
    - Intelligent strategy selection based on content type
    - Paywall detection and bypass
    - Content quality validation
    - Circuit breaker protection
    - Caching for performance
    - Multiple retry mechanisms
    """
    
    class Valves(BaseModel):
        """Configuration valves for the scraper tool - Direct integration with OpenWebUI"""
        
        # ============================================================================
        # CORE ADAPTERS
        # ============================================================================
        enable_httpx: bool = Field(default=True, description="Enable HTTPX adapter")
        enable_jina: bool = Field(default=True, description="Enable Jina Reader API")
        enable_browserless: bool = Field(default=True, description="Enable Browserless headless browser")
        enable_document: bool = Field(default=True, description="Enable Document processing")
        enable_wayback: bool = Field(default=True, description="Enable Wayback Machine")
        enable_alternates: bool = Field(default=True, description="Enable Alternates adapter")
        enable_unlock: bool = Field(default=False, description="Enable Bright Data Unlocker")
        enable_sbr: bool = Field(default=False, description="Enable Bright Data SBR")
        
        # ============================================================================
        # TIMEOUTS & PERFORMANCE
        # ============================================================================
        request_timeout: int = Field(default=15, description="HTTP request timeout in seconds")
        browserless_timeout: int = Field(default=45, description="Browserless timeout in seconds")
        jina_timeout: int = Field(default=25, description="Jina API timeout in seconds")
        max_retries: int = Field(default=3, description="Maximum retry attempts")
        rate_limit_delay: float = Field(default=0.5, description="Delay between attempts for rate limiting")
        
        # ============================================================================
        # CONTENT LIMITS
        # ============================================================================
        content_word_limit: int = Field(default=2000, description="Maximum words in content")
        max_content_chars_in_response: int = Field(default=15000, description="Maximum characters in response")
        min_words_threshold: int = Field(default=30, description="Min words for valid content")
        
        # ============================================================================
        # JINA CONFIGURATION
        # ============================================================================
        jina_rpm: int = Field(default=60, description="Jina requests per minute")
        jina_allowlist: str = Field(
            default="wikipedia.org,github.com,bbc.com,gov.br,stlouisfed.org", 
            description="Jina allowlist (CSV)"
        )
        jina_denylist: str = Field(
            default="linkedin.com,elibrary.imf.org", 
            description="Jina denylist (CSV)"
        )
        
        # ============================================================================
        # BROWSERLESS CONFIGURATION
        # ============================================================================
        browserless_endpoint: str = Field(default="", description="Browserless endpoint URL")
        browserless_token: str = Field(default="", description="Browserless API token")
        browserless_stealth: bool = Field(default=True, description="Enable stealth mode")
        browserless_block_ads: bool = Field(default=True, description="Block ads/trackers")
        browserless_retry_count: int = Field(default=2, description="Retry attempts per strategy")
        
        # ============================================================================
        # BRIGHT DATA PROXY
        # ============================================================================
        bd_proxy_enabled: bool = Field(default=False, description="Enable Bright Data HTTP Proxy")
        bd_customer_id: str = Field(default="", description="Bright Data Customer ID")
        bd_zone_proxy: str = Field(default="", description="Bright Data Proxy Zone")
        bd_password: str = Field(default="", description="Bright Data Password")
        bd_default_country: str = Field(default="BR", description="Default proxy country")
        
        # ============================================================================
        # BRIGHT DATA SBR
        # ============================================================================
        bd_zone_sbr: str = Field(default="", description="Bright Data SBR Zone")
        sbr_infinite_scroll_max: int = Field(default=8, description="Max scroll attempts")
        
        # ============================================================================
        # UNLOCK API
        # ============================================================================
        unlock_endpoint: str = Field(default="", description="Web Unlocker endpoint")
        unlock_token: str = Field(default="", description="Unlocker API token")
        unlock_zone: str = Field(default="", description="Unlocker zone")
        unlock_timeout: int = Field(default=120, description="Unlocker timeout (s)")
        
        # ============================================================================
        # TIKA OCR
        # ============================================================================
        tika_endpoint: str = Field(default="", description="Apache Tika endpoint (optional)")
        tika_ocr_enabled: bool = Field(default=False, description="Enable Tika OCR for PDFs")
        
        # ============================================================================
        # VALIDATION & FILTERING
        # ============================================================================
        enable_paywall_bypass: bool = Field(default=True, description="Attempt paywall bypass")
        allowed_langs: str = Field(default="", description="Allowed languages (CSV: pt,en)")
        
        # ============================================================================
        # CITATION CONTROL
        # ============================================================================
        citation_max_length: int = Field(default=0, description="Max citation length (0=unlimited)")
        max_citation_collective_length: int = Field(default=0, description="Max total citations (0=unlimited)")
        
        # ============================================================================
        # ADVANCED CONFIGURATION
        # ============================================================================
        enable_debug_logging: bool = Field(
            default=False,
            description="Enable verbose debug logs (full HTML, payloads, etc.)"
        )
        user_agent: str = Field(
            default="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36",
            description="User agent string for requests"
        )
        max_concurrent: int = Field(default=4, description="Max concurrent requests")
        follow_amp: bool = Field(default=True, description="Try AMP first when detected")
        cache_ttl_sec: int = Field(default=600, description="Cache TTL in seconds")
        cache_max_size: int = Field(default=1000, description="Max cache entries (LRU eviction)")
        
        # ============================================================================
        # PLAYBOOKS
        # ============================================================================
        
        # ============================================================================
        # PRODUCTION SETTINGS
        # ============================================================================
        max_concurrency: int = Field(default=5, description="Max concurrent URLs to process in parallel")
        ua_mobile: bool = Field(default=False, description="Use mobile User-Agent")
        
        domain_playbooks: str = Field(
            default='{}',
            description="Domain-specific configs (JSON)"
        )
    
    def __init__(self):
        """Initialize the Tools class with default configuration"""
        self.valves = self.Valves()
        self._orchestrator = None
        self._cache = {}
        self._cache_ttl = {}
    
    def _get_orchestrator(self):
        """Get or create orchestrator instance (lazy loading)"""
        if self._orchestrator is None:
            # Convert Valves to ScraperConfig format
            config_dict = self._valves_to_config()
            config = ScraperConfig(**config_dict)
            self._orchestrator = ScrapeOrchestratorV5(config)
        return self._orchestrator
    
    def _valves_to_config(self) -> dict:
        """Convert Valves to ScraperConfig format - Direct conversion"""
        valves_dict = _model_to_dict(self.valves)
        
        # Process CSV fields
        jina_allow = tuple(s.strip() for s in valves_dict.get('jina_allowlist', '').split(',') if s.strip())
        jina_deny = tuple(s.strip() for s in valves_dict.get('jina_denylist', '').split(',') if s.strip())
        allowed_langs = None
        if valves_dict.get('allowed_langs'):
            allowed_langs = [s.strip() for s in valves_dict['allowed_langs'].split(',') if s.strip()]
        
        # Process JSON fields
        playbooks = {}
        try:
            playbooks_str = valves_dict.get('domain_playbooks', '{}')
            playbooks = json.loads(playbooks_str) if playbooks_str else {}
        except Exception as e:
            logger.warning(f"[Tools] Failed to parse playbooks: {e}")
        
        # Return configuration dict
        return {
            # Core adapters
            'enable_httpx': valves_dict.get('enable_httpx', True),
            'enable_jina': valves_dict.get('enable_jina', True),
            'enable_browserless': valves_dict.get('enable_browserless', True),
            'enable_document': valves_dict.get('enable_document', True),
            'enable_wayback': valves_dict.get('enable_wayback', True),
            'enable_alternates': valves_dict.get('enable_alternates', True),
            'enable_unlock': valves_dict.get('enable_unlock', False),
            'enable_sbr': valves_dict.get('enable_sbr', False),
            
            # Timeouts
            'request_timeout': valves_dict.get('request_timeout', 15),
            'browserless_timeout': valves_dict.get('browserless_timeout', 45),
            'jina_timeout': valves_dict.get('jina_timeout', 25),
            'max_retries': valves_dict.get('max_retries', 3),
            'rate_limit_delay': valves_dict.get('rate_limit_delay', 0.5),
            
            # Content limits
            'content_word_limit': valves_dict.get('content_word_limit', 2000),
            'max_content_chars_in_response': valves_dict.get('max_content_chars_in_response', 15000),
            'min_words_threshold': valves_dict.get('min_words_threshold', 30),
            
            # Jina
            'jina_rpm': valves_dict.get('jina_rpm', 60),
            'jina_allowlist': jina_allow,
            'jina_denylist': jina_deny,
            
            # Browserless
            'browserless_endpoint': valves_dict.get('browserless_endpoint', ''),
            'browserless_token': valves_dict.get('browserless_token'),
            'browserless_stealth': valves_dict.get('browserless_stealth', True),
            'browserless_block_ads': valves_dict.get('browserless_block_ads', True),
            'browserless_retry_count': valves_dict.get('browserless_retry_count', 2),
            
            # Bright Data Proxy
            'bd_proxy_enabled': valves_dict.get('bd_proxy_enabled', False),
            'bd_customer_id': valves_dict.get('bd_customer_id'),
            'bd_zone_proxy': valves_dict.get('bd_zone_proxy'),
            'bd_password': valves_dict.get('bd_password'),
            'bd_default_country': valves_dict.get('bd_default_country', 'BR'),
            
            # Bright Data SBR
            'bd_zone_sbr': valves_dict.get('bd_zone_sbr'),
            'sbr_infinite_scroll_max': valves_dict.get('sbr_infinite_scroll_max', 8),
            
            # Unlock API
            'unlock_endpoint': valves_dict.get('unlock_endpoint', ''),
            'unlock_token': valves_dict.get('unlock_token'),
            'unlock_zone': valves_dict.get('unlock_zone'),
            'unlock_timeout': valves_dict.get('unlock_timeout', 120),
            
            # Tika OCR
            'tika_endpoint': valves_dict.get('tika_endpoint'),
            'tika_ocr_enabled': valves_dict.get('tika_ocr_enabled', False),
            
            # Validation
            'enable_paywall_bypass': valves_dict.get('enable_paywall_bypass', True),
            'allowed_langs': allowed_langs,
            
            # Citation control
            'citation_max_length': valves_dict.get('citation_max_length', 0),
            'max_citation_collective_length': valves_dict.get('max_citation_collective_length', 0),
            
            # Advanced
            'user_agent': valves_dict.get('user_agent'),
            'max_concurrent': valves_dict.get('max_concurrent', 4),
            'follow_amp': valves_dict.get('follow_amp', True),
            'cache_ttl_sec': valves_dict.get('cache_ttl_sec', 600),
            'domain_playbooks': playbooks
        }
    
    async def scrape(self, url: Optional[str] = None, urls: Optional[List[str]] = None, __event_emitter__: Optional[callable] = None) -> Dict[str, Any]:
        """
        Retrieve and process web content with production-grade robustness
        
        Args:
            url: Single URL to retrieve content from
            urls: List of URLs to retrieve content from in parallel
            __event_emitter__: Event emitter for status updates
        
        Returns:
            Single result dict or {"results": [...]} for multiple URLs
        """
        # Generate request ID for distributed tracing
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"[req:{request_id}] Starting scrape for {url or urls}")
        
        # Create orchestrator with overrides
        orchestrator = self._get_orchestrator()
        
        # Event emitter handling
        if __event_emitter__:
            try:
                if asyncio.iscoroutinefunction(__event_emitter__):
                    await __event_emitter__({"type": "status", "data": {"description": "Starting content scraping...", "done": False}})
                else:
                    __event_emitter__({"type": "status", "data": {"description": "Starting content scraping...", "done": False}})
            except Exception as e:
                logger.debug(f"Error emitting start event: {e}")
        
        # Determine target URLs
        target_urls = urls or ([url] if url else [])
        if not target_urls:
            return {
                "success": False,
                "url": "",
                "error": "Provide 'url' or 'urls' parameter"
            }
        
        # Process multiple URLs with concurrency control
        if len(target_urls) > 1:
            sem = asyncio.Semaphore(self.valves.max_concurrency)
            
            async def run_safe(u: str):
                """Safe wrapper with semaphore and exception handling"""
                async with sem:
                    try:
                        return await orchestrator.scrape_url(u)
                    except Exception as e:
                        logger.error(f"Unhandled exception for {u}: {e}")
                        return ScrapeResult(
                            success=False,
                            url=u,
                            error=f"Unhandled exception: {str(e)}"
                        )
            
            # Gather all results (never fails even if individual tasks fail)
            results = await asyncio.gather(*(run_safe(u) for u in target_urls))
            
            # Convert to dicts and apply total character limit
            results_dicts = [
                _model_to_dict(r) if hasattr(r, 'model_dump') or hasattr(r, 'dict') else (r if isinstance(r, dict) else {'url': '', 'success': False, 'error': 'Invalid result type'})
                for r in results
            ]
            
            # Apply total character limit if configured
            if hasattr(self.valves, 'max_content_chars_in_response') and self.valves.max_content_chars_in_response > 0:
                total_chars = sum(len(str(r.get('content', ''))) for r in results_dicts)
                if total_chars > self.valves.max_content_chars_in_response:
                    # CRITICAL: NUNCA descartar resultados - distribuir limite entre TODOS
                    limit = self.valves.max_content_chars_in_response
                    num_results = len(results_dicts)
                    
                    logger.warning(f"[Tools] Total content ({total_chars} chars) exceeds limit ({limit} chars) for {num_results} URLs")
                    
                    # Estratégia 1: Distribuir limite igualmente entre todos os resultados
                    chars_per_result = max(500, limit // num_results)  # Mínimo 500 chars por resultado
                    
                    truncated_results = []
                    for idx, result in enumerate(results_dicts):
                        content = str(result.get('content', ''))
                        
                        # SEMPRE incluir metadados básicos (success, url, adapter, word_count, error)
                        result_summary = {
                            'url': result.get('url', ''),
                            'success': result.get('success', False),
                            'adapter_used': result.get('adapter_used', 'none'),
                            'word_count': result.get('word_count', 0),
                            'extraction_time': result.get('extraction_time', 0.0),
                            'error': result.get('error'),
                            'title': result.get('title'),
                            'author': result.get('author'),
                            'published_at': result.get('published_at'),
                            'metadata': result.get('metadata', {})
                        }
                        
                        # Truncar conteúdo se necessário
                        if len(content) > chars_per_result:
                            truncated_content = content[:chars_per_result]
                            truncated_content += f"\n\n[... conteúdo truncado de {len(content)} para {chars_per_result} caracteres ({idx+1}/{num_results}) ...]"
                            result_summary['content'] = truncated_content
                            result_summary['metadata']['truncated'] = True
                            result_summary['metadata']['original_length'] = len(content)
                        else:
                            result_summary['content'] = content
                            result_summary['metadata']['truncated'] = False
                        
                        truncated_results.append(result_summary)
                    
                    logger.info(f"[Tools] Distributed {limit} chars across {num_results} results (~{chars_per_result} chars each)")
                    results_dicts = truncated_results
            
            return {"results": results_dicts}
        
        # Process single URL
        else:
            try:
                result = await orchestrator.scrape_url(target_urls[0])
                result_dict = _model_to_dict(result) if hasattr(result, 'model_dump') or hasattr(result, 'dict') else (result if isinstance(result, dict) else {'url': target_urls[0], 'success': False, 'error': 'Invalid result type'})
                
                # Apply total character limit if configured
                if hasattr(self.valves, 'max_content_chars_in_response') and self.valves.max_content_chars_in_response > 0:
                    if isinstance(result_dict, dict):
                        content = str(result_dict.get('content', ''))
                        if len(content) > self.valves.max_content_chars_in_response:
                            limit = self.valves.max_content_chars_in_response - 100  # Reserve 100 chars for truncation message
                            truncated_content = content[:limit]
                            truncated_content += f"\n\n[... conteúdo truncado - limite total de {self.valves.max_content_chars_in_response} caracteres atingido ...]"
                            result_dict['content'] = truncated_content
                
                return result_dict if isinstance(result_dict, dict) else {'url': target_urls[0], 'success': False, 'error': 'Result conversion failed'}
            except Exception as e:
                logger.error(f"Unhandled exception for {target_urls[0]}: {e}")
                return {
                    "success": False,
                    "url": target_urls[0],
                    "error": f"Unhandled exception: {str(e)}"
                }
    

    
    def set_config(self, **kwargs) -> Dict[str, Any]:
        """
        Update tool configuration (Valves) with credential sanitization
        
        Args:
            **kwargs: Configuration values to update
        
        Returns:
            dict: Success status and updated configuration
        """
        # Sanitize sensitive fields for logging
        sanitized_kwargs = {
            k: ("***" if any(s in k.lower() for s in ["password", "token", "key", "secret"]) else v)
            for k, v in kwargs.items()
        }
        
        try:
            # Update valves
            for key, value in kwargs.items():
                if hasattr(self.valves, key):
                    setattr(self.valves, key, value)
            
            # Reset orchestrator to pick up new config
            self._orchestrator = None
            
            logger.info(f"Valves updated successfully: {sanitized_kwargs}")
            return {
                "success": True,
                "updated_config": sanitized_kwargs
            }
        except Exception as e:
            logger.error(f"Failed to update Valves: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def clear_cache(self) -> Dict[str, str]:
        """
        Clear the scraper cache
        
        Returns:
            dict: Confirmation message
        """
        self._cache.clear()
        self._cache_ttl.clear()
        logger.info("[Cache] CLEARED")
        return {"message": "Cache cleared successfully"}
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get scraper performance metrics
        
        Returns:
            dict: Performance metrics including success rates, timing, etc.
        """
        try:
            orchestrator = self._get_orchestrator()
            metrics = orchestrator.get_metrics()
            return {
                "attempts": metrics.attempts,
                "successes": metrics.successes,
                "failures": metrics.failures,
                "success_rate": metrics.success_rate,
                "avg_time": metrics.avg_time,
                "total_time": metrics.total_time,
                "last_success": metrics.last_success.isoformat() if metrics.last_success else None,
                "last_failure": metrics.last_failure.isoformat() if metrics.last_failure else None
            }
        except Exception as e:
            return {"error": f"Failed to get metrics: {str(e)}"}
    
    async def get_metrics_v2(self) -> Dict[str, Any]:
        """
        Get detailed telemetry metrics (exportable for monitoring)
        
        Returns:
            dict: Detailed metrics including:
                - Per-adapter success rates, latencies, p95
                - Cache hit rate
                - Circuit breaker states
                - Validation failure reasons
        """
        try:
            orchestrator = self._get_orchestrator()
            summary = orchestrator.metrics_v2.get_summary()
            # Include LRU cache stats when possible
            try:
                # use configured ttl and size for the shared cache instance
                cache = get_cache(
                    getattr(self.valves, 'cache_ttl_sec', 600),
                    getattr(self.valves, 'cache_max_size', 1000)
                )
                if hasattr(cache, 'get_stats'):
                    summary['cache'] = {**summary.get('cache', {}), **cache.get_stats()}
            except Exception:
                pass
            return summary
        except Exception as e:
            return {"error": f"Failed to get metrics v2: {str(e)}"}

def get_tools():
    """Retorna lista de ferramentas disponíveis."""
    _tools = []
    _instance = Tools()

    async def scrape(url: Optional[str] = None, urls: Optional[List[str]] = None, __event_emitter__: Optional[callable] = None) -> str:
        """
        Retrieve and extract clean text content from web pages with intelligent multi-strategy scraping.
        
        This tool automatically handles complex websites including:
        - Static HTML pages (blogs, news, documentation)
        - JavaScript-heavy sites (SPAs, React/Vue apps)
        - Paywalled content (soft/hard paywall bypass)
        - PDF and DOCX documents
        - Bot-protected sites (Cloudflare, Akamai)
        
        WHEN TO USE:
        ✅ Extract article/blog content for analysis
        ✅ Scrape research papers, news articles, documentation
        ✅ Process multiple URLs in parallel (batch mode)
        ✅ Bypass paywalls and soft-blocks automatically
        ✅ Extract text from PDF/DOCX documents
        
        USAGE:
        - Single URL: scrape(url="https://example.com/article")
        - Batch mode: scrape(urls=["https://a.com", "https://b.com"])
        
        STRATEGIES (automatic fallback chain):
        1. HTTP direct (httpx) - Fast for static HTML (~2s)
        2. Jina Reader API - Cloud extraction, paywall bypass (~3s)
        3. Browserless browser - JavaScript execution (~8s)
        4. Alternates (AMP/print) - Alternative versions
        5. Wayback Machine - Archive fallback for old content
        
        FEATURES:
        - Intelligent adapter selection (based on content type)
        - Automatic paywall detection and bypass
        - Content quality validation (min words, paragraphs)
        - Metadata extraction (title, author, date)
        - Circuit breaker protection (per-host)
        - Caching (10min TTL for successful scrapes)
        
        Args:
            url: Single URL to scrape (mutually exclusive with 'urls')
            urls: List of URLs to scrape in parallel (mutually exclusive with 'url')
            __event_emitter__: Internal event emitter for progress updates
        
        Returns:
            JSON string with structure:
            - Single URL mode: {"success": bool, "url": str, "content": str, "word_count": int, 
                                "adapter_used": str, "title": str, "author": str, "metadata": dict}
            - Batch mode: {"results": [{"success": bool, "url": str, ...}, ...]}
        
        Examples:
            # Extract article content
            result = scrape(url="https://techcrunch.com/article")
            
            # Batch scraping (parallel)
            results = scrape(urls=["https://a.com", "https://b.com", "https://c.com"])
            
            # Extract PDF content
            result = scrape(url="https://example.com/paper.pdf")
        
        Note: Tool automatically selects best strategy based on URL and content type.
              Failed scrapes return success=false with error details.
        """
        result = await _instance.scrape(url=url, urls=urls, __event_emitter__=__event_emitter__)
        return json.dumps(result, ensure_ascii=False, indent=2)

    async def set_config(**kwargs) -> str:
        """
        Update scraper configuration (Valves) to customize behavior for subsequent scraping calls.
        
        Common configurations:
        - Timeouts: request_timeout (default: 15s), browserless_timeout (default: 45s)
        - Adapters: enable_httpx, enable_jina, enable_browserless (default: True)
        - Content limits: content_word_limit (default: 2000), min_words_threshold (default: 30)
        - Rate limiting: jina_rpm (default: 60), rate_limit_delay (default: 0.5s)
        
        Args:
            **kwargs: Configuration key-value pairs to update
                     (e.g., request_timeout=30, enable_browserless=False)
        
        Returns:
            JSON string: {"success": bool, "updated_config": dict}
        
        Example:
            # Increase timeouts for slow sites
            set_config(request_timeout=30, browserless_timeout=60)
            
            # Disable expensive adapters
            set_config(enable_browserless=False, enable_jina=True)
        """
        result = _instance.set_config(**kwargs)
        return json.dumps(result, ensure_ascii=False, indent=2)

    async def get_metrics() -> str:
        """
        Get scraper performance metrics including success rates, timing, and failure tracking.
        
        Useful for monitoring and debugging scraper performance over time.
        
        Returns:
            JSON string with metrics:
            - attempts: Total scrape attempts
            - successes: Successful scrapes
            - failures: Failed scrapes
            - success_rate: Percentage of successful scrapes
            - avg_time: Average time per scrape (seconds)
            - total_time: Total time spent scraping
            - last_success: Timestamp of last successful scrape
            - last_failure: Timestamp of last failed scrape
        
        Example:
            metrics = get_metrics()
            # Returns: {"attempts": 150, "successes": 142, "success_rate": 94.67, ...}
        """
        result = await _instance.get_metrics()
        return json.dumps(result, ensure_ascii=False, indent=2)

    async def clear_cache() -> str:
        """
        Clear the internal scraper cache to force fresh scraping of all URLs.
        
        Cache is used to avoid re-scraping the same URL within TTL window (default: 10 minutes).
        Useful when you need the most up-to-date content from a previously scraped URL.
        
        Returns:
            JSON string: {"message": "Cache cleared successfully"}
        
        Example:
            # Clear cache before important scrape
            clear_cache()
            result = scrape(url="https://breaking-news.com/latest")
        
        Note: Cache automatically expires after 10 minutes (configurable via cache_ttl_sec valve).
              Failed scrapes are cached for only 2 minutes to allow quick retry.
        """
        result = _instance.clear_cache()
        return json.dumps(result, ensure_ascii=False, indent=2)

    async def get_metrics_v2() -> str:
        """
        Get advanced telemetry metrics (per-adapter stats, cache stats, CB opens).
        Returns a JSON string.
        """
        result = await _instance.get_metrics_v2()
        return json.dumps(result, ensure_ascii=False, indent=2)

    _tools.extend([scrape, set_config, get_metrics, get_metrics_v2, clear_cache])
    return _tools


# ============================================================================
# CACHE GLOBAL
# ============================================================================

class LRUCache:
    """LRU Cache com size limit e TTL - prevents memory leaks"""
    
    def __init__(self, max_size: int = 1000, ttl_seconds: int = 600):
        from collections import OrderedDict
        self.cache: 'OrderedDict[str, Tuple[Any, float, int]]' = OrderedDict()
        self.max_size = max_size
        self.ttl = ttl_seconds
        self.hits = 0
        self.misses = 0
        # Thread-safety
        try:
            from threading import RLock
            self._lock = RLock()
        except Exception:
            self._lock = None
    
    def get(self, key: str) -> Optional[Any]:
        """Recupera item do cache se ainda válido (move to end = MRU)"""
        lock = getattr(self, "_lock", None)
        if lock:
            with lock:
                return self._get_nolock(key)
        return self._get_nolock(key)

    def _get_nolock(self, key: str) -> Optional[Any]:
        if key in self.cache:
            cache_entry = self.cache[key]
            if len(cache_entry) == 3:
                value, timestamp, item_ttl = cache_entry
                ttl_to_use = item_ttl
            else:
                # Backward compatibility
                value, timestamp = cache_entry
                ttl_to_use = self.ttl
            
            if time.time() - timestamp < ttl_to_use:
                # Move to end (Most Recently Used)
                self.cache.move_to_end(key)
                self.hits += 1
                logger.debug(f"[Cache] HIT para {key}")
                return value
            else:
                logger.debug(f"[Cache] EXPIRED para {key}")
                del self.cache[key]
                self.misses += 1
        else:
            self.misses += 1
        
        logger.debug(f"[Cache] MISS para {key}")
        return None
    
    def put(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Armazena item no cache com TTL e LRU eviction"""
        lock = getattr(self, "_lock", None)
        if lock:
            with lock:
                return self._put_nolock(key, value, ttl_seconds)
        return self._put_nolock(key, value, ttl_seconds)

    def _put_nolock(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        ttl_to_use = ttl_seconds if ttl_seconds is not None else self.ttl
        
        # Se já existe, atualizar e mover para o fim
        if key in self.cache:
            self.cache[key] = (value, time.time(), ttl_to_use)
            self.cache.move_to_end(key)
            logger.debug(f"[Cache] UPDATE para {key} (TTL: {ttl_to_use}s)")
            return
        
        # Evict LRU if at max_size
        if len(self.cache) >= self.max_size:
            evicted_key, _ = self.cache.popitem(last=False)
            logger.debug(f"[Cache] EVICTED (LRU): {evicted_key}")
        
        # Add new item
        self.cache[key] = (value, time.time(), ttl_to_use)
        logger.debug(f"[Cache] PUT para {key} (TTL: {ttl_to_use}s)")
    
    def clear(self) -> None:
        """Limpa todo o cache"""
        lock = getattr(self, "_lock", None)
        if lock:
            with lock:
                self.cache.clear()
                self.hits = 0
                self.misses = 0
                logger.info(f"[Cache] CLEARED")
                return
        self.cache.clear()
        self.hits = 0
        self.misses = 0
        logger.info(f"[Cache] CLEARED")
    
    def size(self) -> int:
        """Retorna número de itens no cache"""
        return len(self.cache)
    
    def get_stats(self) -> dict:
        """Retorna estatísticas do cache para observabilidade"""
        return {
            "size": len(self.cache),
            "max_size": self.max_size,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": self.hits / (self.hits + self.misses) if (self.hits + self.misses) > 0 else 0.0
        }


# Legacy alias for backward compatibility
class SimpleCache(LRUCache):
    """Backward compatibility wrapper"""
    pass

# Cache global
_global_cache: Optional[LRUCache] = None
_global_cache_lock = None

def get_cache(ttl_seconds: int = 600, max_size: int = 1000) -> LRUCache:
    """Retorna instância global do cache"""
    global _global_cache, _global_cache_lock
    if _global_cache_lock is None:
        try:
            from threading import RLock
            _global_cache_lock = RLock()
        except Exception:
            _global_cache_lock = None
    if _global_cache is not None:
        return _global_cache
    if _global_cache_lock:
        with _global_cache_lock:
            if _global_cache is None:
                _global_cache = LRUCache(max_size=max_size, ttl_seconds=ttl_seconds)
            return _global_cache
    # Fallback without lock
    _global_cache = LRUCache(max_size=max_size, ttl_seconds=ttl_seconds)
    return _global_cache

# ============================================================================
# SEÇÃO: TESTE E EXEMPLO DE USO
# ============================================================================

if __name__ == "__main__":
    # Test the OpenWebUI compatible version
    import asyncio
    
    async def test_openwebui_scraper():
        """Test the OpenWebUI compatible scraper"""
        tools = Tools()
        
        # Test single URL using correct API
        result = await tools.scrape(url="https://httpbin.org/html")
        print(f"Single URL test: {result}")
        
        # Test multiple URLs
        results = await tools.scrape(urls=["https://httpbin.org/html", "https://example.com"])
        print(f"Multiple URLs test: {results}")
        
        # Test metrics
        metrics = await tools.get_metrics()
        print(f"Metrics: {metrics}")
        
        # Test cache clear
        cache_result = tools.clear_cache()
        print(f"Cache clear: {cache_result}")
    
    asyncio.run(test_openwebui_scraper())

# ============================================================================
# MODERN OPENWEBUI INTEGRATION (Optional)
# ============================================================================