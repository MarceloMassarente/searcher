"""
title: Context Reducer Tool
author: DeepResearch Team  
version: 2.3.1
requirements: rank-bm25>=0.2.2,sentence-transformers>=2.2.2,rapidfuzz>=3.0.0,simhash>=2.1.2
license: MIT
description: Reduz contexto de scraper/múltiplas fontes usando BM25 + embeddings híbrido com dedup inteligente e adaptador universal
"""

from typing import List, Dict, Any, Tuple, Optional, Callable, Union
from dataclasses import dataclass
from collections import defaultdict
import re, hashlib, os, json, asyncio, threading, logging
from pydantic import BaseModel, Field

# Setup logging
logger = logging.getLogger(__name__)

# Thread-safe lock for state
_jobs_lock = threading.Lock()
_JOBS_STORE: Dict[str, List[Dict[str, Any]]] = {}

# ---------- deps opcionais ----------
_has_bm25 = True
try:
    from rank_bm25 import BM25Okapi
except Exception:
    _has_bm25 = False

_has_st = True
try:
    from sentence_transformers import SentenceTransformer, util
except Exception:
    _has_st = False

_has_fuzz = True
try:
    from rapidfuzz import fuzz
except Exception:
    _has_fuzz = False

_has_simhash = True
try:
    from simhash import Simhash
except Exception:
    _has_simhash = False

# Global model cache
_ST = None
_ST_LOCK = threading.Lock()

# ---------- ESTRUTURAS ----------
@dataclass
class Chunk:
    id: str
    phase: str
    url: str
    title: str
    text: str
    score: float = 0.0
    bm25: float = 0.0
    emb: float = 0.0

# ---------- UTIL ----------
def est_tokens(text: str, factor: float = 0.75) -> int:
    return int(len((text or "").split()) / factor)

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def split_sentences(text: str, min_words: int = 6) -> List[str]:
    sents = re.split(r"(?<=[\.\?!])\s+", text or "")
    return [clean_text(s) for s in sents if len(s.split()) >= min_words]

def group_microchunks_from_text(text: str, window: int = 5, min_words: int = 6) -> List[str]:
    sents = split_sentences(text, min_words)
    out, acc = [], []
    for s in sents:
        acc.append(s)
        if len(acc) >= window:
            out.append(clean_text(" ".join(acc)))
            acc = []
    if acc:
        out.append(clean_text(" ".join(acc)))
    return out

def url_hash(url: str, length: int = 10) -> str:
    return hashlib.md5((url or "").encode("utf-8")).hexdigest()[:length]

def normalize(arr: List[float]) -> List[float]:
    if not arr: return []
    lo, hi = min(arr), max(arr)
    if hi - lo < 1e-9:
        return [0.0 for _ in arr]
    return [(x - lo) / (hi - lo) for x in arr]

# ---------- BM25 ----------
def bm25_index(texts: List[str]):
    if not _has_bm25:
        return None, [t.split() for t in texts]
    tokens = [t.split() for t in texts]
    return BM25Okapi(tokens), tokens

def bm25_scores(bm25, tokenized, query: str) -> List[float]:
    if _has_bm25 and bm25 is not None:
        return bm25.get_scores(query.split())
    # fallback lexical
    q = set(query.lower().split())
    return [sum(1 for w in toks if w.lower() in q) for toks in tokenized]

# ---------- EMBEDDINGS ----------
def _load_st(model_name: str = "intfloat/e5-small-v2"):
    global _ST
    if not _has_st: 
        return None
    
    # Fast path without lock
    if _ST is not None:
        return _ST
    
    # Slow path with lock (double-checked)
    with _ST_LOCK:
        if _ST is None:
            try:
                _ST = SentenceTransformer(model_name)
                logger.info(f"[ContextReducer] Modelo carregado: {model_name}")
            except Exception as e:
                logger.error(f"[ContextReducer] Erro ao carregar modelo: {e}")
                return None
        return _ST

async def emb_scores_async(query: str, texts: List[str], model_name: str, timeout: int = 60) -> List[float]:
    """Calcula scores de embeddings com timeout"""
    if not _has_st:
        return [0.0]*len(texts)
    
    try:
        model = _load_st(model_name)
        if model is None:
            return [0.0]*len(texts)
        
        # Run em thread separada com timeout
        def _encode():
            q_emb = model.encode([query], normalize_embeddings=True, convert_to_tensor=True)
            t_emb = model.encode(texts, normalize_embeddings=True, convert_to_tensor=True)
            import numpy as np
            cos = util.cos_sim(q_emb, t_emb).cpu().numpy()[0]
            return ((cos + 1.0)/2.0).tolist()
        
        result = await asyncio.wait_for(
            asyncio.to_thread(_encode),
            timeout=timeout
        )
        return result
        
    except asyncio.TimeoutError:
        logger.warning(f"[ContextReducer] Timeout ao calcular embeddings ({timeout}s)")
        return [0.0]*len(texts)
    except Exception as e:
        logger.error(f"[ContextReducer] Erro em embeddings: {type(e).__name__}")
        return [0.0]*len(texts)

# ---------- DEDUP ----------
def _is_dup(a: str, b: str, thresh: float = 0.92, simhash_dist: int = 3, fallback_chars: int = 200) -> bool:
    if _has_fuzz:
        try:
            return (fuzz.token_set_ratio(a, b)/100.0) >= thresh
        except Exception:
            pass
    if _has_simhash:
        try:
            return Simhash(a).distance(Simhash(b)) <= simhash_dist
        except Exception:
            pass
    return a[:fallback_chars] == b[:fallback_chars]

def dedup_chunks(chs: List[Chunk], thresh: float = 0.92, simhash_dist: int = 3, fallback_chars: int = 200) -> List[Chunk]:
    out: List[Chunk] = []
    for c in chs:
        if any(_is_dup(c.text, u.text, thresh, simhash_dist, fallback_chars) for u in out):
            continue
        out.append(c)
    return out

def dedup_chunks_best_score(chs: List[Chunk], thresh: float, simhash_dist: int, fallback_chars: int) -> List[Chunk]:
    """
    Mantém chunk com MELHOR score entre duplicatas (melhoria sobre versão original)
    
    Args:
        chs: Lista de chunks ranqueados
        thresh: Threshold de similaridade
        simhash_dist: Distância máxima SimHash
        fallback_chars: Chars para fallback
    
    Returns:
        Lista dedupada mantendo chunks com melhor score
    """
    out: List[Chunk] = []
    
    for c in chs:
        # Procurar duplicata já selecionada
        dup_idx = None
        for i, u in enumerate(out):
            if _is_dup(c.text, u.text, thresh, simhash_dist, fallback_chars):
                dup_idx = i
                break
        
        if dup_idx is None:
            # Novo chunk único
            out.append(c)
        elif c.score > out[dup_idx].score:
            # Substituir por chunk com score melhor
            logger.debug(f"[DedupHíbrida] Substituindo chunk (score {out[dup_idx].score:.3f} → {c.score:.3f})")
            out[dup_idx] = c
        # else: manter chunk atual (score pior)
    
    return out

def _get_threshold_for_mode(mode: str, base_threshold: float) -> float:
    """
    Ajusta threshold de deduplicação baseado no modo
    
    - coarse: +0.03 (0.95) → Mais agressivo (máxima compressão)
    - light: base (0.92) → Balanceado
    - ultra: -0.04 (0.88) → Mais permissivo (máxima qualidade)
    
    Args:
        mode: "coarse" | "light" | "ultra"
        base_threshold: Threshold base das valves
    
    Returns:
        Threshold ajustado
    """
    if mode == "coarse":
        return min(base_threshold + 0.03, 0.95)
    elif mode == "ultra":
        return max(base_threshold - 0.04, 0.85)
    return base_threshold

# ---------- STATE MANAGEMENT (thread-safe) ----------
def _store_append(job_id: str, items: List[Dict[str, Any]]):
    with _jobs_lock:
        _JOBS_STORE.setdefault(job_id, []).extend(items)

def _store_get_all(job_id: str) -> List[Dict[str, Any]]:
    with _jobs_lock:
        return list(_JOBS_STORE.get(job_id, []))

def _store_clear(job_id: str):
    with _jobs_lock:
        _JOBS_STORE.pop(job_id, None)

def _dedup_leve_pre_embed(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicação leve ANTES de gerar embeddings (economiza ~50-80% de processamento)
    
    Estratégia:
    1. Remove duplicatas exatas (string slice 500 chars)
    2. Remove near-duplicates (SimHash distance ≤ 2)
    3. Preserva ordem (importante para narrativa)
    
    Complexidade: O(n) com SimHash
    
    Args:
        docs: Lista de documentos do scraper
    
    Returns:
        Lista dedupada
    """
    if not docs or len(docs) < 2:
        return docs
    
    # ETAPA 1: Duplicatas exatas (slice de 500 chars)
    seen_slices = set()
    unique_docs = []
    
    for doc in docs:
        text = doc.get("text", "")
        text_slice = text[:500].strip()
        
        if text_slice not in seen_slices:
            seen_slices.add(text_slice)
            unique_docs.append(doc)
    
    if len(docs) > len(unique_docs):
        logger.info(f"[DedupLeve] Duplicatas exatas: {len(docs)} → {len(unique_docs)} docs (-{len(docs)-len(unique_docs)})")
    
    # ETAPA 2: Near-duplicates com SimHash
    if _has_simhash and len(unique_docs) > 1:
        final_docs = []
        seen_hashes = []
        
        for doc in unique_docs:
            text = doc.get("text", "")
            try:
                doc_hash = Simhash(text)
                
                # Verificar se já existe hash similar (distância ≤ 2)
                is_duplicate = False
                for existing_hash in seen_hashes:
                    if doc_hash.distance(existing_hash) <= 2:
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    seen_hashes.append(doc_hash)
                    final_docs.append(doc)
            except Exception as e:
                logger.warning(f"[DedupLeve] Erro ao calcular SimHash: {e}")
                final_docs.append(doc)  # Manter doc em caso de erro
        
        if len(unique_docs) > len(final_docs):
            logger.info(f"[DedupLeve] Near-duplicates: {len(unique_docs)} → {len(final_docs)} docs (-{len(unique_docs)-len(final_docs)})")
        
        return final_docs
    
    return unique_docs

def normalize_custom_source(
    fonte_data: Any,
    source_name: str = "custom_source",
    text_fields: Optional[List[str]] = None,
    url_fields: Optional[List[str]] = None,
    title_fields: Optional[List[str]] = None,
    date_fields: Optional[List[str]] = None,
    author_fields: Optional[List[str]] = None,
    min_text_length: int = 100,
    min_word_count: int = 50
) -> Dict[str, Any]:
    """
    ADAPTADOR UNIVERSAL para qualquer fonte de dados
    
    Detecta automaticamente estrutura e mapeia campos para formato do reducer
    
    Args:
        fonte_data: Dados da fonte (list, dict, iterator, etc.)
        source_name: Nome da fonte (usado no phase name)
        text_fields: Lista de campos possíveis para texto (ordem de prioridade)
        url_fields: Lista de campos possíveis para URL
        title_fields: Lista de campos possíveis para título
        date_fields: Lista de campos possíveis para data
        author_fields: Lista de campos possíveis para autor
        min_text_length: Mínimo de caracteres no texto
        min_word_count: Mínimo de palavras
    
    Returns:
        Dict no formato do reducer: {"phases": [{"name": str, "docs": [...]}]}
    
    Examples:
        # API REST
        api_data = requests.get("...").json()["results"]
        normalized = normalize_custom_source(api_data, source_name="news_api")
        
        # Database
        cur.execute("SELECT * FROM articles")
        rows = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]
        normalized = normalize_custom_source(rows, source_name="database")
        
        # JSON file
        with open("data.json") as f:
            data = json.load(f)
        normalized = normalize_custom_source(data, source_name="json_file")
    """
    # Default field names (ordem de prioridade)
    text_fields = text_fields or ["text", "content", "body", "description", "summary", "message"]
    url_fields = url_fields or ["url", "link", "uri", "href", "id"]
    title_fields = title_fields or ["title", "subject", "headline", "name"]
    date_fields = date_fields or ["published_at", "date", "created_at", "timestamp", "publishedAt", "updated_at"]
    author_fields = author_fields or ["author", "user", "creator", "by", "author_name"]
    
    def _extract_field(item: dict, field_names: List[str], default: str = "") -> str:
        """Extrai primeiro campo encontrado na lista de prioridade"""
        for field in field_names:
            value = item.get(field)
            if value:
                return str(value)
        return default
    
    def _normalize_item(item: Any, idx: int) -> Optional[Dict[str, Any]]:
        """Normaliza um item individual"""
        if not isinstance(item, dict):
            return None
        
        # Extrair texto (obrigatório)
        text = _extract_field(item, text_fields)
        if not text or len(text) < min_text_length:
            logger.debug(f"[UniversalAdapter] Skipping item {idx}: texto insuficiente ({len(text)} chars)")
            return None
        
        # Validar word count
        word_count = len(text.split())
        if word_count < min_word_count:
            logger.debug(f"[UniversalAdapter] Skipping item {idx}: word count baixo ({word_count} words)")
            return None
        
        # Extrair campos
        url = _extract_field(item, url_fields, f"{source_name}://{idx}")
        title = _extract_field(item, title_fields, f"Item {idx+1}")
        published_at = _extract_field(item, date_fields, None)
        author = _extract_field(item, author_fields, None)
        
        return {
            "text": text,
            "url": url,
            "title": title,
            "published_at": published_at if published_at else None,
            "author": author if author else None,
            "word_count": word_count,
            # Metadados
            "source_type": source_name,
            "item_index": idx
        }
    
    # Detectar estrutura da fonte
    items = []
    
    # Caso 1: Lista direta
    if isinstance(fonte_data, list):
        items = fonte_data
    
    # Caso 2: Dict com lista (detectar automaticamente)
    elif isinstance(fonte_data, dict):
        # Procurar chaves comuns que contêm listas
        list_keys = ["results", "data", "items", "articles", "documents", "entries", "hits", "rows"]
        for key in list_keys:
            if key in fonte_data and isinstance(fonte_data[key], list):
                items = fonte_data[key]
                logger.info(f"[UniversalAdapter] Detectado lista em chave '{key}'")
                break
        
        # Se não encontrou lista, tentar usar valores do dict
        if not items:
            # Verificar se é um único item (não uma lista)
            if any(field in fonte_data for field in text_fields):
                items = [fonte_data]
                logger.info(f"[UniversalAdapter] Detectado item único")
    
    # Caso 3: Iterator/Generator
    elif hasattr(fonte_data, '__iter__'):
        try:
            items = list(fonte_data)
            logger.info(f"[UniversalAdapter] Convertido iterator para lista")
        except Exception as e:
            logger.error(f"[UniversalAdapter] Erro ao converter iterator: {e}")
            return {"phases": []}
    
    else:
        logger.error(f"[UniversalAdapter] Tipo de fonte não suportado: {type(fonte_data)}")
        return {"phases": []}
    
    if not items:
        logger.warning(f"[UniversalAdapter] Nenhum item encontrado na fonte")
        return {"phases": []}
    
    # Normalizar todos os items
    docs = []
    for idx, item in enumerate(items):
        normalized_item = _normalize_item(item, idx)
        if normalized_item:
            docs.append(normalized_item)
    
    if not docs:
        logger.warning(f"[UniversalAdapter] Nenhum documento válido após normalização (de {len(items)} items)")
        return {"phases": []}
    
    logger.info(f"[UniversalAdapter] Normalizados {len(docs)} docs de {len(items)} items ({(len(docs)/len(items)*100):.1f}% aproveitamento)")
    
    return {
        "phases": [
            {
                "name": source_name,
                "docs": docs
            }
        ]
    }

def _docs_to_microchunks(phase_name: str, docs: List[Dict[str, Any]], window: int = 5, min_words: int = 6) -> List[Dict[str, Any]]:
    out = []
    for d in docs:
        if not d.get("text"): 
            continue
        url = d["url"]
        title = d.get("title") or url
        for txt in group_microchunks_from_text(d["text"], window, min_words):
            out.append({
                "text": txt,
                "url": url,
                "title": title,
                "phase": phase_name,
                "published_at": d.get("published_at")
            })
    return out

# ---------- SUB-QUERIES ----------
def gen_subqueries(phase_name: str, sample_texts: List[str], n: int = 3, max_blob_chars: int = 6000) -> List[str]:
    blob = (" ".join(sample_texts)[:max_blob_chars] + " " + phase_name).lower()
    seeds = []
    if "brasil" in blob or "brazil" in blob: 
        seeds.append("brasil")
    if re.search(r"20(1|2)\d", blob): 
        seeds.append("2019..2025")
    base = " ".join(sorted(set(seeds)))
    facets = [
        "demanda", "capacidade", "importacao", "precos tarifas",
        "players mercado", "riscos regulacao", "tendencias 2024 2025"
    ]
    out = []
    for f in facets:
        q = " ".join([f, phase_name, base]).strip()
        out.append(re.sub(r"\s+", " ", q))
        if len(out) >= n: 
            break
    return out

# ---------- SELEÇÃO POR FASE (async) ----------
async def _select_phase_async(
    phase_name: str,
    docs: List[Dict[str, Any]],
    queries: Optional[List[str]],
    mode: str,
    token_budget: int,
    k_per_query: int,
    alpha: float,
    enable_embeddings: bool,
    model_name: str,
    embedding_timeout: int,
    similarity_threshold: float,
    max_per_url_ratio: float,
    token_margin: float,
    chunk_window: int,
    min_sentence_words: int,
    simhash_distance: int,
    fallback_dup_chars: int,
    __event_emitter__: Optional[Callable] = None
) -> Tuple[str, List[Dict[str,Any]], Dict[str,Any]]:
    
    # 0) Microchunks
    if __event_emitter__:
        await __event_emitter__({"type": "status", "data": {"description": f"[{phase_name}] Criando microchunks...", "done": False}})
    
    micro: List[Chunk] = []
    for d in docs:
        if not d.get("text"): 
            continue
        url = d["url"]
        title = d.get("title") or url
        did = url_hash(url)
        for txt in group_microchunks_from_text(d["text"], chunk_window, min_sentence_words):
            micro.append(Chunk(id=f"{did}:{hash(txt)}", phase=phase_name, url=url, title=title, text=txt))

    if not micro:
        return "", [], {"phase": phase_name, "info": "sem microchunks"}

    # 1) BM25
    if __event_emitter__:
        await __event_emitter__({"type": "status", "data": {"description": f"[{phase_name}] Indexando BM25...", "done": False}})
    
    bm25, tokenized = bm25_index([m.text for m in micro])
    texts = [m.text for m in micro]
    
    # 2) Embeddings (cache)
    chunk_emb = None
    if enable_embeddings and _has_st and mode in ("light","ultra"):
        if __event_emitter__:
            await __event_emitter__({"type": "status", "data": {"description": f"[{phase_name}] Gerando embeddings...", "done": False}})
        
        try:
            model = _load_st(model_name)
            if model:
                chunk_emb = await asyncio.wait_for(
                    asyncio.to_thread(
                        lambda: model.encode(texts, normalize_embeddings=True, convert_to_tensor=True)
                    ),
                    timeout=embedding_timeout
                )
        except asyncio.TimeoutError:
            logger.warning(f"[{phase_name}] Timeout em embeddings ({embedding_timeout}s)")
            chunk_emb = None
        except Exception as e:
            logger.error(f"[{phase_name}] Erro em embeddings: {type(e).__name__}")
            chunk_emb = None

    # 3) Queries
    if not queries:
        sample = [docs[i]["text"][:4000] for i in range(min(3, len(docs))) if docs[i].get("text")]
        n_q = 3 if mode=="light" else 2
        queries = gen_subqueries(phase_name, sample, n=n_q)

    # 4) Ranking
    if __event_emitter__:
        await __event_emitter__({"type": "status", "data": {"description": f"[{phase_name}] Ranqueando ({len(queries)} queries)...", "done": False}})
    
    cand: List[Chunk] = []
    for q_idx, q in enumerate(queries):
        bm = bm25_scores(bm25, tokenized, q)
        
        if enable_embeddings and _has_st and mode in ("light","ultra") and chunk_emb is not None:
            try:
                model = _load_st(model_name)
                q_emb_result = await asyncio.wait_for(
                    asyncio.to_thread(
                        lambda: model.encode([q], normalize_embeddings=True, convert_to_tensor=True)
                    ),
                    timeout=10
                )
                import numpy as np
                em = util.cos_sim(q_emb_result, chunk_emb).cpu().numpy()[0]
                em = ((em + 1.0)/2.0).tolist()
            except Exception as e:
                logger.warning(f"[{phase_name}] Erro em query embedding {q_idx+1}: {type(e).__name__}")
                em = [0.0]*len(micro)
        else:
            em = [0.0]*len(micro)
        
        bmn = normalize(bm)
        scored = []
        for i,m in enumerate(micro):
            s = alpha*bmn[i] + (1-alpha)*em[i]
            c = Chunk(**m.__dict__)
            c.score = s
            c.bm25 = bm[i]
            c.emb = em[i]
            scored.append(c)
        
        scored.sort(key=lambda x: x.score, reverse=True)
        top_k = int(k_per_query * (1.5 if mode=="coarse" else 2.0 if mode=="ultra" else 1.0))
        cand.extend(scored[:top_k])

    # 5) Dedup + Diversidade
    if __event_emitter__:
        await __event_emitter__({"type": "status", "data": {"description": f"[{phase_name}] Dedupando...", "done": False}})
    
    # Threshold adaptativo por modo
    thresh = _get_threshold_for_mode(mode, similarity_threshold)
    logger.debug(f"[{phase_name}] Threshold dedup: {thresh:.2f} (modo: {mode}, base: {similarity_threshold:.2f})")
    
    # Dedup mantendo melhor score
    cand = dedup_chunks_best_score(cand, thresh, simhash_distance, fallback_dup_chars)
    cand.sort(key=lambda x: x.score, reverse=True)

    # Cap por URL + tokens
    per_url_tokens = defaultdict(int)
    limit_tokens = token_budget
    selected, total = [], 0
    max_one_url = int(limit_tokens * max_per_url_ratio)
    
    for c in cand:
        if per_url_tokens[c.url] >= max_one_url:
            continue
        t = est_tokens(c.text)
        if total + t > int(limit_tokens * token_margin):
            continue
        selected.append(c)
        per_url_tokens[c.url] += t
        total += t
        if total >= limit_tokens:
            break

    # 6) Markdown
    lines, sources = [], []
    for i,c in enumerate(selected, start=1):
        tag = f"[{phase_name} - {i}]"
        lines.append(f"{tag} {c.title} — {c.url}\n{c.text}\n")
        sources.append({"id": tag, "url": c.url, "title": c.title})
    
    md = "\n---\n".join(lines)

    if __event_emitter__:
        await __event_emitter__({"type": "status", "data": {
            "description": f"[{phase_name}] ✅ {len(selected)} chunks ({total} tokens)",
            "done": True
        }})

    return md, sources, {
        "phase": phase_name,
        "queries": queries,
        "microchunks_total": len(micro),
        "candidatos": len(cand),
        "selecionados": len(selected),
        "tokens": total,
        "mode": mode
    }

# ---------- OPENWEBUI TOOL CLASS ----------
class Tools:
    class Valves(BaseModel):
        # Chunking
        chunk_window: int = Field(default=5, ge=3, le=10, description="Janela de microchunks (sentenças)")
        min_sentence_words: int = Field(default=6, ge=3, le=15, description="Mínimo de palavras por sentença")
        
        # Ranqueamento
        alpha_light: float = Field(default=0.55, ge=0.0, le=1.0, description="Alpha para modo light (BM25 weight)")
        alpha_coarse: float = Field(default=1.0, ge=0.0, le=1.0, description="Alpha para modo coarse (BM25 weight)")
        
        # Deduplicação
        similarity_threshold: float = Field(default=0.92, ge=0.5, le=1.0, description="Threshold de similaridade")
        simhash_distance: int = Field(default=3, ge=1, le=10, description="Distância máxima SimHash")
        fallback_dup_chars: int = Field(default=200, ge=50, le=500, description="Chars para fallback dedup")
        
        # Diversificação
        max_per_url_ratio: float = Field(default=0.5, ge=0.1, le=1.0, description="Máximo % do orçamento por URL")
        token_margin: float = Field(default=1.15, ge=1.0, le=2.0, description="Margem de tokens")
        
        # Orçamento
        token_budget_light: int = Field(default=9000, ge=3000, le=30000, description="Budget para modo light")
        token_budget_ultra: int = Field(default=15000, ge=5000, le=50000, description="Budget para modo ultra")
        token_budget_coarse: int = Field(default=60000, ge=10000, le=100000, description="Budget para modo coarse")
        
        # Embeddings
        embedding_model: str = Field(default="intfloat/e5-small-v2", description="Modelo de embeddings")
        embedding_timeout: int = Field(default=60, ge=10, le=300, description="Timeout embeddings (s)")
        
        # Input validation
        max_corpus_size_mb: int = Field(default=100, ge=10, le=500, description="Tamanho máx corpus (MB)")
        max_phases: int = Field(default=10, ge=1, le=20, description="Máximo de fases")
    
    def __init__(self):
        self.valves = self.Valves()
        self.citation = False
    
    def _normalize_scraper_output(self, scraper_result: Union[dict, str]) -> Dict[str, Any]:
        """
        Normaliza saída do scraper para formato do reducer
        
        INPUT (scraper):
            {"results": [
                {
                    "url": "https://...",
                    "content": "texto limpo extraído",
                    "title": "Título",
                    "author": "Autor",
                    "published_at": "2024-01-15",
                    "word_count": 1500,
                    "success": True,
                    "adapter_used": "HttpxAdapterV5",
                    "quality_score": 8.5
                },
                ...
            ]}
        
        OUTPUT (reducer):
            {
                "phases": [
                    {
                        "name": "scraper_batch_1",
                        "docs": [
                            {
                                "text": "conteúdo extraído",
                                "url": "https://...",
                                "title": "Título",
                                "published_at": "2024-01-15",
                                "author": "Autor",
                                "word_count": 1500,
                                "quality_score": 8.5
                            },
                            ...
                        ]
                    }
                ]
            }
        """
        # Parse JSON se vier como string
        if isinstance(scraper_result, str):
            try:
                scraper_result = json.loads(scraper_result)
            except json.JSONDecodeError as e:
                logger.error(f"[Reducer] Erro ao parsear JSON: {e}")
                return {"phases": []}
        
        if not isinstance(scraper_result, dict):
            return {"phases": []}
        
        # Detectar formato: single result vs batch
        if "results" in scraper_result:
            results = scraper_result["results"]
        else:
            results = [scraper_result]
        
        # Converter + validar qualidade
        docs = []
        for result in results:
            if not isinstance(result, dict):
                continue
            
            # FILTRO 1: Só aceitar scrapes bem-sucedidos
            if not result.get("success", False):
                logger.warning(f"[Reducer] Skipping failed scrape: {result.get('url')} - {result.get('error')}")
                continue
            
            # FILTRO 2: Validar conteúdo mínimo
            content = result.get("content", "")
            word_count = result.get("word_count", 0)
            
            if not content or len(content.strip()) < 100:
                logger.warning(f"[Reducer] Skipping short content: {result.get('url')} ({len(content)} chars)")
                continue
            
            if word_count < 100:
                logger.warning(f"[Reducer] Skipping low word count: {result.get('url')} ({word_count} words)")
                continue
            
            # NORMALIZAR: Mapear campos scraper → reducer
            doc = {
                "text": content,
                "url": result.get("url", ""),
                "title": result.get("title", result.get("url", "")),
                "published_at": result.get("published_at"),
                "author": result.get("author"),
                "word_count": word_count,
                # Metadados úteis para ranking
                "adapter_used": result.get("adapter_used"),
                "quality_score": result.get("quality_score"),
                "extraction_time": result.get("extraction_time")
            }
            docs.append(doc)
        
        if not docs:
            logger.warning("[Reducer] Nenhum documento válido após normalização")
            return {"phases": []}
        
        logger.info(f"[Reducer] Normalizados {len(docs)} docs de {len(results)} scrapes")
        
        return {
            "phases": [
                {
                    "name": "scraper_results",
                    "docs": docs
                }
            ]
        }
    
    async def reduce_context(
        self,
        corpo: dict = None,
        scraper_output: Optional[Union[dict, str]] = None,
        custom_source: Optional[Any] = None,
        custom_source_name: str = "custom_source",
        custom_text_fields: Optional[List[str]] = None,
        custom_url_fields: Optional[List[str]] = None,
        custom_title_fields: Optional[List[str]] = None,
        mode: str = "light",
        queries: Optional[list] = None,
        tipo: str = "fase",
        job_id: Optional[str] = None,
        __event_emitter__: Optional[Callable] = None
    ) -> str:
        """
        Reduz contexto de scraper/múltiplas fontes mantendo relevância e diversidade
        
        Args:
            corpo: Dict legado com docs/phases (compatibilidade)
            scraper_output: Saída DIRETA do scraper (prioridade 1)
            custom_source: Fonte customizada (API, DB, arquivo) - usa adaptador universal (prioridade 2)
            custom_source_name: Nome da fonte customizada (padrão: "custom_source")
            custom_text_fields: Campos possíveis para texto (None = auto-detectar)
            custom_url_fields: Campos possíveis para URL (None = auto-detectar)
            custom_title_fields: Campos possíveis para título (None = auto-detectar)
            mode: "coarse" | "light" | "ultra"
            queries: Lista de queries (todas as fases)
            tipo: "fase" (acumula) | "global" (finaliza)
            job_id: ID do job para acumulação
            
        Returns:
            JSON string com final_markdown, sources, metrics
            
        Examples:
            # Scraper (nativo)
            await reducer.reduce_context(scraper_output=scraper_result)
            
            # API REST
            api_data = requests.get("...").json()
            await reducer.reduce_context(custom_source=api_data, custom_source_name="news_api")
            
            # Database
            cur.execute("SELECT * FROM articles")
            rows = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]
            await reducer.reduce_context(custom_source=rows, custom_source_name="database")
            
            # JSON file
            with open("data.json") as f:
                data = json.load(f)
            await reducer.reduce_context(custom_source=data, custom_source_name="json_file")
        """
        try:
            if __event_emitter__:
                await __event_emitter__({"type": "status", "data": {"description": "🔄 Iniciando redução de contexto...", "done": False}})
            
            # PRIORIDADE 1: Se scraper_output fornecido, normalizar
            if scraper_output:
                if __event_emitter__:
                    await __event_emitter__({"type": "status", "data": {"description": "📥 Normalizando saída do scraper...", "done": False}})
                
                corpo_normalizado = self._normalize_scraper_output(scraper_output)
                
                if not corpo_normalizado.get("phases"):
                    return json.dumps({
                        "error": "Nenhum conteúdo válido após normalização"
                    }, ensure_ascii=False)
                
                # Substituir corpo pelo normalizado
                corpo = corpo_normalizado
            
            # PRIORIDADE 2: Se custom_source fornecido, usar adaptador universal
            elif custom_source is not None:
                if __event_emitter__:
                    await __event_emitter__({"type": "status", "data": {"description": f"🔌 Normalizando fonte customizada ({custom_source_name})...", "done": False}})
                
                corpo_normalizado = normalize_custom_source(
                    fonte_data=custom_source,
                    source_name=custom_source_name,
                    text_fields=custom_text_fields,
                    url_fields=custom_url_fields,
                    title_fields=custom_title_fields
                )
                
                if not corpo_normalizado.get("phases"):
                    return json.dumps({
                        "error": "Nenhum conteúdo válido após normalização da fonte customizada"
                    }, ensure_ascii=False)
                
                # Substituir corpo pelo normalizado
                corpo = corpo_normalizado
            
            # Validação de input
            if not corpo:
                return json.dumps({"error": "corpo vazio"}, ensure_ascii=False)
            
            corpus_size = len(json.dumps(corpo).encode('utf-8')) / (1024 * 1024)
            if corpus_size > self.valves.max_corpus_size_mb:
                error_msg = f"Corpus muito grande: {corpus_size:.1f}MB (máx: {self.valves.max_corpus_size_mb}MB)"
                if __event_emitter__:
                    await __event_emitter__({"type": "status", "data": {"description": f"❌ {error_msg}", "done": True}})
                return json.dumps({"error": error_msg}, ensure_ascii=False)
            
            # Normalizar mode
            if mode == "heavy":
                mode = "coarse"
            
            # Config por modo
            enable_embeddings = mode != "coarse"
            
            if mode == "coarse":
                token_budget = self.valves.token_budget_coarse
                k_per_query = 5
                alpha = self.valves.alpha_coarse
            elif mode == "ultra":
                token_budget = self.valves.token_budget_ultra
                k_per_query = 10
                alpha = self.valves.alpha_light
            else:  # light
                token_budget = self.valves.token_budget_light
                k_per_query = 7
                alpha = self.valves.alpha_light
            
            # Processar fases
            phase_list: List[Dict[str,Any]] = []
            
            if "phases" in corpo and len(corpo["phases"]) > 0:
                phase_list = corpo["phases"][:self.valves.max_phases]
            elif "docs" in corpo and "phase_name" in corpo:
                phase_list = [{"name": corpo["phase_name"], "docs": corpo["docs"]}]
            # ✅ NOVO (v2.3.1): Suporte a formato legado ultrasearcher_result (compatibilidade Pipe v4.5)
            elif "ultrasearcher_result" in corpo:
                if __event_emitter__:
                    await __event_emitter__({"type": "status", "data": {
                        "description": "📥 Convertendo formato legado ultrasearcher_result...", 
                        "done": False
                    }})
                
                scraped_content = corpo["ultrasearcher_result"].get("scraped_content", [])
                
                if not scraped_content:
                    return json.dumps({
                        "error": "ultrasearcher_result sem scraped_content"
                    }, ensure_ascii=False)
                
                # Converter para formato scraper moderno e normalizar
                scraper_format = {"results": scraped_content}
                corpo_normalizado = self._normalize_scraper_output(scraper_format)
                
                if not corpo_normalizado.get("phases"):
                    return json.dumps({
                        "error": "Nenhum conteúdo válido em ultrasearcher_result"
                    }, ensure_ascii=False)
                
                phase_list = corpo_normalizado["phases"][:self.valves.max_phases]
                
                logger.info(f"[Reducer] Formato legado ultrasearcher convertido: {len(phase_list)} phases, {sum(len(p.get('docs', [])) for p in phase_list)} docs")
            else:
                return json.dumps({"error": "corpo inválido (faltam 'phases' ou 'docs'+'phase_name' ou 'ultrasearcher_result')"}, ensure_ascii=False)
            
            # DEDUP LEVE: Antes de processar fases (economiza 50-80% embeddings)
            if __event_emitter__:
                await __event_emitter__({"type": "status", "data": {"description": "🧹 Removendo duplicatas...", "done": False}})
            
            for phase in phase_list:
                original_count = len(phase.get("docs", []))
                phase["docs"] = _dedup_leve_pre_embed(phase["docs"])
                deduped_count = len(phase["docs"])
                
                if original_count > deduped_count:
                    reduction_pct = ((original_count-deduped_count)/original_count*100)
                    logger.info(f"[{phase['name']}] Dedup pré-embed: {original_count} → {deduped_count} docs ({reduction_pct:.1f}% reduzido)")
            
            # Processar cada fase
            sections, per_phase_out, all_sources, metrics_ph = [], [], [], []
            per_budget = token_budget // max(1, len(phase_list))
            
            for idx, ph in enumerate(phase_list, 1):
                name = ph["name"]
                
                if __event_emitter__:
                    await __event_emitter__({"type": "status", "data": {"description": f"📊 Fase {idx}/{len(phase_list)}: {name}", "done": False}})
                
                md, src, m = await _select_phase_async(
                    phase_name=name,
                    docs=ph["docs"],
                    queries=queries,
                    mode=mode,
                    token_budget=per_budget,
                    k_per_query=k_per_query,
                    alpha=alpha,
                    enable_embeddings=enable_embeddings,
                    model_name=self.valves.embedding_model,
                    embedding_timeout=self.valves.embedding_timeout,
                    similarity_threshold=self.valves.similarity_threshold,
                    max_per_url_ratio=self.valves.max_per_url_ratio,
                    token_margin=self.valves.token_margin,
                    chunk_window=self.valves.chunk_window,
                    min_sentence_words=self.valves.min_sentence_words,
                    simhash_distance=self.valves.simhash_distance,
                    fallback_dup_chars=self.valves.fallback_dup_chars,
                    __event_emitter__=__event_emitter
                )
                
                sections.append(f"# {name}\n\n{md}" if md else f"# {name}\n\n(Nenhum trecho selecionado)\n")
                per_phase_out.append({"name": name, "context_markdown": md, "sources": src, "metrics": m})
                all_sources += src
                metrics_ph.append(m)
            
            final_md = "\n\n\n".join(sections)
            
            if __event_emitter__:
                total_tokens = est_tokens(final_md)
                reduction_pct = ((token_budget - total_tokens) / token_budget * 100) if token_budget > 0 else 0
                await __event_emitter__({"type": "status", "data": {
                    "description": f"✅ Contexto reduzido: {total_tokens} tokens (redução: {reduction_pct:.1f}%)",
                    "done": True
                }})
            
            result = {
                "final_markdown": final_md,
                "per_phase": per_phase_out,
                "sources": all_sources,
                "metrics": {
                    "final_tokens_est": est_tokens(final_md),
                    "phases": metrics_ph,
                    "mode": mode,
                    "job_id": job_id,
                    "tipo": tipo
                }
            }
            
            return json.dumps(result, ensure_ascii=False)
            
        except asyncio.TimeoutError as e:
            error_msg = f"Timeout ao processar contexto: {str(e)}"
            logger.error(f"[ContextReducer] {error_msg}")
            if __event_emitter__:
                await __event_emitter__({"type": "status", "data": {"description": f"⏱️ {error_msg}", "done": True}})
            return json.dumps({"error": error_msg}, ensure_ascii=False)
            
        except MemoryError as e:
            error_msg = f"Memória insuficiente: {str(e)}"
            logger.error(f"[ContextReducer] {error_msg}")
            if __event_emitter__:
                await __event_emitter__({"type": "status", "data": {"description": f"💾 {error_msg}", "done": True}})
            return json.dumps({"error": error_msg}, ensure_ascii=False)
            
        except Exception as e:
            error_msg = f"Erro ao reduzir contexto: {type(e).__name__} - {str(e)}"
            logger.error(f"[ContextReducer] {error_msg}")
            if __event_emitter__:
                await __event_emitter__({"type": "status", "data": {"description": f"❌ {error_msg}", "done": True}})
            return json.dumps({"error": error_msg}, ensure_ascii=False)

