# tool_reduce_context_from_scraper.py
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict
import re, hashlib, os, json
from pydantic import BaseModel, Field

# ---------- CONFIGURAÇÃO COM VALVES ----------
class ReduceContextValves(BaseModel):
    """Valves configuráveis para redução de contexto"""
    
    # Configurações de chunking
    chunk_window: int = Field(default=5, description="Tamanho da janela de microchunks (sentenças)")
    min_sentence_words: int = Field(default=6, description="Mínimo de palavras por sentença")
    token_estimation_factor: float = Field(default=0.75, description="Fator de estimação de tokens")
    
    # Configurações de ranqueamento
    bm25_weight: float = Field(default=0.6, description="Peso do BM25 no ranqueamento híbrido")
    embedding_weight: float = Field(default=0.4, description="Peso dos embeddings no ranqueamento híbrido")
    alpha_light: float = Field(default=0.55, description="Alpha para modo light")
    alpha_coarse: float = Field(default=1.0, description="Alpha para modo coarse")
    
    # Configurações de deduplicação
    similarity_threshold: float = Field(default=0.92, description="Threshold de similaridade para deduplicação")
    simhash_distance: int = Field(default=3, description="Distância máxima do SimHash")
    fallback_dup_chars: int = Field(default=200, description="Chars para fallback de deduplicação")
    
    # Configurações de diversificação
    max_per_url_ratio: float = Field(default=0.5, description="Máximo % do orçamento por URL")
    token_margin: float = Field(default=1.15, description="Margem de tokens (15%)")
    
    # Configurações de orçamento
    min_budget_per_phase: int = Field(default=3000, description="Orçamento mínimo por fase")
    coarse_budget_multiplier: int = Field(default=60000, description="Multiplicador de orçamento para modo coarse")
    
    # Configurações de queries
    max_sample_chars: int = Field(default=4000, description="Máximo de chars por amostra")
    max_sample_docs: int = Field(default=3, description="Máximo de docs para amostra")
    light_queries_count: int = Field(default=3, description="Número de queries para modo light")
    coarse_queries_count: int = Field(default=2, description="Número de queries para modo coarse")
    max_blob_chars: int = Field(default=6000, description="Máximo de chars no blob de queries")
    
    # Configurações de candidatos
    ultra_multiplier: float = Field(default=2.0, description="Multiplicador de candidatos para modo ultra")
    ultra_extra: int = Field(default=10, description="Candidatos extras para modo ultra")
    coarse_multiplier: float = Field(default=1.5, description="Multiplicador de candidatos para modo coarse")
    
    # Configurações de modelo
    embedding_model: str = Field(default="intfloat/e5-small-v2", description="Modelo de embeddings")
    
    # Configurações de hash
    url_hash_length: int = Field(default=10, description="Comprimento do hash de URL")

# Instância global das valves
_default_valves = ReduceContextValves()

# ======= STATE: memória/Redis para índice efêmero por job =======
_JOBS_STORE: Dict[str, List[Dict[str, Any]]] = {}  # {job_id: [ {text,url,title,phase,published_at} ] }

_redis = None
def _get_redis():
    global _redis
    if _redis is not None:
        return _redis
    try:
        import redis
        url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        _redis = redis.from_url(url)
        return _redis
    except Exception:
        return None

def _key(job_id: str) -> str:
    return f"dr:{job_id}:microchunks"

def _store_append(job_id: str, items: List[Dict[str, Any]], use_redis: bool):
    if use_redis and _get_redis():
        r = _get_redis()
        # armazena como JSON lines
        pipe = r.pipeline()
        for it in items:
            pipe.rpush(_key(job_id), json.dumps(it, ensure_ascii=False))
        pipe.execute()
    else:
        _JOBS_STORE.setdefault(job_id, []).extend(items)

def _store_get_all(job_id: str, use_redis: bool) -> List[Dict[str, Any]]:
    if use_redis and _get_redis():
        r = _get_redis()
        vals = r.lrange(_key(job_id), 0, -1)
        return [json.loads(v) for v in vals]
    else:
        return list(_JOBS_STORE.get(job_id, []))

def _store_clear(job_id: str, use_redis: bool):
    if use_redis and _get_redis():
        _get_redis().delete(_key(job_id))
    _JOBS_STORE.pop(job_id, None)

# Pequena ajuda: criar micro-chunks com metadados para acumular
def _docs_to_microchunks_for_accumulation(phase_name: str, docs: List[Dict[str, Any]], valves: ReduceContextValves = None) -> List[Dict[str, Any]]:
    valves = valves or _default_valves
    out = []
    for d in docs:
        if not d.get("text"): 
            continue
        url = d["url"]; title = d.get("title") or url
        for txt in group_microchunks_from_text(d["text"], valves=valves):
            out.append({
                "text": txt,
                "url": url,
                "title": title,
                "phase": phase_name,
                "published_at": d.get("published_at")
            })
    return out
# ======= FIM STATE =======

# ---------- CONFIGURAÇÕES POR MODO ----------
def _get_token_budget_for_mode(mode: str, valves: ReduceContextValves) -> int:
    """Retorna orçamento de tokens baseado no modo"""
    if mode == "coarse":
        return valves.coarse_budget_multiplier
    elif mode == "light":
        return 9000  # Padrão para light
    elif mode == "ultra":
        return 15000  # Maior orçamento para ultra
    return 9000

def _get_k_per_query_for_mode(mode: str, valves: ReduceContextValves) -> int:
    """Retorna k_per_query baseado no modo"""
    if mode == "coarse":
        return 5
    elif mode == "light":
        return 7
    elif mode == "ultra":
        return 10
    return 7

def _get_max_per_url_for_mode(mode: str, valves: ReduceContextValves) -> int:
    """Retorna max_per_url baseado no modo"""
    if mode == "coarse":
        return 3
    elif mode == "light":
        return 2
    elif mode == "ultra":
        return 1  # Mais diversidade
    return 2

# --- deps opcionais ---
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


# ---------- util ----------
def est_tokens(text: str, valves: ReduceContextValves = None) -> int:
    valves = valves or _default_valves
    return int(len((text or "").split()) / valves.token_estimation_factor)

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def split_sentences(text: str, valves: ReduceContextValves = None) -> List[str]:
    valves = valves or _default_valves
    sents = re.split(r"(?<=[\.\?!])\s+", text or "")
    return [clean_text(s) for s in sents if len(s.split()) >= valves.min_sentence_words]

def group_microchunks_from_text(text: str, window: int = None, valves: ReduceContextValves = None) -> List[str]:
    valves = valves or _default_valves
    window = window or valves.chunk_window
    sents = split_sentences(text, valves)
    out, acc = [], []
    for s in sents:
        acc.append(s)
        if len(acc) >= window:
            out.append(clean_text(" ".join(acc)))
            acc = []
    if acc:
        out.append(clean_text(" ".join(acc)))
    return out

def url_hash(url: str, valves: ReduceContextValves = None) -> str:
    valves = valves or _default_valves
    return hashlib.md5((url or "").encode("utf-8")).hexdigest()[:valves.url_hash_length]

def normalize(arr: List[float]) -> List[float]:
    if not arr: return []
    lo, hi = min(arr), max(arr)
    if hi - lo < 1e-9:
        return [0.0 for _ in arr]
    return [(x - lo) / (hi - lo) for x in arr]

# ---------- estruturas ----------
@dataclass
class Doc:
    id: str
    url: str
    title: str
    text: str
    published_at: str | None = None

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

# ---------- BM25 ----------
def bm25_index(texts: List[str]):
    if not _has_bm25:
        return None, [t.split() for t in texts]
    tokens = [t.split() for t in texts]
    return BM25Okapi(tokens), tokens

def bm25_scores(bm25, tokenized, query: str) -> List[float]:
    if _has_bm25 and bm25 is not None:
        return bm25.get_scores(query.split())
    # fallback lexical tosco: contagem de termos
    q = set(query.lower().split())
    return [sum(1 for w in toks if w.lower() in q) for toks in tokenized]

# ---------- Embeddings ----------
_ST = None
def _load_st(valves: ReduceContextValves = None):
    global _ST
    valves = valves or _default_valves
    if not _has_st: return None
    if _ST is None:
        _ST = SentenceTransformer(valves.embedding_model)
    return _ST

def emb_scores(query: str, texts: List[str], valves: ReduceContextValves = None) -> List[float]:
    valves = valves or _default_valves
    if not _has_st:
        return [0.0]*len(texts)
    model = _load_st(valves)
    q_emb = model.encode([query], normalize_embeddings=True, convert_to_tensor=True)
    t_emb = model.encode(texts, normalize_embeddings=True, convert_to_tensor=True)
    import numpy as np
    cos = util.cos_sim(q_emb, t_emb).cpu().numpy()[0]
    return ((cos + 1.0)/2.0).tolist()  # [0..1]

# ---------- dedup & diversidade ----------
def _is_dup(a: str, b: str, thresh: float = None, valves: ReduceContextValves = None) -> bool:
    valves = valves or _default_valves
    thresh = thresh or valves.similarity_threshold
    if _has_fuzz:
        try:
            return (fuzz.token_set_ratio(a, b)/100.0) >= thresh
        except Exception:
            pass
    if _has_simhash:
        try:
            return Simhash(a).distance(Simhash(b)) <= valves.simhash_distance
        except Exception:
            pass
    return a[:valves.fallback_dup_chars] == b[:valves.fallback_dup_chars]

def dedup_chunks(chs: List[Chunk], thresh: float = None, valves: ReduceContextValves = None) -> List[Chunk]:
    valves = valves or _default_valves
    thresh = thresh or valves.similarity_threshold
    out: List[Chunk] = []
    for c in chs:
        if any(_is_dup(c.text, u.text, thresh, valves) for u in out):
            continue
        out.append(c)
    return out

def diversify_by_url(chs: List[Chunk], max_per_url: int, limit_tokens: int, valves: ReduceContextValves = None) -> Tuple[List[Chunk], int]:
    valves = valves or _default_valves
    per_url = defaultdict(int)
    total = 0
    selected = []
    for c in chs:
        if per_url[c.url] >= max_per_url:
            continue
        t = est_tokens(c.text, valves)
        if total + t > int(limit_tokens * valves.token_margin):
            continue
        selected.append(c)
        per_url[c.url] += 1
        total += t
        if total >= limit_tokens:
            break
    return selected, total

# ---------- sub-queries ----------
def gen_subqueries(phase_name: str, sample_texts: List[str], n: int = 3, valves: ReduceContextValves = None) -> List[str]:
    valves = valves or _default_valves
    blob = (" ".join(sample_texts)[:valves.max_blob_chars] + " " + phase_name).lower()
    seeds = []
    if "brasil" in blob or "brazil" in blob: seeds.append("brasil")
    if re.search(r"20(1|2)\d", blob): seeds.append("2019..2025")
    base = " ".join(sorted(set(seeds)))
    facets = [
        "demanda", "capacidade", "importacao", "precos tarifas",
        "players mercado", "riscos regulacao", "tendencias 2024 2025"
    ]
    out = []
    for f in facets:
        q = " ".join([f, phase_name, base]).strip()
        out.append(re.sub(r"\s+", " ", q))
        if len(out) >= n: break
    return out

# ---------- seleção por fase ----------
def _select_phase(phase_name: str,
                  docs: List[Dict[str, Any]],
                  queries: List[str] | None,
                  mode: str = "light",
                  token_budget_phase: int = 9000,
                  k_per_query: int = 7,
                  max_per_url: int = 2,
                  enable_embeddings: bool = True,
                  valves: ReduceContextValves = None) -> Tuple[str, List[Dict[str,Any]], Dict[str,Any]]:
    valves = valves or _default_valves
    # 0) transforma docs e microchunks
    micro: List[Chunk] = []
    for d in docs:
        if not d.get("text"): 
            continue
        url = d["url"]; title = d.get("title") or url
        did = url_hash(url, valves)
        for txt in group_microchunks_from_text(d["text"], valves=valves):
            micro.append(Chunk(id=f"{did}:{hash(txt)}", phase=phase_name, url=url, title=title, text=txt))

    if not micro:
        return "", [], {"phase": phase_name, "info": "sem microchunks"}

    # 1) índice BM25
    bm25, tokenized = bm25_index([m.text for m in micro])
    texts = [m.text for m in micro]
    # cache de embeddings dos micro-chunks (uma vez por fase)
    chunk_emb = None
    if enable_embeddings and _has_st and mode in ("light","ultra"):
        try:
            model = _load_st()
            chunk_emb = model.encode(texts, normalize_embeddings=True, convert_to_tensor=True)
        except Exception:
            chunk_emb = None

    # 2) queries
    if not queries or len(queries) == 0:
        ms = valves.max_sample_docs
        mc = valves.max_sample_chars
        sample = [docs[i]["text"][:mc] for i in range(min(ms, len(docs))) if docs[i].get("text")]
        n_q = valves.light_queries_count if mode=="light" else valves.coarse_queries_count
        queries = gen_subqueries(phase_name, sample, n=n_q, valves=valves)

    # 3) ranqueamento por sub-query
    if enable_embeddings and _has_st and mode in ("light","ultra"):
        alpha = valves.alpha_light
    else:
        alpha = valves.alpha_coarse
    cand: List[Chunk] = []
    for q in queries:
        bm = bm25_scores(bm25, tokenized, q)
        if enable_embeddings and _has_st and mode in ("light","ultra") and chunk_emb is not None:
            model = _load_st()
            q_emb = model.encode([q], normalize_embeddings=True, convert_to_tensor=True)
            import numpy as np
            em = util.cos_sim(q_emb, chunk_emb).cpu().numpy()[0]
            em = ((em + 1.0)/2.0).tolist()
        else:
            em = [0.0]*len(micro)
        bmn = normalize(bm)
        scored = []
        for i,m in enumerate(micro):
            s = alpha*bmn[i] + (1-alpha)*em[i]
            c = Chunk(**m.__dict__)
            c.score = s; c.bm25 = bm[i]; c.emb = em[i]
            scored.append(c)
        scored.sort(key=lambda x: x.score, reverse=True)
        top_k = (k_per_query if mode=="light" else int(k_per_query*valves.coarse_multiplier))
        # ultra: segura mais candidatos para rerank a seguir
        if mode == "ultra":
            top_k = max(int(top_k*valves.ultra_multiplier), top_k+valves.ultra_extra)
        cand.extend(scored[:top_k])

    # 4) consolidar, dedup, diversidade e orçamento
    cand = dedup_chunks(cand, thresh=valves.similarity_threshold, valves=valves)
    
    # (opcional) bônus de recência se houver published_at e consulta temporal
    def _fresh_bonus(c: Chunk) -> float:
        return 0.0  # mantenha 0 por padrão; se quiser, implemente parsing de data e exponencial
    cand.sort(key=lambda x: x.score + _fresh_bonus(x), reverse=True)

    # cap por URL também por tokens (evita URL gigante dominar)
    per_url_tokens = defaultdict(int)
    limit_tokens = (token_budget_phase if mode=="light" else max(token_budget_phase, valves.coarse_budget_multiplier))
    selected, total = [], 0
    for c in cand:
        max_url_tokens = int(limit_tokens * valves.max_per_url_ratio)
        if per_url_tokens[c.url] >= max_url_tokens:
            continue
        t = est_tokens(c.text, valves)
        if total + t > int(limit_tokens * valves.token_margin):
            continue
        selected.append(c)
        per_url_tokens[c.url] += t
        total += t
        if total >= limit_tokens:
            break
    sel, tokens = selected, total

    # 5) montar markdown e fontes
    lines, sources = [], []
    for i,c in enumerate(sel, start=1):
        tag = f"[{phase_name} - {i}]"
        lines.append(f"{tag} {c.title} — {c.url}\n{c.text}\n")
        sources.append({"id": tag, "url": c.url, "title": c.title})
    md = "\n---\n".join(lines)

    return md, sources, {
        "phase": phase_name,
        "queries": queries,
        "microchunks_total": len(micro),
        "candidatos": len(cand),
        "selecionados": len(sel),
        "tokens": tokens,
        "mode": mode
    }

# ---------- CONVERSÃO ULTRA SEARCHER ----------
def _convert_ultrasearcher_result(ultrasearcher_result: Dict[str,Any]) -> List[Dict[str,Any]]:
    """
    Converte resultado do UltraSearcher para formato esperado pela tool
    
    Args:
        ultrasearcher_result: Resultado do UltraSearcher com scraped_content
        
    Returns:
        Lista de documentos no formato esperado
    """
    docs = []
    
    # Extrair scraped_content
    scraped_content = ultrasearcher_result.get("scraped_content", [])
    
    for item in scraped_content:
        if isinstance(item, dict) and item.get("content"):
            doc = {
                "text": item.get("content", ""),
                "url": item.get("url", ""),
                "title": item.get("title", ""),
                "published_at": item.get("published_at"),
                "author": item.get("author"),
                "language": item.get("language"),
                "word_count": item.get("word_count", 0)
            }
            docs.append(doc)
    
    return docs

# ---------- SELEÇÃO SOBRE CORPUS GLOBAL ----------
def _select_over_corpus(corpus: List[Dict[str, Any]],
                        label: str,
                        queries: List[str] | None,
                        mode: str,
                        token_budget: int,
                        k_per_query: int,
                        max_per_url: int,
                        enable_embeddings: bool,
                        valves: ReduceContextValves = None) -> Tuple[str, List[Dict[str,Any]], Dict[str,Any]]:
    """
    Seleciona trechos relevantes sobre um CORPUS (lista de micro-chunks acumulados).
    'label' vira o prefixo das citações (ex.: 'GLOBAL' ou o nome da fase).
    """
    valves = valves or _default_valves
    
    # 1) transcreve p/ objetos Chunk
    micro: List[Chunk] = []
    for it in corpus:
        url = it["url"]; title = it.get("title") or url
        micro.append(Chunk(
            id=f"{url_hash(url, valves)}:{hash(it['text'])}",
            phase=it.get("phase") or label,
            url=url, title=title, text=clean_text(it["text"])
        ))
    if not micro:
        return "", [], {"label": label, "info": "corpus vazio"}

    # 2) BM25 + (opcional) embeddings em toda a lista micro
    bm25, tokenized = bm25_index([m.text for m in micro])
    texts = [m.text for m in micro]
    chunk_emb = None
    if enable_embeddings and _has_st and mode in ("light","ultra"):
        try:
            model = _load_st(valves)
            chunk_emb = model.encode(texts, normalize_embeddings=True, convert_to_tensor=True)
        except Exception:
            chunk_emb = None

    # 3) queries (auto se não vier)
    if not queries or len(queries)==0:
        # gera queries do próprio corpus (amostra simples)
        sample_texts = [m.text[:valves.max_sample_chars] for m in micro[: min(60, len(micro)) : max(1, len(micro)//20)]]  # amostra espaçada
        n_q = valves.light_queries_count if mode=="light" else valves.coarse_queries_count
        queries = gen_subqueries(label, sample_texts, n=n_q, valves=valves)

    # 4) ranking
    if enable_embeddings and _has_st and mode in ("light","ultra"):
        alpha = valves.alpha_light
    else:
        alpha = valves.alpha_coarse
    
    cand: List[Chunk] = []
    for q in queries:
        bm = bm25_scores(bm25, tokenized, q)
        if enable_embeddings and _has_st and mode in ("light","ultra") and chunk_emb is not None:
            model = _load_st(valves)
            q_emb = model.encode([q], normalize_embeddings=True, convert_to_tensor=True)
            import numpy as np
            em = util.cos_sim(q_emb, chunk_emb).cpu().numpy()[0]
            em = ((em + 1.0)/2.0).tolist()
        else:
            em = [0.0]*len(micro)
        bmn = normalize(bm)
        scored = []
        for i,m in enumerate(micro):
            s = alpha*bmn[i] + (1-alpha)*em[i]
            c = Chunk(**m.__dict__)
            c.score = s; c.bm25 = bm[i]; c.emb = em[i]
            scored.append(c)
        scored.sort(key=lambda x: x.score, reverse=True)
        top_k = (k_per_query if mode=="light" else int(k_per_query*valves.coarse_multiplier))
        if mode=="ultra":
            top_k = max(int(top_k*valves.ultra_multiplier), top_k+valves.ultra_extra)
        cand.extend(scored[:top_k])

    # 5) dedup + diversidade + orçamento global
    cand = dedup_chunks(cand, thresh=valves.similarity_threshold, valves=valves)
    cand.sort(key=lambda x: x.score, reverse=True)
    per_url_tokens = defaultdict(int)
    limit_tokens = token_budget if mode=="light" else max(token_budget, valves.coarse_budget_multiplier)
    selected, total = [], 0
    max_one_url = int(limit_tokens * valves.max_per_url_ratio)
    for c in cand:
        if per_url_tokens[c.url] >= max_one_url:
            continue
        t = est_tokens(c.text, valves)
        if total + t > int(limit_tokens * valves.token_margin):
            continue
        selected.append(c)
        per_url_tokens[c.url] += t
        total += t
        if total >= limit_tokens:
            break

    lines, sources = [], []
    for i,c in enumerate(selected, start=1):
        tag = f"[{label} - {i}]"
        lines.append(f"{tag} {c.title} — {c.url}\n{c.text}\n")
        sources.append({"id": tag, "url": c.url, "title": c.title})
    return "\n---\n".join(lines), sources, {"label": label, "queries": queries, "candidatos": len(cand), "selecionados": len(selected), "tokens": total}

# ---------- ENTRYPOINT da TOOL ----------
def reduce_context_from_scraper(corpo: Dict[str, Any],
                                mode: str = "light",
                                queries: List[str] = None,
                                tipo: str = "fase",
                                job_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Interface simplificada:
    - corpo: Dict com docs, phases ou ultrasearcher_result
    - mode: "coarse" | "light" | "ultra" (detalhes controlados pelas valves)
    - queries: Lista de queries (pipes devem passar TODAS as queries de todas as fases)
    - tipo: "fase" (acumula) | "global" (finaliza)
    - job_id: ID único do job para acumulação
    
    Retorna:
    {
      "final_markdown": "...",
      "per_phase": [{"name":..., "context_markdown":..., "sources":[...], "metrics":{...}}, ...],
      "sources": [...],
      "metrics": {"final_tokens_est": ..., "mode": str, "job_id": str, "tipo": str }
    }
    """
    # normaliza "heavy" para "coarse" por compatibilidade retro
    if mode == "heavy":
        mode = "coarse"
    
    # Configurações baseadas no modo (controladas pelas valves)
    enable_embeddings = mode != "coarse"
    
    # Extrair configurações das valves baseadas no modo
    valves = _default_valves
    token_budget = _get_token_budget_for_mode(mode, valves)
    k_per_query = _get_k_per_query_for_mode(mode, valves)
    max_per_url = _get_max_per_url_for_mode(mode, valves)
    
    # Se tipo="global", roda UM pack global sobre o corpus acumulado
    if tipo == "global" and job_id:
        corpus = _store_get_all(job_id, False)  # Sempre memória por simplicidade
        if not corpus:
            return {"final_markdown": "", "per_phase": [], "sources": [], "metrics": {"error": "corpus vazio para finalização"}}
        
        # Usa TODAS as queries passadas pelos pipes
        md, src, m = _select_over_corpus(
            corpus=corpus,
            label="GLOBAL",
            queries=queries,  # TODAS as queries de todas as fases
            mode=mode,
            token_budget=token_budget,
            k_per_query=k_per_query,
            max_per_url=max_per_url,
            enable_embeddings=enable_embeddings,
            valves=valves
        )
        
        # Limpa o corpus após finalização
        _store_clear(job_id, False)
        
        return {
            "final_markdown": md,
            "per_phase": [{"name": "GLOBAL", "context_markdown": md, "sources": src, "metrics": m}],
            "sources": src,
            "metrics": {"final_tokens_est": est_tokens(md, valves), "mode": mode, "job_id": job_id, "tipo": tipo}
        }

    # Normaliza entradas para lista de fases
    phase_list: List[Dict[str,Any]] = []
    
    # Processar corpo baseado no tipo de entrada
    if "ultrasearcher_result" in corpo:
        docs_from_ultra = _convert_ultrasearcher_result(corpo["ultrasearcher_result"])
        if docs_from_ultra:
            phase_list = [{"name": corpo.get("phase_name", "ultrasearcher_analysis"), "docs": docs_from_ultra}]
        else:
            return {"final_markdown": "", "per_phase": [], "sources": [], "metrics": {"error": "ultrasearcher_result vazio"}}
    elif "phases" in corpo and len(corpo["phases"]) > 0:
        phase_list = corpo["phases"]
    elif "docs" in corpo and "phase_name" in corpo:
        phase_list = [{"name": corpo["phase_name"], "docs": corpo["docs"]}]
    else:
        return {"final_markdown": "", "per_phase": [], "sources": [], "metrics": {"error": "corpo inválido"}}

    # Se tipo="fase", acumula micro-chunks no corpus global
    if tipo == "fase" and job_id:
        for ph in phase_list:
            mc = _docs_to_microchunks_for_accumulation(ph["name"], ph["docs"], valves)
            _store_append(job_id, mc, False)  # Sempre memória por simplicidade

    sections, per_phase_out, all_sources, metrics_ph = [], [], [], []
    
    # Orçamento por fase baseado no modo
    per_budget = token_budget // max(1, len(phase_list))

    for ph in phase_list:
        name = ph["name"]
        if job_id and tipo == "fase":
            # usa corpus global acumulado + docs desta fase (garante cross-fase)
            corpus = _store_get_all(job_id, False) + _docs_to_microchunks_for_accumulation(name, ph["docs"], valves)
            md, src, m = _select_over_corpus(
                corpus=corpus, label=name, queries=queries,  # Usa queries globais
                mode=mode, token_budget=per_budget, k_per_query=k_per_query,
                max_per_url=max_per_url, enable_embeddings=enable_embeddings,
                valves=valves
            )
        else:
            # comportamento padrão: só docs desta fase
            md, src, m = _select_phase(
                phase_name=name,
                docs=ph["docs"],
                queries=queries,  # Usa queries globais
                mode=mode,
                token_budget_phase=per_budget,
                k_per_query=k_per_query,
                max_per_url=max_per_url,
                enable_embeddings=enable_embeddings,
                valves=valves
            )
        sections.append(f"# {name}\n\n{md}" if md else f"# {name}\n\n(Nenhum trecho selecionado)\n")
        per_phase_out.append({"name": name, "context_markdown": md, "sources": src, "metrics": m})
        all_sources += src
        metrics_ph.append(m)

    final_md = "\n\n\n".join(sections)
    
    # Ajuste final se exceder o orçamento (corte proporcional)
    if est_tokens(final_md, valves) > token_budget:
        scale = token_budget / est_tokens(final_md, valves)
        # reprocessa com orçamento reduzido
        sections, per_phase_out, all_sources, metrics_ph = [], [], [], []
        adj = max(valves.min_budget_per_phase, int(per_budget * scale))
        
        for ph in phase_list:
            name = ph["name"]
            if job_id and tipo == "fase":
                corpus = _store_get_all(job_id, False) + _docs_to_microchunks_for_accumulation(name, ph["docs"], valves)
                md, src, m = _select_over_corpus(
                    corpus=corpus, label=name, queries=queries,
                    mode=mode, token_budget=adj, k_per_query=k_per_query,
                    max_per_url=max_per_url, enable_embeddings=enable_embeddings,
                    valves=valves
                )
            else:
                md, src, m = _select_phase(
                    phase_name=name,
                    docs=ph["docs"],
                    queries=queries,
                    mode=mode,
                    token_budget_phase=adj,
                    k_per_query=k_per_query,
                    max_per_url=max_per_url,
                    enable_embeddings=enable_embeddings,
                    valves=valves
                )
            sections.append(f"# {name}\n\n{md}" if md else f"# {name}\n\n(Nenhum trecho selecionado)\n")
            per_phase_out.append({"name": name, "context_markdown": md, "sources": src, "metrics": m})
            all_sources += src
            metrics_ph.append(m)
        final_md = "\n\n\n".join(sections)

    return {
        "final_markdown": final_md,
        "per_phase": per_phase_out,
        "sources": all_sources,
        "metrics": {
            "final_tokens_est": est_tokens(final_md, valves),
            "phases": metrics_ph,
            "mode": mode,
            "job_id": job_id,
            "tipo": tipo
        }
    }

def get_tools() -> List[Dict[str, Any]]:
    """
    Retorna lista de tools disponíveis
    
    Returns:
        Lista de dicionários com metadados das tools
    """
    return [
        {
            "name": "reduce_context_from_scraper",
            "description": "Reduz o tamanho do conteúdo de scraper mantendo relevância e diversidade",
            "version": "2.0.0",
            "requirements": [
                "rank-bm25>=0.2.2",
                "sentence-transformers>=2.2.2", 
                "rapidfuzz>=3.0.0",
                "simhash>=2.1.2",
                "pydantic>=2.0"
            ],
            "license": "MIT",
            "author": "DeepResearch Team",
            "function": reduce_context_from_scraper,
            "parameters": {
                "type": "object",
                "properties": {
                    "corpo": {
                        "type": "object",
                        "description": "Corpo da requisição contendo docs, phases ou ultrasearcher_result"
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["coarse", "light", "ultra"],
                        "description": "Modo de processamento (detalhes controlados pelas valves)",
                        "default": "light"
                    },
                    "queries": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "TODAS as queries de todas as fases (pipes devem passar todas)",
                        "default": None
                    },
                    "tipo": {
                        "type": "string",
                        "enum": ["fase", "global"],
                        "description": "Tipo de processamento: fase (acumula) ou global (finaliza)",
                        "default": "fase"
                    },
                    "job_id": {
                        "type": "string",
                        "description": "ID único do job para acumulação global",
                        "default": None
                    }
                },
                "required": ["corpo"]
            }
        }
    ]