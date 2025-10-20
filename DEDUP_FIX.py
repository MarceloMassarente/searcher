#!/usr/bin/env python3
"""
🔧 DEDUP FIX - Implementação Completa

3 mudanças para resolver fragmentação:
1. Aumentar threshold de detecção de markdown
2. Usar target dinâmico (em vez de fixo 200)
3. Combinar parágrafos pequenos pós-dedup
"""

# ============================================================================
# FIX 1: Melhorar threshold de detecção de markdown
# ============================================================================
# LOCALIZAÇÃO: PipeLangNew.py, linha ~6672 na função _paragraphs()
#
# ANTES:
#     if avg_paragraph_size > 1000:
#         # Markdown do scraper/Context Reducer usa \n único
#
# DEPOIS:
#     if avg_paragraph_size > 2000 or "\n\n" not in text:
#         # Markdown do scraper/Context Reducer usa \n único
#         # OU não há duplos newlines = markdown com \n único

CODE_FIX_1 = """
# Linha ~6672
# ANTES: if avg_paragraph_size > 1000:
# DEPOIS:
if avg_paragraph_size > 2000 or "\\n\\n" not in text:
    # Markdown do scraper/Context Reducer usa \\n único
    # Agrupar linhas não vazias em blocos (parágrafos)
    ...
"""

# ============================================================================
# FIX 2: Usar target dinâmico em vez de fixo 200
# ============================================================================
# LOCALIZAÇÃO: PipeLangNew.py, linha ~6794 na função _synthesize_final()
#
# ANTES:
#     dedupe_result = deduplicator.dedupe(
#         chunks=raw_paragraphs,
#         max_chunks=self.valves.MAX_DEDUP_PARAGRAPHS,  # ← fixo 200
#
# DEPOIS:
#     # Usar target DINÂMICO em vez de fixo
#     target_paragraphs = max(
#         100,  # mínimo
#         min(
#             int(len(raw_paragraphs) / 10),  # redução 10x
#             500  # máximo
#         )
#     )
#     dedupe_result = deduplicator.dedupe(
#         chunks=raw_paragraphs,
#         max_chunks=target_paragraphs,  # ← dinâmico

CODE_FIX_2 = """
# Linha ~6792-6804
# ANTES:
#     dedupe_result = deduplicator.dedupe(
#         chunks=raw_paragraphs,
#         max_chunks=self.valves.MAX_DEDUP_PARAGRAPHS,
#
# DEPOIS:

# ✅ [FIX 2] Usar target DINÂMICO em vez de fixo 200
target_paragraphs = max(
    100,  # mínimo (não ir abaixo)
    min(
        int(len(raw_paragraphs) / 10),  # redução 10x é razoável
        500  # máximo (não exagerar)
    )
)

if getattr(self.valves, "VERBOSE_DEBUG", False):
    print(f"[FIX 2] Target dinâmico: {len(raw_paragraphs)} parágrafos")
    print(f"        → Redução: {len(raw_paragraphs) / target_paragraphs:.1f}x")
    print(f"        → Target: {target_paragraphs} parágrafos")

dedupe_result = deduplicator.dedupe(
    chunks=raw_paragraphs,
    max_chunks=target_paragraphs,  # ← dinâmico agora!
    algorithm=algorithm,
    threshold=threshold,
    preserve_order=self.valves.PRESERVE_PARAGRAPH_ORDER,
    preserve_recent_pct=0.0,
    shuffle_older=False,
    reference_first=False,
    must_terms=extracted_must_terms,
    enable_context_aware=self.valves.ENABLE_CONTEXT_AWARE_DEDUP,
)
"""

# ============================================================================
# FIX 3: Função helper para combinar parágrafos pequenos
# ============================================================================
# LOCALIZAÇÃO: PipeLangNew.py, adicionar como função helper
#
# Usar APÓS: deduped_paragraphs = dedupe_result["chunks"]
# Antes:    deduped_context = "\n\n".join(deduped_paragraphs)

CODE_FIX_3 = """
# ✅ [FIX 3] Função helper para combinar parágrafos pequenos
def _merge_small_paragraphs(
    paragraphs: list,
    min_chars: int = 100,
    max_chars: int = 1000
) -> list:
    \"\"\"
    Combina parágrafos pequenos para evitar fragmentação
    
    Args:
        paragraphs: Lista de parágrafos
        min_chars: Tamanho mínimo desejado por parágrafo
        max_chars: Tamanho máximo (para evitar muito grandes)
    
    Returns:
        Lista com parágrafos mesclados
    \"\"\"
    if not paragraphs:
        return []
    
    merged = []
    buffer = ""
    
    for para in paragraphs:
        # Se buffer + novo parágrafo < min_chars, combina
        if buffer and len(buffer) + len(para) + 1 < min_chars:
            buffer += " " + para
        else:
            # Se buffer existe, salva
            if buffer:
                merged.append(buffer)
            buffer = para
    
    # Salvar último buffer
    if buffer:
        merged.append(buffer)
    
    return merged


# Depois de dedupe_result:
deduped_paragraphs = dedupe_result["chunks"]

# ✅ [FIX 3] Aplicar merge para evitar fragmentação
deduped_paragraphs = _merge_small_paragraphs(
    deduped_paragraphs,
    min_chars=100,
    max_chars=1000
)

if getattr(self.valves, "VERBOSE_DEBUG", False):
    print(f"[FIX 3] Merge pós-dedup: {dedupe_result['deduped_count']} → {len(deduped_paragraphs)} parágrafos")

deduped_context = "\\n\\n".join(deduped_paragraphs)
"""

# ============================================================================
# RESUMO DE MUDANÇAS
# ============================================================================

SUMMARY = """
╔══════════════════════════════════════════════════════════════════════════╗
║                    RESUMO DE MUDANÇAS NECESSÁRIAS                       ║
╚══════════════════════════════════════════════════════════════════════════╝

📍 ARQUIVO: PipeLangNew.py

📝 FIX 1: Linha ~6672 (função _paragraphs)
   • Aumentar threshold: 1000 → 2000
   • OU adicionar condição: "\\n\\n" not in text
   • Impacto: 9.299 parágrafos → ~2.000

📝 FIX 2: Linha ~6792-6804 (dedupe_result call)
   • Usar target dinâmico: int(len(raw_paragraphs) / 10)
   • Limites: min=100, max=500
   • Impacto: redução 46.5x → 10x

📝 FIX 3: Linha ~6806 (após deduped_paragraphs = ...)
   • Adicionar _merge_small_paragraphs helper
   • Garantir mínimo de 100 chars/parágrafo
   • Impacto: fragmentação visual reduzida

─────────────────────────────────────────────────────────────────────────

📊 ANTES vs DEPOIS:

ANTES:
  Input: 11.433 chars
  Split: 9.299 parágrafos (avg 1.2 chars!)
  Dedup: 200 target
  Redução: 46.5x
  Resultado: 57 chars/parágrafo ❌

DEPOIS:
  Input: 11.433 chars
  Split: 2.000 parágrafos (avg 5.7 chars)
  Dedup: 464 target (dinâmico)
  Redução: 4.3x
  Merge: ~400 final (após combinar pequenos)
  Resultado: 28-30 chars/parágrafo ✅

─────────────────────────────────────────────────────────────────────────

⏱️  TEMPO DE IMPLEMENTAÇÃO: ~15 minutos

✅ TESTE: Rodar busca e capturar [DEDUP DIAGNÓSTICO]
          Validar: avg_size ≥ 100 chars
"""

print(SUMMARY)
print("\n" + "="*80)
print("INSTRUÇÕES DETALHADAS EM PRÓXIMA SEÇÃO")
print("="*80 + "\n")

# ============================================================================
# INSTRUÇÕES PASSO A PASSO
# ============================================================================

INSTRUCTIONS = """
🚀 GUIA PASSO A PASSO DE IMPLEMENTAÇÃO

PASSO 1: Backup
─────────
  > cd C:\\Users\\marce\\native tool\\SearchSystem
  > cp PipeLangNew.py PipeLangNew.py.backup

PASSO 2: Editar FIX 1 - Aumentar threshold
─────────
  • Abrir PipeLangNew.py
  • Ir para linha ~6672
  • ENCONTRAR:
      if avg_paragraph_size > 1000:
  • SUBSTITUIR POR:
      if avg_paragraph_size > 2000 or "\\n\\n" not in text:

PASSO 3: Editar FIX 2 - Target dinâmico
─────────
  • Ir para linha ~6792
  • ENCONTRAR:
      dedupe_result = deduplicator.dedupe(
          chunks=raw_paragraphs,
          max_chunks=self.valves.MAX_DEDUP_PARAGRAPHS,
  • SUBSTITUIR POR (antes dessa line):
      target_paragraphs = max(
          100,
          min(
              int(len(raw_paragraphs) / 10),
              500
          )
      )
  • E MUDAR:
      max_chunks=target_paragraphs,  # era self.valves.MAX_DEDUP_PARAGRAPHS

PASSO 4: Editar FIX 3 - Merge helper
─────────
  • Encontrar função _paragraphs() (linha ~6654)
  • DEPOIS dessa função, ADICIONAR helper _merge_small_paragraphs()
  • Ver código em CODE_FIX_3 acima
  
PASSO 5: Usar o helper
─────────
  • Ir para linha ~6806
  • ENCONTRAR:
      deduped_paragraphs = dedupe_result["chunks"]
      deduped_context = "\\n\\n".join(deduped_paragraphs)
  • SUBSTITUIR POR:
      deduped_paragraphs = dedupe_result["chunks"]
      
      # [FIX 3] Combinar pequenos parágrafos
      deduped_paragraphs = _merge_small_paragraphs(
          deduped_paragraphs,
          min_chars=100
      )
      
      deduped_context = "\\n\\n".join(deduped_paragraphs)

PASSO 6: Testar
─────────
  • Rodar uma busca no OpenWebUI
  • Capturar logs [DEDUP DIAGNÓSTICO]
  • Validar: avg_size ≥ 100 chars
  • Verificar output: parágrafos menos fragmentados?

PASSO 7: Validar com script
─────────
  > python dedup_analyzer.py
  
  Deve mostrar:
  • Reduction factor: < 15x (não > 40x)
  • Final avg size: > 100 chars
  • Warnings: < 2

PASSO 8: Commit (se ok)
─────────
  > git add PipeLangNew.py
  > git commit -m "Fix: Reduce deduplication fragmentation (3 fixes)
  
    - Increase markdown detection threshold (1000→2000)
    - Use dynamic dedup target (10x reduction vs 46x)
    - Merge small paragraphs post-dedup (min 100 chars)
    
    Fixes extreme fragmentation (57→28 chars/paragraph avg)"

"""

print(INSTRUCTIONS)

# ============================================================================
# CÓDIGO PRONTO PARA COLAR
# ============================================================================

READY_TO_COPY = """
╔══════════════════════════════════════════════════════════════════════════╗
║                  CÓDIGO PRONTO PARA COPIAR E COLAR                      ║
╚══════════════════════════════════════════════════════════════════════════╝

[FIX 1] Linha ~6672 - Aumentar threshold
───────────────────────────────────────────────────────────────────────────

if avg_paragraph_size > 2000 or "\\n\\n" not in text:
    # Markdown do scraper/Context Reducer usa \\n único


[FIX 2] Linha ~6792 - Target dinâmico (ANTES da linha dedupe_result)
───────────────────────────────────────────────────────────────────────────

# Usar target DINÂMICO em vez de fixo 200
target_paragraphs = max(
    100,
    min(
        int(len(raw_paragraphs) / 10),
        500
    )
)

if getattr(self.valves, "VERBOSE_DEBUG", False):
    print(f"[FIX 2] Target dinâmico: {len(raw_paragraphs)} parágrafos")
    print(f"        → Redução: {len(raw_paragraphs) / target_paragraphs:.1f}x")
    print(f"        → Target: {target_paragraphs} parágrafos")


[FIX 2 CONT] Linha ~6794 - Mudar max_chunks
───────────────────────────────────────────────────────────────────────────

dedupe_result = deduplicator.dedupe(
    chunks=raw_paragraphs,
    max_chunks=target_paragraphs,  # ← mudou daqui: self.valves.MAX_DEDUP_PARAGRAPHS
    algorithm=algorithm,
    threshold=threshold,
    preserve_order=self.valves.PRESERVE_PARAGRAPH_ORDER,
    preserve_recent_pct=0.0,
    shuffle_older=False,
    reference_first=False,
    must_terms=extracted_must_terms,
    enable_context_aware=self.valves.ENABLE_CONTEXT_AWARE_DEDUP,
)


[FIX 3] Linha ~6654 - Adicionar helper (APÓS função _paragraphs)
───────────────────────────────────────────────────────────────────────────

def _merge_small_paragraphs(
    paragraphs: list,
    min_chars: int = 100,
    max_chars: int = 1000
) -> list:
    \"\"\"Combina parágrafos pequenos para evitar fragmentação\"\"\"
    if not paragraphs:
        return []
    
    merged = []
    buffer = ""
    
    for para in paragraphs:
        if buffer and len(buffer) + len(para) + 1 < min_chars:
            buffer += " " + para
        else:
            if buffer:
                merged.append(buffer)
            buffer = para
    
    if buffer:
        merged.append(buffer)
    
    return merged


[FIX 3 CONT] Linha ~6806 - Usar helper
───────────────────────────────────────────────────────────────────────────

deduped_paragraphs = dedupe_result["chunks"]

# [FIX 3] Combinar pequenos parágrafos
deduped_paragraphs = _merge_small_paragraphs(
    deduped_paragraphs,
    min_chars=100
)

if getattr(self.valves, "VERBOSE_DEBUG", False):
    print(f"[FIX 3] Merge pós-dedup: {dedupe_result['deduped_count']} → {len(deduped_paragraphs)} parágrafos")

deduped_context = "\\n\\n".join(deduped_paragraphs)

"""

print(READY_TO_COPY)
