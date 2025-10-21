# Domain Diversity Strategy: Pré-LLM vs Pós-LLM

## Problema Identificado

A aplicação de domain diversity **PRÉ-LLM** era muito agressiva:

```
[Zipper] 283 candidatos
  ↓ [Truncate global OOM]
[Zipper output] 200 candidatos
  ↓ [Domain cap: max 3 por domínio]
[Domain capped] 70 candidatos ⚠️ PERDEU 60% dos dados!
  ↓ [LLM análise]
[Final] 10 URLs
```

**Resultado:** LLM teve pouco contexto para análise semântica, perdendo URLs relevantes.

---

## Solução: Remover Pré-LLM, Aplicar Pós-LLM

### ✅ Fluxo Novo (Mais Inteligente)

```
[Zipper] 283 candidatos
  ↓ [Truncate global OOM]
[Zipper output] 200 candidatos
  ↓ [LLM análise com 200 candidatos] ← MAIS CONTEXTO
[LLM selected] ~20 URLs (curadas semanticamente)
  ↓ [Rails pós-LLM com min_domains]
[Final] 10 URLs (com diversidade garantida)
```

### Vantagens

1. **LLM tem mais contexto:** Analisa 200 candidatos em vez de 70
   - Mais exemplos de cada domínio
   - Melhor ranking semântico
   - Menos chance de perder URL relevante

2. **Diversidade ainda garantida:** Rails pós-LLM aplicam `min_domains`
   - Se `min_domains=5`, garante 5 domínios diferentes
   - Mais inteligente que cap rígido pré-LLM

3. **Trade-off melhor:** 
   - Antes: Quantidade > Qualidade (200→70 → poucos candidatos)
   - Depois: Qualidade > Quantidade (200 → LLM curada → 10 final)

---

## Como Funciona Rails Pós-LLM

### Código em `_apply_rails_post()`

```python
# Diversidade (bonus para domínios novos)
seen_domains = set()
for c in cands:
    if c.domain not in seen_domains:
        score += DiscoveryConstants.SCORE_DOMAIN_DIVERSITY  # +1.0
        seen_domains.add(c.domain)

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
```

### O que faz

1. **Scoring:** Cada domínio novo leva +1.0 no score
2. **Min domains enforcement:** Garante pelo menos N domínios diferentes
3. **Preserva ranking:** Após satisfazer `min_domains`, ordena por score

---

## Configuração Recomendada

### `enable_domain_diversity` (Removed)
**Status:** Removido do pré-LLM (era muito agressivo)

### `max_urls_per_domain` (Now post-LLM only)
**Default:** 4
**Aplicado em:** Rails pós-LLM (como bonus, não hard cap)
**Impacto:** 
- Soft preference (preferir domínios novos, não bloquear)
- Não remove candidatos, apenas os desclassifica em score

### `min_domains` (Novo/Melhorado)
**Default:** None
**Aplicado em:** Rails pós-LLM
**Impacto:**
- Hard constraint: garante N domínios diferentes
- Exemplo: `min_domains=5` → top 10 URLs têm ≥5 domínios

---

## Exemplos

### Cenário 1: Query genérica, sem rails

```
Entrada: 200 candidatos (10 domínios, múltiplas URLs por domínio)

Pré-LLM (ANTIGO - removido):
  [Domain cap 3] → 30 candidatos

Pós-LLM (NOVO):
  [LLM análise] → 20 URLs curadas
  [Rails sem min_domains] → 20 final (diversidade implícita de 10 domínios)
```

### Cenário 2: Query com rails `min_domains=5`

```
Entrada: 200 candidatos

Pós-LLM:
  [LLM análise] → 20 URLs (pode ter 3 domínios apenas)
  [Rails min_domains=5] → 10 final (força 5 domínios diferentes)
  
Resultado: Top 10 URLs com pelo menos 5 domínios
```

---

## Impacto nos Logs

### Antes (Pré-LLM removed)
```
[Discovery] enable_domain_diversity enabled, using max_urls_per_domain=3
[Discovery] soft_domain_cap reduced candidates from 200 to 70
```

### Depois (Pós-LLM)
```
[Discovery] Skipping pre-LLM domain diversity (was too aggressive)
[Discovery] Proceeding with 200 candidates for LLM analysis
```

---

## Commit

- **Commit:** Remove pre-LLM domain diversity, delegate to post-LLM rails

---

## Testes Recomendados

1. **Verificar que LLM recebe mais candidatos:**
   - Antes: 70 candidatos
   - Depois: 200 candidatos
   - ✅ Resultado: LLM com melhor contexto

2. **Verificar que min_domains funciona:**
   ```
   Query: "agentes autônomos"
   Rails: min_domains=5
   ```
   Esperado: Top 10 URLs com ≥5 domínios diferentes

3. **Verificar que max_urls_per_domain é soft (não hard cap):**
   ```
   Query: "agentes autônomos"
   Rails: max_urls_per_domain=2
   ```
   Esperado: Possível ter 3+ URLs do mesmo domínio se forem top-ranked
