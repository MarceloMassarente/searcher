# Melhorias do LLM Planner - Simplificação de Queries

## Resumo Executivo
O LLM Planner estava gerando queries muito complexas com múltiplos operadores (`site:`, `OR`, aspas excessivas), prejudicando a qualidade dos resultados. As melhorias forçam **queries semânticas simples** (4-7 termos) confiando nos **rails pós-busca** para filtros complexos.

---

## 4 Mudanças Principais

### 1. **SIMPLICIDADE: Queries SIMPLES (4-7 termos)**

**Antes:**
```
"agentes autônomos" OR "autonomous agents" "LLMs" "arquiteturas híbridas" 
OR "hybrid architectures" (SOTA 2023 OR SOTA 2024 OR SOTA 2025) benchmarks 
site:arxiv.org OR site:acm.org OR site:ieee.org OR site:doi.org filetype:pdf
```
❌ Muito complexa, SearXNG fica confuso

**Depois:**
```
autonomous agents LLMs benchmarks
```
✅ Simples, clara, semântica

**Como é forçado:**
- Prompt limita `core_query` a **4-7 palavras-chave**
- Máximo 1 OR por plano
- Máximo 1 frase com aspas por plano
- `_simplify_complex_query()` reduz queries muito complexas

---

### 2. **SITE: EXPLÍCITO - Usar site: APENAS se user pediu**

**Novo:** Função `_detect_explicit_site_request(query: str)`

Detecta quando user pediu explicitamente por domínios:
- "site:arxiv.org" → usar site:
- "papers do arxiv" → usar site:
- "em github" → usar site:
- "apenas em Reuters" → usar site:
- Nenhuma menção a domínios → **NÃO usar site:**

**Prompt menciona claramente:**
```
4. ⚠️ SITE: APENAS SE USUÁRIO PEDIU EXPLICITAMENTE:
   - Usuário pediu? → use site:
   - Usuário NÃO pediu? → NÃO use site: (deixe vazio)
```

**Rails disponíveis:**
```
- official_domains: Domínios oficiais a priorizar (bonus de score pós-busca)
```

Então: Se user não pediu `site:`, Planner gera query simples, rails aplicam `official_domains` depois!

---

### 3. **IDIOMAS: Estratégia Multi-Idioma por Plano**

**Novo:** Permite EN e PT em planos **diferentes** (não no mesmo plano)

Estratégia orientada por contexto:
```
- Papers/SOTA/acadêmico → INGLÊS (maioria em EN)
- Frameworks/código/docs → INGLÊS (documentação oficial)
- Notícias/aplicações BR → PORTUGUÊS
- Contexto global/padrões → INGLÊS
```

**Exemplo bom:**
- Plan 1 (EN): `autonomous agents frameworks benchmarks`
- Plan 2 (PT): `agentes autônomos adoção Brasil aplicações`

❌ **RUIM (não fazer):**
```
agentes autônomos autonomous agents frameworks
```
Misturar EN/PT no mesmo plano confunde SearXNG.

---

### 4. **RAILS DOCUMENTADOS: Planner sabe que filtros são pós-busca**

**Novo:** Seção no prompt: "RAILS DISPONÍVEIS - JÁ APLICADOS PÓS-BUSCA"

Lista todos os rails que já cobrem os casos de filtro:
```
- must_terms: Termos obrigatórios (priorizam)
- avoid_terms: Termos a evitar (penalizam)
- lang_bias: Preferências de idioma (soft)
- geo_bias: Preferências geográficas (soft)
- official_domains: Domínios oficiais (bonus)
- min_domains: Mínimo de domínios únicos
- time_hint: Controle temporal fino
```

**Impacto:**
- Planner entende que `avoid_terms` cuida de filtros como "NOT vagas"
- Gera query simples sem tentar fazer filtros no `core_query`
- Confia em rails pós-busca para aplicar regras complexas

---

## Fluxo Simplificado

```
User Query
    ↓
[Planner - SIMPLES (4-7 termos)]
    ↓
Core Query (ex: "autonomous agents benchmarks")
    ↓
SearXNG/Exa (menos confusão, mais recall)
    ↓
[Rails Pós-Busca]
    • must_terms: boost
    • avoid_terms: penalize
    • official_domains: prioritize
    • lang_bias/geo_bias: soft rerank
    ↓
Final Results (curados)
```

---

## Exemplos de Query Bom vs Ruim

### ✅ BOM

```json
{
  "plans": [
    {
      "core_query": "autonomous agents LLMs benchmarks",
      "sites": [],
      "filetypes": [],
      "suggested_domains": ["arxiv.org", "acm.org"]
    },
    {
      "core_query": "agentes autônomos frameworks adoção Brasil",
      "sites": [],
      "filetypes": [],
      "suggested_domains": ["valor.globo.com"]
    }
  ]
}
```

### ❌ RUIM (não gerar assim)

```json
{
  "plans": [
    {
      "core_query": "\"agentes autônomos\" OR \"autonomous agents\" \"LLMs\" 
                    \"frameworks\" (\"LangChain\" OR \"AutoGen\") 
                    site:arxiv.org OR site:acm.org filetype:pdf",
      "sites": ["arxiv.org", "acm.org", "ieee.org"],
      "filetypes": ["pdf"]
    }
  ]
}
```

---

## Commits

- **4e0ea8a**: Simplify LLM Planner - enforce simple queries, clarify site: usage, document available rails
- **5e2ad6c**: Add comprehensive docstring documenting improvements

---

## Testes Recomendados

1. **Query complexa com must_terms + avoid_terms:**
   ```
   Query: "estado da arte agentes autônomos LLMs"
   Rails: must_terms=["agentes autônomos"], avoid_terms=["vagas", "currículos"]
   ```
   Esperado: Query simples, rails aplicam filtros pós-busca

2. **Query com site: explícito:**
   ```
   Query: "papers de agentes autônomos site:arxiv.org"
   ```
   Esperado: Planner usa `site:arxiv.org` na query

3. **Query sem site: (user não pediu):**
   ```
   Query: "agentes autônomos benchmarks frameworks"
   Rails: official_domains=["arxiv.org", "acm.org"]
   ```
   Esperado: Query sem site:, rails aplicam official_domains pós-busca

