# 🔍 Análise de Fragmentação do Deduplicador

## Status: INVESTIGAÇÃO EM PROGRESSO

### Problema Relatado
Você achando que o resultado está **fragmentado demais**, com parágrafos muito pequenos.

---

## 📊 Números dos Logs

```
Entrada:        11.433 caracteres originais
↓ Scraping
Parágrafos raw: 9.299 (split por \n\n)
Depois:         6.431 parágrafos (outro regate?)
↓ Deduplicação
Saída final:    200 parágrafos (98.3% redução)
```

### Cálculos de Densidade

```
Cenário 1: 11.433 chars ÷ 9.299 paragrafos = 1,23 chars/parágrafo ❌ IMPOSSÍVEL
                          (normal seria 100-300 chars/parágrafo)

Cenário 2: 11.433 chars ÷ 200 paragrafos = 57 chars/parágrafo ❌ SUPER FRAGMENTADO
                      (muito menor que o mínimo viável)
                      
Cenário 3: Se realmente 11.433 chars total com 200 parágrafos = fragmentação extrema
           Um parágrafo seria ~57 chars = meio-parágrafo de texto
```

---

## 🔧 Como o Deduplicador Funciona

### 1️⃣ **Fase de Extração** (`_paragraphs()`)

```python
# DETECTA DENSIDADE AUTOMATICAMENTE

if avg_paragraph_size > 1000:  # Markdown com \n único
    # Agrupa linhas em blocos
    # Resultado: MAIS fragmentado
else:
    # Split por \n\n
    # Resultado: PARÁGRAFOS NATURAIS
```

### 2️⃣ **Limite de Tamanho**

```python
# Se parágrafo > 1200 chars, quebra por sentenças:
max_paragraph_chars = 1200  # ← VALVE configurável

# Isso pode criar fragmentação AINDA MAIOR
```

### 3️⃣ **Deduplicação MMR**

```python
# Seleciona TOP-200 parágrafos mais diversos
# Preserva ordem original
# Algoritmo: Maximal Marginal Relevance
```

---

## 🎯 Diagnóstico Implementado

Adicionei logs detalhados em `_synthesize_final()`:

```python
[DEDUP DIAGNÓSTICO]
  📊 Total chars: XXX
  📝 Parágrafos: YYY
  📈 Tamanho médio: ZZ chars
  📍 Mediana: WW chars
  ⬇️  P25: AA chars
  ⬆️  P75: BB chars
  🔸 Min: CC chars | Max: DD chars
  ⚡ Fator redução necessário: EE.Ex
  ✅ Redução viável: [SIM/ALERTA]
```

### Rodando o Próximo Teste

Execute sua busca novamente e **capture esses logs**. Eles vão revelar:

- ✅ Se a fragmentação é REAL (parágrafos pequenos demais)
- ✅ Se é ESPERADA (redução drástica é necessária)
- ✅ Onde o problema está (split inicial vs. dedup vs. limite de tamanho)

---

## 💡 Possíveis Causas

### Causa 1: Split Detectando Markdown
```
Se o scraper retornar markdown com \n único:
  - Função detecta density > 1000 chars/parágrafo
  - Agrupa linhas em blocos
  - Resultado: 9.299 parágrafos (MUITO fragmentado!)
```

### Causa 2: Parágrafos Gigantes
```
Se houver parágrafos > 1200 chars:
  - Quebra por sentenças
  - Cada sentença vira um "parágrafo"
  - Resultado: EXPLOSÃO de fragmentação
```

### Causa 3: MAX_DEDUP_PARAGRAPHS Muito Baixo
```
Se configurado para 200 parágrafos:
  - Com 9.299 parágrafos de entrada
  - Redução de 46x é EXTREMA
  - Algoritmo seleciona apenas "principais"
  - Pode resultar em "colcha de retalhos"
```

---

## 🛠️ Soluções Propostas

### Opção A: Aumentar TARGET
```python
# Em valves (config OpenWebUI):
MAX_DEDUP_PARAGRAPHS = 500  # em vez de 200
# Resultado: Menos agressivo, melhor narrativa
```

### Opção B: Melhorar Split
```python
# Problemático: avg_paragraph_size > 1000

# Solução: Mudar threshold
if avg_paragraph_size > 2000:  # em vez de 1000
    # Agrupa com menos agressividade
```

### Opção C: Combinar Pequenos
```python
# Após dedup, MESCLAR parágrafos pequenos:
final_paragraphs = []
buffer = ""
MIN_CHARS = 100

for para in deduped_paragraphs:
    if len(buffer) + len(para) < MIN_CHARS:
        buffer += " " + para
    else:
        if buffer:
            final_paragraphs.append(buffer)
        buffer = para

if buffer:
    final_paragraphs.append(buffer)
```

### Opção D: Usar Semantic Chunking
```python
# Em vez de split por \n\n, usar embeddings:
# - Parágrafos semanticamente coerentes
# - Tamanho mais consistente
# - Menos fragmentação visual
```

---

## 📋 Checklist para Investigação

- [ ] **Rodar novo teste** com diagnóstico ativo
- [ ] **Capturar logs completos** de `[DEDUP DIAGNÓSTICO]`
- [ ] **Identificar causa**: qual é o tamanho MÉDIO real dos parágrafos?
- [ ] **Validar valve**: `MAX_DEDUP_PARAGRAPHS = 200` é intencional?
- [ ] **Testar solução**: Qual opção é melhor?

---

## 🔗 Arquivos Afetados

- `PipeLangNew.py` linha ~6654: Função `_paragraphs()`
- `PipeLangNew.py` linha ~6767: Síntese com dedup
- `PipeLangNew.py` linha ~1048: Classe `Deduplicator`
- `PipeLangNew.py` linha ~980: Função `_mmr_select()`

---

## ✅ Próximas Ações

1. Execute uma busca com DEBUG ativo
2. Capture os logs de `[DEDUP DIAGNÓSTICO]`
3. Compartilhe os números
4. Vamos decidir qual solução implementar
