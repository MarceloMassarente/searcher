# Paragraph Chunking: Comparação entre Pipes

## Resumo Executivo

| Aspecto | tool_discovery.py | PipeLangNew.py | PipeHaystack |
|---------|------------------|----------------|--------------|
| **Chunking** | ❌ Não tem | ✅ Sim (adaptive) | ✅ Sim (adaptive) |
| **Detecção Density** | - | ✅ Sim (>2000 chars) | ✅ Sim (>1000 chars) |
| **Splitting** | - | Duplo newline + linhas | Duplo newline + linhas |
| **Max paragraf** | - | 1200 chars (configurable) | 1200 chars (configurable) |
| **Merge small** | - | ✅ 100 chars min | ✅ 100 chars min |
| **Sentence split** | - | ✅ regex `[.!?]` | ✅ regex `[.!?]` |

---

## 1. tool_discovery.py

**Status:** ❌ SEM chunking de paragraphs

- Trabalha apenas com URLs e snippets
- Não faz splitting de conteúdo bruto
- Não é uma ferramenta de síntese (apenas descoberta)

---

## 2. PipeLangNew.py - `_paragraphs()`

### Estratégia: Detecção Adaptativa de Densidade

```python
def _paragraphs(text: str) -> List[str]:
    """
    Detecta automaticamente o formato baseado na densidade de parágrafos:
    - Densidade BAIXA (>2000 chars/parágrafo médio) → markdown com \n único
    - Densidade ALTA (<500 chars/parágrafo médio) → formato normal com \n\n
    """
```

### Algoritmo

**Passo 1: Tentar split por duplo newline**
```python
parts = [p.strip() for p in text.split("\n\n") if p.strip()]
avg_paragraph_size = len(text) / max(len(parts), 1)
```

**Passo 2: Detectar densidade**
```
avg_paragraph_size > 2000 ou "\n\n" não existe
    → usar extração linha-por-linha (markdown)
    
caso contrário
    → usar split por \n\n (formato normal)
```

**Passo 3: Extração linha-por-linha (se markdown)**
```python
# Agrupar linhas consecutivas até linha vazia
current_block = []
for line in lines:
    if line.strip():
        current_block.append(line.strip())
    else:
        # Linha vazia = fim do parágrafo
        paragraph = " ".join(current_block)  # ← JOIN into single line
        if len(paragraph) > 20:
            paragraphs.append(paragraph)
```

**Passo 4: Fragmentação adicional (v4.8.1)**
```python
# Se ainda houver parágrafos gigantes (>1200 chars), quebrar por sentenças
max_paragraph_chars = 1200  # configurable: DEDUP_MAX_PARAGRAPH_CHARS

for paragraph in paragraphs:
    if len(paragraph) > 1200:
        # Usar regex para split em sentenças: (?<=[.!?])\s+
        sentences = [s.strip() for s in sentence_splitter.split(paragraph)]
        
        # Agrupar sentenças em chunks de até 1200 chars
        chunk = []
        chunk_len = 0
        for sentence in sentences:
            if chunk and chunk_len + len(sentence) > 1200:
                final_paragraphs.append(" ".join(chunk))
                chunk = [sentence]
            else:
                chunk.append(sentence)
```

**Passo 5: Filtrar pequenos**
```python
return [p for p in final_paragraphs if len(p) > 20]
```

### Fluxo Completo

```
Raw text
  ↓
[Densidade test]
  ├─ >2000 chars/parágrafo → markdown mode (linha-por-linha)
  └─ <500 chars/parágrafo → normal mode (duplo newline)
  ↓
[Extração inicial]
  └─ Parágrafos candidatos
  ↓
[Fragmentação (se necessário)]
  └─ Se parágrafo > 1200 chars: split por sentenças
  ↓
[Merge pequenos]
  └─ Via _merge_small_paragraphs (combina < 100 chars)
  ↓
[Filtrar <20 chars]
  └─ Parágrafos finais
```

---

## 3. PipeHaystack - `_paragraphs()`

### Estratégia: Semelhante ao PipeLangNew, mas com threshold diferente

```python
def _paragraphs(text: str) -> List[str]:
    """
    Detecta automaticamente o formato baseado na densidade de parágrafos:
    - Densidade BAIXA (>1000 chars/parágrafo médio) → markdown com \n único
    - Densidade ALTA (<500 chars/parágrafo médio) → formato normal com \n\n
    """
```

### Principais Diferenças

| Aspecto | PipeLangNew | PipeHaystack |
|---------|-------------|--------------|
| Threshold de densidade | **>2000 chars/parágrafo** | **>1000 chars/parágrafo** |
| Sensibilidade | Menos sensível (permite blocos maiores) | Mais sensível (quebra blocos antes) |
| Max parágrafo | 1200 chars | 1200 chars |
| Sentence regex | `(?<=[.!?])\s+` | `(?<=[.!?])\s+` |

### Impacto da Diferença

```
Exemplo: Texto com avg 1500 chars/parágrafo

PipeLangNew (>2000):
  → Usa split por \n\n (densidade considerada normal)
  → Menos fragmentação

PipeHaystack (>1000):
  → Usa extração linha-por-linha (densidade considerada baixa)
  → Mais fragmentação, mais chances de quebra por sentença
```

---

## 4. Funções Auxiliares

### _merge_small_paragraphs (em ambos)

```python
def _merge_small_paragraphs(paragraphs: list, min_chars: int = 100) -> list:
    """Combina parágrafos pequenos para evitar fragmentação"""
    merged = []
    buffer = ""
    for para in paragraphs:
        if buffer and len(buffer) + len(para) + 1 < min_chars:
            buffer += " " + para  # Juntar
        else:
            if buffer:
                merged.append(buffer)
            buffer = para
    if buffer:
        merged.append(buffer)
    return merged
```

**Impacto:**
- Evita fragmentação excessiva
- Mantém parágrafos ≥100 chars por padrão
- Aplicado APÓS deduplicação em Analyst

---

## 5. Configurações (Valves)

### PipeLangNew.py

```python
MAX_DEDUP_PARAGRAPHS: int = 200  # default
DEDUP_MAX_PARAGRAPH_CHARS: int = 1200  # threshold de fragmentação
PRESERVE_PARAGRAPH_ORDER: bool = True  # manter ordem original
```

### PipeHaystack

```python
MAX_DEDUP_PARAGRAPHS: int = 300  # default (maior)
DEDUP_MAX_PARAGRAPH_CHARS: int = 1200  # mesmo threshold
```

---

## 6. Exemplos de Saída

### Entrada (markdown com linhas únicas)

```
URL: https://example.com/news
Título: AI Revolution
Conteúdo:
The future of AI is here
Machine learning models are improving rapidly
New techniques emerge every day
```

### PipeLangNew (>2000 threshold)

Se `avg_size = 1500`:
```
[1] "The future of AI is here Machine learning models are improving rapidly New techniques emerge every day"
```

Sem fragmentação (densidade considerada normal).

### PipeHaystack (>1000 threshold)

Se `avg_size = 1500`:
```
[1] "The future of AI is here"
[2] "Machine learning models are improving rapidly"
[3] "New techniques emerge every day"
```

Mais fragmentado (densidade considerada baixa).

---

## 7. Recomendações de Tuning

### Para Síntese Rápida
```
MAX_DEDUP_PARAGRAPHS: 150  # Reduzir para síntese mais rápida
DEDUP_MAX_PARAGRAPH_CHARS: 1000  # Mais fragmentação = menos dúvida semântica
```

### Para Síntese Qualidade Máxima
```
MAX_DEDUP_PARAGRAPHS: 300  # Mais contexto
DEDUP_MAX_PARAGRAPH_CHARS: 1500  # Menos fragmentação = preservar narrativa
```

### Para Análise (Analyst)
```
MAX_DEDUP_PARAGRAPHS: 100  # Menos, mais focado
DEDUP_MAX_PARAGRAPH_CHARS: 800  # Mais curto = análise mais precisa
PRESERVE_PARAGRAPH_ORDER: True  # Manter estrutura
```

---

## 8. Comparação Visual: Density Thresholds

```
Text: "Lorem ipsum dolor. Sit amet."  (30 chars)
Split by \n\n: 1 part
Density: 30 / 1 = 30 chars/parágrafo

┌─────────────────────────────────────────────────┐
│  Density Threshold                              │
├─────────────────────────────────────────────────┤
│  0 ────── 500 ────── 1000 ────── 1500 ─── 2000 │
│           |                              |      │
│      PipeHaystack threshold         PipeLang    │
│      (>1000 = markdown)             (>2000)    │
│                                                 │
│   Zona de Decisão: 1000-2000 chars/par         │
│   PipeHaystack: usa linha-por-linha             │
│   PipeLangNew: usa duplo newline                │
└─────────────────────────────────────────────────┘
```

---

## 9. Conclusão

| Pipe | Quando Usar | Força | Fraqueza |
|------|-------------|-------|----------|
| **tool_discovery** | URLs only | Rápido, simples | Sem contexto bruto |
| **PipeLangNew** | Síntese final | Menos fragmentação | Pode perder estrutura em markdown |
| **PipeHaystack** | Análise detalhada | Fragmentação controlada | Um pouco mais overhead |

**Recomendação:**
- Use **PipeLangNew** para síntese rápida
- Use **PipeHaystack** para análise com menos risco de perda de contexto
- Tune `DEDUP_MAX_PARAGRAPH_CHARS` conforme necessário
