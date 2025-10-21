# 🚀 PipeLangNew - Sistema de Pesquisa Inteligente Multi-Agent

Sistema avançado de pesquisa que combina LangGraph com avaliação holística de completude. Implementa fluxo multi-agent com has-enough-context mechanism para pesquisas abrangentes e inteligentes.

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Fluxo Principal](#fluxo-principal)
- [Has-Enough-Context Mechanism](#has-enough-context-mechanism)
- [Ferramentas Auxiliares](#ferramentas-auxiliares)
- [Configuração](#configuração)
- [Casos de Uso](#casos-de-uso)
- [Instalação e Uso](#instalação-e-uso)

## 🎯 Visão Geral

O PipeLangNew é um sistema de pesquisa inteligente que utiliza múltiplos agentes LLM coordenados via LangGraph para realizar pesquisas abrangentes e estruturadas. O sistema implementa um mecanismo de "has-enough-context" que avalia continuamente a completude da pesquisa e gera dinamicamente novas fases quando necessário.

### Características Principais

- **Multi-Agent Architecture**: Coordenador, Planner, Researcher, Analyst, Judge, Reporter
- **Has-Enough-Context Mechanism**: Avaliação holística de completude (Deerflow-style)
- **Dynamic Phase Generation**: Criação automática de fases adicionais
- **Router V3**: Decisões inteligentes baseadas em completeness gates
- **Telemetria Completa**: Monitoramento em tempo real e métricas detalhadas
- **State Validation**: Prevenção de corrupção e recovery robusto

## 🏗️ Arquitetura

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   COORDENADOR   │───▶│     PLANNER     │───▶│   RESEARCHER    │
│                 │    │                 │    │                 │
│ • Analisa query │    │ • Cria plano    │    │ • Descobre URLs │
│ • Roteia fluxo  │    │ • Define fases  │    │ • Scraping      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     ANALYST     │───▶│      JUDGE      │───▶│   ROUTER V3     │
│                 │    │                 │    │                 │
│ • Extrai fatos  │    │ • Completeness  │    │ • Decisões      │
│ • Estrutura     │    │ • Local/Global  │    │ • Próxima ação  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  GLOBAL_CHECK   │───▶│ GENERATE_PHASES │───▶│    REPORTER     │
│                 │    │                 │    │                 │
│ • Avaliação     │    │ • Cria fases    │    │ • Síntese final │
│ • Holística     │    │ • Adicionais    │    │ • Insights      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🔄 Fluxo Principal

### 1. **COORDENADOR** - Análise e Roteamento
- Analisa a query do usuário
- Determina o tipo de pesquisa necessária
- Roteia para PLANNER (pesquisas complexas) ou RESEARCHER (pesquisas diretas)

### 2. **PLANNER** - Criação de Plano Estruturado
- Cria plano de pesquisa com múltiplas fases
- Define objetivos específicos para cada fase
- Configura seed queries e must_terms
- Estabelece janelas temporais e critérios de qualidade

### 3. **RESEARCHER** - Descoberta e Scraping
- Executa descoberta de URLs relevantes
- Realiza scraping de conteúdo
- Aplica filtros de qualidade e relevância
- Coleta evidências e fontes primárias

### 4. **ANALYST** - Análise e Estruturação
- Analisa conteúdo coletado
- Extrai fatos estruturados com confiança
- Identifica lacunas e contradições
- Estrutura informações para análise

### 5. **JUDGE** - Avaliação de Completude
- **Local Completeness**: Avalia completude por fase (threshold 0.85)
- **Global Completeness**: Avalia completude cross-fase (threshold 0.85)
- Combina estimativa LLM (60%) + métricas objetivas (40%)
- Identifica dimensões faltantes e gaps

### 6. **ROUTER V3** - Decisões Inteligentes
- **Priority 1**: High local completeness (≥0.85) → next phase ou global check
- **Priority 2**: Max loops → global check ou next phase
- **Priority 3**: Done verdict com moderate completeness → global check
- **Priority 4**: Failed query → recovery
- **Priority 5**: Flat streak → global check
- **Priority 6**: Semantic loop detection → global check

### 7. **GLOBAL_CHECK** - Avaliação Holística
- Avalia completude acumulada de todas as fases
- Identifica dimensões faltantes
- Determina se pesquisa está completa
- Sugere fases adicionais se necessário

### 8. **GENERATE_PHASES** - Geração Dinâmica
- Cria até 3 fases adicionais quando global < 0.85
- Foca em dimensões identificadas como faltantes
- Mantém consistência com plano original
- Valida estrutura das novas fases

### 9. **REPORTER** - Síntese Final
- Consolida todas as informações coletadas
- Gera insights e recomendações
- Estrutura relatório final
- Exporta resultados em múltiplos formatos

## 🧠 Has-Enough-Context Mechanism

### Local Completeness (Por Fase)
```python
completeness = w1 * coverage + w2 * fact_quality + w3 * source_diversity - w4 * contradiction_score
```
- **w1 = 0.40**: Cobertura do objetivo da fase
- **w2 = 0.30**: Qualidade dos fatos (alta/média/baixa confiança)
- **w3 = 0.20**: Diversidade de fontes (domínios únicos)
- **w4 = 0.50**: Penalização por contradições

### Global Completeness (Cross-Fase)
- **Cobertura Dimensional (40%)**: Todas dimensões relevantes exploradas?
- **Qualidade das Fontes (25%)**: Fontes diversas, primárias, recentes?
- **Profundidade (20%)**: 30+ fatos, nível de detalhe adequado?
- **Consistência (15%)**: Informações consistentes, contradições resolvidas?

### Dynamic Phase Generation
- Gera até 3 fases adicionais por iteração
- Máximo 2 iterações de global check + phase generation
- Foca em dimensões identificadas como faltantes
- Mantém consistência com plano original

## 🛠️ Ferramentas Auxiliares

### `tool_discovery.py`
- **Função**: Descoberta de URLs relevantes
- **Algoritmo**: TF-IDF similarity + domain diversity
- **Configuração**: Max URLs, similarity threshold, domain limits

### `tool_content_scraperv5_production_grade_clean.py`
- **Função**: Scraping de conteúdo web
- **Recursos**: Anti-bot detection, content cleaning, error handling
- **Configuração**: Timeouts, retry logic, content filters

### `tool_reduce_context_from_scraper_fixed.py`
- **Função**: Redução de contexto para LLM
- **Algoritmo**: Semantic chunking + relevance scoring
- **Configuração**: Max tokens, chunk size, relevance threshold

### `tool_export_pdf.py`
- **Função**: Exportação de resultados para PDF
- **Recursos**: Formatação profissional, tabelas, gráficos
- **Configuração**: Template, styling, metadata

### `tool_simplesearch.py`
- **Função**: Busca simples e direta
- **Uso**: Pesquisas rápidas e específicas
- **Configuração**: Query limits, result formatting

## ⚙️ Configuração

### Valves (50+ Parâmetros Configuráveis)

#### Has-Enough-Context Configuration
```python
ENABLE_GLOBAL_COMPLETENESS_CHECK: bool = True
GLOBAL_COMPLETENESS_THRESHOLD: float = 0.85
LOCAL_COMPLETENESS_THRESHOLD: float = 0.85
MAX_ADDITIONAL_PHASES: int = 3
MAX_GLOBAL_ITERATIONS: int = 2
```

#### Orquestração
```python
USE_LANGGRAPH: bool = True
MAX_AGENT_LOOPS: int = 3
DEFAULT_PHASE_COUNT: int = 6
MAX_PHASES: int = 6
```

#### Qualidade e Filtros
```python
MIN_UNIQUE_DOMAINS: int = 3
NEED_OFFICIAL_OR_TWO_INDEPENDENT: bool = True
ENABLE_DOMAIN_DIVERSITY_GUARD: bool = True
```

### Profiles Disponíveis
- **company_profile**: Pesquisas de empresas e competidores
- **regulation_review**: Análises regulatórias e compliance
- **market_analysis**: Estudos de mercado e tendências
- **due_diligence**: Investigações e verificações

## 🎯 Casos de Uso

### 1. Pesquisas de Mercado
- **Objetivo**: Mapear mercado, competidores, tendências
- **Fases**: Volume setorial, perfis de empresas, notícias recentes
- **Output**: Relatório de mercado com insights e recomendações

### 2. Análises Regulatórias
- **Objetivo**: Compliance, mudanças regulatórias, impactos
- **Fases**: Regulamentações atuais, mudanças propostas, impactos
- **Output**: Análise de compliance com recomendações

### 3. Due Diligence
- **Objetivo**: Investigação de empresas, pessoas, transações
- **Fases**: Histórico, reputação, associações, notícias
- **Output**: Relatório de due diligence com red flags

### 4. Estudos de Tendências
- **Objetivo**: Identificar tendências, inovações, oportunidades
- **Fases**: Análise temporal, players emergentes, tecnologias
- **Output**: Relatório de tendências com projeções

## 🚀 Instalação e Uso

### Pré-requisitos
```bash
pip install langgraph langchain openai anthropic
pip install beautifulsoup4 requests tiktoken
pip install pydantic typing-extensions
```

### Uso Básico
```python
from PipeLangNew import Pipe

# Configuração
valves = Pipe.Valves(
    DEFAULT_PHASE_COUNT=4,
    MAX_AGENT_LOOPS=3,
    ENABLE_GLOBAL_COMPLETENESS_CHECK=True
)

# Execução
pipe = Pipe(valves=valves)
result = await pipe.pipe(
    user_query="Pesquisar mercado de IA no Brasil",
    correlation_id="research_001"
)
```

### Uso Avançado
```python
# Configuração personalizada
valves = Pipe.Valves(
    # Has-enough-context
    GLOBAL_COMPLETENESS_THRESHOLD=0.90,
    LOCAL_COMPLETENESS_THRESHOLD=0.85,
    MAX_ADDITIONAL_PHASES=5,
    
    # Orquestração
    MAX_AGENT_LOOPS=5,
    DEFAULT_PHASE_COUNT=8,
    
    # Qualidade
    MIN_UNIQUE_DOMAINS=5,
    ENABLE_DOMAIN_DIVERSITY_GUARD=True
)

# Execução com telemetria
pipe = Pipe(valves=valves)
result = await pipe.pipe(
    user_query="Análise completa do setor de fintechs brasileiras",
    correlation_id="fintech_analysis_001",
    profile="company_profile"
)
```

## 📊 Telemetria e Monitoramento

### Event Emission
- **Real-time events**: Progresso da pesquisa em tempo real
- **Completeness metrics**: Métricas de completude local e global
- **Phase generation**: Tracking de fases adicionais criadas
- **Error handling**: Captura e report de erros

### Telemetry Sink
- **Structured data**: Dados estruturados para análise
- **Usage tracking**: Monitoramento de uso de recursos
- **Performance metrics**: Métricas de performance e eficiência
- **Cost estimation**: Estimativa de custos de LLM

## 🔧 Troubleshooting

### Problemas Comuns
1. **Timeout errors**: Ajustar `LLM_CALL_TIMEOUT` e `TOOL_EXECUTION_TIMEOUT`
2. **Memory issues**: Reduzir `MAX_PHASES` e `DEFAULT_PHASE_COUNT`
3. **Low completeness**: Ajustar thresholds ou aumentar `MAX_ADDITIONAL_PHASES`
4. **Loop detection**: Verificar `MAX_AGENT_LOOPS` e `flat_streak_max`

### Debug Mode
```python
valves = Pipe.Valves(
    DEBUG_LOGGING=True,
    VERBOSE_DEBUG=True,
    ENABLE_LINE_BUDGET_GUARD=True
)
```

## 📈 Roadmap

### Próximas Funcionalidades
- [ ] **Multi-language support**: Pesquisas em múltiplos idiomas
- [ ] **Custom LLM providers**: Suporte a outros provedores LLM
- [ ] **Advanced analytics**: Dashboard de métricas e insights
- [ ] **API endpoints**: REST API para integração
- [ ] **Batch processing**: Processamento em lote de pesquisas

### Melhorias Planejadas
- [ ] **Performance optimization**: Otimização de performance
- [ ] **Cost reduction**: Redução de custos de LLM
- [ ] **Accuracy improvement**: Melhoria na precisão das avaliações
- [ ] **User experience**: Interface mais intuitiva

---

## 📞 Suporte

Para dúvidas, sugestões ou problemas:
- **Issues**: Abra uma issue no repositório
- **Documentation**: Consulte a documentação completa
- **Examples**: Veja exemplos de uso na pasta `archive/`

---

**🎊 PipeLangNew - Pesquisa Inteligente, Resultados Excepcionais!**
