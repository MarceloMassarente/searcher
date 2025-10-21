# ✅ Deploy Checklist - PipeLangNew v4.0

## Pré-Deploy (Local)

- [ ] Testes passando: `python -c "from PipeLangNew import run_langgraph_tests; run_langgraph_tests()"`
- [ ] Dependências verificadas: `pip list | grep -E "langgraph|langchain|pydantic|sklearn"`
- [ ] Tools testados: Discovery + Scraper funcionais
- [ ] Valves configuradas: USE_LANGGRAPH=true

## Deploy em Staging

- [ ] Upload de PipeLangNew.py no OpenWebUI
- [ ] Smoke Test 1: Query simples (padrão)
- [ ] Smoke Test 2: Query vaga (clarificação)
- [ ] Smoke Test 3: Query comparativa (planner)
- [ ] Telemetria validada: Logs estruturados presentes
- [ ] Performance check: <50ms overhead por validação

## Monitoramento (24h)

- [ ] Zero crashes por state corruption
- [ ] <1% semantic loops detectados
- [ ] Telemetria completa (100% coverage)
- [ ] Cost tracking funcional ($/query)

## Go/No-Go para Produção

### Go (Promover para Prod):
- ✅ Todos os smoke tests passaram
- ✅ Zero crashes em 24h
- ✅ Telemetria operacional
- ✅ Métricas dentro do target

### No-Go (Rollback):
- ❌ >1 crash por state corruption
- ❌ >5% semantic loops
- ❌ Telemetria falhando
- ❌ Latency >100ms overhead

## Rollback (Se necessário)

- [ ] Desabilitar USE_LANGGRAPH
- [ ] Ou: Reverter para v3.0.0-multi-agent
- [ ] Notificar usuários
- [ ] Investigar root cause

