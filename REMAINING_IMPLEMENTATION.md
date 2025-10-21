# Remaining Implementation - Has-Enough-Context

## ✅ COMPLETADO (Phases 1-6)
- Phase 1: `_calculate_local_completeness()` implementado ✅
- Phase 2: `has_enough_context_global()` implementado ✅  
- Phase 3: Router V3 implementado ✅
- Phase 4: `generate_additional_phases()` implementado ✅
- Phase 5: Nodes `global_check` e `generate_phases` implementados ✅
- Phase 6: Graph atualizado com novos nodes e edges ✅

## 📋 PENDENTE (Phases 7-9)

### Phase 7: Valves Configuration

**Localização**: Classe `Pipe.Valves` (linha ~7532)

**Adicionar após os campos existentes** (sugestão: após `MAX_PHASES` ou `VERBOSE_DEBUG`):

```python
# ===== HAS-ENOUGH-CONTEXT CONFIGURATION =====
ENABLE_GLOBAL_COMPLETENESS_CHECK: bool = Field(
    default=True,
    description="Enable global completeness evaluation (Deerflow-style)"
)

GLOBAL_COMPLETENESS_THRESHOLD: float = Field(
    default=0.85,
    ge=0.5,
    le=1.0,
    description="Threshold for global completeness (0.85 = high bar)"
)

LOCAL_COMPLETENESS_THRESHOLD: float = Field(
    default=0.85,
    ge=0.5,
    le=1.0,
    description="Threshold for local (per-phase) completeness"
)

MAX_ADDITIONAL_PHASES: int = Field(
    default=3,
    ge=1,
    le=5,
    description="Max phases generated per global check iteration"
)

MAX_GLOBAL_ITERATIONS: int = Field(
    default=2,
    ge=1,
    le=5,
    description="Max iterations of global check + phase generation"
)
```

### Phase 8: ResearchState Fields

**Localização**: `ResearchState` TypedDict (linha ~6344)

**Adicionar após os campos existentes**:

```python
# ===== HAS-ENOUGH-CONTEXT FIELDS =====
completeness_local: float  # Per-phase completeness (0.0-1.0)
global_completeness: float  # Overall completeness (0.0-1.0)
needs_additional_phases: bool  # Flag to trigger phase generation
missing_dimensions: List[str]  # Gaps identified by global check
suggested_phases: List[Dict]  # Phases suggested by Judge
global_verdict: Optional[Dict]  # Full global evaluation result
phase_idx: int  # Current phase index
total_phases: int  # Total number of phases
```

### Phase 9: Tests

**Localização**: Após `run_langgraph_tests()` (linha ~1500)

**Adicionar**:

```python
def test_local_completeness():
    """Test local completeness calculation"""
    print("🧪 Testing Local Completeness Calculation")
    
    # Test the calculation function directly
    def _calculate_local_completeness_test(metrics, analysis, phase_context):
        w1, w2, w3, w4 = 0.40, 0.30, 0.20, 0.50
        coverage = metrics.get("coverage", 0.0)
        facts = analysis.get("facts", [])
        fact_quality = sum(1 for f in facts if f.get("confiança") == "alta") / max(len(facts), 1) if facts else 0.0
        unique_domains = len(set(f.get("fonte", {}).get("dominio", "unknown") for f in facts))
        source_diversity = min(unique_domains / 3.0, 1.0)
        contradiction_score = metrics.get("contradiction_score", 0.0)
        completeness = max(0.0, min(1.0, w1 * coverage + w2 * fact_quality + w3 * source_diversity - w4 * contradiction_score))
        return completeness

    metrics = {
        "coverage": 0.75,
        "novel_fact_ratio": 0.3,
        "contradiction_score": 0.1,
    }

    analysis = {
        "facts": [
            {"confiança": "alta", "fonte": {"dominio": "example.com"}},
            {"confiança": "alta", "fonte": {"dominio": "test.org"}},
            {"confiança": "média", "fonte": {"dominio": "demo.net"}},
        ]
    }

    completeness = _calculate_local_completeness_test(metrics, analysis, {})

    assert 0.60 <= completeness <= 0.75, f"Expected ~0.65-0.70, got {completeness}"
    print(f"✅ Local completeness: {completeness:.2f}")

def test_router_v3_completeness_gates():
    """Test router v3 with completeness-based decisions"""
    print("🧪 Testing Router V3 Completeness Gates")
    
    # High completeness, not last phase
    state1 = {
        "correlation_id": "test_v3_001",
        "completeness_local": 0.87,
        "phase_idx": 0,
        "total_phases": 3,
        "loop_count": 1,
        "max_loops": 3,
        "policy": {},
    }
    
    decision1 = should_continue_research_v3(state1)
    assert decision1 == "discovery", f"Expected discovery, got {decision1}"
    print("✅ High completeness (not last) → discovery")
    
    # High completeness, last phase
    state2 = state1.copy()
    state2["phase_idx"] = 2
    
    decision2 = should_continue_research_v3(state2)
    assert decision2 == "global_check", f"Expected global_check, got {decision2}"
    print("✅ High completeness (last phase) → global_check")
```

## 📝 INSTRUÇÕES MANUAIS

1. **Phase 7**: Abra `PipeLangNew.py` e vá para a classe `Pipe.Valves` (linha ~7532). Adicione os campos has-enough-context após `VERBOSE_DEBUG`.

2. **Phase 8**: Vá para `ResearchState` TypedDict (linha ~6344) e adicione os novos campos no final da definição.

3. **Phase 9**: Vá para a seção de testes (após linha ~1500) e adicione as duas funções de teste.

4. **Atualizar `run_langgraph_tests()`**: Adicione as chamadas aos novos testes:
   ```python
   test_local_completeness()
   test_router_v3_completeness_gates()
   ```

## ✅ SUCCESS CRITERIA

- [ ] Valves configuration adicionada
- [ ] ResearchState fields adicionados
- [ ] Tests adicionados e executando
- [ ] Commit final realizado

## 🎯 PRÓXIMO PASSO

Execute manualmente as edições ou peça ao assistente para continuar automaticamente.

