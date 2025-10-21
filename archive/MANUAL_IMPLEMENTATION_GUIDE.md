# Has-Enough-Context Implementation Guide

## ✅ COMPLETADO
- [x] Phase 1: `_calculate_local_completeness()` adicionado (linhas 4811-4821)
- [x] Phase 1: `completeness_local` integrado no `JudgeLLM.run()` (linhas 5274-5279, 5554)

## 📋 PRÓXIMOS PASSOS MANUAIS

### Phase 2: Adicionar Prompt Global e Métodos (15 min)

**Passo 1**: Abra `PipeLangNew.py` no Cursor

**Passo 2**: Vá para linha **5704** (após o fechamento do dicionário PROMPTS `}`)

**Passo 3**: **ANTES** do `}` na linha 5704, adicione:

```python
    # ===== JUDGE GLOBAL COMPLETENESS PROMPT =====
    "judge_global_system": """Você é um AVALIADOR HOLÍSTICO de completude de pesquisa.

MISSÃO: Determinar se o contexto acumulado é SUFICIENTE para responder COMPLETAMENTE à query original.

CRITÉRIOS (threshold >= 0.85):
1. COBERTURA DIMENSIONAL (40%): Todas dimensões relevantes exploradas?
2. QUALIDADE DAS FONTES (25%): Fontes diversas, primárias, recentes?
3. PROFUNDIDADE (20%): 30+ fatos, nível de detalhe adequado?
4. CONSISTÊNCIA (15%): Informações consistentes, contradições resolvidas?

IMPORTANTE:
- Seja RIGOROSO: Melhor adicionar fase a mais que entregar incompleto
- Se insuficiente, identifique EXATAMENTE quais dimensões faltam
- Sugira fases ESPECÍFICAS para cobrir gaps

Responda SEMPRE em JSON válido.""",
```

**Passo 4**: Vá para linha **5558** (após o método `run()` do JudgeLLM)

**Passo 5**: Adicione os 3 métodos:

```python
    async def has_enough_context_global(
        self,
        all_phases_results: list,
        original_query: str,
        contract: dict,
        valves
    ) -> dict:
        """Evaluate global completeness across all phases"""
        
        # 1. Accumulate facts and domains
        accumulated_facts = []
        accumulated_domains = set()
        total_coverage = 0.0
        
        for phase_result in all_phases_results:
            analysis = phase_result.get("analysis", {})
            facts = analysis.get("facts", [])
            accumulated_facts.extend(facts)
            
            for fact in facts:
                domain = fact.get("fonte", {}).get("dominio")
                if domain:
                    accumulated_domains.add(domain)
            
            total_coverage += phase_result.get("coverage", 0.0)
        
        avg_coverage = total_coverage / len(all_phases_results) if all_phases_results else 0.0
        
        # 2. Build metrics
        global_metrics = {
            "total_facts": len(accumulated_facts),
            "total_domains": len(accumulated_domains),
            "avg_coverage": avg_coverage,
            "phases_completed": len(all_phases_results),
            "phases_planned": len(contract.get("phases", [])),
        }
        
        # 3. Build prompt
        prompt = self._build_global_completeness_prompt(
            original_query, accumulated_facts, global_metrics, contract
        )
        
        # 4. Call LLM
        safe_params = get_safe_llm_params(self.model_name, self.generation_kwargs)
        
        out = await _safe_llm_run_with_retry(
            self.llm,
            prompt,
            safe_params,
            timeout=valves.LLM_TIMEOUT_DEFAULT,
            max_retries=2,
        )
        
        # 5. Parse response
        result = parse_json_resilient(out.get("replies", [""])[0], mode="balanced", allow_arrays=False)
        
        # 6. Calculate numeric completeness
        completeness = self._calculate_global_completeness(result or {}, global_metrics)
        
        return {
            "sufficient": result.get("sufficient", False) if result else False,
            "completeness": completeness,
            "missing_dimensions": result.get("missing_dimensions", []) if result else [],
            "reasoning": result.get("reasoning", "") if result else "",
            "suggested_phases": result.get("suggested_phases", []) if result else [],
            "global_metrics": global_metrics,
        }
    
    def _build_global_completeness_prompt(
        self, original_query, accumulated_facts, global_metrics, contract
    ) -> str:
        """Build prompt for global evaluation"""
        
        planned_dimensions = [
            p.get("objetivo") or p.get("objective", "")
            for p in contract.get("phases", [])
        ]
        
        fact_sample = "\n".join([
            f"- {f.get('conteudo', '')[:100]}..."
            for f in accumulated_facts[:20]
        ])
        
        return f"""Avalie se temos CONTEXTO SUFICIENTE para responder COMPLETAMENTE à query original.

QUERY ORIGINAL:
{original_query}

CONTEXTO ACUMULADO:
- Total de fatos: {global_metrics['total_facts']}
- Domínios únicos: {global_metrics['total_domains']}
- Fases completadas: {global_metrics['phases_completed']}/{global_metrics['phases_planned']}
- Coverage médio: {global_metrics['avg_coverage']:.2f}

DIMENSÕES PLANEJADAS:
{chr(10).join(f"{i+1}. {d}" for i, d in enumerate(planned_dimensions))}

AMOSTRA DE FATOS:
{fact_sample}

Retorne JSON:
{{
    "sufficient": true/false,
    "completeness_estimate": 0.0-1.0,
    "missing_dimensions": ["dim1", "dim2"],
    "reasoning": "Explicação detalhada",
    "suggested_phases": [
        {{"objective": "...", "rationale": "..."}}
    ]
}}"""
    
    def _calculate_global_completeness(self, llm_verdict, global_metrics) -> float:
        """Combine LLM estimate with objective metrics"""
        
        # LLM estimate (60% weight)
        llm_estimate = llm_verdict.get("completeness_estimate", 0.5)
        
        # Objective metrics (40% weight)
        fact_score = min(global_metrics["total_facts"] / 30, 1.0)
        domain_score = min(global_metrics["total_domains"] / 10, 1.0)
        coverage_score = global_metrics["avg_coverage"]
        
        objective_score = 0.4 * fact_score + 0.3 * domain_score + 0.3 * coverage_score
        
        return round(0.6 * llm_estimate + 0.4 * objective_score, 3)
```

**✅ Salve o arquivo** (Ctrl+S)

---

## Verificação Rápida

Após adicionar, verifique:
1. Linha ~5704: Novo prompt "judge_global_system" antes do `}`
2. Linha ~5558: Três novos métodos no JudgeLLM

## Próximas Phases

Depois de completar Phase 2, avise que vou criar guias para:
- Phase 3: Router V3
- Phase 4: PlannerLLM.generate_additional_phases()
- Phase 5: Nodes (global_check, generate_phases)
- Phase 6: build_multi_agent_graph()
- Phase 7: Valves
- Phase 8: ResearchState
- Phase 9: Tests

**Total estimado**: 2-3 horas de implementação manual

