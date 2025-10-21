#!/usr/bin/env python3
"""
🔍 Ferramenta de Análise de Deduplicação

Analisa fragmentação de parágrafos e ajuda diagnosticar problemas.
"""

def analyze_fragmentation(
    total_chars: int,
    num_paragraphs: int,
    max_target_paragraphs: int,
    individual_sizes: list = None
) -> dict:
    """
    Analisa se a fragmentação é apropriada
    
    Args:
        total_chars: total de caracteres originais
        num_paragraphs: número de parágrafos extraído
        max_target_paragraphs: target após deduplicação
        individual_sizes: lista com tamanho de cada parágrafo (opcional)
    
    Returns:
        dict com diagnóstico
    """
    
    avg_size = total_chars / num_paragraphs if num_paragraphs > 0 else 0
    reduction_factor = num_paragraphs / max_target_paragraphs if max_target_paragraphs > 0 else 0
    final_avg_size = total_chars / max_target_paragraphs if max_target_paragraphs > 0 else 0
    
    # Calcular percentis se temos dados individuais
    stats = {
        "total_chars": total_chars,
        "num_paragraphs": num_paragraphs,
        "target_paragraphs": max_target_paragraphs,
        "avg_size": avg_size,
        "reduction_factor": reduction_factor,
        "final_avg_size": final_avg_size,
    }
    
    if individual_sizes:
        sorted_sizes = sorted(individual_sizes)
        n = len(sorted_sizes)
        stats.update({
            "min_size": sorted_sizes[0],
            "max_size": sorted_sizes[-1],
            "median_size": sorted_sizes[n // 2],
            "p25": sorted_sizes[int(n * 0.25)],
            "p75": sorted_sizes[int(n * 0.75)],
            "p95": sorted_sizes[int(n * 0.95)],
        })
    
    # Avaliação
    health = "✅ OK"
    warnings = []
    
    # Verificações
    if avg_size < 100:
        health = "⚠️ AVISO"
        warnings.append(f"Parágrafos muito pequenos ({avg_size:.0f} chars avg)")
    
    if avg_size > 2000:
        health = "⚠️ AVISO"
        warnings.append(f"Parágrafos gigantes ({avg_size:.0f} chars avg)")
    
    if reduction_factor > 40:
        health = "🔴 CRÍTICO"
        warnings.append(f"Redução extremamente agressiva ({reduction_factor:.1f}x)")
    elif reduction_factor > 20:
        health = "⚠️ AVISO"
        warnings.append(f"Redução muito agressiva ({reduction_factor:.1f}x)")
    
    if final_avg_size < 50:
        health = "🔴 CRÍTICO"
        warnings.append(f"Resultado final será fragmentado ({final_avg_size:.0f} chars/parágrafo)")
    elif final_avg_size < 100:
        health = "⚠️ AVISO"
        warnings.append(f"Resultado final pode ser fragmentado ({final_avg_size:.0f} chars/parágrafo)")
    
    stats["health"] = health
    stats["warnings"] = warnings
    
    return stats


def print_analysis(stats: dict):
    """Imprime análise formatada"""
    print("\n" + "="*60)
    print("🔍 ANÁLISE DE FRAGMENTAÇÃO DE DEDUPLICAÇÃO")
    print("="*60 + "\n")
    
    # Status geral
    print(f"{stats['health']} Status: {len(stats.get('warnings', []))} aviso(s)")
    print()
    
    # Números principais
    print("📊 NÚMEROS PRINCIPAIS:")
    print(f"  • Total de caracteres:   {stats['total_chars']:,} chars")
    print(f"  • Parágrafos extraídos:  {stats['num_paragraphs']:,}")
    print(f"  • Target após dedup:     {stats['target_paragraphs']:,}")
    print(f"  • Fator redução:         {stats['reduction_factor']:.1f}x")
    print()
    
    # Tamanhos
    print("📐 TAMANHO DE PARÁGRAFOS:")
    print(f"  • Média:                 {stats['avg_size']:.0f} chars")
    if 'median_size' in stats:
        print(f"  • Mediana:               {stats['median_size']:.0f} chars")
        print(f"  • Min/Max:               {stats['min_size']:.0f} / {stats['max_size']:.0f} chars")
        print(f"  • P25/P75:               {stats['p25']:.0f} / {stats['p75']:.0f} chars")
        print(f"  • P95:                   {stats['p95']:.0f} chars")
    print()
    
    # Resultado esperado
    print("📈 RESULTADO ESPERADO:")
    print(f"  • Tamanho médio final:   {stats['final_avg_size']:.0f} chars/parágrafo")
    print()
    
    # Avisos
    if stats['warnings']:
        print("⚠️  AVISOS:")
        for i, warning in enumerate(stats['warnings'], 1):
            print(f"  {i}. {warning}")
        print()
    
    # Recomendações
    print("💡 RECOMENDAÇÕES:")
    
    if stats['reduction_factor'] > 40:
        print("  1. ⬆️  Aumentar MAX_DEDUP_PARAGRAPHS (atualmente agressivo demais)")
        suggested_target = int(stats['num_paragraphs'] / 20)  # Redução 20x em vez de 40x+
        print(f"     → Sugestão: {suggested_target} em vez de {stats['target_paragraphs']}")
        print()
    
    if stats['avg_size'] > 1500:
        print("  2. 🔍 Split detectou markdown com \\n único (densidade baixa)")
        print("     → Aumentar threshold: avg_paragraph_size > 2000 (era 1000)")
        print()
    
    if stats['final_avg_size'] < 100:
        print("  3. 🔗 Combinar parágrafos pequenos pós-dedup")
        print("     → Implementar merge para min 100-150 chars/parágrafo")
        print()
    
    if stats.get('p95', 0) > 3000:
        print("  4. 📏 Ajustar DEDUP_MAX_PARAGRAPH_CHARS (parágrafos muito grandes)")
        print("     → P95 = {:.0f} chars, considere quebrar em {:.0f}")
        print()
    
    print("="*60 + "\n")


# ============================================================================
# EXEMPLOS
# ============================================================================

if __name__ == "__main__":
    print("📋 EXEMPLOS DE ANÁLISE\n")
    
    # Cenário 1: Os números dos logs atuais
    print("\n[CENÁRIO 1] Números atuais dos logs:")
    print("Input: 11.433 chars → 9.299 parágrafos → 200 final")
    
    stats1 = analyze_fragmentation(
        total_chars=11433,
        num_paragraphs=9299,
        max_target_paragraphs=200,
        individual_sizes=None
    )
    print_analysis(stats1)
    
    # Cenário 2: Simulação com dados individuais (distribuição normal)
    print("\n[CENÁRIO 2] Simulação com distribuição normal de tamanhos:")
    import random
    random.seed(42)
    simulated_sizes = [random.gauss(150, 50) for _ in range(1000)]
    simulated_sizes = [max(50, int(s)) for s in simulated_sizes]
    simulated_total = sum(simulated_sizes)
    
    stats2 = analyze_fragmentation(
        total_chars=simulated_total,
        num_paragraphs=len(simulated_sizes),
        max_target_paragraphs=200,
        individual_sizes=simulated_sizes
    )
    print_analysis(stats2)
    
    # Cenário 3: Configuração recomendada
    print("\n[CENÁRIO 3] Configuração melhor (target 500 em vez de 200):")
    stats3 = analyze_fragmentation(
        total_chars=11433,
        num_paragraphs=9299,
        max_target_paragraphs=500,
        individual_sizes=None
    )
    print_analysis(stats3)
    
    # Cenário 4: Se split fosse melhor (menos parágrafos iniciais)
    print("\n[CENÁRIO 4] Se split fosse melhor (2.000 parágrafos em vez de 9.299):")
    stats4 = analyze_fragmentation(
        total_chars=11433,
        num_paragraphs=2000,
        max_target_paragraphs=200,
        individual_sizes=None
    )
    print_analysis(stats4)
