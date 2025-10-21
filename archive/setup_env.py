#!/usr/bin/env python3
"""
Script simples para configurar variáveis de ambiente para o SearchSystem
Compatível com OpenWebUI - tudo inline
"""

import os

def setup_environment():
    """Configura as variáveis de ambiente necessárias"""
    print("🔧 Configurando variáveis de ambiente para SearchSystem...")
    
    # Verificar se já está configurado
    if os.getenv("OPENAI_API_KEY"):
        print("✅ OPENAI_API_KEY já está configurado")
    else:
        print("❌ OPENAI_API_KEY não encontrado")
        print("📝 Para configurar, execute:")
        print("   set OPENAI_API_KEY=your_api_key_here")
        print("   ou")
        print("   $env:OPENAI_API_KEY='your_api_key_here'")
        return False
    
    # Configurar outras variáveis se necessário
    if not os.getenv("OPENAI_BASE_URL"):
        os.environ["OPENAI_BASE_URL"] = "https://api.openai.com/v1"
        print("✅ OPENAI_BASE_URL configurado para padrão")
    
    if not os.getenv("LLM_MODEL"):
        os.environ["LLM_MODEL"] = "gpt-4o"
        print("✅ LLM_MODEL configurado para padrão")
    
    print("\n✅ Configuração concluída!")
    print("📋 Variáveis configuradas:")
    print(f"   OPENAI_API_KEY: {'*' * 20}")
    print(f"   OPENAI_BASE_URL: {os.getenv('OPENAI_BASE_URL')}")
    print(f"   LLM_MODEL: {os.getenv('LLM_MODEL')}")
    
    return True

if __name__ == "__main__":
    setup_environment()
