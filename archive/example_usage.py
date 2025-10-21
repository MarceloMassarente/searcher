#!/usr/bin/env python3
"""
Exemplo de uso do PipeLangNew.py para OpenWebUI
Mostra como configurar e usar o pipe inline
"""

import os
import asyncio
from PipeLangNew import Pipe

async def main():
    """Exemplo de uso do PipeLangNew"""
    
    # Configurar variáveis de ambiente (se não estiverem configuradas)
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ OPENAI_API_KEY não configurado!")
        print("Configure com: set OPENAI_API_KEY=your_key_here")
        return
    
    # Criar instância do Pipe
    pipe = Pipe()
    
    # Exemplo de uso
    body = {
        "messages": [
            {
                "role": "user",
                "content": "Pesquise sobre inteligência artificial no Brasil"
            }
        ]
    }
    
    print("🚀 Iniciando pesquisa...")
    
    try:
        # Executar o pipe
        async for chunk in pipe.pipe(body):
            print(chunk, end="", flush=True)
            
    except Exception as e:
        print(f"\n❌ Erro: {e}")

if __name__ == "__main__":
    asyncio.run(main())
