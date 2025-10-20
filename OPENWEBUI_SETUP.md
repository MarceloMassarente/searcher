# Configuração do PipeLangNew para OpenWebUI

## 📋 Resumo

O `PipeLangNew.py` é um Pipe compatível com OpenWebUI que agora carrega automaticamente as configurações LLM de **três fontes** (em ordem de prioridade):

1. **Valves do OpenWebUI** (quando você clica no ícone de engrenagem na UI)
2. **Variáveis de Ambiente** (fallback se OpenWebUI não passar Valves)
3. **Defaults** (último recurso, apenas para desenvolvimento local)

## 🔑 Configurações Necessárias

### Opção A: Variáveis de Ambiente (Recomendado para OpenWebUI em Produção)

Configure estas variáveis de ambiente no seu servidor OpenWebUI:

```bash
# Linux/Mac
export OPENAI_API_KEY="sk-..."
export OPENAI_BASE_URL="https://api.openai.com/v1"
export LLM_MODEL="gpt-4o"

# Windows PowerShell
$env:OPENAI_API_KEY="sk-..."
$env:OPENAI_BASE_URL="https://api.openai.com/v1"
$env:LLM_MODEL="gpt-4o"

# Windows CMD
set OPENAI_API_KEY=sk-...
set OPENAI_BASE_URL=https://api.openai.com/v1
set LLM_MODEL=gpt-4o
```

### Opção B: Valves da UI OpenWebUI (Recomendado para Desenvolvimento)

1. Abra o OpenWebUI
2. Crie ou edite uma ferramenta/pipe
3. Clique no ícone de ⚙️ (engrenagem) para abrir as Valves
4. Configure os campos:
   - `OPENAI_API_KEY`: Sua chave de API OpenAI
   - `OPENAI_BASE_URL`: URL da base OpenAI (padrão: `https://api.openai.com/v1`)
   - `LLM_MODEL`: Modelo a usar (padrão: `gpt-4o`)

## ✅ Verificação

Para verificar se a configuração está funcionando:

```python
import os
from PipeLangNew import Pipe

# Testar se as variáveis de ambiente estão configuradas
api_key = os.getenv("OPENAI_API_KEY")
base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
model = os.getenv("LLM_MODEL", "gpt-4o")

print(f"API Key: {'✅ Set' if api_key else '❌ Not set'}")
print(f"Base URL: {base_url}")
print(f"Model: {model}")

# Criar instância do Pipe
pipe = Pipe()
print(f"Pipe initialized: ✅")
print(f"LLM Config: {pipe.valves.LLM_MODEL}")
```

## 🚨 Troubleshooting

### Erro: "LLM configuration missing"

**Causa**: OpenWebUI não passou Valves e variáveis de ambiente não estão configuradas.

**Solução**:
1. Configure variáveis de ambiente (Opção A acima)
2. OU configure Valves na UI (Opção B acima)

### Erro: "Invalid API Key"

**Causa**: A chave de API fornecida é inválida.

**Solução**:
1. Verifique se a chave começa com `sk-`
2. Verifique se a chave não foi truncada
3. Gere uma nova chave no painel OpenAI

### Erro: "Connection refused"

**Causa**: A URL base está incorreta ou o servidor OpenAI está indisponível.

**Solução**:
1. Verifique se a URL é `https://api.openai.com/v1`
2. Teste a conectividade: `curl https://api.openai.com/v1/models -H "Authorization: Bearer sk-..."`

## 📝 Exemplo Completo

```bash
# 1. Configure variáveis de ambiente
export OPENAI_API_KEY="sk-proj-xxxxxxxxxxxx"
export OPENAI_BASE_URL="https://api.openai.com/v1"
export LLM_MODEL="gpt-4o"

# 2. Inicie seu OpenWebUI
python -m openwebui serve

# 3. Adicione PipeLangNew.py como Pipe/Ferramenta
# (via UI do OpenWebUI)

# 4. Use no chat
# "Pesquise sobre inteligência artificial no Brasil"
```

## 🔐 Segurança

**NUNCA** coloque sua `OPENAI_API_KEY` no código ou no Git!

Use apenas:
- Variáveis de ambiente
- Secrets do OpenWebUI
- Arquivos `.env` (adicione `.env` ao `.gitignore`)

## 📚 Mais Informações

- [Documentação OpenWebUI](https://docs.openwebui.com/)
- [OpenAI API Docs](https://platform.openai.com/docs)
- [PipeLangNew.py](./PipeLangNew.py)
