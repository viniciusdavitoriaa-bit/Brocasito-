# Deploy do Bot YOV

## Pré-requisitos

- Conta no [Railway](https://railway.app) ou [Render](https://render.com)
- Conta no [GitHub](https://github.com)
- Token do bot Discord (obtido no [Discord Developer Portal](https://discord.com/developers/applications))

---

## 1. Configurar variável de ambiente

O bot lê o token pela variável `DISCORD_TOKEN`.  
**Nunca coloque o token diretamente no código ou no repositório.**

Crie um arquivo `.env` local (só para testes, não commitar):
```
DISCORD_TOKEN=seu_token_aqui
```

---

## 2. Subir no GitHub

```bash
git clone https://github.com/viniciusdavitoriaa-bit/Brocasito-.git
cd Brocasito-

# Copie os arquivos desta pasta para a raiz do repositório
cp bot.py requirements.txt runtime.txt Procfile nixpacks.toml railway.json render.yaml .

git add .
git commit -m "feat: adiciona bot e configurações de deploy"
git push origin main
```

> **Importante:** adicione `data.json` e `.env` ao `.gitignore` para não vazar dados e configurações.

Exemplo de `.gitignore`:
```
.env
data.json
data.json.bak
__pycache__/
*.pyc
```

---

## 3. Deploy no Railway

1. Acesse [railway.app](https://railway.app) e clique em **New Project**.
2. Selecione **Deploy from GitHub repo** e escolha `Brocasito-`.
3. Railway detecta o `nixpacks.toml` automaticamente.
4. Vá em **Variables** e adicione:
   - `DISCORD_TOKEN` = `seu_token_aqui`
5. Clique em **Deploy**. O bot inicia como **worker** (sem porta exposta).

> O arquivo `railway.json` já configura reinício automático em caso de falha.

---

## 4. Deploy no Render (alternativa)

1. Acesse [render.com](https://render.com) e clique em **New > Background Worker**.
2. Conecte o repositório `Brocasito-`.
3. Render lê o `render.yaml` automaticamente.
4. Adicione a variável `DISCORD_TOKEN` no painel de **Environment**.
5. Clique em **Create Background Worker**.

---

## 5. Persistência do data.json

O `data.json` é criado automaticamente na primeira execução.  
No Railway/Render, o sistema de arquivos é **efêmero** — os dados são perdidos em cada redeploy.

**Solução recomendada:** use um volume persistente no Railway:
1. No painel do projeto → **Volumes** → **Add Volume**.
2. Monte em `/app` (ou o diretório onde o bot roda).
3. O `data.json` ficará salvo entre deploys.

---

## Arquivos incluídos

| Arquivo | Descrição |
|---|---|
| `bot.py` | Código principal do bot |
| `requirements.txt` | Dependências Python |
| `runtime.txt` | Versão do Python (3.11.9) |
| `Procfile` | Comando de inicialização (Heroku-style) |
| `nixpacks.toml` | Build config para Railway/Nixpacks |
| `railway.json` | Config específica do Railway |
| `render.yaml` | Config específica do Render |

---

## Comandos úteis (local)

```bash
# Instalar dependências
pip install -r requirements.txt

# Rodar o bot localmente
python bot.py
```
