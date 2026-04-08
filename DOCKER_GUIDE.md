# 🐳 WeatherBot — Docker Deployment Guide

Uma imagem Docker leve, otimizada e pronta para VPS.

---

## 📋 Pré-requisitos

```bash
# Instalar Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Instalar Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verificar instalação
docker --version
docker-compose --version
```

---

## 🚀 Quick Start

### 1️⃣ Clone/configure o projeto

```bash
cd /home/ubuntu/weatherbotpy

# Copiar .env.example para .env
cp .env.example .env

# Editar .env com suas credenciais
nano .env
```

### 2️⃣ Build da imagem

```bash
# Build (primeiro uso ou após mudanças no código)
docker-compose build

# Ou rebuild forçado
docker-compose build --no-cache
```

### 3️⃣ Iniciar o bot

```bash
# Iniciar em background
docker-compose up -d

# Ver logs em tempo real
docker-compose logs -f weatherbot

# Ver status
docker-compose ps
```

### 4️⃣ Acessar dashboard

```
http://seu-vps:8877
```

---

## 📊 Tamanho da Imagem

```
python:3.12-slim          ~130 MB (base)
+ dependências Python     ~60 MB
+ código WeatherBot       ~2 MB
─────────────────────────────────
Total final               ~192 MB (LEVE!)
```

Sem otimização seria ~800+ MB com `python:3.12` full.

---

## 🛠️ Comandos Úteis

### Gerenciar containers

```bash
# Iniciar
docker-compose up -d

# Parar
docker-compose down

# Ver logs
docker-compose logs weatherbot
docker-compose logs -f weatherbot      # live
docker-compose logs --tail=50           # últimas 50 linhas

# Restart
docker-compose restart

# Status
docker-compose ps

# Inspecionar container
docker-compose exec weatherbot bash
```

### Acessar o container

```bash
# Terminal interativo
docker-compose exec weatherbot bash

# Dentro do container:
python -m py_compile core/strategy.py
ls -la data/
cat /app/data/state.json
```

### Ver recursos usados

```bash
# CPU/Memory em tempo real
docker stats weatherbot

# Histórico de uso
docker stats --no-stream
```

### Limpar dados

```bash
# Parar container
docker-compose down

# Remover volume de dados (apaga tudo!)
docker volume rm weatherbot_weatherbot-data

# Remover imagem
docker rmi weatherbot_weatherbot

# Limpeza completa
docker-compose down -v
docker system prune -a
```

---

## 🔄 Atualizar o código

### Se mudou arquivos Python

```bash
# 1. Parar o container
docker-compose down

# 2. Rebuild (pega novo código)
docker-compose build

# 3. Reiniciar
docker-compose up -d

# 4. Ver logs
docker-compose logs -f weatherbot
```

### Se mudou dependências (requirements.txt)

```bash
# Rebuild force (ignora cache)
docker-compose build --no-cache

# Restart
docker-compose up -d
```

---

## 📁 Volumes (Persistência)

### Dados guardados em volumes

```bash
# Listar volumes
docker volume ls

# Inspecionar volume
docker volume inspect weatherbot_weatherbot-data

# Path no host (onde os dados estão)
/var/lib/docker/volumes/weatherbot_weatherbot-data/_data/
```

### Backup de dados

```bash
# Fazer backup
docker run --rm \
  -v weatherbot_weatherbot-data:/data \
  -v $(pwd):/backup \
  busybox tar czf /backup/weatherbot-data-backup.tar.gz -C /data .

# Restaurar backup
docker run --rm \
  -v weatherbot_weatherbot-data:/data \
  -v $(pwd):/backup \
  busybox tar xzf /backup/weatherbot-data-backup.tar.gz -C /data
```

---

## 🔒 Segurança

### Limitar recursos

```yaml
# Já configurado em docker-compose.yml:
deploy:
  resources:
    limits:
      cpus: '1'
      memory: 512M
```

### Executar como usuário não-root (opcional)

```dockerfile
# Adicionar ao Dockerfile:
RUN useradd -m -u 1000 weatherbot
USER weatherbot
```

### Network isolada

```bash
# Ver networks
docker network ls

# Inspecionar
docker network inspect weatherbot_weatherbot-net
```

---

## 🌐 Exposição na VPS

### Opção 1: Acesso direto (simples)

```bash
# Já configurado em docker-compose.yml
ports:
  - "8877:8877"
```

→ Acesse: `http://seu-vps-ip:8877`

### Opção 2: Nginx reverse proxy (seguro)

```bash
# Instalar Nginx
sudo apt-get install nginx

# Criar config
sudo nano /etc/nginx/sites-available/weatherbot
```

```nginx
server {
    listen 80;
    server_name seu-dominio.com;
    
    location / {
        proxy_pass http://localhost:8877;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

```bash
# Ativar
sudo ln -s /etc/nginx/sites-available/weatherbot /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx

# HTTPS com Let's Encrypt
sudo apt-get install certbot python3-certbot-nginx
sudo certbot --nginx -d seu-dominio.com
```

### Opção 3: SSH tunnel (máximo seguro)

```bash
# No seu PC
ssh -L 8877:localhost:8877 ubuntu@seu-vps.com

# Depois acesse: http://localhost:8877
```

---

## 📊 Monitoramento

### Ver health status

```bash
# Verificar health check
docker-compose ps

# STDOUT da aplicação
docker-compose logs -f weatherbot
```

### Portainer (UI para Docker)

```bash
docker run -d \
  --name portainer \
  --restart always \
  -p 9000:9000 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  portainer/portainer-ce
```

Acesse: `http://seu-vps:9000`

---

## 🐛 Troubleshooting

### Container não inicia

```bash
# Ver erro
docker-compose logs weatherbot

# Verificar se porta está em uso
sudo lsof -i :8877

# Se tiver outro processo, matar
sudo kill -9 <PID>
```

### Dados não persistem

```bash
# Verificar se volume foi criado
docker volume ls | grep weatherbot

# Ver path do volume
docker volume inspect weatherbot_weatherbot-data
```

### Container restarta continuamente

```bash
# Ver motivo
docker-compose logs --tail=100 weatherbot

# Possíveis causas:
# - Erro no código (syntax error)
# - .env com valores inválidos
# - Porta 8877 já em uso
# - Out of memory (aumentar limite)
```

### Conexão Telegram ainda com conflito

```bash
# 1. Parar container
docker-compose down

# 2. Aguardar 3s
sleep 3

# 3. Restart
docker-compose up -d

# Docker aplica nova instância com lock file clean
```

---

## 📈 Performance

### Stats em tempo real

```bash
watch -n 1 'docker stats weatherbot --no-stream'
```

### Típico para WeatherBot

```
CONTAINER CPU    MEM USAGE
weatherbot ~0.2% 180-250M

(Varia conforme atividade de polling/scan)
```

### Se tiver problema de memória

```bash
# Aumentar limit em docker-compose.yml
deploy:
  resources:
    limits:
      memory: 1024M    # De 512M para 1GB
```

---

## 🔄 Auto-restart em VPS

### Systemd service (recomendado)

```bash
# Criar arquivo
sudo nano /etc/systemd/system/weatherbot-docker.service
```

```ini
[Unit]
Description=WeatherBot Docker Container
After=docker.service
Requires=docker.service

[Service]
Type=simple
WorkingDirectory=/home/ubuntu/weatherbotpy
ExecStart=/usr/bin/docker-compose up
ExecStop=/usr/bin/docker-compose down
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```

```bash
# Ativar
sudo systemctl daemon-reload
sudo systemctl enable weatherbot-docker.service
sudo systemctl start weatherbot-docker.service

# Ver status
sudo systemctl status weatherbot-docker.service
```

---

## 📋 Checklist de Deploy

- [ ] Docker instalado (`docker --version`)
- [ ] Docker Compose instalado (`docker-compose --version`)
- [ ] `.env` configurado corretamente
- [ ] Firewall permite porta 8877
- [ ] `docker-compose build` completou sem erros
- [ ] `docker-compose up -d` iniciou container
- [ ] `docker-compose ps` mostra container rodando
- [ ] `curl http://localhost:8877` retorna HTML
- [ ] Dashboard acessível externamente
- [ ] Logs mostram bot rodando (`docker-compose logs -f`)

---

## 🎯 Próximos Passos

1. **Monitorar logs**: `docker-compose logs -f weatherbot`
2. **Acessar dashboard**: `http://seu-vps:8877`
3. **Fazer login** com credenciais configuradas
4. **Verificar trades** na primeira hora
5. **Configurar backup** automático dos volumes

---

## 💡 Dicas

- Use `docker-compose logs -f` constantemente durante testes
- Mantenha Docker atualizado: `docker system prune && docker pull python:3.12-slim`
- Set up alertas para container restarts
- Use volumes para persistência, não o filesystem do container
- Revise docker-compose.yml antes de deploy em produção

---

**Pronto para deploy!** 🚀
