#!/bin/bash
# ════════════════════════════════════════════════════════════
# WeatherBot Pre-Deployment Check Script
# Validates environment, configuration, and dependencies
# ════════════════════════════════════════════════════════════

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Tracking
TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0

print_header() {
  echo -e "${BLUE}╔════════════════════════════════════════════╗${NC}"
  echo -e "${BLUE}║ WeatherBot Pre-Deployment Checker${NC}"
  echo -e "${BLUE}╚════════════════════════════════════════════╝${NC}"
  echo ""
}

check_command() {
  local name=$1
  local command=$2
  
  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  
  if command -v $command &> /dev/null; then
    echo -e "${GREEN}✓${NC} $name: $(which $command)"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
    return 0
  else
    echo -e "${RED}✗${NC} $name: NOT FOUND"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    return 1
  fi
}

check_file() {
  local name=$1
  local file=$2
  
  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  
  if [ -f "$file" ]; then
    local size=$(du -h "$file" | cut -f1)
    echo -e "${GREEN}✓${NC} $name: $file ($size)"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
    return 0
  else
    echo -e "${RED}✗${NC} $name: $file (NOT FOUND)"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    return 1
  fi
}

check_env_var() {
  local name=$1
  local var=$2
  local required=${3:-0}
  
  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  
  if [ -z "${!var}" ]; then
    if [ $required -eq 1 ]; then
      echo -e "${RED}✗${NC} $name ($var): NOT SET (required)"
      FAILED_CHECKS=$((FAILED_CHECKS + 1))
      return 1
    else
      echo -e "${YELLOW}⚠${NC} $name ($var): NOT SET (optional)"
      return 2
    fi
  else
    # Mask sensitive values
    if [[ "$var" == *"TOKEN"* ]] || [[ "$var" == *"KEY"* ]] || [[ "$var" == *"PASSWORD"* ]]; then
      local value="${!var:0:4}...${!var: -4}"
    else
      local value="${!var}"
    fi
    echo -e "${GREEN}✓${NC} $name ($var): $value"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
    return 0
  fi
}

check_python_module() {
  local name=$1
  local module=$2
  
  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  
  if python3 -c "import $module" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Python module: $name"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
    return 0
  else
    echo -e "${RED}✗${NC} Python module: $name (NOT INSTALLED)"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    return 1
  fi
}

check_port() {
  local name=$1
  local port=$2
  
  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  
  if command -v nc &> /dev/null; then
    if nc -z localhost $port &> /dev/null; then
      echo -e "${RED}✗${NC} Port $port: IN USE (might conflict with Docker)"
      FAILED_CHECKS=$((FAILED_CHECKS + 1))
      return 1
    else
      echo -e "${GREEN}✓${NC} Port $port: Available"
      PASSED_CHECKS=$((PASSED_CHECKS + 1))
      return 0
    fi
  else
    echo -e "${YELLOW}⚠${NC} Port $port: Cannot check (nc not installed)"
    return 2
  fi
}

check_disk_space() {
  local required_mb=500
  local available=$(df -m . | awk 'NR==2 {print $4}')
  
  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  
  if [ "$available" -gt "$required_mb" ]; then
    echo -e "${GREEN}✓${NC} Disk space: ${available}MB available (need: ${required_mb}MB)"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
    return 0
  else
    echo -e "${RED}✗${NC} Disk space: ${available}MB available (need: ${required_mb}MB)"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    return 1
  fi
}

check_requirements_txt() {
  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  
  if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}✗${NC} requirements.txt: NOT FOUND"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    return 1
  fi
  
  local count=$(wc -l < requirements.txt)
  echo -e "${GREEN}✓${NC} requirements.txt: Found ($count dependencies)"
  PASSED_CHECKS=$((PASSED_CHECKS + 1))
  return 0
}

print_summary() {
  echo ""
  echo -e "${BLUE}════════════════════════════════════════════${NC}"
  echo -e "Checks: ${PASSED_CHECKS}/${TOTAL_CHECKS} passed"
  
  if [ $FAILED_CHECKS -eq 0 ]; then
    echo -e "${GREEN}✓ All checks passed! Ready to deploy.${NC}"
  else
    echo -e "${RED}✗ $FAILED_CHECKS checks failed.${NC}"
  fi
  echo -e "${BLUE}════════════════════════════════════════════${NC}"
}

print_recommendations() {
  echo ""
  echo "Next steps:"
  
  if grep -q "^TELEGRAM_TOKEN=" .env 2>/dev/null; then
    if grep -q "TELEGRAM_TOKEN=your-" .env; then
      echo -e "  ${YELLOW}⚠${NC}  Update TELEGRAM_TOKEN in .env"
    fi
  fi
  
  echo ""
  echo "To start bot:"
  echo "  docker-compose up -d"
  echo ""
  echo "To monitor:"
  echo "  docker-compose logs -f weatherbot"
  echo ""
  echo "To access:"
  echo "  http://localhost:8877"
}

main() {
  print_header
  
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "1️⃣  System Dependencies"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  check_command "Docker" "docker"
  check_command "Docker Compose" "docker-compose"
  check_command "Python" "python3"
  check_command "Git" "git"
  echo ""
  
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "2️⃣  Project Files"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  check_file "docker-compose.yml" "docker-compose.yml"
  check_file "Dockerfile" "Dockerfile"
  check_file "requirements.txt" "requirements.txt"
  check_requirements_txt
  check_file ".env or .env.example" ".env.example"
  echo ""
  
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "3️⃣  Python Modules (for local testing)"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  check_python_module "requests" "requests" || true
  check_python_module "telegram" "telegram" || true
  check_python_module "aiohttp" "aiohttp" || true
  echo ""
  
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "4️⃣  Configuration Variables"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  
  # Load .env if it exists
  if [ -f ".env" ]; then
    export $(cat .env | xargs)
    
    check_env_var "Telegram Token" "TELEGRAM_TOKEN" 1 || true
    check_env_var "Telegram Chat ID" "TELEGRAM_CHAT_ID" 1 || true
    check_env_var "Bot Mode" "BOT_MODE" 1 || true
    check_env_var "Dashboard Port" "DASHBOARD_PORT" 0 || true
    check_env_var "Dashboard Auth Enabled" "DASHBOARD_AUTH_ENABLED" 0 || true
    check_env_var "Dashboard Username" "DASHBOARD_USERNAME" 0 || true
    check_env_var "Polymarket Private Key" "POLYMARKET_PRIVATE_KEY" 0 || true
  else
    echo -e "${YELLOW}⚠${NC}  .env not found (copy from .env.example first)"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  fi
  echo ""
  
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "5️⃣  System Resources"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  check_disk_space
  check_port "Dashboard (8877)" 8877 || true
  echo ""
  
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "6️⃣  Docker Configuration"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  
  TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
  if docker-compose config > /dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} docker-compose.yml: Valid configuration"
    PASSED_CHECKS=$((PASSED_CHECKS + 1))
  else
    echo -e "${RED}✗${NC} docker-compose.yml: Invalid configuration"
    docker-compose config 2>&1 | head -10
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
  fi
  echo ""
  
  print_summary
  print_recommendations
  
  if [ $FAILED_CHECKS -eq 0 ]; then
    exit 0
  else
    exit 1
  fi
}

main "$@"
