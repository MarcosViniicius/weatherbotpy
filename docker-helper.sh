#!/bin/bash
# ════════════════════════════════════════════════════════════
# WeatherBot Docker Helper Script
# Simplifies common Docker operations
# ════════════════════════════════════════════════════════════

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
print_header() {
  echo -e "${BLUE}╔════════════════════════════════════════════╗${NC}"
  echo -e "${BLUE}║ WeatherBot Docker Helper${NC}"
  echo -e "${BLUE}╚════════════════════════════════════════════╝${NC}"
}

print_success() {
  echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
  echo -e "${RED}✗ $1${NC}"
}

print_warning() {
  echo -e "${YELLOW}⚠ $1${NC}"
}

print_info() {
  echo -e "${BLUE}ℹ $1${NC}"
}

# Check Docker
check_docker() {
  if ! command -v docker &> /dev/null; then
    print_error "Docker not found. Install Docker first:"
    echo "  curl -fsSL https://get.docker.com -o get-docker.sh && sudo sh get-docker.sh"
    exit 1
  fi
  
  if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose not found. Install Docker Compose first:"
    echo "  sudo curl -L https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-\$(uname -s)-\$(uname -m) -o /usr/local/bin/docker-compose"
    echo "  sudo chmod +x /usr/local/bin/docker-compose"
    exit 1
  fi
  
  print_success "Docker and Docker Compose found"
}

# Show menu
show_menu() {
  echo ""
  echo "Select an option:"
  echo "  1) Start container (up -d)"
  echo "  2) Stop container (down)"
  echo "  3) View logs (live)"
  echo "  4) Rebuild image"
  echo "  5) Restart container"
  echo "  6) Shell into container"
  echo "  7) View resource usage"
  echo "  8) Health status"
  echo "  9) Full clean + rebuild + start"
  echo "  0) Exit"
  echo ""
}

# Operations
start_container() {
  print_info "Starting container..."
  docker-compose up -d
  print_success "Container started"
  sleep 2
  docker-compose ps
}

stop_container() {
  print_info "Stopping container..."
  docker-compose down
  print_success "Container stopped"
}

view_logs() {
  print_info "Showing live logs (press Ctrl+C to exit)..."
  docker-compose logs -f weatherbot
}

rebuild_image() {
  print_warning "This may take a minute..."
  docker-compose build
  print_success "Image rebuilt"
}

rebuild_no_cache() {
  print_warning "Rebuilding without cache (may take longer)..."
  docker-compose build --no-cache
  print_success "Image rebuilt (no cache)"
}

restart_container() {
  print_info "Restarting container..."
  docker-compose restart
  print_success "Container restarted"
  sleep 2
  docker-compose ps
}

shell_into_container() {
  print_info "Entering container shell..."
  print_warning "Type 'exit' to leave"
  docker-compose exec weatherbot bash
}

view_resources() {
  print_info "Container resource usage (press Ctrl+C to exit)..."
  docker stats weatherbot
}

health_status() {
  print_info "Checking health status..."
  STATUS=$(docker-compose ps | grep weatherbot | awk '{print $NF}')
  
  if [[ "$STATUS" == *"Up"* ]]; then
    print_success "Container is UP"
    
    # Try health check
    if curl -sf http://localhost:8877/ > /dev/null 2>&1; then
      print_success "Dashboard is responding (port 8877)"
    else
      print_error "Dashboard not responding on port 8877"
    fi
    
    # Show resources
    echo ""
    docker stats weatherbot --no-stream
  else
    print_error "Container is DOWN"
    echo "Status: $STATUS"
  fi
}

clean_rebuild_start() {
  print_warning "This will:"
  echo "  1. Stop container"
  echo "  2. Remove volumes (DATA LOSS!)"
  echo "  3. Rebuild image (no cache)"
  echo "  4. Start container"
  echo ""
  read -p "Continue? (y/N): " -n 1 -r
  echo
  
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_info "Cancelled"
    return
  fi
  
  docker-compose down -v
  print_success "Containers and volumes removed"
  
  docker-compose build --no-cache
  print_success "Image rebuilt"
  
  docker-compose up -d
  print_success "Container started"
  
  sleep 3
  docker-compose ps
}

# Main loop
main() {
  print_header
  
  # Check prerequisites
  check_docker
  echo ""
  
  while true; do
    show_menu
    read -p "Choice: " choice
    
    case $choice in
      1) start_container ;;
      2) stop_container ;;
      3) view_logs ;;
      4) rebuild_image ;;
      5) restart_container ;;
      6) shell_into_container ;;
      7) view_resources ;;
      8) health_status ;;
      9) clean_rebuild_start ;;
      0) print_success "Goodbye!"; exit 0 ;;
      *) print_error "Invalid option" ;;
    esac
    
    read -p "Press Enter to continue..."
    clear
    print_header
  done
}

# Run if not sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  main "$@"
fi
