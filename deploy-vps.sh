#!/bin/bash
# ════════════════════════════════════════════════════════════
# WeatherBot VPS Deployment Script
# Prepares VPS and deploys bot with Docker
# ════════════════════════════════════════════════════════════

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
  echo -e "${BLUE}╔════════════════════════════════════════════╗${NC}"
  echo -e "${BLUE}║ WeatherBot VPS Deployment${NC}"
  echo -e "${BLUE}╚════════════════════════════════════════════╝${NC}"
}

print_success() {
  echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
  echo -e "${RED}✗ $1${NC}"
  exit 1
}

print_step() {
  echo -e "${YELLOW}→ $1${NC}"
}

print_info() {
  echo -e "${BLUE}ℹ $1${NC}"
}

# Check requirements
check_requirements() {
  print_step "Checking requirements..."
  
  # Check if running as root
  if [[ $EUID -ne 0 ]]; then
    print_error "This script must be run as root. Use: sudo bash deploy-vps.sh"
  fi
  
  print_success "Running as root"
}

# Update system
update_system() {
  print_step "Updating system packages..."
  apt-get update -qq
  apt-get upgrade -y -qq
  print_success "System updated"
}

# Install Docker
install_docker() {
  print_step "Checking Docker installation..."
  
  if command -v docker &> /dev/null; then
    print_success "Docker already installed: $(docker --version)"
    return
  fi
  
  print_info "Installing Docker..."
  curl -fsSL https://get.docker.com -o get-docker.sh
  sh get-docker.sh
  rm get-docker.sh
  
  # Add current user to docker group (optional)
  if [ ! -z "$SUDO_USER" ]; then
    usermod -aG docker "$SUDO_USER"
    print_info "Added $SUDO_USER to docker group (may need to re-login)"
  fi
  
  print_success "Docker installed: $(docker --version)"
}

# Install Docker Compose
install_docker_compose() {
  print_step "Checking Docker Compose installation..."
  
  if command -v docker-compose &> /dev/null; then
    print_success "Docker Compose already installed: $(docker-compose --version)"
    return
  fi
  
  print_info "Installing Docker Compose..."
  DOCKER_COMPOSE_VERSION=$(curl -s https://api.github.com/repos/docker/compose/releases/latest | grep 'tag_name' | cut -d'"' -f4)
  DOCKER_COMPOSE_URL="https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)"
  
  curl -L "$DOCKER_COMPOSE_URL" -o /usr/local/bin/docker-compose
  chmod +x /usr/local/bin/docker-compose
  
  print_success "Docker Compose installed: $(docker-compose --version)"
}

# Install additional tools
install_tools() {
  print_step "Installing additional tools..."
  apt-get install -y -qq curl wget git htop
  print_success "Tools installed"
}

# Create deployment directory
setup_deployment() {
  print_step "Setting up deployment directory..."
  
  DEPLOY_DIR="/opt/weatherbot"
  
  if [ ! -d "$DEPLOY_DIR" ]; then
    mkdir -p "$DEPLOY_DIR"
    print_success "Created $DEPLOY_DIR"
  else
    print_info "$DEPLOY_DIR already exists"
  fi
  
  # Create data directories
  mkdir -p "$DEPLOY_DIR/data/markets"
  mkdir -p "$DEPLOY_DIR/logs"
  
  print_success "Created data and logs directories"
}

# Setup firewall
setup_firewall() {
  print_step "Checking firewall..."
  
  # Enable UFW if available
  if command -v ufw &> /dev/null; then
    print_info "Enabling UFW firewall..."
    
    ufw default deny incoming &> /dev/null || true
    ufw default allow outgoing &> /dev/null || true
    
    # Allow SSH
    ufw allow 22/tcp &> /dev/null || true
    
    # Allow dashboard port
    ufw allow 8877/tcp &> /dev/null || true
    
    echo "y" | ufw enable &> /dev/null || true
    
    print_success "Firewall configured (SSH:22, Dashboard:8877)"
  else
    print_info "UFW not available, skipping firewall setup"
  fi
}

# Create systemd service
create_systemd_service() {
  print_step "Creating systemd service..."
  
  cat > /etc/systemd/system/weatherbot.service << 'EOF'
[Unit]
Description=WeatherBot Service
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/weatherbot
User=root
ExecStart=/usr/local/bin/docker-compose up -d
ExecStop=/usr/local/bin/docker-compose down
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  print_success "Systemd service created"
}

# Print next steps
print_next_steps() {
  echo ""
  echo -e "${BLUE}════════════════════════════════════════════${NC}"
  echo -e "${GREEN}Deployment completed!${NC}"
  echo -e "${BLUE}════════════════════════════════════════════${NC}"
  echo ""
  echo "Next steps:"
  echo ""
  echo "1. Copy your code to the VPS:"
  echo "   scp -r . root@YOUR_VPS_IP:/opt/weatherbot/"
  echo ""
  echo "2. SSH into VPS:"
  echo "   ssh root@YOUR_VPS_IP"
  echo ""
  echo "3. Start the bot:"
  echo "   cd /opt/weatherbot"
  echo "   docker-compose up -d"
  echo ""
  echo "4. View logs:"
  echo "   docker-compose logs -f weatherbot"
  echo ""
  echo "5. Access dashboard:"
  echo "   http://YOUR_VPS_IP:8877"
  echo ""
  echo "Dashboard will ask for login if DASHBOARD_AUTH_ENABLED=true"
  echo "Default: username='admin', password='changeme' (change in .env!)"
  echo ""
  echo "Optional: Enable as system service:"
  echo "   systemctl start weatherbot"
  echo "   systemctl enable weatherbot"
  echo ""
}

# Main execution
main() {
  print_header
  echo ""
  
  check_requirements
  echo ""
  
  update_system
  echo ""
  
  install_docker
  echo ""
  
  install_docker_compose
  echo ""
  
  install_tools
  echo ""
  
  setup_deployment
  echo ""
  
  setup_firewall
  echo ""
  
  create_systemd_service
  echo ""
  
  print_next_steps
}

main "$@"
