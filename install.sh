#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
#  AkalDB Installer
#  Install the latest (or a specific) release of AkalDB from GitHub.
#
#  Usage:
#    curl -fsSL https://raw.githubusercontent.com/wansatya/akaldb/main/install.sh | bash
#    curl -fsSL ... | bash -s -- --version 0.2.0
#    curl -fsSL ... | bash -s -- --install-dir /opt/bin
# ──────────────────────────────────────────────────────────────────────────────

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────────────
REPO="wansatya/akaldb"
BINARY_NAME="akaldb"
INSTALL_DIR="/usr/local/bin"
VERSION="latest"

# ── Colors & Formatting ─────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
AMBER='\033[38;5;214m'
CYAN='\033[38;5;45m'
BLUE='\033[38;5;33m'
INDIGO='\033[38;5;63m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'

info()    { printf "${CYAN}▸${RESET} %s\n" "$1"; }
success() { printf "${GREEN}✔${RESET} %s\n" "$1"; }
warn()    { printf "${YELLOW}⚠${RESET} %s\n" "$1"; }
error()   { printf "${RED}✗${RESET} %s\n" "$1" >&2; exit 1; }

# ── Banner ───────────────────────────────────────────────────────────────────
banner() {
    printf "\n"
    printf "  ${AMBER}    ▄████▄   ██  ▄█   ▄████▄   ██      ${RESET}\n"
    printf "  ${AMBER}   ██▀  ▀██  ██ ▄█   ██▀  ▀██  ██      ${RESET}\n"
    printf "  ${AMBER}   ████████  █████   ████████  ██      ${RESET}\n"
    printf "  ${AMBER}   ██    ██  ██  ██  ██    ██  ██      ${RESET}\n"
    printf "  ${AMBER}   ██    ██  ██   █  ██    ██  ███████ ${RESET}\n"
    printf "\n"
    printf "  ${DIM}   The Reasoning Database for AI • Context Layer${RESET}\n"
    printf "\n"
}

# ── Parse Arguments ──────────────────────────────────────────────────────────
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --version|-v)
                VERSION="$2"
                shift 2
                ;;
            --install-dir|-d)
                INSTALL_DIR="$2"
                shift 2
                ;;
            --repo|-r)
                REPO="$2"
                shift 2
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                error "Unknown option: $1 (use --help for usage)"
                ;;
        esac
    done
}

usage() {
    cat << EOF
${BOLD}Usage:${RESET}
  curl -fsSL https://raw.githubusercontent.com/wansatya/akaldb/main/install.sh | bash
  curl -fsSL ... | bash -s -- [OPTIONS]

${BOLD}Options:${RESET}
  --version, -v      Version to install (default: latest)
  --install-dir, -d  Installation directory (default: /usr/local/bin)
  --repo, -r         GitHub repository (default: ${REPO})
  --help, -h         Show this help message

${BOLD}Examples:${RESET}
  # Install latest release
  curl -fsSL https://raw.githubusercontent.com/wansatya/akaldb/main/install.sh | bash

  # Install specific version
  curl -fsSL ... | bash -s -- --version 0.2.0

  # Install to custom directory
  curl -fsSL ... | bash -s -- --install-dir ~/.local/bin
EOF
}

# ── Detect Platform ──────────────────────────────────────────────────────────
detect_platform() {
    local os arch

    os="$(uname -s)"
    arch="$(uname -m)"

    case "$os" in
        Linux)   os="linux" ;;
        Darwin)  os="darwin" ;;
        MINGW*|MSYS*|CYGWIN*) os="windows" ;;
        *)       error "Unsupported OS: $os" ;;
    esac

    case "$arch" in
        x86_64|amd64)   arch="x86_64" ;;
        aarch64|arm64)  arch="aarch64" ;;
        armv7*)         arch="armv7" ;;
        *)              error "Unsupported architecture: $arch" ;;
    esac

    PLATFORM="${os}"
    ARCH="${arch}"
    TARGET="${arch}-unknown-${os}-gnu"

    if [[ "$os" == "darwin" ]]; then
        TARGET="${arch}-apple-darwin"
    elif [[ "$os" == "windows" ]]; then
        TARGET="${arch}-pc-windows-msvc"
    fi
}

# ── Resolve Version ─────────────────────────────────────────────────────────
resolve_version() {
    if [[ "$VERSION" == "latest" ]]; then
        info "Fetching latest release version..."
        VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
            | grep '"tag_name"' \
            | sed -E 's/.*"tag_name":\s*"([^"]+)".*/\1/' \
            | sed 's/^v//')

        if [[ -z "$VERSION" ]]; then
            error "Could not determine latest version. Please specify with --version"
        fi
    fi

    # Strip leading 'v' if present
    VERSION="${VERSION#v}"
    success "Version: ${BOLD}v${VERSION}${RESET}"
}

# ── Check Dependencies ───────────────────────────────────────────────────────
check_deps() {
    local missing=()

    command -v curl  >/dev/null 2>&1 || missing+=("curl")
    command -v tar   >/dev/null 2>&1 || missing+=("tar")

    if [[ ${#missing[@]} -gt 0 ]]; then
        error "Missing required tools: ${missing[*]}"
    fi
}

# ── Download & Install ───────────────────────────────────────────────────────
download_and_install() {
    local tarball_name="akaldb-v${VERSION}-${TARGET}.tar.gz"
    local download_url="https://github.com/${REPO}/releases/download/v${VERSION}/${tarball_name}"
    local tmp_dir

    tmp_dir=$(mktemp -d)
    trap 'rm -rf "$tmp_dir"' EXIT

    info "Platform:   ${BOLD}${PLATFORM} / ${ARCH}${RESET}"
    info "Target:     ${BOLD}${TARGET}${RESET}"
    info "Downloading ${BOLD}${tarball_name}${RESET}..."
    printf "  ${DIM}↳ %s${RESET}\n" "$download_url"
    echo

    local http_code
    http_code=$(curl -fSL \
        --progress-bar \
        -o "${tmp_dir}/${tarball_name}" \
        -w "%{http_code}" \
        "$download_url" 2>&1) || true

    if [[ ! -f "${tmp_dir}/${tarball_name}" ]] || [[ $(stat -c%s "${tmp_dir}/${tarball_name}" 2>/dev/null || stat -f%z "${tmp_dir}/${tarball_name}" 2>/dev/null) -lt 1000 ]]; then
        echo
        error "Download failed. The release asset might not exist for your platform.
  URL:  $download_url
  Hint: Check available releases at https://github.com/${REPO}/releases"
    fi

    info "Extracting..."
    tar -xzf "${tmp_dir}/${tarball_name}" -C "${tmp_dir}"

    # Find the binary (could be at root or inside a directory)
    local binary_path
    binary_path=$(find "${tmp_dir}" -name "${BINARY_NAME}" -type f | head -1)

    if [[ -z "$binary_path" ]]; then
        error "Binary '${BINARY_NAME}' not found in downloaded archive"
    fi

    chmod +x "$binary_path"

    # Install to target directory
    info "Installing to ${BOLD}${INSTALL_DIR}/${BINARY_NAME}${RESET}..."

    if [[ -w "$INSTALL_DIR" ]]; then
        mv "$binary_path" "${INSTALL_DIR}/${BINARY_NAME}"
    else
        warn "Elevated permissions required for ${INSTALL_DIR}"
        sudo mv "$binary_path" "${INSTALL_DIR}/${BINARY_NAME}"
    fi

    success "Installed ${BOLD}${BINARY_NAME}${RESET} v${VERSION} to ${INSTALL_DIR}/${BINARY_NAME}"
}

# ── Verify Installation ─────────────────────────────────────────────────────
verify() {
    echo
    if command -v "$BINARY_NAME" >/dev/null 2>&1; then
        success "Verification passed — ${BINARY_NAME} is in PATH"
    else
        warn "${BINARY_NAME} is installed but not in your PATH"
        printf "  ${DIM}Add this to your shell config:${RESET}\n"
        printf "  ${CYAN}export PATH=\"%s:\$PATH\"${RESET}\n" "$INSTALL_DIR"
    fi

    echo
    printf "${GREEN}${BOLD}  ┌───────────────────────────────────────────┐${RESET}\n"
    printf "${GREEN}${BOLD}  │  AkalDB installed successfully! 🧠      │${RESET}\n"
    printf "${GREEN}${BOLD}  │                                           │${RESET}\n"
    printf "${GREEN}${BOLD}  │  Start the server:                        │${RESET}\n"
    printf "${GREEN}${BOLD}  │    akaldb start                           │${RESET}\n"
    printf "${GREEN}${BOLD}  │                                           │${RESET}\n"
    printf "${GREEN}${BOLD}  │  Docs: github.com/${REPO}  │${RESET}\n"
    printf "${GREEN}${BOLD}  └───────────────────────────────────────────┘${RESET}\n"
    echo
}

# ── Main ─────────────────────────────────────────────────────────────────────
main() {
    banner
    parse_args "$@"
    check_deps
    detect_platform
    resolve_version
    download_and_install
    verify
}

main "$@"
