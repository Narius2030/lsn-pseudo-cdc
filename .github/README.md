# Production-style CI/CD Architecture (Docker Compose + Self-hosted Runner)

> Version: v1.0

## 1. Scope

This document defines the agreed CI/CD architecture.

### In Scope

- GitHub Actions
- Self-hosted GitHub Runner
- Runner & Application on same host (phase 1)
- Pull-based deployment
- Docker Compose deployment target
- Deployment Layer outside GitHub workflow
- Production-oriented design

### Out of Scope

- Kubernetes
- GitOps
- SSH-based deployment
- Multi-host orchestration

---

# 2. High-level Architecture

```text
Developer
    │
    ▼
GitHub Repository
    │
    ▼
GitHub Actions
(CI + CD Orchestrator)
    │
    ▼
GitHub Job Queue
    │
HTTPS (443)
    ▼
Self-hosted Runner
(on Application Server)
    │
    ▼
Deployment Layer
(deploy.sh / future CLI)
    │
    ▼
Docker Compose
    │
    ▼
Application Containers
```

---

# 3. Core Architecture Rules

1. GitHub Actions is ONLY an orchestrator.
2. Deployment logic MUST NOT exist inside workflow YAML.
3. Runner executes jobs only.
4. Runner is stateless.
5. Docker Compose is current deployment driver.
6. Deployment Layer owns deployment lifecycle.
7. Application state belongs to Docker volumes/databases, never Runner.
8. CI and CD are isolated responsibilities.

---

# 4. Technology Stack

| Layer             | Technology                |
| ----------------- | ------------------------- |
| SCM               | GitHub                    |
| CI/CD             | GitHub Actions            |
| Runner            | Self-hosted GitHub Runner |
| Runtime           | Docker Engine             |
| Deployment Driver | Docker Compose v2         |
| Registry          | GHCR                      |
| Image Signing     | Cosign                    |
| Build             | Docker Buildx             |
| Health Check      | HTTP/TCP/Custom Script    |
| Host OS           | Ubuntu Server LTS         |

---

# 5. Runner Responsibilities

Runner SHALL:

- poll GitHub
- receive jobs
- execute shell commands
- report status

Runner SHALL NOT:

- own deployment config
- own compose files
- own secrets
- own database
- own application data

---

# 6. Stateless Runner

Runner directory example:

```text
/opt/actions-runner
```

Deployment example:

```text
/opt/deployment
```

Docker data:

```text
/var/lib/docker
```

If Runner is deleted:

- reinstall runner
- register again
- application keeps running

---

# 7. Deployment Layer

Responsibilities

- verify image
- pull image
- update compose
- execute compose
- health check
- rollback
- cleanup

GitHub MUST call Deployment Layer only.

---

# 8. CI Workflow

```text
Developer
    │
Push / PR
    │
GitHub Actions
    │
Quality Gate
    │
Unit Test
    │
Lint
    │
Security Scan
    │
Docker Build
    │
Cosign Sign
    │
Push GHCR
```

Output:

Immutable signed image.

---

# 9. CD Workflow

```text
Release Event
      │
GitHub Actions
      │
Queue Job
      │
Self-hosted Runner
      │
Deployment Layer
      │
Verify Cosign Signature
      │
docker compose pull
      │
docker compose up -d
      │
Health Check
      │
Success
```

If Health Check fails

```text
Rollback
      │
Restore Previous Version
```

---

# 10. Deployment Sequence

```text
Developer
      │
Release
      │
GitHub
      │
Runner
      │
Deployment Layer
      │
Cosign Verify
      │
Compose Pull
      │
Compose Up
      │
Health Check
      │
Completed
```

---

# 11. Required Configuration

Server

- Ubuntu Server LTS
- Docker Engine
- Docker Compose v2
- Self-hosted Runner service
- Cosign
- GHCR authentication

GitHub

- Repository Secrets
- Environments
- Required Reviewers (optional)
- Protected Branch
- Protected Tags

---

# 12. Secrets

Examples

- GHCR_PAT
- COSIGN_PUBLIC_KEY (if key-based)
- Application Secrets

Secrets MUST NOT be hardcoded.

---

# 13. Repository Responsibilities

GitHub Actions

- orchestration only

Deployment Layer

- deployment lifecycle

Docker Compose

- runtime topology

Runner

- execution only

---

# 14. Phase-1 Target

Runner and Application share one server.

```text
Ubuntu Server

├── Docker
├── Docker Compose
├── Application
├── Self-hosted Runner
└── Deployment Layer
```

---

# 15. Future Expansion (Reference Only)

Application Server(s)

← Deployment Layer

← Runner Pool

← GitHub Actions

This is future architecture only and not part of current implementation.
