# syntax=docker/dockerfile:1.7
#
# Slim production image for the subtensor-listener service. Targets Kubernetes:
# runs as non-root and exposes :3020 for the health probes.
#   docker build -t subtensor-listener .

# ── Stage 1: Install deps + compile TypeScript ───────────────────
FROM node:22-slim@sha256:4b150b48b4116e2a22f2c39cc27025fbf4f153e2078fd3b14d159fdb5c7279da AS builder

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig*.json nest-cli.json ./
COPY src/ src/

RUN npm run build

# ── Stage 2: Slim runtime ────────────────────────────────────────
FROM node:22-slim@sha256:4b150b48b4116e2a22f2c39cc27025fbf4f153e2078fd3b14d159fdb5c7279da

WORKDIR /app

# Prod-only deps — the dev toolchain (nest cli, ts, jest) stays in the builder.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

COPY --from=builder --chown=node:node /app/dist/ dist/

USER node

ENV NODE_ENV=production
EXPOSE 3020

CMD ["node", "dist/main.js"]
