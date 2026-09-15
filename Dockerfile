FROM node:24-bookworm-slim AS base
COPY --from=oven/bun:1.4.2 /usr/local/bin/bun /usr/local/bin/bun
WORKDIR /app

FROM base AS build
COPY package.json bun.lock tsconfig*.json ./
COPY scripts ./scripts
# Installs the exact GitHub dependency and runs the consumer-side build script.
RUN bun install --frozen-lockfile
COPY src ./src
RUN bun run build

FROM base AS runtime
ENV NODE_ENV=production HOST=0.0.0.0 PORT=3000 DATA_DIR=/app/data NODE_BINARY=node
COPY --from=build --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/dist ./dist
COPY --from=build --chown=node:node /app/scripts ./scripts
COPY --chown=node:node package.json ./
RUN mkdir -p /app/data && chown node:node /app/data
USER node
EXPOSE 3000
CMD ["bun", "dist/main.js"]
