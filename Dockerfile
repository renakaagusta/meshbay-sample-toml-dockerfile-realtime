FROM oven/bun:1.3.14-alpine
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY tsconfig.json ./
COPY --chown=bun:bun src ./src
ENV NODE_ENV=production \
    PORT=8080
USER bun
EXPOSE 8080
CMD ["bun", "run", "src/index.ts"]
