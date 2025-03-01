# Stage 1: Build the React app.
FROM node:22-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

# Stage 2: Serve the built app with Caddy.
FROM caddy:2-alpine
WORKDIR /var/www/arroyo.skylon.dev
COPY --from=build /app/dist ./
COPY Caddyfile /etc/caddy/
# Fix formatting in the Caddyfile, if needed.
RUN caddy fmt --overwrite /etc/caddy/Caddyfile

EXPOSE 80 443
