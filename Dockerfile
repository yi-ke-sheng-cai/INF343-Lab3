# Dockerfile único parametrizado por ARG ENTIDAD (datanode, broker, gateway,
# client, producer). Multi-stage: compila estático y corre sobre alpine.
ARG ENTIDAD

FROM golang:1.25-alpine AS builder
ARG ENTIDAD
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /app/servicio ./cmd/${ENTIDAD}

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /app/servicio .
# El CSV se incluye para que el Productor lo lea (ruta configurable por flag/env).
COPY data/ data/
ENTRYPOINT ["./servicio"]
