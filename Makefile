.PHONY: help proto build test up down logs clean docker-build docker-all docker-VM1 docker-VM2 docker-VM3 docker-VM4

.DEFAULT_GOAL := help

help:
	@echo "DistriEats - targets:"
	@echo "  make proto         regenerar codigo gRPC desde proto/distrieats.proto"
	@echo "  make build         compilar los 5 binarios a bin/ (sin Docker)"
	@echo "  make test          correr tests unitarios (relojes vectoriales/conflictos)"
	@echo "  make up            LOCAL: construir y levantar los 9 contenedores (bridge)"
	@echo "  make down          detener y limpiar los contenedores locales"
	@echo "  make logs          seguir logs de los contenedores locales"
	@echo "  make docker-build  construir la imagen Docker base"
	@echo "  make docker-VM1    MV1: Broker + Productor"
	@echo "  make docker-VM2    MV2: Gateway + Cliente 1 + Datanode 1"
	@echo "  make docker-VM3    MV3: Cliente 2 + Datanode 2"
	@echo "  make docker-VM4    MV4: Cliente 3 + Datanode 3"
	@echo "  make clean         borrar bin/ y resultados/"

proto:
	protoc --go_out=. --go_opt=module=distrieats \
	       --go-grpc_out=. --go-grpc_opt=module=distrieats \
	       proto/distrieats.proto

build:
	@mkdir -p bin
	go build -o bin/datanode ./cmd/datanode
	go build -o bin/broker   ./cmd/broker
	go build -o bin/gateway  ./cmd/gateway
	go build -o bin/client   ./cmd/client
	go build -o bin/producer ./cmd/producer

test:
	go test ./...

docker-build:
	docker compose build

up:
	@mkdir -p resultados
	docker compose build
	@./scripts/banner.sh || true
	docker compose up

down:
	docker compose down

logs:
	docker compose logs -f

docker-VM1:
	@mkdir -p resultados
	docker compose -f docker-compose-vm1.yml build
	@./scripts/banner.sh || true
	docker compose -f docker-compose-vm1.yml up

docker-VM2:
	@mkdir -p resultados
	docker compose -f docker-compose-vm2.yml up --build

docker-VM3:
	@mkdir -p resultados
	docker compose -f docker-compose-vm3.yml up --build

docker-VM4:
	@mkdir -p resultados
	docker compose -f docker-compose-vm4.yml up --build

clean:
	rm -rf bin resultados Reporte.txt estado_final_*.log
