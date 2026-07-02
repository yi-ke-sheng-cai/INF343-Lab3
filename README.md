# INF343 — Laboratorio 3: DistriEats

Simulación de un sistema de delivery distribuido con **consistencia eventual** (gossip + relojes vectoriales) y **Read Your Writes** (afinidad de sesión en Gateway). Stack: **Go + gRPC + Docker**.

## Integrantes

| Nombre | Rol |
|---|---|
| _(completar)_ | _(completar)_ |
| _(completar)_ | _(completar)_ |

## Topología

| VM | Contenedores |
|---|---|
| MV1 (dist057 · 10.35.168.67) | Broker + Productor |
| MV2 (dist058 · 10.35.168.68) | Gateway + Cliente 1 + Datanode 1 |
| MV3 (dist059 · 10.35.168.69) | Cliente 2 + Datanode 2 |
| MV4 (dist060 · 10.35.168.70) | Cliente 3 + Datanode 3 |

## Ejecución en VMs

Configurar `.env` con IPs reales. Ejecutar en orden: **MV2 → MV3 → MV4 → MV1**.

```
MV2: docker-compose -f docker-compose-vm2.yml up --build
MV3: docker-compose -f docker-compose-vm3.yml up --build
MV4: docker-compose -f docker-compose-vm4.yml up --build
MV1: mkdir -p resultados && docker-compose -f docker-compose-vm1.yml up --build
```

El sistema corre automáticamente: el Productor emite eventos del CSV, los Datanodes convergen por gossip, los Clientes validan RYW. Al terminar se genera `resultados/Reporte.txt` y los `estado_final_DN*.log`.

### Desarrollo local

```bash
make up       # levanta todo en red bridge Docker
make down     # derriba
make proto    # regenerar proto stubs
make build    # compilar binarios
make test     # correr tests
```
