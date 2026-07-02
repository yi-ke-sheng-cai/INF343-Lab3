# DistriEats — Laboratorio 3 (INF343 Sistemas Distribuidos, USM 2026-1)

Simulación de un sistema de delivery donde las réplicas de base de datos
(**Datanodes**) convergen de forma **eventualmente consistente** mediante
**gossip + relojes vectoriales**, mientras el **Gateway** garantiza **Read Your
Writes (RYW)** a los clientes vía afinidad de sesión. El **Broker** es un router
sin estado y el **Productor** emite eventos leídos desde un CSV.

Stack: **Go + gRPC + Protocol Buffers + Docker**. Sin colas externas, sin HTTP.

---

## Integrantes

| Nombre | Rol |
|---|---|
| _(completar)_ | _(completar)_ |
| _(completar)_ | _(completar)_ |

---

## Arquitectura

| Entidad | Instancias | Estado | Rol |
|---|---|---|---|
| Cliente Hambriento (`cmd/client`) | 3 | Stateful | Genera pedidos y valida RYW |
| Gateway (`cmd/gateway`) | 1 | Stateful | Punto de entrada; afinidad de sesión (RYW) |
| Broker (`cmd/broker`) | 1 | Sin estado | Router Round Robin + health check + Reporte.txt |
| Datanode (`cmd/datanode`) | 3 | Stateful | Almacenamiento replicado, conflictos, gossip |
| Productor (`cmd/producer`) | 1 | Sin estado | Emite eventos desde `pedidos.csv` al Broker |

Código compartido en `internal/`:
- `internal/vclock` — relojes vectoriales y política de resolución de conflictos (con tests).
- `internal/util` — configuración (flag > env), generación de IDs y helpers gRPC.

Contrato gRPC único: `proto/distrieats.proto` (generado en `proto/pb/`).

### Distribución en VMs (enunciado §10)

| VM | Contenedores |
|---|---|
| MV1 | Broker Central, Productor de Eventos |
| MV2 | Gateway, Cliente 1, Datanode 1 |
| MV3 | Cliente 2, Datanode 2 |
| MV4 | Cliente 3, Datanode 3 |

---

## Ejecución

### Local (una máquina, red bridge Docker)

```bash
make up
make down
```

Salidas en `resultados/`: `Reporte.txt` y `estado_final_DN{1,2,3}.log`.

### Evaluación en 4 VMs (network_mode host)

Las 4 VMs deben tener conectividad de red entre sí y el `.env` con las IPs reales.
Ejecutar en el siguiente orden — **MV2 → MV3 → MV4 → MV1** (Datanodes primero, Broker al final).

#### 1. Terminal MV2 (dist058 · 10.35.168.68) — DN1 + Gateway + Client1
```bash
cd INF343-Lab3
docker-compose -f docker-compose-vm2.yml up --build
```

#### 2. Terminal MV3 (dist059 · 10.35.168.69) — DN2 + Client2
```bash
cd INF343-Lab3
docker-compose -f docker-compose-vm3.yml up --build
```

#### 3. Terminal MV4 (dist060 · 10.35.168.70) — DN3 + Client3
```bash
cd INF343-Lab3
docker-compose -f docker-compose-vm4.yml up --build
```

#### 4. Terminal MV1 (dist057 · 10.35.168.67) — Broker + Producer
```bash
cd INF343-Lab3
mkdir -p resultados
docker-compose -f docker-compose-vm1.yml build
docker-compose -f docker-compose-vm1.yml up
```

> Una vez iniciado, el sistema ejecuta la simulación automáticamente: el Productor emite eventos del CSV, los Datanodes convergen por gossip, los Clientes validan RYW. Al terminar se genera `resultados/Reporte.txt` y los `estado_final_DN*.log`.

### Desarrollo sin Docker

```bash
make proto
make build
make test
```

Toda la configuración es por **flags** (con fallback a **variables de entorno**).
Ejemplo:

```bash
bin/datanode -id DN1 -puerto 50061 -peers DN2@host:50062,DN3@host:50063 \
             -gossip-min 3s -gossip-max 7s -final-log estado_final_DN1.log
```

---

## Decisiones de diseño (según enunciado)

1. **Distribución de eventos del Broker → Round Robin a UN Datanode por evento.**
   La convergencia global la garantiza el gossip entre Datanodes. Es coherente
   con "distribuir la carga operativa hacia la capa de almacenamiento" (§6).

2. **Selección de Datanode en escrituras.** El Gateway delega en el Broker
   (`EnrutarEscritura`); el Broker elige por Round Robin y devuelve el
   `datanode_id` que procesó. El Gateway registra la afinidad `client_id →
   datanode_id`.

3. **Read Your Writes.** Con afinidad activa (no expirada), el Gateway fuerza la
   lectura **directamente** a ese Datanode (bypass del Broker). Sin afinidad,
   delega la lectura al Broker (Round Robin). TTL de afinidad configurable
   (`-ttl`, def. 60s) con limpieza lazy + goroutine periódica.

4. **Relojes vectoriales.** Cada pedido lleva su propio `VectorClock` (una
   entrada por Datanode). Al **originar** un cambio (`UpdateOrder` desde
   Broker/Gateway/Productor) el Datanode incrementa su propia entrada; al
   **replicar** por gossip aplica el algoritmo causal completo. El merge del
   vector (máximo entrada por entrada) se hace **siempre**.

5. **Resolución de conflictos (concurrencia).** Política determinista:
   `Recibido < Preparando < En Camino < Entregado`, y `Cancelado` con prioridad
   absoluta (nunca se sobrescribe y sobrescribe a cualquiera). Desempates por
   timestamp y `order_id` para garantizar convergencia idéntica en todos los
   nodos. Cada conflicto se loguea con **ambos vectores** y la decisión.

6. **Persistencia y recuperación.** Estado **en memoria**. Un Datanode que
   reinicia arranca **vacío** e inmediatamente inicia gossip, solicitando el
   estado histórico a sus pares y convergiendo sin intervención manual.

7. **Idempotencia.** Cada `CrearPedido`/evento lleva un `request_id` único
   (generador propio sin librería `uuid`). El Gateway cachea la respuesta por
   `request_id`; el Datanode descarta reintentos ya aplicados (no re-avanza el
   reloj). Ambos con TTL.

8. **Reporte.txt (Fase 5).** Cuando el Productor termina el CSV, señala al Broker
   (`SenalarFinEventos`); el Broker espera una **ventana de gracia** configurable
   (`-grace`, def. 15s) para que el gossip converja y genera `Reporte.txt`
   automáticamente, tomando el estado global de un Datanode (`Snapshot`) y la
   auditoría RYW del Gateway (`ObtenerAuditoriaRYW`). También se genera como
   fallback ante SIGINT/SIGTERM (sin sobrescribir uno bueno con uno vacío).

9. **Tolerancia a fallos.** El Broker hace `Ping` periódico (health check) y
   excluye del Round Robin a los Datanodes caídos, reincorporándolos al volver.
   El Gateway, si su Datanode afín no responde, cae al Broker en vez de fallar.
   El gossip tolera peers caídos (timeout → log → continúa).

---

## Fases observables

- **Fase 1 – Init:** todos los contenedores levantan; Datanodes escuchan gRPC;
  Broker y Gateway conectan. Cada componente loguea su arranque.
- **Fase 2 – Red logística:** el Productor emite eventos del CSV al Broker
  (intervalo aleatorio configurable); el gossip corre en paralelo.
- **Fase 3 – RYW:** los 3 Clientes crean pedidos y consultan de inmediato,
  imprimiendo `RYW OK: pedido ... confirmado en DN...`.
- **Fase 4 – Caos:** `docker compose stop dn2` durante tráfico; el sistema sigue
  operando. `docker compose start dn2` → re-sincroniza por gossip.
- **Fase 5 – Convergencia:** tras la ventana de gracia, `Reporte.txt` se genera y
  los `estado_final_DN*.log` de los 3 Datanodes quedan **idénticos**.

### Verificar convergencia

```bash
for n in 1 2 3; do grep '^Pedido' resultados/estado_final_DN$n.log | sort > /tmp/c$n; done
diff /tmp/c1 /tmp/c2 && diff /tmp/c2 /tmp/c3 && echo "CONVERGENCIA OK"
```

---

## Formato del CSV

`data/pedidos_*.csv` con cabecera:
`pedido_id,restaurante,actor,estado,tiempo_relativo`. El parser es **defensivo**
(mapea columnas por nombre, salta filas corruptas sin crashear). Los eventos que
comparten `tiempo_relativo` se emiten en ráfaga (concurrentes) para ejercitar la
resolución de conflictos.

---

## Librerías

Solo se usan `google.golang.org/grpc` y `google.golang.org/protobuf` (obligatorias
para gRPC) además de la biblioteca estándar de Go permitida por el enunciado.
