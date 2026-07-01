# LAB3_INIT.md — DistriEats: Especificación Técnica de Implementación

> Documento de referencia para implementación completa del Laboratorio 3 (Sistemas Distribuidos, USM, 2026-1).
> Objetivo: implementar el sistema descrito abajo en Go + gRPC + Protocol Buffers + Docker, siguiendo estrictamente las reglas de consistencia (Eventual con Relojes Vectoriales + Read Your Writes).

---

## 0. Contexto en una frase

Simular "DistriEats": un sistema de delivery donde los **Datanodes** (réplicas de BD) convergen de forma **eventualmente consistente** vía **gossip + relojes vectoriales**, mientras el **Gateway** garantiza **Read Your Writes** a los clientes mediante afinidad de sesión. El **Broker** es un router sin estado. Los **Restaurantes/Repartidores** son un generador de eventos leído desde CSV.

---

## 1. Stack y restricciones DURAS

- Lenguaje: **Go**
- RPC: **gRPC** + **Protocol Buffers** (comunicación síncrona entre TODAS las entidades — nada de HTTP REST, nada de colas externas)
- Contenedores: **Docker** (obligatorio — nota 0 si no se usa)
- **Prohibido**: RabbitMQ, Kafka, NATS o cualquier cola de mensajes externa.
- **Librerías Go permitidas** (además de gRPC/protobuf):
  `fmt, log, errors, os, io, bufio, time, sync, sort, math/rand, encoding/csv, encoding/json, net, context, flag, strconv, strings`
  → Cualquier otra librería externa debe evitarse o quedar claramente justificada.
- **Nada hardcodeado**: direcciones IP, puertos, número de Datanodes, rutas de archivos, TTLs, intervalos → todo debe venir de flags/env vars/archivo de config, porque la evaluación usa una configuración distinta a la de desarrollo.

---

## 2. Entidades y conteo requerido

| Entidad | Instancias | Estado | Rol |
|---|---|---|---|
| Cliente Hambriento | **3** | Stateful (en memoria, por cliente) | Genera pedidos, valida RYW |
| Gateway de Pedidos | **1** | Stateful (mapa de afinidad de sesión) | Único punto de entrada de clientes; garantiza RYW |
| Broker Central | **1** | Sin estado | Router / balanceador Round Robin / distribuidor de eventos CSV |
| Datanode | **3** | Stateful (persistente en memoria/disco) | Almacenamiento replicado, resolución de conflictos, gossip |
| Restaurante/Repartidor | 1 proceso ligero (puede vivir junto al Broker) | Sin estado propio relevante | Emisor de eventos desde `pedidos.csv` |

Cada entidad = **1 contenedor Docker independiente**.

---

## 3. Definición de Protobuf (esqueleto obligatorio)

Crear un único paquete `.proto` compartido (o varios coherentes) con como mínimo estos mensajes y servicios. Ajustar nombres de campos pero mantener la semántica.

```proto
syntax = "proto3";
package distrieats;

// --- Reloj vectorial ---
message VectorClock {
  map<string, int64> entries = 1; // clave: "DN1", "DN2", "DN3" (o node_id)
}

// --- Pedido ---
message Order {
  string order_id = 1;
  string client_id = 2;
  string restaurant = 3;      // BurgerNode, DockerPizza, SushiStream, GopherTacos
  string status = 4;          // Recibido, Preparando, En Camino, Entregado, Cancelado
  VectorClock clock = 5;
  int64 timestamp = 6;
}

// --- Cliente -> Gateway ---
message CrearPedidoRequest {
  string request_id = 1;      // idempotencia
  string client_id = 2;
  string order_id = 3;
  string restaurant = 4;
  repeated string items = 5;
}
message CrearPedidoResponse {
  bool success = 1;
  string message = 2;
  Order order = 3;
}
message ConsultarEstadoRequest {
  string client_id = 1;
  string order_id = 2;
}
message ConsultarEstadoResponse {
  bool found = 1;
  Order order = 2;
}

// --- Gateway/Broker -> Datanode ---
message UpdateOrderRequest {
  Order order = 1;            // incluye vector clock del emisor
}
message UpdateOrderResponse {
  bool applied = 1;
  Order resulting_order = 2;  // estado final tras merge/resolución
}
message GetOrderRequest {
  string order_id = 1;
}
message GetOrderResponse {
  bool found = 1;
  Order order = 2;
}

// --- Gossip entre Datanodes ---
message GossipSyncRequest {
  repeated Order orders = 1;  // snapshot parcial o total del estado local
  VectorClock sender_clock = 2;
}
message GossipSyncResponse {
  repeated Order orders = 1;  // lo que el receptor devuelve para fusionar
}

// --- Broker -> Datanode: registro/salud ---
message PingRequest {}
message PingResponse { bool alive = 1; }

service GatewayService {
  rpc CrearPedido(CrearPedidoRequest) returns (CrearPedidoResponse);
  rpc ConsultarEstado(ConsultarEstadoRequest) returns (ConsultarEstadoResponse);
}

service BrokerService {
  rpc EnrutarEscritura(UpdateOrderRequest) returns (UpdateOrderResponse);
  rpc EnrutarLectura(GetOrderRequest) returns (GetOrderResponse);
  rpc EmitirEventoLogistico(UpdateOrderRequest) returns (UpdateOrderResponse); // desde Restaurante/Repartidor
}

service DatanodeService {
  rpc UpdateOrder(UpdateOrderRequest) returns (UpdateOrderResponse);
  rpc GetOrder(GetOrderRequest) returns (GetOrderResponse);
  rpc GossipSync(GossipSyncRequest) returns (GossipSyncResponse);
  rpc Ping(PingRequest) returns (PingResponse);
}
```

> Nota: el Gateway actúa como cliente gRPC del Broker Y como cliente gRPC directo de los Datanodes (para lecturas con afinidad). Debe implementar ambos stubs.

---

## 4. Lógica crítica #1 — Relojes Vectoriales (Datanodes)

### Estructura
- Vector con una entrada por Datanode: `{DN1: int, DN2: int, DN3: int}`.
- Cada Datanode incrementa **su propia entrada** cada vez que **origina o aplica localmente** un cambio.

### Algoritmo de recepción de actualización (`UpdateOrder` y también aplicable dentro de `GossipSync`)
1. Recibir `Order` con su `VectorClock` adjunto.
2. Comparar contra el clock almacenado localmente para ese `order_id` (si no existe, aplicar directo y guardar).
3. Determinar relación causal:
   - **A domina a B** (A > B) si A[i] >= B[i] para todo i, y A[j] > B[j] para al menos un j.
   - **Concurrentes** si ni A domina a B ni B domina a A.
4. Reglas de aplicación:
   - Si el mensaje entrante **domina causalmente** al estado guardado → aplicar el nuevo estado sin ambigüedad.
   - Si el estado guardado domina al mensaje entrante → **descartar** el mensaje (ya está superado, evita retroceso).
   - Si son **concurrentes** → **conflicto detectado** → aplicar política determinista (ver abajo).
5. **Merge del vector**: nuevo_clock[i] = max(clock_local[i], clock_mensaje[i]) para cada i. Este merge se hace SIEMPRE, independiente de qué estado "gane".
6. Loggear explícitamente: recepción, comparación, resultado (dominancia/concurrencia), y estado final aplicado.

### Política de resolución determinista (orden de avance lógico)
```
Recibido  <  Preparando  <  En Camino  <  Entregado
Cancelado  =  prioridad absoluta sobre cualquier otro estado (siempre gana)
```
En conflicto (concurrencia): se aplica el estado **más avanzado** en esta cadena. `Cancelado` nunca es sobrescrito por ningún otro estado, y sobrescribe a cualquiera.

### Gossip (sincronización entre pares)
- Cada Datanode corre una **goroutine en background** con `time.Sleep` aleatorio (ej. 3-7s, configurable) o ticker fijo (ej. cada 5s).
- En cada ciclo: elegir un peer al azar (`math/rand`) del conjunto de Datanodes vivos (excluyéndose a sí mismo).
- Enviar `GossipSync` con su estado local (o un digest/subset — decidir e implementar consistentemente).
- Al recibir, aplicar el mismo algoritmo de merge/resolución de conflictos descrito arriba, orden por orden.
- Debe tolerar que el peer esté caído (timeout/error de conexión) → loggear y continuar el ciclo, sin crashear.

### Recuperación tras caída
- Al reiniciar un Datanode caído, debe:
  1. Arrancar con estado vacío o persistido (a elección, pero debe declararse en README).
  2. Iniciar inmediatamente su ciclo de gossip para solicitar el estado histórico a sus pares.
  3. Converger sin intervención manual adicional.

---

## 5. Lógica crítica #2 — Read Your Writes (Gateway)

### Estructura de datos en el Gateway
```go
type SessionEntry struct {
    DatanodeID string
    ExpiresAt  time.Time
}
// map protegido por sync.RWMutex o sync.Map
sessionAffinity map[string]SessionEntry // key = client_id
```
- **TTL obligatorio** (configurable, ej. 60s) para evitar crecimiento indefinido de memoria. Implementar limpieza (goroutine periódica de expiración, o verificación lazy al leer).

### Flujo de escritura (`CrearPedido`)
1. Gateway recibe `CrearPedidoRequest` (incluye `request_id` único para idempotencia).
2. Selecciona un Datanode (puede pedírselo al Broker o eligir directamente — documentar la decisión).
3. Envía la escritura al Broker (`EnrutarEscritura`) indicando el Datanode destino, o el Broker decide y el Gateway recibe la respuesta con el Datanode que la procesó.
4. **Registra `client_id → datanode_id` en el mapa de afinidad con TTL renovado.**
5. Devuelve confirmación al cliente.

### Flujo de lectura (`ConsultarEstado`)
1. Gateway recibe `ConsultarEstadoRequest`.
2. Si existe afinidad activa (no expirada) para ese `client_id` → **forzar** la consulta directamente a ESE Datanode (bypass del Broker).
3. Si no existe afinidad → reenviar al Broker (`EnrutarLectura`) para balanceo normal.

### Idempotencia en Clientes
- Cada `CrearPedidoRequest` lleva un `request_id` único (ej. UUID simple con `math/rand` + timestamp, ya que no hay librería `uuid` permitida — implementar generador propio).
- El Gateway o el Datanode debe poder detectar y descartar reintentos duplicados con el mismo `request_id` (mantener un set de `request_id` procesados, con TTL razonable).

### Manejo de errores esperado
- Producto agotado / condición de carrera al pagar → el Gateway debe poder recibir y propagar un error estructurado (`success=false, message=...`) sin crashear.
- Cliente debe manejar ese error y loggearlo (no debe intentar validar RYW sobre una escritura fallida).

---

## 6. Broker Central — comportamiento

- **Sin estado de negocio.** Solo enrutamiento.
- Balanceo de escrituras/lecturas sin afinidad → **Round Robin** simple sobre la lista de Datanodes vivos (mantener un índice con `sync.Mutex` o `atomic`).
- Debe detectar Datanodes caídos (timeout en la llamada gRPC) y excluirlos temporalmente del round robin; debe poder reincorporarlos cuando vuelvan a responder (health check periódico simple con `Ping`).
- Lee `pedidos.csv` línea por línea (usar `encoding/csv` + `bufio`), simulando emisión de eventos con espera aleatoria entre 1-3s (`math/rand` + `time.Sleep`).
- Cada evento leído se envía como `UpdateOrderRequest` — el Broker debe decidir a qué Datanode(s) enviarlo (broadcast a todos, o round robin — el enunciado dice "distribuye la carga operativa hacia la capa de almacenamiento", y separadamente que Datanodes convergen por gossip, así que **round robin a un Datanode por evento es válido y coherente con el diseño**; documentar la decisión en README).

### Formato esperado de `pedidos.csv` (a confirmar con el archivo real de los ayudantes, pero implementar parser flexible)
Columnas esperadas: `order_id, restaurant, status, tiempo_relativo` (o similar). Implementar parsing defensivo (validar columnas, loggear filas corruptas sin crashear el proceso).

---

## 7. Fases de ejecución — checklist de comportamiento observable

- [ ] **Fase 1 — Init**: todos los contenedores levantan vía `docker-compose`/Makefile, Datanodes escuchan gRPC, Broker y Gateway conectan a Datanodes disponibles. Loggear conexión exitosa de cada componente.
- [ ] **Fase 2 — Apertura red logística**: Broker lee CSV y empieza a emitir eventos (1-3s de intervalo) hacia Datanodes; gossip corriendo en paralelo cada ~5s entre Datanodes.
- [ ] **Fase 3 — RYW transaccional**: 3 Clientes envían `CrearPedido` concurrentemente al Gateway; cada uno hace `ConsultarEstado` inmediatamente después y **debe imprimir en consola un mensaje explícito de éxito de validación RYW** (ej. `"[Cliente 1] RYW OK: pedido Ped-004 confirmado en Datanode 2"`).
- [ ] **Fase 4 — Caos**: detener manualmente un Datanode (`docker stop <nombre>`) durante tráfico activo. Broker/Gateway deben seguir operando sin caer (manejar error de conexión, excluir el nodo caído). Al reiniciar (`docker start`), el nodo debe re-sincronizar vía gossip solicitando estado a sus pares.
- [ ] **Fase 5 — Convergencia y cierre**: tras terminar el CSV y las transacciones de clientes, esperar ventana de gracia (~15s, configurable) para que gossip termine de propagar. Cada Datanode escribe su log final de estado completo a archivo. **Los logs de todos los Datanodes activos deben ser idénticos** (mismo contenido, mismo estado final por pedido).

---

## 8. Reporte final (`Reporte.txt`)

Generado por el Broker o un script de auditoría al cierre de la Fase 5. Debe contener:

1. **Resumen global de pedidos**: `order_id | estado final | vector clock final` — uno por línea, para TODOS los pedidos que entraron al sistema.
2. **Auditoría RYW**: lista de validaciones exitosas de clientes con el Datanode donde se confirmó la afinidad.

Formato exacto de referencia (mantener este formato):
```
=== REPORTE FINAL : DISTRIEATS ===

[ESTADO GLOBAL DE PEDIDOS - Convergencia Alcanzada]
Pedido ID: Ped-001 | Estado Final: Entregado | Reloj Vectorial: [DN1:3, DN2:2, DN3:2]
Pedido ID: Ped-002 | Estado Final: Cancelado | Reloj Vectorial: [DN1:1, DN2:1, DN3:3]

[AUDITORIA READ YOUR WRITES]
- Cliente 1 (Ped-004): Validacion Exitosa en Datanode 2 (Afinidad de sesion confirmada).
- Cliente 2 (Ped-005): Validacion Exitosa en Datanode 1 (Afinidad de sesion confirmada).
=================================
```

**Criterio de éxito verificable**: el contenido de estado global en los logs de cada Datanode debe coincidir exactamente entre sí (usarlo como test de convergencia).

---

## 9. Estructura de proyecto sugerida

```
/proto/
    distrieats.proto
    generated/                 # código generado por protoc (go)
/gateway/
    main.go
    session_affinity.go
    Dockerfile
/broker/
    main.go
    roundrobin.go
    csv_reader.go
    Dockerfile
/datanode/
    main.go
    vector_clock.go
    conflict_resolution.go
    gossip.go
    storage.go
    Dockerfile
/client/
    main.go
    Dockerfile
/restaurant_producer/
    main.go                    # o vive dentro de /broker si se implementa como proceso adjunto
    Dockerfile
/data/
    pedidos.csv                # provisto por ayudantes
/Makefile                      # atajos make docker-VM1..VM4
/docker-compose.yml            # o equivalente por VM
/README.md
```

---

## 10. Distribución en VMs (Docker) — según enunciado

| VM | Contenedores |
|---|---|
| MV1 | Broker Central, Productor de Eventos |
| MV2 | Gateway de Pedidos, Cliente Hambriento 1, Datanode 1 |
| MV3 | Cliente Hambriento 2, Datanode 2 |
| MV4 | Cliente Hambriento 3, Datanode 3 |

Makefile debe exponer al menos: `make docker-VM1`, `make docker-VM2`, `make docker-VM3`, `make docker-VM4` (y probablemente un `make docker-all` o `make build` conveniente para desarrollo local).

**Todas las direcciones/puertos entre entidades deben ser configurables** (flags o variables de entorno), ya que en evaluación se usará una topología/config distinta.

---

## 11. Logging — requisito transversal obligatorio

Cada entidad debe imprimir logs claros y trazables de:
- Mensajes recibidos/enviados (tipo, origen/destino, IDs relevantes).
- Detección y resolución de conflictos (mostrar los dos vectores comparados y la decisión tomada).
- Redirecciones de sesión del Gateway (a qué Datanode se redirige cada lectura/escritura y por qué).
- Eventos de gossip (con quién se sincronizó y qué se fusionó).
- Caídas/reconexiones de Datanodes detectadas por Broker/Gateway.

Usar `log` estándar con prefijos por entidad, ej: `[DATANODE-2] `, `[GATEWAY] `, `[CLIENTE-1] `.

---

## 12. Checklist final de verificación antes de entregar

- [ ] Código indentado (`gofmt`/`go vet` limpio), comentado, sin warnings, sin errores.
- [ ] Sin librerías externas no autorizadas.
- [ ] Nada hardcodeado (IPs, puertos, N° de nodos, paths, TTLs, intervalos → todos configurables).
- [ ] Cada entidad en contenedor Docker separado.
- [ ] Makefile funcional por VM.
- [ ] README con integrantes, roles, e instrucciones de ejecución completas.
- [ ] `Reporte.txt` se genera automáticamente al final de la simulación.
- [ ] Logs de todos los Datanodes coinciden exactamente al converger (test manual de éxito).
- [ ] Demostrado: RYW funcionando (mensaje de validación en consola de cada cliente).
- [ ] Demostrado: tolerancia a fallos (caída y recuperación de un Datanode sin detener el sistema).
- [ ] Demostrado: resolución de conflictos concurrentes con relojes vectoriales (loggeado explícitamente al menos una vez).
- [ ] `.zip` de entrega: `GrupoXX-Lab3.zip` con carpetas separadas por entidad.

---

## 13. Orden de implementación recomendado (para Claude Code)

1. `proto/distrieats.proto` completo + generación de código Go (`protoc`).
2. `datanode`: storage en memoria + vector clock + resolución de conflictos (unit tests simples de merge/dominancia/concurrencia).
3. `datanode`: gossip (goroutine + selección de peer aleatorio + aplicar merge).
4. `broker`: round robin + health check de Datanodes + lector CSV con emisión temporizada.
5. `gateway`: mapa de afinidad con TTL + lógica RYW + idempotencia de `request_id`.
6. `client`: flujo lectura→escritura→lectura→validación, con logging de éxito/fallo RYW.
7. `restaurant_producer` (si se separa del broker).
8. Generación de `Reporte.txt` (al final del broker o script separado).
9. Dockerfiles + docker-compose/Makefile por VM.
10. Pruebas de caos (docker stop/start) y validación de convergencia final.
11. README + limpieza de logs/código.