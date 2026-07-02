#!/usr/bin/env bash

if [ -n "$NO_COLOR" ]; then
  B= D= R= RED= GRN= YEL= BLU= MAG= CYA=
else
  R=$'\033[0m'      # reset
  B=$'\033[1m'      # negrita
  D=$'\033[90m'     # gris (dim)
  RED=$'\033[31m'
  GRN=$'\033[32m'
  YEL=$'\033[33m'
  BLU=$'\033[34m'
  MAG=$'\033[35m'
  CYA=$'\033[36m'
fi

rule()  { printf '%s\n' "${D}════════════════════════════════════════════════════════════════════${R}"; }
say()   { printf '%b\n' "$1"; }

echo
rule
say "  ${B}${CYA}DistriEats${R} ${B}· Delivery Distribuido Eventualmente Consistente${R}"
say "  ${D}Go + gRPC/Protobuf · 5 tipos de proceso · gossip + relojes vectoriales · RYW${R}"
rule
echo

say "  ${B}TOPOLOGIA${R}   ${D}(el Broker enruta; los Datanodes convergen por gossip)${R}"
echo
say "   ${MAG}${B}CLIENTES${R} ${MAG}(x3)${R}                        ${YEL}${B}PRODUCTOR${R}"
say "   ${MAG}Hambriento 1 · 2 · 3${R}                ${YEL}lee pedidos.csv${R}"
say "        ${D}│${R}                                   ${D}│${R}"
say "        ${D}│${R} ${D}CrearPedido / ConsultarEstado${R}     ${D}│${R} ${D}evento cada 1-3s${R}"
say "        ${D}▼${R}                                   ${D}│${R}"
say "     ${CYA}╭────────────────────────╮${R}              ${D}│${R}"
say "     ${CYA}│${R}        ${B}${CYA}GATEWAY${R}          ${CYA}│${R} ${D}afinidad${R}     ${D}│${R}"
say "     ${CYA}│${R}   ${D}Read Your Writes (RYW)${R}   ${CYA}│${R} ${D}de sesion${R}    ${D}│${R}"
say "     ${CYA}╰────────────────────────╯${R}              ${D}│${R}"
say "        ${D}│${R} ${D}EnrutarEscritura${R}                ${D}│${R}"
say "        ${D}▼${R}                                   ${D}▼${R}"
say "     ${GRN}╭──────────────────────────────────────────────╮${R}"
say "     ${GRN}│${R}                  ${B}${GRN}BROKER${R}                      ${GRN}│${R}"
say "     ${GRN}│${R}   ${D}Round Robin · health check · sin estado${R}     ${GRN}│${R}"
say "     ${GRN}╰──────────────────────────────────────────────╯${R}"
say "              ${D}│  reparte 1 evento → 1 Datanode${R}"
say "     ${D}┌────────────┼────────────┐${R}"
say "     ${D}▼${R}            ${D}▼${R}            ${D}▼${R}"
say "  ${BLU}${B}[DN1]${R} ${D}◀gossip▶${R} ${BLU}${B}[DN2]${R} ${D}◀gossip▶${R} ${BLU}${B}[DN3]${R}"
say "  ${BLU}relojes vectoriales · merge · resolucion de conflictos${R}"
say "  ${D}(la lectura RYW del Gateway va DIRECTO al DN con afinidad, sin Broker)${R}"
echo

say "  ${B}FLUJO ESPERADO${R}"
say "   ${CYA}1.${R} Productor emite eventos del CSV ${D}→${R} Broker los reparte por ${B}Round Robin${R}"
say "   ${CYA}2.${R} Datanodes convergen por ${B}gossip${R} ${D}(cada ~5s, peer aleatorio)${R} + reloj vectorial"
say "   ${CYA}3.${R} Conflicto ${D}(clocks concurrentes)${R} ${D}→${R} gana el estado mas avanzado ${D}·${R} ${RED}Cancelado${R} manda"
say "   ${CYA}4.${R} Cliente crea pedido ${D}→${R} Gateway registra ${B}afinidad${R} ${D}client→datanode (TTL)${R}"
say "   ${CYA}5.${R} Cliente relee ${D}→${R} Gateway fuerza el ${B}mismo Datanode${R} ${D}→${R} ${GRN}RYW OK${R}"
say "   ${CYA}6.${R} Si un DN ${RED}cae${R}, sale del Round Robin; al volver ${GRN}resincroniza${R} por gossip"
echo

say "  ${B}GARANTIAS${R}"
say "   ${GRN}✓${R} Convergencia eventual ${D}(logs finales identicos)${R}   ${GRN}✓${R} Read Your Writes"
say "   ${GRN}✓${R} Resolucion determinista de conflictos          ${GRN}✓${R} Idempotencia por request_id"
say "   ${GRN}✓${R} Tolerancia a caida y recuperacion de Datanodes"
echo

say "  ${B}LEYENDA DE LOGS${R}   ${D}formato:${R}  ${D}hh:mm:ss${R} ${B}[ENTIDAD]${R} ${D}mensaje${R}"
say "   ${GRN}[BROKER]${R}    round robin · health check · genera Reporte.txt"
say "   ${CYA}[GATEWAY]${R}   afinidad de sesion · RYW · idempotencia"
say "   ${BLU}[DATANODE-x]${R} update · gossip · ${D}conflicto→decision (ambos vectores)${R}"
say "   ${MAG}[CLIENTE-x]${R}  crea pedido · valida ${GRN}RYW OK${R} / ${RED}fallo${R}"
say "   ${YEL}[PRODUCTOR]${R}  emite eventos desde el CSV"
echo
rule
say "  ${B}Levantando contenedores...${R} ${D}los logs comienzan a continuacion${R}"
rule
echo
