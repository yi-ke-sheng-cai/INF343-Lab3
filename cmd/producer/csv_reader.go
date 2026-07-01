package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

// Evento es una línea del CSV ya validada: una transición de estado de un pedido.
type Evento struct {
	OrderID    string
	Restaurant string
	Actor      string // Restaurante | Repartidor (informativo)
	Status     string
	T          int // tiempo_relativo (orden/concurrencia)
}

// columnas esperadas del CSV (cabecera): pedido_id,restaurante,actor,estado,tiempo_relativo
const (
	colPedidoID = "pedido_id"
	colRest     = "restaurante"
	colActor    = "actor"
	colEstado   = "estado"
	colTiempo   = "tiempo_relativo"
)

// LeerEventos parsea el CSV de forma defensiva: usa la cabecera para mapear
// columnas por nombre (tolera reordenamientos), salta filas corruptas sin
// crashear y las loggea. Devuelve los eventos en el orden del archivo.
func LeerEventos(path string, lg *log.Logger) ([]Evento, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.FieldsPerRecord = -1 // no exigir número fijo; validamos manualmente
	r.TrimLeadingSpace = true

	// Cabecera -> índices de columna.
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	idx := map[string]int{}
	for i, h := range header {
		idx[strings.TrimSpace(strings.ToLower(h))] = i
	}
	req := []string{colPedidoID, colRest, colEstado, colTiempo}
	for _, c := range req {
		if _, ok := idx[c]; !ok {
			lg.Printf("ADVERTENCIA: columna '%s' ausente en la cabecera; se intentará por posición", c)
		}
	}

	get := func(row []string, name string, pos int) string {
		if i, ok := idx[name]; ok && i < len(row) {
			return strings.TrimSpace(row[i])
		}
		if pos < len(row) {
			return strings.TrimSpace(row[pos])
		}
		return ""
	}

	var out []Evento
	line := 1
	for {
		row, err := r.Read()
		line++
		if err == io.EOF {
			break
		}
		if err != nil {
			lg.Printf("fila %d corrupta (%v) -> saltada", line, err)
			continue
		}
		pedido := get(row, colPedidoID, 0)
		rest := get(row, colRest, 1)
		actor := get(row, colActor, 2)
		estado := get(row, colEstado, 3)
		tStr := get(row, colTiempo, 4)

		if pedido == "" || estado == "" {
			lg.Printf("fila %d inválida (pedido/estado vacío): %v -> saltada", line, row)
			continue
		}
		t, err := strconv.Atoi(tStr)
		if err != nil {
			lg.Printf("fila %d con tiempo_relativo inválido (%q), uso 0", line, tStr)
			t = 0
		}
		out = append(out, Evento{OrderID: pedido, Restaurant: rest, Actor: actor, Status: estado, T: t})
	}
	return out, nil
}
