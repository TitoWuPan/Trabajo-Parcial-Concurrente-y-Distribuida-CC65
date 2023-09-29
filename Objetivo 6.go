package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

type Datos struct {
	Institucion string
	Resultado   string
}

type InstitucionData struct {
	Institucion         string
	TotalPruebas        int
	TotalPositivos      int
	PorcentajePositivos float64
}

func main() {
	// Abre el archivo CSV (ajusta el nombre del archivo)
	file, err := os.Open("data.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Crea un lector CSV
	reader := csv.NewReader(file)
	reader.Comma = ';'

	var data []Datos

	// Lee y almacena los datos del archivo CSV
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		if len(record) >= 6 {
			institucion := record[5]
			resultado := record[7]
			data = append(data, Datos{Institucion: institucion, Resultado: resultado})
		}
	}

	// Define el tamaño del lote (100, 1000, 10000)
	tamanoLote := 100

	// Divide los datos en lotes
	var lotes [][]Datos
	for i := 0; i < len(data); i += tamanoLote {
		lote := data[i:min(i+tamanoLote, len(data))]
		lotes = append(lotes, lote)
	}

	// Crea un mapa para almacenar la relación institución-salud y el recuento de resultados
	institucionSalud := make(map[string]int)
	resultados := make(map[string]int)

	// Procesa los lotes de datos de manera concurrente
	var wg sync.WaitGroup
	var mutex sync.Mutex

	startTime := time.Now()

	for _, lote := range lotes {
		wg.Add(1)
		go func(l []Datos) {
			defer wg.Done()
			localInstitucionSalud := make(map[string]int)
			localResultados := make(map[string]int)

			for _, datos := range l {
				localInstitucionSalud[datos.Institucion]++
				if datos.Resultado == "POSITIVO" {
					localResultados[datos.Institucion]++
				}
			}

			// Actualiza el mapa global con los resultados locales
			mutex.Lock()
			for institucion, count := range localInstitucionSalud {
				institucionSalud[institucion] += count
			}
			for institucion, count := range localResultados {
				resultados[institucion] += count
			}
			mutex.Unlock()
		}(lote)
	}

	// Espera a que se completen todas las goroutines
	wg.Wait()

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)

	// Crea una lista de estructuras InstitucionData para clasificar y mostrar los resultados
	var institucionesData []InstitucionData
	for institucion, totalPruebas := range institucionSalud {
		totalPositivos := resultados[institucion]
		porcentajePositivos := float64(totalPositivos) / float64(totalPruebas) * 100.0
		institucionesData = append(institucionesData, InstitucionData{
			Institucion:         institucion,
			TotalPruebas:        totalPruebas,
			TotalPositivos:      totalPositivos,
			PorcentajePositivos: porcentajePositivos,
		})
	}

	// Ordena la lista por el total de pruebas realizadas de manera ascendente
	sort.SliceStable(institucionesData, func(i, j int) bool {
		return institucionesData[i].TotalPruebas < institucionesData[j].TotalPruebas
	})

	// Muestra los resultados ordenados
	for _, data := range institucionesData {
		fmt.Printf("Institución de Salud: %s\n", data.Institucion)
		fmt.Printf("   Total de Pruebas Realizadas: %d\n", data.TotalPruebas)
		fmt.Printf("   Total de Resultados Positivos: %d\n", data.TotalPositivos)
		fmt.Printf("   Porcentaje de Positivos: %.2f%%\n", data.PorcentajePositivos)
	}

	// Muestra el tiempo de ejecución en milisegundos
	fmt.Printf("Tiempo de ejecución: %d milisegundos\n", elapsedTime.Milliseconds())
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
