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
	Edad   int
	Genero string
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
		if len(record) >= 4 {
			edad := atoi(record[3])
			genero := record[4]
			data = append(data, Datos{Edad: edad, Genero: genero})
		}
	}

	// Especifica el tamaño del lote deseado (por ejemplo, 100, 1000 o 10000)
	tamanoLote := 100

	// Crea un mapa para almacenar la relación género-edad
	generoPorEdad := make(map[string]map[int]int)

	// Procesa los datos en lotes de tamaño tamanoLote de manera concurrente
	var wg sync.WaitGroup
	var mutex sync.Mutex

	startTime := time.Now()

	for i := 0; i < len(data); i += tamanoLote {
		end := i + tamanoLote
		if end > len(data) {
			end = len(data)
		}
		lote := data[i:end]

		wg.Add(1)
		go func(l []Datos) {
			defer wg.Done()
			for _, datos := range l {
				mutex.Lock()
				if _, ok := generoPorEdad[datos.Genero]; !ok {
					generoPorEdad[datos.Genero] = make(map[int]int)
				}
				generoPorEdad[datos.Genero][datos.Edad]++
				mutex.Unlock()
			}
		}(lote)
	}

	// Espera a que se completen todas las goroutines
	wg.Wait()

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)

	// Muestra la relación género-edad en orden ascendente por edad
	for genero, edades := range generoPorEdad {
		fmt.Printf("Género: %s\n", genero)

		// Obtén las edades y ordénalas
		var edadesOrdenadas []int
		for edad := range edades {
			edadesOrdenadas = append(edadesOrdenadas, edad)
		}
		sort.Ints(edadesOrdenadas)

		// Imprime las edades ordenadas
		for _, edad := range edadesOrdenadas {
			cantidad := edades[edad]
			fmt.Printf("   Edad: %d, Cantidad: %d\n", edad, cantidad)
		}
	}

	// Muestra el tiempo de ejecución en milisegundos
	fmt.Printf("Tiempo de ejecución: %d milisegundos\n", elapsedTime.Milliseconds())
}

func atoi(s string) int {
	i := 0
	for _, r := range s {
		i = i*10 + int(r-'0')
	}
	return i
}
