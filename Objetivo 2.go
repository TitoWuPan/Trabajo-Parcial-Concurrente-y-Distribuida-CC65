package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type CovidTest struct {
	Departamento string
	Provincia    string
	Distrito     string
}

type GeographicResult struct {
	Location string
	Count    int
}

func main() {
	file, err := os.Open("data.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	startTime := time.Now()

	scanner := bufio.NewScanner(file)

	// Map: Divide las pruebas en grupos por ubicación geográfica
	geographicResult := make(map[string]int)
	var mutex sync.Mutex

	// Función para el proceso de Map concurrente
	mapFunction := func(tests []CovidTest, wg *sync.WaitGroup) {
		defer wg.Done()
		for _, test := range tests {
			mutex.Lock()
			geoKey := fmt.Sprintf("%s-%s-%s", test.Departamento, test.Provincia, test.Distrito)
			geographicResult[geoKey]++
			mutex.Unlock()
		}
	}

	// Proceso de Map en lotes de 1,000 con concurrencia
	batchSize := 1000
	var batch []CovidTest
	var wg sync.WaitGroup

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ";")
		if len(fields) >= 14 {
			departamento := fields[7]
			provincia := fields[8]
			distrito := fields[9]
			batch = append(batch, CovidTest{
				Departamento: departamento,
				Provincia:    provincia,
				Distrito:     distrito,
			})
			if len(batch) == batchSize {
				wg.Add(1)
				go mapFunction(batch, &wg)
				batch = nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	if len(batch) > 0 {
		wg.Add(1)
		go mapFunction(batch, &wg)
	}

	wg.Wait() // Esperar a que todas las goroutines finalicen

	// Convertir el mapa en un slice para ordenarlo
	var geographicResults []GeographicResult
	for geoKey, count := range geographicResult {
		geographicResults = append(geographicResults, GeographicResult{
			Location: geoKey,
			Count:    count,
		})
	}

	// Ordenar los resultados por número de pruebas (ascendente)
	sort.Slice(geographicResults, func(i, j int) bool {
		return geographicResults[i].Count < geographicResults[j].Count
	})

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)

	// Resultados de pruebas por ubicación geográfica ordenados
	fmt.Println("Resultados geográficos ordenados:")
	for _, result := range geographicResults {
		fmt.Printf("Ubicación: %s, Pruebas: %d\n", result.Location, result.Count)
	}

	fmt.Printf("Tiempo transcurrido: %s\n", elapsedTime)
}
