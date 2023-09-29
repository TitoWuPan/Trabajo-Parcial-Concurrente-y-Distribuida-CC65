package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type CovidTest struct {
	Departamento string
	Provincia    string
	Distrito     string
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

	batchSize := 10000
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

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)

	// Resultados de pruebas por ubicación geográfica
	fmt.Println("Resultados geográficos:")
	for geoKey, count := range geographicResult {
		fmt.Printf("Ubicación: %s, Pruebas: %d\n", geoKey, count)
	}

	fmt.Printf("Tiempo transcurrido: %s\n", elapsedTime)
}
