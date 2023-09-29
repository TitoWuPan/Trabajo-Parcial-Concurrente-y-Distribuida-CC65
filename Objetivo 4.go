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
	TipoMuestra string
	Resultado   string
}

func main() {
	file, err := os.Open("data.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	startTime := time.Now()
	scanner := bufio.NewScanner(file)

	// Map: Divide las pruebas en grupos por tipo de muestra
	mapperResult := make(map[string]map[string]int)
	var mutex sync.Mutex

	// Función para el proceso de Map concurrente
	mapFunction := func(tests []CovidTest, wg *sync.WaitGroup) {
		defer wg.Done()
		localResult := make(map[string]int)
		for _, test := range tests {
			mutex.Lock()
			if _, exists := localResult[test.Resultado]; !exists {
				localResult[test.Resultado] = 0
			}
			localResult[test.Resultado]++
			mutex.Unlock()
		}
		mutex.Lock()
		for key, value := range localResult {
			if _, exists := mapperResult[tests[0].TipoMuestra]; !exists {
				mapperResult[tests[0].TipoMuestra] = make(map[string]int)
			}
			mapperResult[tests[0].TipoMuestra][key] += value
		}
		mutex.Unlock()
	}

	batchSize := 1000
	var batch []CovidTest
	var wg sync.WaitGroup

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ";")
		if len(fields) >= 15 {
			tipoMuestra := fields[13]
			resultado := fields[14]
			batch = append(batch, CovidTest{TipoMuestra: tipoMuestra, Resultado: resultado})
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

	wg.Wait()

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)

	fmt.Println("Resultados por tipo de muestra:")
	for tipoMuestra, resultados := range mapperResult {
		fmt.Printf("Tipo de Muestra: %s\n", tipoMuestra)
		for resultado, count := range resultados {
			fmt.Printf("Resultado: %s, Pruebas: %d\n", resultado, count)
		}
	}
	fmt.Printf("Tiempo transcurrido: %s\n", elapsedTime)
}
