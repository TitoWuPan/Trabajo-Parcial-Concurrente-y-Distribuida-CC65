package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CovidTest struct {
	Year      string
	Resultado string
}

func main() {
	file, err := os.Open("data.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	startTime := time.Now()
	scanner := bufio.NewScanner(file)

	// Map: Divide las pruebas en grupos por año
	mapperResult := make(map[string]int)
	totalTests := 0
	positiveTests := 0
	negativeTests := 0
	var mutex sync.Mutex

	// Función para el proceso de Map concurrente
	mapFunction := func(tests []CovidTest, wg *sync.WaitGroup) {
		defer wg.Done()
		localPositiveTests := 0
		localNegativeTests := 0
		for _, test := range tests {
			mutex.Lock()
			mapperResult[test.Year]++
			if test.Resultado == "POSITIVO" {
				localPositiveTests++
			}
			if test.Resultado == "NEGATIVO" {
				localNegativeTests++
			}
			totalTests++
			mutex.Unlock()
		}
		mutex.Lock()
		positiveTests += localPositiveTests
		negativeTests += localNegativeTests
		mutex.Unlock()
	}

	batchSize := 1000
	var batch []CovidTest
	var wg sync.WaitGroup

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ";")
		if len(fields) >= 3 {
			dateStr := fields[2]
			resultado := fields[14]
			date, err := time.Parse("20060102", dateStr)
			if err == nil {
				year := date.Year()
				numeroString := strconv.Itoa(year)
				batch = append(batch, CovidTest{Year: numeroString, Resultado: resultado})
				if len(batch) == batchSize {
					wg.Add(1)
					go mapFunction(batch, &wg)
					batch = nil
				}
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
	fmt.Printf("Tiempo transcurrido: %s\n", elapsedTime)

	positivityRate := float64(positiveTests) / float64(totalTests) * 100
	negativityRate := float64(negativeTests) / float64(totalTests) * 100

	fmt.Printf("Número total de pruebas: %d\n", totalTests)
	fmt.Printf("Número de pruebas positivas: %d\n", positiveTests)
	fmt.Printf("Número de pruebas negativas: %d\n", negativeTests)
	fmt.Printf("Tasa de positividad: %.2f%%\n", positivityRate)
	fmt.Printf("Tasa de negatividad: %.2f%%\n", negativityRate)
}
