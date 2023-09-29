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
	Year string
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
	var mutex sync.Mutex

	// Función para el proceso de Map
	mapFunction := func(tests []CovidTest, wg *sync.WaitGroup) {
		defer wg.Done()
		for _, test := range tests {
			mutex.Lock()
			mapperResult[test.Year]++
			mutex.Unlock()
		}
	}

	// Proceso de Map en lotes de 10,000 con concurrencia
	batchSize := 10000
	var batch []CovidTest
	var wg sync.WaitGroup

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ";")
		if len(fields) >= 3 {
			dateStr := fields[2]
			date, err := time.Parse("20060102", dateStr)
			if err == nil {
				year := date.Year()
				numeroString := strconv.Itoa(year)
				batch = append(batch, CovidTest{Year: numeroString})
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

	endTime := time.Now()
	elapsedTime := endTime.Sub(startTime)
	fmt.Printf("Tiempo transcurrido: %s\n", elapsedTime)

	wg.Wait()

	for year, count := range mapperResult {
		fmt.Printf("Año %s: %d pruebas\n", year, count)
	}
}
