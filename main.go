package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	_ "github.com/lib/pq"
)

const (
	dbHost      = "localhost"
	dbPort      = 5432
	dbUser      = "postgres"
	dbPassword  = "postgres"
	dbName      = "postgres"
	threadCount = 80
)

// Admin SQL commands:
// create table loadtest_1 (
//     id1 uuid primary key,
//     created_at timestamp with time zone
// )
// select count(*) from loadtest_1
// select id1, created_at from loadtest_1 order by id1 asc

// Export sorted table (by created_at date field) to txt shell command:
// psql -U postgres -h localhost -p 5432 -c "\pset pager off" -c "select id1 from loadtest order by id1 asc;" > log.txt

func main() {
	fmt.Println("Running ...")

	connectionString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName,
	)

	// Connect to the Postgres database
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Make select operation
	selectExample(db)

	// Make insert operation
	parallelInsertExample(db, threadCount)

	fmt.Println("Done.")
}

func selectExample(db *sql.DB) {
	fmt.Println("Listing Postgres roles:")
	rows, err := db.Query("select rolname from pg_roles")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			panic(err)
		}
		fmt.Println("> " + name)
	}
}

func parallelInsertExample(db *sql.DB, threads int) {
	query := `insert into loadtest_1 values ($1, $2);`
	stmt, err := db.Prepare(query)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	startDate := time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		var index = i
		go func() {
			defer wg.Done()
			for j := 0; j < 20000; j++ {
				fmt.Println("Insert from thread #" + strconv.Itoa(index))
				id1 := uuid.NewString()
				created_at := randomDate(startDate, endDate)
				parameters := []interface{}{id1, created_at}
				_, err := stmt.Exec(parameters...)
				if err != nil {
					panic(err)
				}
			}
		}()
	}
	wg.Wait()
}

func randomDate(startDate time.Time, endDate time.Time) string {
	duration := endDate.Sub(startDate)
	randomDuration := time.Duration(rand.Int63n(int64(duration)))
	randomDate := startDate.Add(randomDuration)
	return randomDate.Format("2006-01-02")
}
