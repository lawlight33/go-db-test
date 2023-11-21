package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"sync"

	"github.com/google/uuid"

	_ "github.com/lib/pq"
)

const (
	dbHost      = "localhost"
	dbPort      = 5432
	dbUser      = "postgres"
	dbPassword  = "postgres"
	dbName      = "postgres"
	threadCount = 50
)

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
	query := `insert into loadtest values ($1, $2, $3);`
	stmt, err := db.Prepare(query)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	var wg sync.WaitGroup
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		var index = i
		go func() {
			defer wg.Done()
			fmt.Println("Insert from thread #" + strconv.Itoa(index))
			id1 := uuid.NewString()
			id2 := uuid.NewString()
			id3 := uuid.NewString()
			parameters := []interface{}{id1, id2, id3}
			_, err := stmt.Exec(parameters...)
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
}
