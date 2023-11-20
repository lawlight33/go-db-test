package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq" // Import the PostgreSQL driver
)

func main() {
	fmt.Println("List Postgres roles ...")

	dbHost := "localhost"
	dbPort := 5432
	dbUser := "postgres"
	dbPassword := "postgres"
	dbName := "postgres"

	connectionString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName,
	)

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		panic(err)
	}
	defer db.Close()

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
