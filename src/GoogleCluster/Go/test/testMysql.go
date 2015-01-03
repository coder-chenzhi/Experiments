package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
)

const (
	QUERY = "insert into task_events values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

func main() {
	db, err := sql.Open("mysql", "root:123@/GoogleCluster")
	rows, err := db.Query("show tables;")
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	db.Close()
}
