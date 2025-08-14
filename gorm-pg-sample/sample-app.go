package main

import (
	"fmt"
	"log"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	_ "github.com/googleapis/go-sql-spanner"
)

type AllTypes struct {
	ColBigint  int64
	ColVarchar string
}

func main() {
	project := "appdev-soda-spanner-staging"
	instance := "knut-test-ycsb"
	database := "knut-test-db"
	db, err := gorm.Open(postgres.New(postgres.Config{
		DriverName: "spanner",
		DSN:        fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database),
	}), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
	var row AllTypes
	if err := db.Find(&row, 1).Error; err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%+v\n", row)
}
