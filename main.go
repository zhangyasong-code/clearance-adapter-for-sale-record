package main

import (
	"clearance/clearance-adapter-for-sale-record/config"
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-xorm/xorm"
)

func main() {
	c := config.Init(os.Getenv("APP_ENV"), "")
	// get saleRecordDB Engine
	saleRecordDB, err := initDB(c.SaleRecordConnDatabase.Driver, c.SaleRecordConnDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetSrEngine(saleRecordDB)
	defer saleRecordDB.Close()

	// get cslDB Engine
	cslDB, err := initDB(c.CslConnDatabase.Driver, c.CslConnDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetCSLEngine(cslDB)
	defer saleRecordDB.Close()

	// get clearanceForSaleRecordDB Engine
	cfsrDB, err := initDB(c.CfsrConnDatabase.Driver, c.CfsrConnDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetCfsrEngine(cfsrDB)
	if err := models.InitDb(cfsrDB); err != nil {
		log.Fatal(err)
	}
	defer cfsrDB.Close()

	pmDb, err := initDB(c.PmConnDatabase.Driver, c.PmConnDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetPmEngine(pmDb)
	defer pmDb.Close()

	fmt.Println("Start :========================")
	// etl := goetl.New(adapter.SrToClearanceETL{})
	// etl.After(adapter.SrToClearanceETL{}.ReadyToLoad)
	// err = etl.Run(context.Background())
	// fmt.Println(err)
	fmt.Println("End :========================")
}

func initDB(driver, connection string) (*xorm.Engine, error) {
	db, err := xorm.NewEngine(driver, connection)
	if err != nil {
		panic(err)
	}
	env := os.Getenv("APP_ENV")
	if env != "production" {
		db.ShowSQL(true)
	}
	db.ShowSQL(false)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(30)
	db.SetConnMaxLifetime(time.Minute * 10)
	return db, err
}
