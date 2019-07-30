package adapter

import (
	"clearance/clearance-adapter-for-sale-record/config"
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"
	"log"
	"os"
	"time"

	"github.com/go-xorm/xorm"
)

func init() {
	c := config.Init(os.Getenv("APP_ENV"), "../")
	// get saleRecordDB Engine
	saleRecordDB, err := initDB(c.SaleRecordConnDatabase.Driver, c.SaleRecordConnDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetSrEngine(saleRecordDB)
	// defer saleRecordDB.Close()

	// get cslDB Engine
	cslDB, err := initDB(c.CslConnDatabase.Driver, c.CslConnDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetCSLEngine(cslDB)
	// defer saleRecordDB.Close()

	// get clearanceForSaleRecordDB Engine
	cfsrDB, err := initDB(c.CfsrConnDatabase.Driver, c.CfsrConnDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetCfsrEngine(cfsrDB)
	if err := models.InitDb(cfsrDB); err != nil {
		log.Fatal(err)
	}
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
