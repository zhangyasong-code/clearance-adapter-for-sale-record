package main

import (
	"context"
	"log"
	"net/http"
	"nomni/utils/auth"
	"os"
	"sort"
	"strings"
	"time"

	"clearance/clearance-adapter-for-sale-record/adapter"
	"clearance/clearance-adapter-for-sale-record/config"
	"clearance/clearance-adapter-for-sale-record/controllers"
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	//_ "github.com/mattn/go-sqlite3"
	"github.com/pangpanglabs/goetl"
	"github.com/pangpanglabs/goutils/behaviorlog"
	"github.com/pangpanglabs/goutils/echomiddleware"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

var (
	startAt = "2019-09-23 15:13:00"
	endAt   = "2019-09-23 15:17:00"
)

/*
$ cd cmd/api-app

MSL > Clearance:         APP_ENV=mslv2-qa-local go run main.go etl-1 -b EE
Clearance > CSL:         APP_ENV=mslv2-qa-local go run main.go etl-2 -b EE
MSL > Clearance > CSL:   APP_ENV=mslv2-qa-local go run main.go etl -b EE
*/
func main() {
	config := config.Init(os.Getenv("APP_ENV"), "./../../")
	// get saleRecordDB Engine
	saleRecordDB, err := initDB(config.SaleRecordConnDatabase.Driver, config.SaleRecordConnDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetSrEngine(saleRecordDB)
	defer saleRecordDB.Close()

	// get cslDB Engine
	cslDB, err := initDB(config.CslConnDatabase.Driver, config.CslConnDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetCSLEngine(cslDB)
	defer saleRecordDB.Close()

	// get clearanceForSaleRecordDB Engine
	cfsrDB, err := initDB(config.CfsrConnDatabase.Driver, config.CfsrConnDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetCfsrEngine(cfsrDB)
	if err := models.InitDb(cfsrDB); err != nil {
		log.Fatal(err)
	}
	defer cfsrDB.Close()

	pmDb, err := initDB(config.PmConnDatabase.Driver, config.PmConnDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetPmEngine(pmDb)
	defer pmDb.Close()

	productDB, err := initDB(config.ProductDatabase.Driver, config.ProductDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetProductEngine(productDB)
	defer productDB.Close()

	colleagueAuthDB, err := initDB(config.ColleagueAuthDatabase.Driver, config.ColleagueAuthDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetColleagueAuthEngine(colleagueAuthDB)
	defer colleagueAuthDB.Close()

	shopEmployeeDatabase, err := initDB(config.ShopEmployeeDatabase.Driver, config.ShopEmployeeDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetShopEmployeeEngine(shopEmployeeDatabase)
	defer shopEmployeeDatabase.Close()

	mslv2ReadonlyDatabase, err := initDB(config.Mslv2ReadonlyDatabase.Driver, config.Mslv2ReadonlyDatabase.Connection)
	if err != nil {
		panic(err)
	}
	factory.SetMslv2ReadonlyEngine(mslv2ReadonlyDatabase)
	defer mslv2ReadonlyDatabase.Close()

	app := cli.NewApp()
	app.Name = "clearance-adapter-for-sale-record"
	app.Commands = []cli.Command{
		{
			Name:  "etl-1",
			Usage: "etl-1",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "brand,b",
				},
			},
			Action: func(cliContext *cli.Context) {
				for _, b := range strings.Split(cliContext.String("brand"), ",") {
					if b == "" {
						return
					}

					etl := goetl.New(adapter.SrToClearanceETL{})
					etl.After(adapter.SrToClearanceETL{}.ReadyToLoad)
					if err := etl.Run(context.WithValue(context.Background(), "data", map[string]string{
						"brandCode":   b,
						"channelType": "POS",
						"startAt":     startAt,
						"endAt":       endAt,
					})); err != nil {
						logrus.WithError(err).Error("Fail Clearance")
						return
					}
					logrus.Info("Success Clearance")

				}
			},
		},
		{
			Name:  "etl-2",
			Usage: "etl-2",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "brand,b",
				},
			},
			Action: func(cliContext *cli.Context) {
				for _, b := range strings.Split(cliContext.String("brand"), ",") {
					if b == "" {
						return
					}
					etl := goetl.New(adapter.ClearanceToCslETL{})
					etl.After(adapter.ClearanceToCslETL{}.ReadyToLoad)
					if err := etl.Run(context.WithValue(context.Background(), "data", map[string]string{
						"brandCode":   b,
						"channelType": "POS",
						"startAt":     startAt,
						"endAt":       endAt,
					})); err != nil {
						logrus.WithError(err).Error("Fail CSL")
						return
					}
					logrus.Info("Success CSL")
				}
			},
		},
		{
			Name:  "etl",
			Usage: "etl",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "brand,b",
				},
			},
			Action: func(cliContext *cli.Context) {
				for _, b := range strings.Split(cliContext.String("brand"), ",") {
					if b == "" {
						return
					}
					{
						etl := goetl.New(adapter.SrToClearanceETL{})
						etl.After(adapter.SrToClearanceETL{}.ReadyToLoad)
						if err := etl.Run(context.WithValue(context.Background(), "data", map[string]string{
							"brandCode":   b,
							"channelType": "POS",
							"startAt":     startAt,
							"endAt":       endAt,
						})); err != nil {
							logrus.WithError(err).Error("Fail Clearance")
							return
						}
						logrus.Info("Success Clearance")
					}

					{
						etl := goetl.New(adapter.ClearanceToCslETL{})
						etl.After(adapter.ClearanceToCslETL{}.ReadyToLoad)
						if err := etl.Run(context.WithValue(context.Background(), "data", map[string]string{
							"brandCode":   b,
							"channelType": "POS",
							"startAt":     startAt,
							"endAt":       endAt,
						})); err != nil {
							logrus.WithError(err).Error("Fail CSL")
							return
						}
						logrus.Info("Success CSL")
					}
				}
			},
		},
		{
			Name:  "api",
			Usage: "run api",
			Action: func(cliContext *cli.Context) {
				e := echo.New()

				e.GET("/ping", func(c echo.Context) error {
					return c.String(http.StatusOK, "pong")
				})
				e.GET("/swagger", func(c echo.Context) error {
					return c.File("./swagger.yml")
				})
				e.GET("/whoami", func(c echo.Context) error {
					return c.String(http.StatusOK, config.ServiceName)
				})
				e.Static("/docs", "./swagger-ui")
				controllers.TransactionController{}.Init(e.Group("/v1/transaction"))
				controllers.CslTransactionController{}.Init(e.Group("/v1/csl/transaction"))
				controllers.CslRefundController{}.Init(e.Group("/v1/csl/refund"))
				controllers.CslSellController{}.Init(e.Group("/v1/csl/sell"))
				e.Pre(middleware.RemoveTrailingSlash())
				e.Use(middleware.Recover())
				e.Use(middleware.CORS())
				e.Use(echomiddleware.BehaviorLogger(config.ServiceName, config.BehaviorLog.Kafka))
				e.Use(auth.UserClaimMiddleware("/ping", "/docs"))

				if !strings.HasSuffix(config.AppEnv, "production") {
					behaviorlog.SetLogLevel(logrus.InfoLevel)
					logrus.SetLevel(logrus.InfoLevel)
				}

				if err := e.Start(":8000"); err != nil {
					log.Println("Shutdown:", err)
				}
			},
		},
	}
	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))
	app.Run(os.Args)
}

func initDB(driver, connection string) (*xorm.Engine, error) {
	db, err := xorm.NewEngine(driver, connection)
	if err != nil {
		panic(err)
	}
	env := os.Getenv("APP_ENV")
	if env != "mslv2-production" {
		db.ShowSQL(true)
	}
	// db.ShowSQL(false)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(30)
	db.SetConnMaxLifetime(time.Minute * 10)
	return db, err
}
