package factory

import (
	"clearance/clearance-adapter-for-sale-record/config"
	"fmt"
	"sync"

	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/core"
	"github.com/go-xorm/xorm"
)

var (
	// cslEngine CSL 数据库
	cslEngine *xorm.Engine
	// srEngine SaleRecord 数据库
	srEngine *xorm.Engine
	once     sync.Once
)

// Init 初始化 数据库引擎
func Init() {
	cslEngine = CreateMSSQLEngine(config.GetCSLConnString())
	SetCSLEngine(cslEngine)

	srEngine = CreateMySQLEngine(config.GetSaleRecordConnString())
	SetSrEngine(srEngine)
}

// GetCSLEngine 获取CSL数据库引擎
func GetCSLEngine() *xorm.Engine {
	return cslEngine
}

// SetCSLEngine 设置CSL数据库引擎
func SetCSLEngine(engine *xorm.Engine) {
	once.Do(func() {
		cslEngine = engine
	})
}

// GetSrEngine 获取SaleRecord数据库引擎
func GetSrEngine() *xorm.Engine {
	return srEngine
}

// SetClrEngine 设置SaleRecord数据库引擎
func SetSrEngine(engine *xorm.Engine) {
	once.Do(func() {
		srEngine = engine
	})
}

// CreateMSSQLEngine 创建SQLServer数据库引擎
func CreateMSSQLEngine(connString string) *xorm.Engine {
	engine, err := xorm.NewEngine("mssql", connString)
	if err != nil {
		fmt.Println("createMSSQLEngine error")
	}
	engine.TZLocation, _ = time.LoadLocation("UTC")
	engine.SetTableMapper(core.SameMapper{})
	engine.SetColumnMapper(core.SameMapper{})

	return engine
}

// CreateMySQLEngine 创建MySQL数据库引擎
func CreateMySQLEngine(connString string) *xorm.Engine {
	var err error
	engine, err := xorm.NewEngine("mysql", connString)
	if err != nil {
		fmt.Println("createMySQLEngine error")
	}
	engine.SetTableMapper(core.SnakeMapper{})
	engine.SetColumnMapper(core.SnakeMapper{})

	return engine
}
