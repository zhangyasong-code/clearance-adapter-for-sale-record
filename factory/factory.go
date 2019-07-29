package factory

import (
	"sync"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
)

var (
	// cslEngine CSL DB
	cslEngine *xorm.Engine
	// srEngine SaleRecord DB
	srEngine *xorm.Engine
	// cfsrEngine clearanceForSaleRecord DB
	cfsrEngine *xorm.Engine
	once       sync.Once
)

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

// GetCfsrEngine 获取clearanceForSaleRecord数据库引擎
func GetCfsrEngine() *xorm.Engine {
	return cfsrEngine
}

// SetCfsrEngine 设置clearanceForSaleRecord数据库引擎
func SetCfsrEngine(engine *xorm.Engine) {
	once.Do(func() {
		cfsrEngine = engine
	})
}
