package adapter

import (

	//"clearance/clearance-adapter-for-sale-record/models"
	//_ "clearance/clearance-adapter-for-sale-record/test"

	"context"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTransform(t *testing.T) {
	// Convey("测试SrToCslETL的Transform方法", t, func() {
	// 	)
	// })
}

func TestSrToClearanceForSaleRecordETL(t *testing.T) {
	Convey("测试SrToClearanceETL的Run方法", t, func() {
		Convey("可以把DATA 从sale-record导入到Clearance", func() {
			etl := buildETL()
			etl.After(SrToClearanceETL{}.ReadyToLoad)
			err := etl.Run(context.Background())
			So(err, ShouldBeNil)
		})
	})
}
