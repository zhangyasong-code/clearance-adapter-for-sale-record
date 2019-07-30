package adapter

import (
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"fmt"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTransform(t *testing.T) {
	Convey("测试SrToClearanceETL的Transform方法", t, func() {
		source := []models.AssortedSaleRecord{
			{
				StoreId:        1,
				TotalSalePrice: 200,
				Quantity:       2,
				SkuId:          3,
			},
		}
		saleTransactions, err := SrToClearanceETL{}.Transform(context.Background(), source)
		So(err, ShouldBeNil)
		saleTransaction := saleTransactions.([]models.SaleTransaction)[0]
		So(saleTransaction.Id, ShouldEqual, 0)
		So(saleTransaction.Quantity, ShouldEqual, 2)
		So(saleTransaction.TotalSalePrice, ShouldEqual, 200.00)
		So(saleTransaction.StoreId, ShouldEqual, 1)
		So(saleTransaction.SkuId, ShouldEqual, 3)
	})
}

func TestSrToClearanceForSaleRecordETL(t *testing.T) {
	Convey("测试SrToClearanceETL的Run方法", t, func() {
		Convey("可以把DATA 从sale-record导入到Clearance", func() {
			etl := buildETL()
			etl.After(SrToClearanceETL{}.ReadyToLoad)
			err := etl.Run(context.Background())
			fmt.Println(err)
			So(err, ShouldBeNil)
		})
	})
}
