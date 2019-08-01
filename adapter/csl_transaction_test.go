package adapter

import (
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"fmt"
	"strconv"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/smartystreets/goconvey/convey"
)

func TestClearanceToCslETLTransform(t *testing.T) {
	Convey("测试ClearanceToCslETL的Transform方法", t, func() {
		source := []models.SaleTransaction{
			{
				OrderId:        1,
				StoreId:        1,
				TotalSalePrice: 200,
				Quantity:       1,
				SalePrice:      100,
				SkuId:          3,
			},
			{
				OrderId:        1,
				StoreId:        1,
				TotalSalePrice: 200,
				Quantity:       2,
				SalePrice:      50,
				SkuId:          4,
			},
		}
		saleMstsAndSaleDtls, err := ClearanceToCslETL{}.Transform(context.Background(), source)
		So(err, ShouldBeNil)
		sas := saleMstsAndSaleDtls.(models.SaleMstsAndSaleDtls)
		saleDtls := sas.SaleDtls
		saleMsts := sas.SaleMsts
		So(saleMsts[0].SaleNo, ShouldEqual, strconv.FormatInt(1, 10))
		So(saleMsts[0].ShopCode, ShouldEqual, strconv.FormatInt(1, 10))
		So(saleMsts[0].ActualSaleAmt, ShouldEqual, 200)

		So(saleDtls[0].SaleQty, ShouldEqual, 1)
		So(saleDtls[0].SaleNo, ShouldEqual, strconv.FormatInt(1, 10))
		So(saleDtls[0].ShopCode, ShouldEqual, strconv.FormatInt(1, 10))
		So(saleDtls[0].ProdCode, ShouldEqual, strconv.FormatInt(3, 10))
		So(saleDtls[0].SaleAmt, ShouldEqual, 100)
	})
}

func TestClearanceToCslETL(t *testing.T) {
	Convey("测试ClearanceToCslETL的Run方法", t, func() {
		Convey("可以把DATA 从Clearance导入到csl", func() {
			etl := buildClearanceToCslETL()
			etl.After(ClearanceToCslETL{}.ReadyToLoad)
			err := etl.Run(context.Background())
			fmt.Println(err)
			So(err, ShouldBeNil)
		})
	})
}
