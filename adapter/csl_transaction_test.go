package adapter

import (
	"clearance/clearance-adapter-for-sale-record/config"
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	callPlaceManagementApiServer *httptest.Server
)

func TestCTCETLTransform(t *testing.T) {
	setUpRestAPIStubFixture()
	Convey("测试ClearanceToCslETL的Transform方法", t, func() {
		saleTAndSaleTDtls := models.SaleTAndSaleTDtls{
			SaleTransactions: []models.SaleTransaction{
				{
					OrderId:        1,
					StoreId:        1,
					TotalSalePrice: 200,
					SaleDate:       time.Now(),
				},
			},
			SaleTransactionDtls: []models.SaleTransactionDtl{
				{
					OrderId:   1,
					StoreId:   1,
					Quantity:  1,
					SalePrice: 100,
					SkuId:     3,
				},
				{
					OrderId:   1,
					StoreId:   1,
					Quantity:  2,
					SalePrice: 50,
					SkuId:     4,
				},
			},
		}
		saleMstsAndSaleDtls, err := ClearanceToCslETL{}.Transform(context.Background(), saleTAndSaleTDtls)
		So(err, ShouldBeNil)
		sas := saleMstsAndSaleDtls.(models.SaleMstsAndSaleDtls)
		saleDtls := sas.SaleDtls
		saleMsts := sas.SaleMsts
		nowDate := time.Now().Format("20060102")
		So(saleMsts[0].SaleNo, ShouldEqual, "test"+nowDate[len(nowDate)-6:len(nowDate)]+"80000")
		So(saleMsts[0].ShopCode, ShouldEqual, "test")
		So(saleMsts[0].ActualSaleAmt, ShouldEqual, 200)

		So(saleDtls[0].SaleQty, ShouldEqual, 1)
		So(saleDtls[0].ShopCode, ShouldEqual, "test")
		So(saleDtls[0].ProdCode, ShouldEqual, strconv.FormatInt(3, 10))
		So(saleDtls[0].SaleAmt, ShouldEqual, 100)
	})
}

func TestClearanceToCslETL(t *testing.T) {
	setUpRestAPIStubFixture()
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

func setUpRestAPIStubFixture() {
	config.Init("", "", func(c *config.C) {
		callPlaceManagementApiServer = httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if strings.Contains(req.URL.String(), "/outside/v1") {
				rw.WriteHeader(http.StatusOK)
				response := `{
					"success": true,
					"result": [
							{
								"id":1,"code":"test"
							}
					]
				}`
				if _, err := rw.Write([]byte(response)); err != nil {
					rw.WriteHeader(http.StatusInternalServerError)
				}
			}
		}))
		c.Services.PlaceManagementApi = callPlaceManagementApiServer.URL

	})
}
