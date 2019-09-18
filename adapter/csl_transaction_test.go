package adapter

import (
	"clearance/clearance-adapter-for-sale-record/config"
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
					TransactionId:  1,
					CustomerId:     1,
					Mileage:        20,
					MileagePrice:   20,
					OuterOrderNo:   "123",
				},
			},
			SaleTransactionDtls: []models.SaleTransactionDtl{
				{
					Quantity:              1,
					SalePrice:             100,
					ListPrice:             120,
					SkuId:                 3,
					ProductId:             1,
					TransactionId:         1,
					TotalTransactionPrice: 100,
				},
				{
					Quantity:              2,
					SalePrice:             50,
					ListPrice:             70,
					SkuId:                 4,
					ProductId:             2,
					TransactionId:         1,
					TotalTransactionPrice: 100,
				},
			},
		}
		//param >>> storeId
		store, _ := models.Store{}.GetStore(1)

		saleMstsAndSaleDtls, err := ClearanceToCslETL{}.Transform(context.Background(), saleTAndSaleTDtls)
		So(err, ShouldBeNil)
		sas := saleMstsAndSaleDtls.(models.SaleMstsAndSaleDtls)
		saleDtls := sas.SaleDtls
		saleMsts := sas.SaleMsts
		nowDate := time.Now().Format("20060102")
		So(saleMsts[0].SaleNo, ShouldEqual, store.Code+nowDate[len(nowDate)-6:len(nowDate)]+"80001")
		So(saleMsts[0].ShopCode, ShouldEqual, store.Code)
		So(saleMsts[0].ActualSaleAmt, ShouldEqual, 200)

		So(saleDtls[0].SaleQty, ShouldEqual, 1)
		So(saleDtls[0].ShopCode, ShouldEqual, store.Code)
		So(saleDtls[0].ProdCode, ShouldEqual, "PBAC45341M40048")
		So(saleDtls[0].SaleAmt, ShouldEqual, 100)
	})
}

func TestClearanceToCslETL(t *testing.T) {
	setUpRestAPIStubFixture()
	Convey("测试ClearanceToCslETL的Run方法", t, func() {
		Convey("可以把DATA 从Clearance导入到csl", func() {
			etl := buildClearanceToCslETL()
			etl.After(ClearanceToCslETL{}.ReadyToLoad)
			ctx := context.Background()
			data := map[string]string{"brandCode": "PC", "channelType": "POS", "startAt": "", "endAt": ""}
			err := etl.Run(context.WithValue(ctx, "data", data))
			fmt.Println(err)
			So(err, ShouldBeNil)
		})
	})
}

func TestSaleNoLogic(t *testing.T) {
	store, _ := models.Store{}.GetStore(1)
	setUpRestAPIStubFixture()
	Convey("First add data with the SaleNo test19081289999", t, func() {
		saleMstsAndSaleDtls := models.SaleMstsAndSaleDtls{
			SaleMsts: []models.SaleMst{
				{
					SaleNo:   "test119081289999",
					ShopCode: store.Code,
					Dates:    "20190812",
					PosNo:    "8",
					SeqNo:    1,
				},
			},
			SaleDtls: []models.SaleDtl{
				{
					SaleNo:   "test119081289999",
					DtSeq:    0,
					ShopCode: store.Code,
					Dates:    "20190812",
					SeqNo:    1,
				},
			},
		}
		err := ClearanceToCslETL{}.Load(context.Background(), saleMstsAndSaleDtls)
		So(err, ShouldBeNil)
	})

	Convey("Then test whether the next SaleNo is test1908128A001", t, func() {
		saleDate, _ := time.Parse("2006-01-02", "2019-08-12")
		saleTAndSaleTDtls := models.SaleTAndSaleTDtls{
			SaleTransactions: []models.SaleTransaction{
				{
					OrderId:        1,
					StoreId:        1,
					TotalSalePrice: 200,
					SaleDate:       saleDate,
					CustomerId:     1,
				},
			},
			SaleTransactionDtls: []models.SaleTransactionDtl{
				{
					Quantity:  1,
					SalePrice: 100,
					SkuId:     3,
				},
				{
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
		So(saleMsts[0].SaleNo, ShouldEqual, store.Code+"190812"+"8A001")
		So(saleDtls[0].SaleNo, ShouldEqual, store.Code+"190812"+"8A001")
	})

	Convey("First add data with the SaleNo test1908128A999", t, func() {
		saleMstsAndSaleDtls := models.SaleMstsAndSaleDtls{
			SaleMsts: []models.SaleMst{
				{
					SaleNo:   "test11908128A999",
					ShopCode: store.Code,
					Dates:    "20190812",
					PosNo:    "8",
					SeqNo:    1,
				},
			},
			SaleDtls: []models.SaleDtl{
				{
					SaleNo:   "test11908128A999",
					DtSeq:    0,
					ShopCode: "test1",
					Dates:    "20190812",
					SeqNo:    1,
				},
			},
		}
		err := ClearanceToCslETL{}.Load(context.Background(), saleMstsAndSaleDtls)
		So(err, ShouldBeNil)
	})

	Convey("Then test whether the next SaleNo is test1908128B001", t, func() {
		saleDate, _ := time.Parse("2006-01-02", "2019-08-12")
		saleTAndSaleTDtls := models.SaleTAndSaleTDtls{
			SaleTransactions: []models.SaleTransaction{
				{
					OrderId:        1,
					StoreId:        1,
					TotalSalePrice: 200,
					SaleDate:       saleDate,
					CustomerId:     1,
				},
			},
			SaleTransactionDtls: []models.SaleTransactionDtl{
				{
					Quantity:  1,
					SalePrice: 100,
					SkuId:     3,
				},
				{
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
		So(saleMsts[0].SaleNo, ShouldEqual, store.Code+"190812"+"8B001")
		So(saleDtls[0].SaleNo, ShouldEqual, store.Code+"190812"+"8B001")
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

func TestGetSumsFields(t *testing.T) {
	Convey("测试GetSumsFields的方法", t, func() {
		res, err := models.AssortedSaleRecordDtl{}.GetSumsFields(1)
		So(err, ShouldBeNil)
		fmt.Println("YYYYYYYYY", res)
	})
}
