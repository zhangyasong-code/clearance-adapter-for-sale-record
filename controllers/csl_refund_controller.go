package controllers

import (
	"clearance/clearance-adapter-for-sale-record/models"
	"net/http"
	"nomni/utils/api"

	"github.com/labstack/echo"
)

type CslRefundController struct{}

// POST data such as > {"brandCode": "EE", "channelType": "POS", "startAt": "2019-09-21 16:47:00", "endAt": "2019-09-21 16:49:00"}
func (c CslRefundController) Init(g *echo.Group) {
	g.GET("/csl-sale-for-return", c.GetCslSaleForReturn)
	g.GET("/csl-sale-detail-for-return", c.GetCslSaleDetailForReturn)
	g.POST("/csl-return-insert", c.CslReturnInsert)
}

func (CslRefundController) GetCslSaleDetailForReturn(c echo.Context) error {
	brandCode := c.QueryParam("brandCode")
	shopCode := c.QueryParam("shopCode")
	startSaleDate := c.QueryParam("startSaleDate")
	endSaleDate := c.QueryParam("endSaleDate")
	saleNo := c.QueryParam("saleNo")
	deptStoreReceiptNo := c.QueryParam("deptStoreReceiptNo")
	customerNo := c.QueryParam("customerNo")
	productCode := c.QueryParam("productCode")
	items, err := models.CslRefundDtl{}.GetCslSaleDetailForReturn(brandCode, shopCode, startSaleDate, endSaleDate, saleNo, deptStoreReceiptNo, customerNo, productCode)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}
	return c.JSON(http.StatusOK, api.Result{
		Success: true,
		Result: api.ArrayResult{
			Items: items,
		},
	})
}

func (CslRefundController) GetCslSaleForReturn(c echo.Context) error {
	brandCode := c.QueryParam("brandCode")
	shopCode := c.QueryParam("shopCode")
	startSaleDate := c.QueryParam("startSaleDate")
	endSaleDate := c.QueryParam("endSaleDate")
	saleNo := c.QueryParam("saleNo")
	deptStoreReceiptNo := c.QueryParam("deptStoreReceiptNo")
	customerNo := c.QueryParam("customerNo")
	productCode := c.QueryParam("productCode")
	items, err := models.CslRefundDtl{}.GetCslSaleForReturn(brandCode, shopCode, startSaleDate, endSaleDate, saleNo, deptStoreReceiptNo, customerNo, productCode)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}
	return c.JSON(http.StatusOK, api.Result{
		Success: true,
		Result: api.ArrayResult{
			Items: items,
		},
	})
}

func (CslRefundController) CslReturnInsert(c echo.Context) error {
	var data models.CslRefundInput
	if err := c.Bind(&data); err != nil {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}
	err := models.CslRefundInput{}.CslRefundInput(data)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}
	return c.JSON(http.StatusOK, api.Result{
		Success: true,
		Result:  api.ArrayResult{},
	})
}
