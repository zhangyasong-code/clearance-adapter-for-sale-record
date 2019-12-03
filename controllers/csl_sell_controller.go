package controllers

import (
	"clearance/clearance-adapter-for-sale-record/models"
	"net/http"
	"nomni/utils/api"

	"github.com/labstack/echo"
)

type CslSellController struct{}

func (c CslSellController) Init(g *echo.Group) {
	g.GET("/csl-sale-mst", c.GetCslSaleMst)
	g.GET("/csl-sale-dtl", c.GetCslSaleDtl)
}

func (CslSellController) GetCslSaleDtl(c echo.Context) error {
	saleNo := c.QueryParam("saleNo")
	items, err := models.CslSaleDtlStruct{}.GetCslSaleDtl(saleNo)
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

func (CslSellController) GetCslSaleMst(c echo.Context) error {
	brandCode := c.QueryParam("brandCode")
	shopCode := c.QueryParam("shopCode")
	startSaleDate := c.QueryParam("startSaleDate")
	endSaleDate := c.QueryParam("endSaleDate")
	saleNo := c.QueryParam("saleNo")
	deptStoreReceiptNo := c.QueryParam("deptStoreReceiptNo")
	// if err := data.Validate(); err != nil {
	// 	return c.JSON(http.StatusBadRequest, api.Result{
	// 		Error: api.Error{
	// 			Message: err.Error(),
	// 		},
	// 	})
	// }
	items, err := models.CslSaleMstStruct{}.GetCslSaleMst(brandCode, shopCode, startSaleDate, endSaleDate, saleNo, deptStoreReceiptNo)
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
