package controllers

import (
	"errors"
	"net/http"

	"clearance/clearance-adapter-for-sale-record/models"
	"nomni/utils/api"

	"github.com/labstack/echo"
)

type CslTransactionController struct{}

func (c CslTransactionController) Init(g *echo.Group) {
	g.GET("/sales", c.GetCslSaleTransactions)
	g.GET("/t-sales", c.GetCslTSaleTransactions)
}

func (CslTransactionController) GetCslSaleTransactions(c echo.Context) error {
	var data models.RequestInput
	if err := c.Bind(&data); err != nil {
		return renderFail(c, http.StatusBadRequest, err)
	}
	data.SaleNos = splitTolist(c.QueryParam("saleNos"))
	if data.SaleNo != "" {
		data.SaleNos = append(data.SaleNos, data.SaleNo)
	}
	if len(data.SaleNos) == 0 && (data.ShopCode == "" || data.Dates == "" || data.PosNo == "") {
		return renderFail(c, http.StatusBadRequest, errors.New("Must be input ShopCode,Dates,PosNo !"))
	}

	totalCount, items, err := models.SaleMst{}.GetCslSales(c.Request().Context(), data)
	if err != nil {
		return renderFail(c, http.StatusInternalServerError, err)
	}

	return c.JSON(http.StatusOK, api.Result{
		Success: true,
		Result: api.ArrayResult{
			TotalCount: totalCount,
			Items:      items,
		},
	})
}

func (CslTransactionController) GetCslTSaleTransactions(c echo.Context) error {
	var data models.RequestInput
	if err := c.Bind(&data); err != nil {
		return renderFail(c, http.StatusBadRequest, err)
	}
	data.SaleNos = splitTolist(c.QueryParam("saleNos"))
	if data.SaleNo != "" {
		data.SaleNos = append(data.SaleNos, data.SaleNo)
	}
	if len(data.SaleNos) == 0 && (data.ShopCode == "" || data.Dates == "") {
		return renderFail(c, http.StatusBadRequest, errors.New("Must be input ShopCode,Dates !"))
	}

	totalCount, items, err := models.T_SaleMst{}.GetCslTSales(c.Request().Context(), data)
	if err != nil {
		return renderFail(c, http.StatusInternalServerError, err)
	}

	return c.JSON(http.StatusOK, api.Result{
		Success: true,
		Result: api.ArrayResult{
			TotalCount: totalCount,
			Items:      items,
		},
	})
}
