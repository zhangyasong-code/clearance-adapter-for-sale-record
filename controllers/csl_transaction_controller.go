package controllers

import (
	"net/http"

	"clearance/clearance-adapter-for-sale-record/models"
	"nomni/utils/api"

	"github.com/labstack/echo"
)

type CslTransactionController struct{}

func (c CslTransactionController) Init(g *echo.Group) {
	g.GET("/sales", c.GetCslSaleTransactions)
}

func (CslTransactionController) GetCslSaleTransactions(c echo.Context) error {
	var data models.RequestInput
	if err := c.Bind(&data); err != nil {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}
	data.SaleNos = splitTolist(c.QueryParam("saleNos"))
	if data.SaleNo != "" {
		data.SaleNos = append(data.SaleNos, data.SaleNo)
	}
	if len(data.SaleNos) == 0 {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: "Must be input saleNo!",
			},
		})
	}

	totalCount, items, err := models.SaleMst{}.GetCslSales(c.Request().Context(), data)
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
			TotalCount: totalCount,
			Items:      items,
		},
	})
}
