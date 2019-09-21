package controllers

import (
	"context"
	"net/http"
	"strconv"

	"clearance/clearance-adapter-for-sale-record/adapter"
	"clearance/clearance-adapter-for-sale-record/models"
	"nomni/utils/api"

	"github.com/labstack/echo"
	"github.com/pangpanglabs/goetl"
)

type TransactionController struct{}

func (c TransactionController) Init(g *echo.Group) {
	g.POST("/sale", c.RunSaleETL)
	g.POST("/csl", c.RunCslETL)
	g.GET("/sale", c.GetSaleTransactions)
}

func (TransactionController) RunSaleETL(c echo.Context) error {
	var data map[string]string
	if err := c.Bind(&data); err != nil {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}

	etl := goetl.New(adapter.SrToClearanceETL{})
	etl.After(adapter.SrToClearanceETL{}.ReadyToLoad)
	if err := etl.Run(context.WithValue(c.Request().Context(), "data", data)); err != nil {
		return c.JSON(http.StatusInternalServerError, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}

	return c.JSON(http.StatusOK, api.Result{
		Success: true,
	})
}
func (TransactionController) GetSaleTransactions(c echo.Context) error {
	maxResultCount, _ := strconv.Atoi(c.QueryParam("maxResultCount"))
	if maxResultCount == 0 {
		maxResultCount = 10
	}
	skipCount, _ := strconv.Atoi(c.QueryParam("skipCount"))

	totalCount, items, err := models.SaleTransaction{}.GetAll(c.Request().Context(), maxResultCount, skipCount)
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
func (TransactionController) RunCslETL(c echo.Context) error {
	var data map[string]string
	if err := c.Bind(&data); err != nil {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}

	etl := goetl.New(adapter.ClearanceToCslETL{})
	etl.After(adapter.ClearanceToCslETL{}.ReadyToLoad)
	if err := etl.Run(context.WithValue(c.Request().Context(), "data", data)); err != nil {
		return c.JSON(http.StatusInternalServerError, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}

	return c.JSON(http.StatusOK, api.Result{
		Success: true,
	})
}
