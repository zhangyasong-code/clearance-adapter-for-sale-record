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

// POST data such as > {"brandCode": "EE", "channelType": "POS", "startAt": "2019-09-21 16:47:00", "endAt": "2019-09-21 16:49:00"}
func (c TransactionController) Init(g *echo.Group) {
	g.POST("/sale", c.RunSaleETL)
	g.POST("/csl", c.RunCslETL)
	g.POST("/sale-csl", c.RunSaleETLAndCslETL)
	g.GET("/sale", c.GetSaleTransactions)
	g.GET("/fail-log", c.GetFailDataLog)
}

func (TransactionController) RunSaleETL(c echo.Context) error {
	var data models.RequestInput
	if err := c.Bind(&data); err != nil {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}

	if err := data.Validate(); err != nil {
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
	var data models.RequestInput
	if err := c.Bind(&data); err != nil {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}

	if err := data.Validate(); err != nil {
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

func (TransactionController) RunSaleETLAndCslETL(c echo.Context) error {
	var data models.RequestInput
	if err := c.Bind(&data); err != nil {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}

	if err := data.Validate(); err != nil {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}
	clearanceETL := goetl.New(adapter.SrToClearanceETL{})
	clearanceETL.After(adapter.SrToClearanceETL{}.ReadyToLoad)
	if err := clearanceETL.Run(context.WithValue(c.Request().Context(), "data", data)); err != nil {
		return c.JSON(http.StatusInternalServerError, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}

	cslETL := goetl.New(adapter.ClearanceToCslETL{})
	cslETL.After(adapter.ClearanceToCslETL{}.ReadyToLoad)
	if err := cslETL.Run(context.WithValue(c.Request().Context(), "data", data)); err != nil {
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

func (TransactionController) GetFailDataLog(c echo.Context) error {
	storeId, _ := strconv.Atoi(c.QueryParam("storeId"))
	maxResultCount, _ := strconv.Atoi(c.QueryParam("maxResultCount"))
	if maxResultCount == 0 {
		maxResultCount = 10
	}
	skipCount, _ := strconv.Atoi(c.QueryParam("skipCount"))

	totalCount, items, err := models.SaleRecordIdFailMapping{}.GetAll(c.Request().Context(), maxResultCount, skipCount, storeId)
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
