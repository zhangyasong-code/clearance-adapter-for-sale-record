package controllers

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

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
	g.GET("/fail-log", c.GetFailDataLog)
	g.GET("/saleTransactions", c.GetSaleTransactions)
	g.GET("/csl-saleTransactions", c.GetCslSaleTransactions)
	g.GET("/csl-t-saleTransactions", c.GetCslTSaleTransactions)
	g.GET("/csl-success", c.GetAllSaleSuccess)
	g.GET("/csl-success/:saleNo/:dtlSeq", c.GetSaleSuccess)
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
	etl.Before(adapter.SrToClearanceETL{}.Before)
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

func (TransactionController) GetCslSaleTransactions(c echo.Context) error {
	var data models.RequestInput
	if err := c.Bind(&data); err != nil {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}
	if data.MaxResultCount == 0 {
		data.MaxResultCount = 10
	}
	totalCount, items, err := models.CslSaleMst{}.GetCslSaleBySaleTransactions(c.Request().Context(), data)
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

func (TransactionController) GetCslTSaleTransactions(c echo.Context) error {
	var data models.RequestInput
	if err := c.Bind(&data); err != nil {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}
	if data.MaxResultCount == 0 {
		data.MaxResultCount = 10
	}
	totalCount, items, err := models.CslTSaleMst{}.GetCslTSaleBySaleTransactions(c.Request().Context(), data)
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

func (TransactionController) GetSaleTransactions(c echo.Context) error {
	transactionId, _ := strconv.ParseInt(c.QueryParam("transactionId"), 10, 64)
	orderId, _ := strconv.ParseInt(c.QueryParam("orderId"), 10, 64)
	RefundId, _ := strconv.ParseInt(c.QueryParam("RefundId"), 10, 64)
	maxResultCount, _ := strconv.Atoi(c.QueryParam("maxResultCount"))
	if maxResultCount == 0 {
		maxResultCount = 10
	}
	skipCount, _ := strconv.Atoi(c.QueryParam("skipCount"))

	totalCount, items, err := models.SaleTransaction{}.GetSaleTransactions(c.Request().Context(), transactionId, orderId, RefundId, "", maxResultCount, skipCount)
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
	clearanceETL.Before(adapter.SrToClearanceETL{}.Before)
	clearanceETL.After(adapter.SrToClearanceETL{}.ReadyToLoad)
	if err := clearanceETL.Run(context.WithValue(c.Request().Context(), "data", data)); err != nil {
		return c.JSON(http.StatusInternalServerError, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}

	if strings.ToUpper(data.TransactionChannelType) == "POS" {
		cslETL := goetl.New(adapter.ClearanceToCslETL{})
		cslETL.After(adapter.ClearanceToCslETL{}.ReadyToLoad)
		if err := cslETL.Run(context.WithValue(c.Request().Context(), "data", data)); err != nil {
			return c.JSON(http.StatusInternalServerError, api.Result{
				Error: api.Error{
					Message: err.Error(),
				},
			})
		}
	}

	if strings.ToUpper(data.TransactionChannelType) == "EMALL" {
		cslTSaleETL := goetl.New(adapter.ClearanceToCslTSaleETL{})
		cslTSaleETL.After(adapter.ClearanceToCslTSaleETL{}.ReadyToLoad)
		if err := cslTSaleETL.Run(context.WithValue(c.Request().Context(), "data", data)); err != nil {
			return c.JSON(http.StatusInternalServerError, api.Result{
				Error: api.Error{
					Message: err.Error(),
				},
			})
		}
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
	transactionId, _ := strconv.ParseInt(c.QueryParam("transactionId"), 10, 64)
	totalCount, items, err := models.SaleRecordIdFailMapping{}.GetAll(c.Request().Context(), models.RequestInput{TransactionId: transactionId, MaxResultCount: maxResultCount, SkipCount: skipCount, StoreId: storeId})
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

func (TransactionController) GetAllSaleSuccess(c echo.Context) error {
	var data models.RequestInput
	maxResultCount, _ := strconv.Atoi(c.QueryParam("maxResultCount"))
	if maxResultCount == 0 {
		maxResultCount = 10
	}
	skipCount, _ := strconv.Atoi(c.QueryParam("skipCount"))
	saleNo := c.QueryParam("saleNo")
	orderId, _ := strconv.ParseInt(c.QueryParam("orderId"), 10, 64)
	refundId, _ := strconv.ParseInt(c.QueryParam("refundId"), 10, 64)
	saleTransactionId, _ := strconv.ParseInt(c.QueryParam("saleTransactionId"), 10, 64)
	transactionId, _ := strconv.ParseInt(c.QueryParam("transactionId"), 10, 64)
	data.SaleNo = saleNo
	data.OrderId = orderId
	data.RefundId = refundId
	data.SaleTransactionId = saleTransactionId
	data.TransactionId = transactionId
	data.SkipCount = skipCount
	data.MaxResultCount = maxResultCount
	totalCount, items, err := models.SaleRecordIdSuccessMapping{}.GetAllSaleSuccess(c.Request().Context(), data)
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

func (TransactionController) GetSaleSuccess(c echo.Context) error {
	saleNo := c.Param("saleNo")
	dtlSeq, err := strconv.Atoi(c.Param("dtlSeq"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}

	if saleNo == "" {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: errors.New("saleNo is null").Error(),
			},
		})
	}

	if dtlSeq == 0 {
		return c.JSON(http.StatusBadRequest, api.Result{
			Error: api.Error{
				Message: errors.New("dtlSeq is 0").Error(),
			},
		})
	}

	cslSale, err := models.SaleRecordIdSuccessMapping{}.GetBy(saleNo, dtlSeq)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, api.Result{
			Error: api.Error{
				Message: err.Error(),
			},
		})
	}
	return c.JSON(http.StatusOK, api.Result{
		Success: true,
		Result:  cslSale,
	})
}
