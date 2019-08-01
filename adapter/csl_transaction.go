package adapter

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/go-xorm/core"
	"github.com/pangpanglabs/goetl"
)

// Clearance到CSL
type ClearanceToCslETL struct{}

func buildClearanceToCslETL() *goetl.ETL {
	etl := goetl.New(ClearanceToCslETL{})
	return etl
}

// Extract ...
func (etl ClearanceToCslETL) Extract(ctx context.Context) (interface{}, error) {
	saleTransactions := []models.SaleTransaction{}
	//分页查询   一次查1000条
	skipCount := 0
	for {
		sts := []models.SaleTransaction{}
		if err := factory.GetCfsrEngine().Where("1 = 1").Limit(maxResultCount, skipCount).Find(&sts); err != nil {
			return nil, err
		}
		for _, saleTransaction := range sts {
			saleTransactions = append(saleTransactions, saleTransaction)
		}
		if len(sts) < maxResultCount {
			break
		} else {
			skipCount += maxResultCount
		}
	}

	return saleTransactions, nil
}

// Transform ...
func (etl ClearanceToCslETL) Transform(ctx context.Context, source interface{}) (interface{}, error) {
	saleTransactions, ok := source.([]models.SaleTransaction)
	if !ok {
		return nil, errors.New("Convert Failed")
	}
	saleMsts := make([]models.SaleMst, 0)
	saleDtls := make([]models.SaleDtl, 0)

	for _, saleTransaction := range saleTransactions {
		saleNo := strconv.FormatInt(saleTransaction.OrderId, 10)
		storeId := strconv.FormatInt(saleTransaction.StoreId, 10)
		skuId := strconv.FormatInt(saleTransaction.SkuId, 10)
		check := true
		for _, saleMst := range saleMsts {
			if saleNo == saleMst.SaleNo {
				check = false
			}
		}
		if len(saleMsts) == 0 || check {
			saleMsts = append(saleMsts, models.SaleMst{
				SaleNo:        saleNo,
				ShopCode:      storeId,
				InDateTime:    time.Now(),
				ActualSaleAmt: saleTransaction.TotalSalePrice,
			})
		}
		saleDtls = append(saleDtls, models.SaleDtl{
			SaleNo:     saleNo,
			ShopCode:   storeId,
			DtSeq:      int64(len(saleDtls)),
			ProdCode:   skuId,
			InDateTime: time.Now(),
			SaleQty:    saleTransaction.Quantity,
			SaleAmt:    saleTransaction.SalePrice,
		})
	}
	return models.SaleMstsAndSaleDtls{
		SaleMsts: saleMsts,
		SaleDtls: saleDtls,
	}, nil
}

// ReadyToLoad ...
func (etl ClearanceToCslETL) ReadyToLoad(ctx context.Context, source interface{}) error {
	return nil
}

// Load ...
func (etl ClearanceToCslETL) Load(ctx context.Context, source interface{}) error {
	if source == nil {
		return errors.New("source is nil")
	}
	saleMstsAndSaleDtls, ok := source.(models.SaleMstsAndSaleDtls)
	if !ok {
		return errors.New("Convert Failed")
	}
	engine := factory.GetCSLEngine()
	engine.SetMapper(core.SameMapper{})
	for _, saleMst := range saleMstsAndSaleDtls.SaleMsts {
		if _, err := engine.Table("dbo.SaleMst").Insert(&saleMst); err != nil {
			return err
		}
	}
	for _, saleDtl := range saleMstsAndSaleDtls.SaleDtls {
		if _, err := engine.Table("dbo.SaleDtl").Insert(&saleDtl); err != nil {
			return err
		}
	}
	return nil
}
