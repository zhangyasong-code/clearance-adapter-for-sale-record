package adapter

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"errors"

	"github.com/pangpanglabs/goetl"
)

const (
	maxResultCount = 1000
)

// saleRecord到Clearance
type SrToClearanceETL struct{}

func buildSrToClearanceETL() *goetl.ETL {
	etl := goetl.New(SrToClearanceETL{})
	return etl
}

// Extract ...
func (etl SrToClearanceETL) Extract(ctx context.Context) (interface{}, error) {
	saleRecords := []models.AssortedSaleRecord{}
	//分页查询   一次查1000条
	skipCount := 0
	for {
		srs := []models.AssortedSaleRecord{}
		if err := factory.GetSrEngine().Where("transaction_channel_type = ?", "POS").Limit(maxResultCount, skipCount).Find(&srs); err != nil {
			return nil, err
		}
		for _, saleRecord := range srs {
			saleRecords = append(saleRecords, saleRecord)
		}
		if len(srs) < maxResultCount {
			break
		} else {
			skipCount += maxResultCount
		}
	}
	return saleRecords, nil
}

// Transform ...
func (etl SrToClearanceETL) Transform(ctx context.Context, source interface{}) (interface{}, error) {
	saleRecords, ok := source.([]models.AssortedSaleRecord)
	if !ok {
		return nil, errors.New("Convert Failed")
	}
	saleTransactions := make([]models.SaleTransaction, 0)
	saleTransactionDtls := make([]models.SaleTransactionDtl, 0)
	for _, saleRecord := range saleRecords {
		check := true
		for _, saleTransaction := range saleTransactions {
			if saleRecord.OrderId == saleTransaction.OrderId {
				check = false
			}
		}

		if len(saleTransactions) == 0 || check {
			saleTransactions = append(saleTransactions, models.SaleTransaction{
				OrderId:        saleRecord.OrderId,
				StoreId:        saleRecord.StoreId,
				TotalSalePrice: saleRecord.TotalSalePrice,
				SaleDate:       saleRecord.TransactionCreateDate,
			})
		}
		saleTransactionDtls = append(saleTransactionDtls, models.SaleTransactionDtl{
			OrderId:   saleRecord.OrderId,
			StoreId:   saleRecord.StoreId,
			Quantity:  saleRecord.Quantity,
			SalePrice: saleRecord.SalePrice,
			SkuId:     saleRecord.SkuId,
		})
	}
	return models.SaleTAndSaleTDtls{
		SaleTransactions:    saleTransactions,
		SaleTransactionDtls: saleTransactionDtls,
	}, nil
}

// ReadyToLoad ...
func (etl SrToClearanceETL) ReadyToLoad(ctx context.Context, source interface{}) error {
	return nil
}

// Load ...
func (etl SrToClearanceETL) Load(ctx context.Context, source interface{}) error {
	if source == nil {
		return errors.New("source is nil")
	}
	saleTAndSaleTDtls, ok := source.(models.SaleTAndSaleTDtls)
	if !ok {
		return errors.New("Convert Failed")
	}

	engine := factory.GetCfsrEngine()
	session := engine.NewSession()
	defer session.Close()
	if err := session.Begin(); err != nil {
		return err
	}

	for _, saleTransaction := range saleTAndSaleTDtls.SaleTransactions {
		if _, err := session.Insert(&saleTransaction); err != nil {
			session.Rollback()
			return err
		}
	}
	for _, saleTransactionDtl := range saleTAndSaleTDtls.SaleTransactionDtls {
		if _, err := session.Insert(&saleTransactionDtl); err != nil {
			session.Rollback()
			return err
		}
	}
	if err := session.Commit(); err != nil {
		return err
	}
	return nil
}