package adapter

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"errors"
	"fmt"

	"github.com/pangpanglabs/goetl"
)

const (
	maxResultCount = 1000
)

// saleRecord到Clearance
type SrToClearanceETL struct{}

func buildETL() *goetl.ETL {
	etl := goetl.New(SrToClearanceETL{})
	return etl
}

// Extract ...
func (etl SrToClearanceETL) Extract(ctx context.Context) (interface{}, error) {
	engine := factory.GetSrEngine()
	saleRecords := []models.AssortedSaleRecord{}
	//分页查询   一次查1000条
	skipCount := 0
	for {
		srs := []models.AssortedSaleRecord{}
		if err := engine.Where("transaction_channel_type = ?", "POS").Limit(maxResultCount, skipCount).Find(&srs); err != nil {
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
	fmt.Println(">>>>>>>>>>>>>>>>>>", len(saleRecords))
	return saleRecords, nil
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
	//engine := factory.GetCSLEngine()
	return nil
}
