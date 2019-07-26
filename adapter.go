package main

import (
	"context"
	"errors"

	//"clearance/clearance-adapter-for-sale-record/factory"

	"github.com/pangpanglabs/goetl"
)

// saleRecord到csl
type SrToCslETL struct{}

func buildETL() *goetl.ETL {
	etl := goetl.New(SrToCslETL{})
	return etl
}

// Extract ...
func (etl SrToCslETL) Extract(ctx context.Context) (interface{}, error) {
	//engine := factory.GetSrEngine()
	return nil, nil
}

// Transform ...
func (etl SrToCslETL) Transform(ctx context.Context, source interface{}) (interface{}, error) {
	return nil, nil
}

// ReadyToLoad ...
func (etl SrToCslETL) ReadyToLoad(ctx context.Context, source interface{}) error {
	return nil
}

// Load ...
func (etl SrToCslETL) Load(ctx context.Context, source interface{}) error {
	if source == nil {
		return errors.New("source is nil")
	}
	//engine := factory.GetCSLEngine()
	return nil
}
