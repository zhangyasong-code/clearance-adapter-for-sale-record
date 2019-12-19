package adapter

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"errors"

	"github.com/go-xorm/xorm"
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
	dataInput := ctx.Value("data").(models.RequestInput)

	if dataInput.TransactionId == 0 {
		return nil, errors.New("ETL-Extract:TransactionId is zero.")
	}

	var assortedSaleRecord models.AssortedSaleRecord
	query := func() xorm.Interface {
		q := factory.GetSrEngine().Table("assorted_sale_record").
			Where("assorted_sale_record.transaction_id = ?", dataInput.TransactionId)
		return q
	}
	if has, err := query().Get(&assortedSaleRecord); err != nil {
		return nil, err
	} else if !has {
		return nil, errors.New("Search saleRecord error.")
	}

	saleRecordDtls := make([]models.AssortedSaleRecordDtl, 0)
	if err := factory.GetSrEngine().Table("assorted_sale_record_dtl").Where("transaction_id=?", assortedSaleRecord.TransactionId).Find(&saleRecordDtls); err != nil {
		return nil, err
	}
	assortedSaleRecord.AssortedSaleRecordDtls = saleRecordDtls
	saleRecordPayments := make([]models.AssortedSaleRecordPayment, 0)
	if err := factory.GetSrEngine().Table("assorted_sale_record_payment").Where("transaction_id=?", assortedSaleRecord.TransactionId).Find(&saleRecordPayments); err != nil {
		return nil, err
	}
	assortedSaleRecord.AssortedSaleRecordPayments = saleRecordPayments
	return assortedSaleRecord, nil
}

// Transform ...
func (etl SrToClearanceETL) Transform(ctx context.Context, source interface{}) (interface{}, error) {
	assortedSaleRecords, ok := source.([]*models.AssortedSaleRecord)
	if !ok {
		return nil, errors.New("ETL-Transform:Convert Failed")
	}
	saleTransactions := make([]models.SaleTransaction, 0)
	for _, assortedSaleRecord := range assortedSaleRecords {
		saleTransaction := models.SaleTransaction{
			OrderId:                assortedSaleRecord.OrderId,
			RefundId:               assortedSaleRecord.RefundId,
			StoreId:                assortedSaleRecord.StoreId,
			ShopCode:               assortedSaleRecord.ShopCode,
			SalesmanId:             assortedSaleRecord.SalesmanId,
			EmpId:                  assortedSaleRecord.EmpId,
			TransactionCreatedId:   assortedSaleRecord.TransactionCreatedId,
			TotalSalePrice:         assortedSaleRecord.TotalSalePrice,
			TotalListPrice:         assortedSaleRecord.TotalListPrice,
			TotalTransactionPrice:  assortedSaleRecord.TotalTransactionPrice,
			SaleDate:               assortedSaleRecord.TransactionCreateDate,
			TransactionId:          assortedSaleRecord.TransactionId,
			CustomerId:             assortedSaleRecord.CustomerId,
			Mileage:                assortedSaleRecord.Mileage,
			MileagePrice:           assortedSaleRecord.MileagePrice,
			ObtainMileage:          assortedSaleRecord.ObtainMileage,
			OuterOrderNo:           assortedSaleRecord.OuterOrderNo,
			TransactionChannelType: assortedSaleRecord.TransactionChannelType,
			TotalDiscountPrice:     assortedSaleRecord.TotalDiscountPrice,
			BaseTrimCode:           assortedSaleRecord.BaseTrimCode,
			FreightPrice:           assortedSaleRecord.FreightPrice,
			SalesmanShopCode:       assortedSaleRecord.SalesmanShopCode,
			SalesmanEmpId:          assortedSaleRecord.SalesmanEmpId,
		}
		saleTransactionDtls := make([]models.SaleTransactionDtl, 0)
		for _, assortedSaleRecordDtl := range assortedSaleRecord.AssortedSaleRecordDtls {
			saleTransactionDtls = append(saleTransactionDtls, models.SaleTransactionDtl{
				Quantity:                       assortedSaleRecordDtl.Quantity,
				SalePrice:                      assortedSaleRecordDtl.SalePrice,
				TotalDiscountPrice:             assortedSaleRecordDtl.TotalDiscountPrice,
				SkuId:                          assortedSaleRecordDtl.SkuId,
				OrderItemId:                    assortedSaleRecordDtl.OrderItemId,
				RefundItemId:                   assortedSaleRecordDtl.RefundItemId,
				BrandCode:                      assortedSaleRecordDtl.BrandCode,
				BrandId:                        assortedSaleRecordDtl.BrandId,
				ProductId:                      assortedSaleRecordDtl.ProductId,
				ListPrice:                      assortedSaleRecordDtl.ListPrice,
				ItemCode:                       assortedSaleRecordDtl.ItemCode,
				ItemFee:                        assortedSaleRecordDtl.ItemFee,
				TotalTransactionPrice:          assortedSaleRecordDtl.TotalTransactionPrice,
				TotalDistributedCartOfferPrice: assortedSaleRecordDtl.TotalDistributedCartOfferPrice,
				TotalDistributedItemOfferPrice: assortedSaleRecordDtl.TotalDistributedItemOfferPrice,
				TotalDistributedPaymentPrice:   assortedSaleRecordDtl.TotalDistributedPaymentPrice,
				TransactionId:                  assortedSaleRecordDtl.TransactionId,
				TotalSalePrice:                 assortedSaleRecordDtl.TotalSalePrice,
				TotalListPrice:                 assortedSaleRecordDtl.TotalListPrice,
				DistributedCashPrice:           assortedSaleRecordDtl.DistributedCashPrice,
				TransactionDtlId:               assortedSaleRecordDtl.Id,
				Mileage:                        assortedSaleRecordDtl.Mileage,
				MileagePrice:                   assortedSaleRecordDtl.MileagePrice,
				ObtainMileage:                  assortedSaleRecordDtl.ObtainMileage,
			})
		}

		saleTransaction.Dtls = saleTransactionDtls
		saleTransactionPayments := make([]models.SaleTransactionPayment, 0)
		for _, assortedSaleRecordPayment := range assortedSaleRecord.AssortedSaleRecordPayments {
			saleTransactionPayments = append(saleTransactionPayments, models.SaleTransactionPayment{
				TransactionId: assortedSaleRecordPayment.TransactionId,
				SeqNo:         assortedSaleRecordPayment.SeqNo,
				PayMethod:     assortedSaleRecordPayment.PayMethod,
				PayAmt:        assortedSaleRecordPayment.PayAmt,
				CreatedAt:     assortedSaleRecordPayment.CreatedAt,
			})
		}
		saleTransaction.Payments = saleTransactionPayments
		saleTransactions = append(saleTransactions, saleTransaction)
	}
	return saleTransactions, nil
}

// Before ...
func (etl SrToClearanceETL) Before(ctx context.Context, source interface{}) (interface{}, error) {
	assortedSaleRecord, ok := source.(models.AssortedSaleRecord)
	if !ok {
		return nil, errors.New("ETL-Before:Convert Failed")
	}
	newAssortedSaleRecords := make([]*models.AssortedSaleRecord, 0)
	result, err := (&assortedSaleRecord).SplitSaleRecordByBrand(nil)
	if err != nil {
		return nil, err
	}
	newAssortedSaleRecords = append(newAssortedSaleRecords, result...)
	return newAssortedSaleRecords, nil
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
	saleTransactions, ok := source.([]models.SaleTransaction)
	if !ok {
		return errors.New("ETL-Load:Convert Failed")
	}

	engine := factory.GetCfsrEngine()
	session := engine.NewSession()
	defer session.Close()
	if err := session.Begin(); err != nil {
		return err
	}
	for _, saleTransaction := range saleTransactions {
		_, dbSaleTransactions, err := models.SaleTransaction{}.GetSaleTransactions(ctx, saleTransaction.TransactionId, 0, 0, saleTransaction.ShopCode, 1, 0)
		if err != nil {
			return err
		}
		if len(dbSaleTransactions) > 0 {
			dbSaleTransaction := dbSaleTransactions[0]
			if dbSaleTransaction.WhetherSend == false {
				saleTransaction.Id = dbSaleTransaction.Id
				if err := saleTransaction.Update(); err != nil {
					return err
				}
			}
		} else {
			if _, err := session.Insert(&saleTransaction); err != nil {
				session.Rollback()
				return err
			}
			for i, _ := range saleTransaction.Dtls {
				saleTransaction.Dtls[i].SaleTransactionId = saleTransaction.Id
			}
			if _, err := session.Insert(&saleTransaction.Dtls); err != nil {
				session.Rollback()
				return err
			}

			for i, _ := range saleTransaction.Payments {
				saleTransaction.Payments[i].SaleTransactionId = saleTransaction.Id
			}
			if _, err := session.Insert(&saleTransaction.Payments); err != nil {
				session.Rollback()
				return err
			}
		}
	}

	if err := session.Commit(); err != nil {
		return err
	}
	return nil
}
