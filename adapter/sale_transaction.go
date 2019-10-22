package adapter

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"errors"
	"strconv"
	"time"

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
	saleRecords := []models.AssortedSaleRecord{}
	saleRecordsDtl := []models.AssortedSaleRecordDtl{}
	//分页查询   一次查1000条
	skipCount := 0
	data := ctx.Value("data")
	dataInput := data.(models.RequestInput)
	for {
		var assortedSaleRecordAndDtls []struct {
			AssortedSaleRecord    models.AssortedSaleRecord    `xorm:"extends"`
			AssortedSaleRecordDtl models.AssortedSaleRecordDtl `xorm:"extends"`
		}
		query := func() xorm.Interface {
			q := factory.GetSrEngine().Table("assorted_sale_record").
				Join("INNER", "assorted_sale_record_dtl", "assorted_sale_record_dtl.transaction_id = assorted_sale_record.transaction_id").
				Where("1 = 1")
			if dataInput.BrandCode != "" {
				q.And("assorted_sale_record_dtl.brand_code = ?", dataInput.BrandCode)
			}
			if dataInput.ChannelType != "" {
				q.And("assorted_sale_record.transaction_channel_type = ?", dataInput.ChannelType)
			}
			if dataInput.OrderId != 0 {
				q.And("assorted_sale_record.order_id = ?", dataInput.OrderId)
			}
			q.And("assorted_sale_record.refund_id = ?", dataInput.RefundId)
			if dataInput.StartAt != "" && dataInput.EndAt != "" {
				st, _ := time.Parse("2006-01-02 15:04:05", dataInput.StartAt)
				et, _ := time.Parse("2006-01-02 15:04:05", dataInput.EndAt)
				h, _ := time.ParseDuration("-8h")
				q.And("assorted_sale_record.transaction_create_date >= ?", st.Add(h)).And("assorted_sale_record.transaction_create_date < ?", et.Add(h))
			}
			return q
		}
		if err := query().Limit(maxResultCount, skipCount).Find(&assortedSaleRecordAndDtls); err != nil {
			return nil, err
		}
		for _, assortedSaleRecordAndDtl := range assortedSaleRecordAndDtls {
			check := true
			for _, saleRecord := range saleRecords {
				if assortedSaleRecordAndDtl.AssortedSaleRecord.OrderId == saleRecord.OrderId && assortedSaleRecordAndDtl.AssortedSaleRecord.RefundId == saleRecord.RefundId {
					check = false
				}
			}
			if len(saleRecords) == 0 || check {
				saleRecords = append(saleRecords, assortedSaleRecordAndDtl.AssortedSaleRecord)
			}
			saleRecordsDtl = append(saleRecordsDtl, assortedSaleRecordAndDtl.AssortedSaleRecordDtl)
		}
		if len(assortedSaleRecordAndDtls) < maxResultCount {
			break
		} else {
			skipCount += maxResultCount
		}
	}
	return models.AssortedSaleRecordAndDtls{
		AssortedSaleRecords:    saleRecords,
		AssortedSaleRecordDtls: saleRecordsDtl,
	}, nil
}

// Transform ...
func (etl SrToClearanceETL) Transform(ctx context.Context, source interface{}) (interface{}, error) {
	assortedSaleRecordAndDtls, ok := source.(models.AssortedSaleRecordAndDtls)
	if !ok {
		return nil, errors.New("Convert Failed")
	}
	saleTransactions := make([]models.SaleTransaction, 0)
	saleTransactionDtls := make([]models.SaleTransactionDtl, 0)
	for _, assortedSaleRecord := range assortedSaleRecordAndDtls.AssortedSaleRecords {
		saleTransactions = append(saleTransactions, models.SaleTransaction{
			OrderId:                assortedSaleRecord.OrderId,
			RefundId:               assortedSaleRecord.RefundId,
			StoreId:                assortedSaleRecord.StoreId,
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
			OuterOrderNo:           assortedSaleRecord.OuterOrderNo,
			TransactionChannelType: assortedSaleRecord.TransactionChannelType,
			TotalDiscountPrice:     assortedSaleRecord.TotalDiscountPrice,
			BaseTrimCode:           assortedSaleRecord.BaseTrimCode,
		})
	}
	for _, assortedSaleRecordDtl := range assortedSaleRecordAndDtls.AssortedSaleRecordDtls {
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
		dbSaleTransaction, err := models.SaleTransaction{}.Get(saleTransaction.TransactionId)
		if err != nil {
			return err
		}
		if dbSaleTransaction.TransactionId != 0 {
			if dbSaleTransaction.WhetherSend == false {
				if err := saleTransaction.Update(); err != nil {
					return err
				}
			} else {
				return errors.New("Sended to CSL ! TransactionId:" + strconv.FormatInt(dbSaleTransaction.TransactionId, 10))
			}
		} else {
			if _, err := session.Insert(&saleTransaction); err != nil {
				session.Rollback()
				return err
			}
			for _, saleTransactionDtl := range saleTAndSaleTDtls.SaleTransactionDtls {
				if saleTransactionDtl.TransactionId == saleTransaction.TransactionId {
					if _, err := session.Insert(&saleTransactionDtl); err != nil {
						session.Rollback()
						return err
					}
				}
			}
		}
	}

	if err := session.Commit(); err != nil {
		return err
	}
	return nil
}
