package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"time"
)

type SaleTransaction struct {
	TransactionId         int64     `json:"transactionId" xorm:"index default 0 pk" validate:"required"`
	OrderId               int64     `json:"orderId" xorm:"index default 0" validate:"required"`
	RefundId              int64     `json:"refundId" xorm:"index default 0" validate:"required"`
	StoreId               int64     `json:"storeId" xorm:"index default 0" validate:"required"`
	CustomerId            int64     `json:"customerId" xorm:"index default 0" validate:"required"`
	TotalSalePrice        float64   `json:"totalSalePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalTransactionPrice float64   `json:"totalTransactionPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	SaleDate              time.Time `json:"saleDate"`
	Mileage               float64   `json:"mileage" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	MileagePrice          float64   `json:"mileagePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	OuterOrderNo          string    `json:"outerOrderNo" xorm:"index VARCHAR(30) notnull" validate:"required"`
}

type SaleTransactionDtl struct {
	Id                             int64   `json:"id"`
	Quantity                       int64   `json:"quantity" xorm:"notnull" validate:"required"`
	SalePrice                      float64 `json:"salePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	ListPrice                      float64 `json:"listPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalDiscountPrice             float64 `json:"totalDiscountPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	OrderItemId                    int64   `json:"orderItemId" xorm:"index notnull" validate:"required"`
	RefundItemId                   int64   `json:"refundItemId" xorm:"index notnull" validate:"required"`
	ProductId                      int64   `json:"productId" xorm:"index notnull" validate:"required"`
	SkuId                          int64   `json:"skuId" xorm:"index notnull" validate:"gte=0"`
	BrandCode                      string  `json:"brandCode" xorm:"index VARCHAR(30) notnull" validate:"required"`
	BrandId                        int64   `json:"brandId" xorm:"index default 0"`
	ItemFee                        float64 `json:"itemFee" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalTransactionPrice          float64 `json:"totalTransactionPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalDistributedCartOfferPrice float64 `json:"totalDistributedCartOfferPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalSalePrice                 float64 `json:"totalSalePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TransactionId                  int64   `json:"transactionId" xorm:"index default 0" validate:"required"`
}

//SaleTransactionAndSaleTransactionDtl
type SaleTAndSaleTDtls struct {
	SaleTransactions    []SaleTransaction    `json:"saleTransactions"`
	SaleTransactionDtls []SaleTransactionDtl `json:"saleTransactionDtls"`
}

type SaleRecordIdSuccessMapping struct {
	SaleNo        string    `json:"saleNo" xorm:"index VARCHAR(30) notnull pk"`
	TransactionId int64     `json:"transactionId" xorm:"index default 0" validate:"required"`
	CreatedAt     time.Time `json:"createdAt" xorm:"created"`
	CreatedBy     string    `json:"createdBy" xorm:"index VARCHAR(30) notnull"`
}

type SaleRecordIdFailMapping struct {
	TransactionId    int64     `json:"transactionId" xorm:"index default 0" validate:"required"`
	TransactionDtlId int64     `json:"transactionDtlId" xorm:"index default 0"`
	Error            string    `json:"error" xorm:"VARCHAR(50)"`
	IsCreate         bool      `json:"isCreate" xorm:"index notnull default false"`
	CreatedAt        time.Time `json:"createdAt" xorm:"created"`
	CreatedBy        string    `json:"createdBy" xorm:"index VARCHAR(30)"`
}

func (srsm *SaleRecordIdSuccessMapping) CheckAndSave() error {
	saleRecordIdSuccessMapping := SaleRecordIdSuccessMapping{}
	has, err := factory.GetCfsrEngine().Where("saleNo = ?", srsm.SaleNo).Get(&saleRecordIdSuccessMapping)
	if err != nil {
		return err
	}
	if !has {
		if _, err := factory.GetCfsrEngine().Insert(srsm); err != nil {
			return err
		}
	}
	return nil
}

func (srfm *SaleRecordIdFailMapping) Save() error {
	if _, err := factory.GetCfsrEngine().Insert(srfm); err != nil {
		return err
	}
	return nil
}
