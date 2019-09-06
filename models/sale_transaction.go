package models

import "time"

type SaleTransaction struct {
	TransactionId  int64     `json:"transactionId" query:"transactionId" xorm:"index default 0 pk" validate:"required"`
	OrderId        int64     `json:"orderId" query:"orderId" xorm:"index default 0" validate:"required"`
	RefundId       int64     `query:"refundId" json:"refundId" xorm:"index default 0" validate:"required"`
	StoreId        int64     `json:"storeId" query:"storeId" xorm:"index default 0" validate:"required"`
	CustomerId     int64     `json:"customerId" query:"customerId" xorm:"index default 0" validate:"required"`
	TotalSalePrice float64   `json:"totalSalePrice" query:"totalSalePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	SaleDate       time.Time `json:"saleDate" query:"saleDate"`
	Mileage        float64   `query:"mileage" json:"mileage" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	MileagePrice   float64   `query:"mileagePrice" json:"mileagePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	OuterOrderNo   string    `query:"outerOrderNo" json:"outerOrderNo" xorm:"index VARCHAR(30) notnull" validate:"required"`
}

type SaleTransactionDtl struct {
	Id                             int64   `json:"id" query:"id"`
	Quantity                       int64   `json:"quantity" query:"quantity" xorm:"notnull" validate:"required"`
	SalePrice                      float64 `json:"salePrice" query:"salePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	ListPrice                      float64 `json:"listPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalDiscountPrice             float64 `json:"totalDiscountPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	OrderItemId                    int64   `json:"orderItemId" query:"orderItemId" xorm:"index notnull" validate:"required"`
	ProductId                      int64   `json:"productId" query:"productId" xorm:"index notnull" validate:"required"`
	SkuId                          int64   `json:"skuId" query:"skuId" xorm:"index notnull" validate:"gte=0"`
	BrandCode                      string  `json:"brandCode" query:"brandCode" xorm:"index VARCHAR(30) notnull" validate:"required"`
	BrandId                        int64   `json:"brandId" query:"brandId" xorm:"index default 0"`
	TotalTransactionPrice          float64 `json:"totalTransactionPrice" query:"totalTransactionPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalDistributedCartOfferPrice float64 `json:"totalDistributedCartOfferPrice" query:"totalDistributedCartOfferPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalSalePrice                 float64 `json:"totalSalePrice" query:"totalSalePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TransactionId                  int64   `json:"transactionId" query:"transactionId" xorm:"index VARCHAR(30) notnull" validate:"required"`
}

//SaleTransactionAndSaleTransactionDtl
type SaleTAndSaleTDtls struct {
	SaleTransactions    []SaleTransaction    `query:"saleTransactions" json:"saleTransactions" `
	SaleTransactionDtls []SaleTransactionDtl `query:"saleTransactionDtls" json:"saleTransactionDtls" `
}
