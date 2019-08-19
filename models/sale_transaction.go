package models

import "time"

type SaleTransaction struct {
	TransactionId  string    `json:"transactionId" query:"transactionId" xorm:"index VARCHAR(30) notnull pk" validate:"required"`
	OrderId        int64     `json:"orderId" query:"orderId"`
	StoreId        int64     `json:"storeId" query:"storeId" xorm:"index default 0" validate:"required"`
	TotalSalePrice float64   `json:"totalSalePrice" query:"totalSalePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	SaleDate       time.Time `json:"saleDate" query:"saleDate"`
}

type SaleTransactionDtl struct {
	Id            int64   `json:"id" query:"id"`
	Quantity      int64   `json:"quantity" query:"quantity" xorm:"notnull" validate:"required"`
	SalePrice     float64 `json:"salePrice" query:"salePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	SkuId         int64   `json:"skuId" query:"skuId" xorm:"index notnull" validate:"gte=0"`
	BrandCode     string  `json:"brandCode" query:"brandCode" xorm:"index VARCHAR(30) notnull" validate:"required"`
	BrandId       int64   `json:"brandId" query:"brandId" xorm:"index default 0"`
	TransactionId string  `json:"transactionId" query:"transactionId" xorm:"index VARCHAR(30) notnull" validate:"required"`
}

//SaleTransactionAndSaleTransactionDtl
type SaleTAndSaleTDtls struct {
	SaleTransactions    []SaleTransaction    `query:"saleTransactions" json:"saleTransactions" `
	SaleTransactionDtls []SaleTransactionDtl `query:"saleTransactionDtls" json:"saleTransactionDtls" `
}
