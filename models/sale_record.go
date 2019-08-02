package models

import "time"

type AssortedSaleRecord struct {
	TransactionId          string    `query:"transactionId" json:"transactionId" xorm:"pk"`
	Created                time.Time `query:"created" json:"created"`
	CreatedBy              string    `query:"createdBy" json:"createdBy"`
	Modified               time.Time `query:"modified" json:"modified"`
	ModifiedBy             string    `query:"modifiedBy" json:"modifiedBy"`
	CustomerId             int64     `query:"customerId" json:"customerId"`
	FreightPrice           float64   `query:"freightPrice" json:"freightPrice"`
	IsDelivery             int64     `query:"isDelivery" json:"isDelivery"`
	IsRefund               int64     `query:"isRefund" json:"isRefund"`
	ItemCode               string    `query:"itemCode" json:"itemCode"`
	ItemName               string    `query:"itemName" json:"itemName"`
	ListPrice              float64   `query:"listPrice" json:"listPrice"`
	Mileage                float64   `query:"mileage" json:"mileage"`
	Option                 string    `query:"option" json:"option"`
	OrderId                int64     `query:"orderId" json:"orderId"`
	OrderItemId            int64     `query:"orderItemId" json:"orderItemId"`
	ProductId              int64     `query:"productId" json:"productId"`
	Quantity               int64     `query:"quantity" json:"quantity"`
	RefundId               int64     `query:"refundId" json:"refundId"`
	RefundItemId           int64     `query:"refundItemId" json:"refundItemId"`
	SalePrice              float64   `query:"salePrice" json:"salePrice"`
	SalesmanId             int64     `query:"salesmanId" json:"salesmanId"`
	SkuId                  int64     `query:"skuId" json:"skuId"`
	SkuImg                 string    `query:"skuImg" json:"skuImg"`
	StoreId                int64     `query:"storeId" json:"storeId"`
	TenantCode             string    `query:"tenantCode" json:"tenantCode"`
	TotalDiscountPrice     float64   `query:"totalDiscountPrice" json:"totalDiscountPrice"`
	TotalListPrice         float64   `query:"totalListPrice" json:"totalListPrice"`
	TotalSalePrice         float64   `query:"totalSalePrice" json:"totalSalePrice"`
	TotalTransactionPrice  float64   `query:"totalTransactionPrice" json:"totalTransactionPrice"`
	TransactionChannelType string    `query:"transactionChannelType" json:"transactionChannelType"`
	TransactionCreateDate  time.Time `query:"transactionCreateDate" json:"transactionCreateDate"`
	TransactionStatus      string    `query:"transactionStatus" json:"transactionStatus"`
	TransactionType        string    `query:"transactionType" json:"transactionType"`
	TransactionUpdateDate  time.Time `query:"transactionUpdateDate" json:"transactionUpdateDate"`
}
