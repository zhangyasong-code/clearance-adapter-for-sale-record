package models

import "time"

type UseType string

const (
	UseTypeEarn       UseType = "Earn"
	UseTypeEarnCancel UseType = "EarnCancel"
	UseTypeUsed       UseType = "Used"
	UseTypeUsedCancel UseType = "UsedCancel"
)

type AssortedSaleRecord struct {
	TransactionId          int64     `query:"transactionId" json:"transactionId" xorm:"pk"`
	CashPrice              float64   `query:"cashPrice" json:"cashPrice"`
	ChannelId              int64     `query:"channelId" json:"channelId"`
	Created                time.Time `query:"created" json:"created"`
	CreatedBy              string    `query:"createdBy" json:"createdBy"`
	Modified               time.Time `query:"modified" json:"modified"`
	ModifiedBy             string    `query:"modifiedBy" json:"modifiedBy"`
	CustomerId             int64     `query:"customerId" json:"customerId"`
	DiscountCouponPrice    float64   `query:"discountCouponPrice" json:"discountCouponPrice"`
	DiscountOfferPrice     float64   `query:"discountOfferPrice" json:"discountOfferPrice"`
	FreightPrice           float64   `query:"freightPrice" json:"freightPrice"`
	IsOutPaid              int64     `query:"isOutPaid" json:"isOutPaid"`
	IsRefund               int64     `query:"isRefund" json:"isRefund"`
	Mileage                float64   `query:"mileage" json:"mileage"`
	MileagePrice           float64   `query:"mileagePrice" json:"mileagePrice"`
	OrderId                int64     `query:"orderId" json:"orderId"`
	OuterOrderNo           string    `query:"outerOrderNo" json:"outerOrderNo"`
	RefundId               int64     `query:"refundId" json:"refundId"`
	SalesmanId             int64     `query:"salesmanId" json:"salesmanId"`
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

type AssortedSaleRecordDtl struct {
	Id                             int64     `query:"id" json:"id"`
	BrandCode                      string    `query:"brandCode" json:"brandCode"`
	BrandId                        int64     `query:"brandId" json:"brandId"`
	Created                        time.Time `query:"created" json:"created"`
	CreatedBy                      string    `query:"createdBy" json:"createdBy"`
	Modified                       time.Time `query:"modified" json:"modified"`
	ModifiedBy                     string    `query:"modifiedBy" json:"modifiedBy"`
	DistributedCashPrice           float64   `query:"distributedCashPrice" json:"distributedCashPrice"`
	TotalDistributedCartOfferPrice float64   `query:"totalDistributedCartOfferPrice" json:"totalDistributedCartOfferPrice"`
	TotalDistributedItemOfferPrice float64   `query:"totalDistributedItemOfferPrice" json:"totalDistributedItemOfferPrice"`
	TotalDistributedPaymentPrice   float64   `query:"totalDistributedPaymentPrice" json:"totalDistributedPaymentPrice"`
	IsDelivery                     bool      `query:"isDelivery" json:"isDelivery"`
	ItemCode                       string    `query:"itemCode" json:"itemCode"`
	ItemName                       string    `query:"itemName" json:"itemName"`
	ListPrice                      float64   `query:"listPrice" json:"listPrice"`
	OrderItemId                    int64     `query:"orderItemId" json:"orderItemId"`
	ProductId                      int64     `query:"productId" json:"productId"`
	Quantity                       int64     `query:"quantity" json:"quantity"`
	RefundItemId                   int64     `query:"refundItemId" json:"refundItemId"`
	SalePrice                      float64   `query:"salePrice" json:"salePrice"`
	SkuId                          int64     `query:"skuId" json:"skuId"`
	SkuImg                         string    `query:"skuImg" json:"skuImg"`
	Status                         string    `query:"status" json:"status"`
	TotalDiscountPrice             float64   `query:"totalDiscountPrice" json:"totalDiscountPrice"`
	TotalListPrice                 float64   `query:"totalListPrice" json:"totalListPrice"`
	TotalSalePrice                 float64   `query:"totalSalePrice" json:"totalSalePrice"`
	TotalTransactionPrice          float64   `query:"totalTransactionPrice" json:"totalTransactionPrice"`
	TransactionId                  int64     `query:"transactionId" json:"transactionId"`
}

type AssortedSaleRecordAndDtls struct {
	AssortedSaleRecords    []AssortedSaleRecord    `query:"assortedSaleRecords" json:"assortedSaleRecords"`
	AssortedSaleRecordDtls []AssortedSaleRecordDtl `query:"assortedSaleRecordDtls" json:"assortedSaleRecordDtls"`
}

type PostMileage struct {
	Id            int64   `json:"id" query:"id"`
	TenantCode    string  `json:"tenantCode" query:"tenantCode"`
	CustomerId    int64   `json:"customerId" query:"customerId"`
	CustGradeCode string  `json:"custGradeCode" query:"custGradeCode"`
	CustBrandCode string  `json:"custBrandCode" query:"custBrandCode"`
	UseType       string  `json:"useType" xorm:"VARCHAR(25)"`
	Point         float64 `json:"point" xorm:"decimal(19,2)"`
	PointAmount   float64 `json:"pointAmount" xorm:"decimal(19,2)"`
}
