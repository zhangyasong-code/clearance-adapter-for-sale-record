package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"errors"
	"time"

	"github.com/sirupsen/logrus"
)

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
	Id                  int64   `json:"id" query:"id"`
	TransactionId       int64   `query:"transactionId" json:"transactionId"`
	TenantCode          string  `json:"tenantCode" query:"tenantCode"`
	CustomerId          int64   `json:"customerId" query:"customerId"`
	CustGradeCode       string  `json:"custGradeCode" query:"custGradeCode"`
	CustBrandCode       string  `json:"custBrandCode" query:"custBrandCode"`
	CustMileagePolicyNo int64   `json:"custMileagePolicyNo" query:"custMileagePolicyNo"`
	UseType             string  `json:"useType" query:"useType"`
	Point               float64 `json:"point" query:"point"`
	PointAmount         float64 `json:"pointAmount" query:"pointAmount"`
}

type PostMileageDtl struct {
	Id               int64   `json:"id"`
	PostMileageId    int64   `json:"postMileageId"`
	TransactionDtlId int64   `json:"transactionDtlId" xorm:"index default 0"`
	OrderItemId      int64   `json:"orderItemId" xorm:"index default 0"`
	RefundItemId     int64   `json:"refundItemId" xorm:"index default 0"`
	UseType          string  `json:"useType" xorm:"VARCHAR(25)"`
	Point            float64 `json:"point" xorm:"decimal(19,2)"`
	PointAmount      float64 `json:"pointAmount" xorm:"decimal(19,2)"`
}

type AppliedOrderItemOffer struct {
	Id          int64  `json:"id" query:"id"`
	CouponNo    string `json:"couponNo" query:"couponNo"`
	ItemCode    string `json:"itemCode" query:"itemCode"`
	OfferNo     string `json:"offerNo" query:"offerNo"`
	OrderItemId int64  `json:"orderItemId" query:"orderItemId"`
}

type PromotionEvent struct {
	OfferNo                   string    `json:"offerNo" query:"offerNo"`
	BrandCode                 string    `json:"brandCode" query:"brandCode"`
	ShopCode                  string    `json:"shopCode" query:"shopCode"`
	EventTypeCode             string    `json:"eventTypeCode" query:"eventTypeCode"`
	EventName                 string    `json:"eventName" query:"eventName"`
	EventNo                   string    `json:"eventNo" query:"eventNo"`
	EventDescription          string    `json:"eventDescription" query:"eventDescription"`
	StartDate                 time.Time `json:"startDate" query:"startDate"`
	EndDate                   time.Time `json:"endDate" query:"endDate"`
	ExtendSalePermitDateCount int       `json:"extendSalePermitDateCount" query:"extendSalePermitDateCount"` //扩展天数
	NormalSaleRecognitionChk  bool      `json:"normalSaleRecognitionChk" query:"normalSaleRecognitionChk"`   //活动销售额是否正常
	FeeRate                   float64   `json:"feeRate" query:"feeRate"`
	InUserID                  string    `json:"inUserId" query:"inUserId"`
	SaleBaseAmt               float64   `json:"saleBaseAmt" query:"saleBaseAmt"`
	DiscountBaseAmt           float64   `json:"discountBaseAmt" query:"discountBaseAmt"`
	DiscountRate              float64   `json:"discountRate" query:"discountRate"`
	StaffSaleChk              bool      `json:"staffSaleChk" query:"staffSaleChk"`
}

func (PostMileage) GetMileage(customerId, transactionId int64, use_type UseType) (*PostMileage, error) {
	var mileage PostMileage
	exist, err := factory.GetSrEngine().Where("customer_id = ?", customerId).
		And("use_type = ?", string(use_type)).And("transaction_id = ?", transactionId).
		Get(&mileage)
	if err != nil {
		return nil, err
	} else if !exist {
		logrus.WithFields(logrus.Fields{
			"customer_id":    customerId,
			"transaction_id": transactionId,
		}).Error("Fail to get Mileage")
		return nil, errors.New("Mileage is not exist")
	}
	return &mileage, nil
}

func (PostMileage) GetPostMileageDtl(transactionDtlId int64, use_type UseType) (*PostMileageDtl, error) {
	var postMileageDtl PostMileageDtl
	exist, err := factory.GetSrEngine().Where("mileage_type = ?", string(use_type)).
		And("transaction_dtl_id = ?", transactionDtlId).
		Get(&postMileageDtl)
	if err != nil {
		return nil, err
	} else if !exist {
		logrus.WithFields(logrus.Fields{
			"transaction_dtl_id": transactionDtlId,
		}).Error("Fail to GetPostMileageDtl")
		return nil, errors.New("PostMileageDtl is not exist")
	}
	return &postMileageDtl, nil
}

func (AppliedOrderItemOffer) GetAppliedOrderItemOffer(orderItemId int64) (*AppliedOrderItemOffer, error) {
	var appliedOrderItemOffer AppliedOrderItemOffer
	exist, err := factory.GetSrEngine().Where("order_item_id = ?", orderItemId).Get(&appliedOrderItemOffer)
	if err != nil {
		return nil, err
	} else if !exist {
		logrus.WithFields(logrus.Fields{
			"order_item_id": orderItemId,
		}).Error("Fail to GetAppliedOrderItemOffer")
		return nil, errors.New("AppliedOrderItemOffer is not exist")
	}
	return &appliedOrderItemOffer, nil
}

//sum quantity , total_sale_price , total_discount_price
func (AssortedSaleRecordDtl) GetSumsFields(transactionId int64) ([]float64, error) {
	var assortedSaleRecordDtl AssortedSaleRecordDtl
	res, err := factory.GetSrEngine().Where("transaction_id = ?", transactionId).
		Sums(assortedSaleRecordDtl, "quantity", "total_sale_price", "total_discount_price")
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (PromotionEvent) GetPromotionEvent(offerNo string) (*PromotionEvent, error) {
	var promotionEvent PromotionEvent
	exist, err := factory.GetSrEngine().Where("offer_no = ?", offerNo).Get(&promotionEvent)
	if err != nil {
		return nil, err
	} else if !exist {
		logrus.WithFields(logrus.Fields{
			"order_item_id": offerNo,
		}).Error("Fail to GetPromotionEvent")
		return nil, errors.New("PromotionEvent is not exist")
	}
	return &promotionEvent, nil
}
