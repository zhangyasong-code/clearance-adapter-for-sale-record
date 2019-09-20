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
	TransactionId          int64     `json:"transactionId" xorm:"pk"`
	CashPrice              float64   `json:"cashPrice"`
	ChannelId              int64     `json:"channelId"`
	Created                time.Time `json:"created"`
	CreatedBy              string    `json:"createdBy"`
	Modified               time.Time `json:"modified"`
	ModifiedBy             string    `json:"modifiedBy"`
	CustomerId             int64     `json:"customerId"`
	DiscountCouponPrice    float64   `json:"discountCouponPrice"`
	DiscountOfferPrice     float64   `json:"discountOfferPrice"`
	FreightPrice           float64   `json:"freightPrice"`
	IsOutPaid              int64     `json:"isOutPaid"`
	IsRefund               int64     `json:"isRefund"`
	Mileage                float64   `json:"mileage"`
	MileagePrice           float64   `json:"mileagePrice"`
	OrderId                int64     `json:"orderId"`
	OuterOrderNo           string    `json:"outerOrderNo"`
	RefundId               int64     `json:"refundId"`
	SalesmanId             int64     `json:"salesmanId"`
	StoreId                int64     `json:"storeId"`
	TenantCode             string    `json:"tenantCode"`
	TotalDiscountPrice     float64   `json:"totalDiscountPrice"`
	TotalListPrice         float64   `json:"totalListPrice"`
	TotalSalePrice         float64   `json:"totalSalePrice"`
	TotalTransactionPrice  float64   `json:"totalTransactionPrice"`
	TransactionChannelType string    `json:"transactionChannelType"`
	TransactionCreateDate  time.Time `json:"transactionCreateDate"`
	TransactionStatus      string    `json:"transactionStatus"`
	TransactionType        string    `json:"transactionType"`
	TransactionUpdateDate  time.Time `json:"transactionUpdateDate"`
}

type AssortedSaleRecordDtl struct {
	Id                             int64     `json:"id"`
	BrandCode                      string    `json:"brandCode"`
	BrandId                        int64     `json:"brandId"`
	Created                        time.Time `json:"created"`
	CreatedBy                      string    `json:"createdBy"`
	Modified                       time.Time `json:"modified"`
	ModifiedBy                     string    `json:"modifiedBy"`
	DistributedCashPrice           float64   `json:"distributedCashPrice"`
	TotalDistributedCartOfferPrice float64   `json:"totalDistributedCartOfferPrice"`
	TotalDistributedItemOfferPrice float64   `json:"totalDistributedItemOfferPrice"`
	TotalDistributedPaymentPrice   float64   `json:"totalDistributedPaymentPrice"`
	FeeRate                        float64   `json:"feeRate"`
	IsDelivery                     bool      `json:"isDelivery"`
	ItemCode                       string    `json:"itemCode"`
	ItemFee                        float64   `json:"itemFee"`
	ItemName                       string    `json:"itemName"`
	ListPrice                      float64   `json:"listPrice"`
	OrderItemId                    int64     `json:"orderItemId"`
	ProductId                      int64     `json:"productId"`
	Quantity                       int64     `json:"quantity"`
	RefundItemId                   int64     `json:"refundItemId"`
	SalePrice                      float64   `json:"salePrice"`
	SkuId                          int64     `json:"skuId"`
	SkuImg                         string    `json:"skuImg"`
	Status                         string    `json:"status"`
	TotalDiscountPrice             float64   `json:"totalDiscountPrice"`
	TotalListPrice                 float64   `json:"totalListPrice"`
	TotalSalePrice                 float64   `json:"totalSalePrice"`
	TotalTransactionPrice          float64   `json:"totalTransactionPrice"`
	TransactionId                  int64     `json:"transactionId"`
}

type AssortedSaleRecordAndDtls struct {
	AssortedSaleRecords    []AssortedSaleRecord    `json:"assortedSaleRecords"`
	AssortedSaleRecordDtls []AssortedSaleRecordDtl `json:"assortedSaleRecordDtls"`
}

type PostMileage struct {
	Id                  int64   `json:"id"`
	TransactionId       int64   `json:"transactionId"`
	TenantCode          string  `json:"tenantCode"`
	CustomerId          int64   `json:"customerId"`
	GradeId             int64   `json:"gradeId"`
	BrandId             int64   `json:"brandId"`
	CustMileagePolicyNo int64   `json:"custMileagePolicyNo"`
	UseType             string  `json:"useType"`
	Point               float64 `json:"point"`
	PointAmount         float64 `json:"pointAmount"`
}

type PostMileageDtl struct {
	Id                  int64   `json:"id"`
	PostMileageId       int64   `json:"postMileageId"`
	TransactionDtlId    int64   `json:"transactionDtlId" xorm:"index default 0"`
	OrderItemId         int64   `json:"orderItemId" xorm:"index default 0"`
	RefundItemId        int64   `json:"refundItemId" xorm:"index default 0"`
	CustMileagePolicyNo int64   `json:"custMileagePolicyNo"`
	UseType             string  `json:"useType" xorm:"VARCHAR(25)"`
	Point               float64 `json:"point" xorm:"decimal(19,2)"`
	PointPrice          float64 `json:"pointPrice" xorm:"decimal(19,2)"`
}

type AppliedOrderItemOffer struct {
	Id          int64  `json:"id"`
	CouponNo    string `json:"couponNo"`
	ItemCode    string `json:"itemCode"`
	OfferNo     string `json:"offerNo"`
	OrderItemId int64  `json:"orderItemId"`
}

type PromotionEvent struct {
	OfferNo                   string    `json:"offerNo"`
	BrandCode                 string    `json:"brandCode"`
	ShopCode                  string    `json:"shopCode"`
	EventTypeCode             string    `json:"eventTypeCode"`
	EventName                 string    `json:"eventName"`
	EventNo                   string    `json:"eventNo"`
	EventDescription          string    `json:"eventDescription"`
	StartDate                 time.Time `json:"startDate"`
	EndDate                   time.Time `json:"endDate"`
	ExtendSalePermitDateCount int       `json:"extendSalePermitDateCount"` //扩展天数
	NormalSaleRecognitionChk  bool      `json:"normalSaleRecognitionChk"`  //活动销售额是否正常
	FeeRate                   float64   `json:"feeRate"`
	InUserID                  string    `json:"inUserId"`
	SaleBaseAmt               float64   `json:"saleBaseAmt"`
	DiscountBaseAmt           float64   `json:"discountBaseAmt"`
	DiscountRate              float64   `json:"discountRate"`
	StaffSaleChk              bool      `json:"staffSaleChk"`
}

type PostSaleRecordFee struct {
	TransactionDtlId       int64   `json:"transactionDtlId"`
	TransactionId          int64   `json:"transactionId"`
	OrderId                int64   `json:"orderId"`
	OrderItemId            int64   `json:"orderItemId"`
	RefundId               int64   `json:"refundId"`
	RefundItemId           int64   `json:"refundItemId"`
	CustomerId             int64   `json:"customerId"`
	StoreId                int64   `json:"storeId"`
	TotalSalePrice         float64 `json:"totalSalePrice"`
	TotalPaymentPrice      float64 `json:"totalPaymentPrice"`
	Mileage                float64 `json:"mileage"`
	MileagePrice           float64 `json:"mileagePrice"`
	ItemFeeRate            float64 `json:"itemFeeRate"`
	ItemFee                float64 `json:"itemFee"`
	EventFeeRate           float64 `json:"eventFeeRate"`
	AppliedFeeRate         float64 `json:"appliedFeeRate"`
	FeeAmount              float64 `json:"feeAmount"`
	TransactionChannelType string  `json:"transactionChannelType"`
}
type PostPayment struct {
	Id                 int64     `json:"id"`
	TransactionId      int64     `json:"transactionId"`
	SeqNo              int64     `json:"seqNo"`
	PaymentCode        string    `json:"paymentCode"`
	PaymentAmt         float64   `json:"paymentAmt"`
	InUserID           string    `json:"inUserId"`
	InDateTime         time.Time `json:"inDateTime"`
	ModiUserID         string    `json:"modiUserID"`
	ModiDateTime       time.Time `json:"modiDateTime"`
	CreditCardFirmCode string    `json:"creditCardFirmCode"`
}

func (PostPayment) GetPostPayment(transactionId int64) ([]PostPayment, error) {
	var postPayments []PostPayment
	if err := factory.GetSrEngine().Where("transaction_id = ?", transactionId).Find(&postPayments); err != nil {
		return nil, err
	}
	if len(postPayments) == 0 {
		return nil, errors.New("PostPayment is not exist!")
	}
	return postPayments, nil
}

func (PostMileage) GetMileage(customerId, transactionId int64, use_type UseType) (PostMileage, error) {
	var mileage PostMileage
	if _, err := factory.GetSrEngine().Where("customer_id = ?", customerId).
		And("use_type = ?", string(use_type)).And("transaction_id = ?", transactionId).
		Get(&mileage); err != nil {
		return PostMileage{}, err
	}
	return mileage, nil
}

func (PostMileage) GetPostMileageDtl(transactionDtlId int64, use_type UseType) (PostMileageDtl, error) {
	var postMileageDtl PostMileageDtl
	if _, err := factory.GetSrEngine().Where("use_type = ?", string(use_type)).
		And("transaction_dtl_id = ?", transactionDtlId).
		Get(&postMileageDtl); err != nil {
		return PostMileageDtl{}, err
	}
	return postMileageDtl, nil
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
		return nil, errors.New("AppliedOrderItemOffer is not exist!")
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
			"offerNo": offerNo,
		}).Error("Fail to GetPromotionEvent")
		return nil, errors.New("PromotionEvent is not exist!")
	}
	return &promotionEvent, nil
}

func (PostSaleRecordFee) GetPostSaleRecordFee(orderItemId, refundId int64) (PostSaleRecordFee, error) {
	var postSaleRecordFee PostSaleRecordFee
	if _, err := factory.GetSrEngine().Where("order_item_id = ?", orderItemId).And("refund_item_id = ?", refundId).Get(&postSaleRecordFee); err != nil {
		return PostSaleRecordFee{}, err
	}
	return postSaleRecordFee, nil
}

func (PostSaleRecordFee) GetSumFeeAmount(transactionId int64) (float64, error) {
	var postSaleRecordFee PostSaleRecordFee
	res, err := factory.GetSrEngine().Where("transaction_id = ?", transactionId).
		Sum(postSaleRecordFee, "fee_amount")
	if err != nil {
		return 0, err
	}
	return res, nil
}
