package models

import (
	"clearance/clearance-adapter-for-sale-record/config"
	"clearance/clearance-adapter-for-sale-record/factory"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pangpanglabs/goutils/behaviorlog"
	"github.com/pangpanglabs/goutils/httpreq"
	"github.com/pangpanglabs/goutils/number"
	"github.com/sirupsen/logrus"
)

type UseType string

type AssortedSaleRecord struct {
	TransactionId               int64                        `json:"transactionId" xorm:"pk"`
	CashPrice                   float64                      `json:"cashPrice"`
	ChannelId                   int64                        `json:"channelId"`
	Created                     time.Time                    `json:"created"`
	CreatedBy                   string                       `json:"createdBy"`
	Modified                    time.Time                    `json:"modified"`
	ModifiedBy                  string                       `json:"modifiedBy"`
	CustomerId                  int64                        `json:"customerId"`
	TransactionCreatedId        int64                        `json:"transactionCreatedId"`
	DiscountCouponPrice         float64                      `json:"discountCouponPrice"`
	DiscountOfferPrice          float64                      `json:"discountOfferPrice"`
	FreightPrice                float64                      `json:"freightPrice"`
	IsOutPaid                   int64                        `json:"isOutPaid"`
	IsRefund                    int64                        `json:"isRefund"`
	Mileage                     float64                      `json:"mileage"`
	MileagePrice                float64                      `json:"mileagePrice"`
	ObtainMileage               float64                      `json:"obtainMileage"`
	OrderId                     int64                        `json:"orderId"`
	OuterOrderNo                string                       `json:"outerOrderNo"`
	RefundId                    int64                        `json:"refundId"`
	EmpId                       string                       `json:"empId"`
	SalesmanId                  int64                        `json:"salesmanId"`
	StoreId                     int64                        `json:"storeId"`
	TenantCode                  string                       `json:"tenantCode"`
	TotalDiscountPrice          float64                      `json:"totalDiscountPrice"`
	TotalListPrice              float64                      `json:"totalListPrice"`
	TotalSalePrice              float64                      `json:"totalSalePrice"`
	TotalTransactionPrice       float64                      `json:"totalTransactionPrice"`
	TransactionChannelType      string                       `json:"transactionChannelType"`
	TransactionCreateDate       time.Time                    `json:"transactionCreateDate"`
	TransactionStatus           string                       `json:"transactionStatus"`
	TransactionType             string                       `json:"transactionType"`
	TransactionUpdateDate       time.Time                    `json:"transactionUpdateDate"`
	BaseTrimCode                string                       `json:"baseTrimCode"`
	AssortedSaleRecordDtls      []AssortedSaleRecordDtl      `json:"assortedSaleRecordDtls"`
	AssortedSaleRecordPayments  []AssortedSaleRecordPayment  `json:"assortedSaleRecordPayments" xorm:"-"`
	AppliedSaleRecordCartOffers []AppliedSaleRecordCartOffer `json:"appliedSaleRecordCartOffers"`
	ShopCode                    string                       `json:"shopCode"`
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
	Mileage                        float64   `json:"mileage"`
	MileagePrice                   float64   `json:"mileagePrice"`
	ObtainMileage                  float64   `json:"obtainMileage"`
}

type AssortedSaleRecordPayment struct {
	Id            int64     `json:"id"`
	TransactionId int64     `json:"transactionId"`
	SeqNo         int64     `json:"seqNo"`
	PayMethod     string    `json:"payMethod"`
	PayAmt        float64   `json:"payAmt"`
	CreatedAt     time.Time `json:"CreatedBy"`
}

type AssortedSaleRecordAndDtls struct {
	AssortedSaleRecords    []AssortedSaleRecord    `json:"assortedSaleRecords"`
	AssortedSaleRecordDtls []AssortedSaleRecordDtl `json:"assortedSaleRecordDtls"`
}

type PostMileage struct {
	Id            int64  `json:"id"`
	TransactionId int64  `json:"transactionId"`
	CustomerId    int64  `json:"customerId"`
	GradeId       int64  `json:"gradeId"`
	BrandId       int64  `json:"brandId"`
	OrderId       int64  `json:"orderId"`
	RefundId      int64  `json:"refundId"`
	BrandCode     string `json:"brandCode"`
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

type AppliedSaleRecordItemOffer struct {
	Id               int64  `json:"id"`
	CouponNo         string `json:"couponNo"`
	ItemCode         string `json:"itemCode"`
	OfferNo          string `json:"offerNo"`
	TransactionDtlId int64  `json:"transactionDtlId"`
}

type AppliedSaleRecordCartOffer struct {
	Id              int64   `json:"id"`
	TenantCode      string  `json:"tenantCode"`
	OfferNo         string  `json:"offerNo"`
	CouponNo        string  `json:"couponNo"`
	ItemCodes       string  `json:"itemCodes"`
	TargetItemCodes string  `json:"targetItemCodes"`
	Price           float64 `json:"price"`
	Type            string  `json:"type"`
	TransactionId   int64   `json:"transactionId"`
	TargetType      string  `josn:"targetType"`
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

type PostCouponEvent struct {
	Id        int64  `json:"id"`
	BrandCode string `json:"brandCode"`
	EventNo   int64  `json:"eventNo"`
}

type SaleRecordDtlSalesmanAmount struct {
	Id                 int64   `json:"id"`
	TransactionId      int64   `json:"transactionId"`
	OrderId            int64   `json:"orderId"`
	RefundId           int64   `json:"refundId"`
	OrderItemId        int64   `json:"orderItemId"`
	RefundItemId       int64   `json:"refundItemId"`
	SalesmanSaleAmount float64 `json:"salesmanSaleAmount"`
}

func (SaleTransactionPayment) GetSaleTransactionPayment(saleTransactionId int64) ([]SaleTransactionPayment, error) {
	var saleTransactionPayments []SaleTransactionPayment
	if err := factory.GetCfsrEngine().Where("sale_transaction_id = ?", saleTransactionId).Find(&saleTransactionPayments); err != nil {
		return nil, err
	}
	if len(saleTransactionPayments) == 0 {
		return nil, errors.New("SaleTransactionPayment is not exist!")
	}
	return saleTransactionPayments, nil
}

func (PostMileage) GetMileage(customerId, transactionId int64) (PostMileage, error) {
	var mileage PostMileage
	if _, err := factory.GetSrEngine().Where("customer_id = ?", customerId).
		And("transaction_id = ?", transactionId).
		Get(&mileage); err != nil {
		return PostMileage{}, err
	}
	return mileage, nil
}

func (AppliedSaleRecordItemOffer) GetAppliedSaleRecordItemOffer(transactionDtlId int64) (*AppliedSaleRecordItemOffer, error) {
	var appliedSaleRecordItemOffer AppliedSaleRecordItemOffer
	exist, err := factory.GetSrEngine().Where("transaction_dtl_id = ?", transactionDtlId).Get(&appliedSaleRecordItemOffer)
	if err != nil {
		return nil, err
	} else if !exist {
		logrus.WithFields(logrus.Fields{
			"transaction_dtl_id": transactionDtlId,
		}).Error("Fail to GetAppliedSaleRecordItemOffer")
		return nil, errors.New("AppliedSaleRecordItemOffer is not exist!")
	}
	return &appliedSaleRecordItemOffer, nil
}

func (AppliedSaleRecordCartOffer) GetAppliedSaleRecordCartOffers(transactionId int64) ([]AppliedSaleRecordCartOffer, error) {
	var appliedSaleRecordCartOffers []AppliedSaleRecordCartOffer
	if err := factory.GetSrEngine().Where("transaction_id = ?", transactionId).Find(&appliedSaleRecordCartOffers); err != nil {
		return nil, err
	}
	return appliedSaleRecordCartOffers, nil
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

func (PostCouponEvent) GetPostCoupenEvent(brandCode string) (*PostCouponEvent, error) {
	var postCouponEvent PostCouponEvent
	exist, err := factory.GetSrEngine().Where("brand_code = ?", brandCode).Get(&postCouponEvent)
	if err != nil {
		return nil, err
	} else if !exist {
		logrus.WithFields(logrus.Fields{
			"brandCode": brandCode,
		}).Error("Fail to GetPostCoupenEvent")
		return nil, errors.New("PostCoupenEvent is not exist!")
	}
	return &postCouponEvent, nil
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

func (SaleRecordDtlSalesmanAmount) GetSaleRecordDtlSalesmanAmount(orderItemId, refundItemId int64) (SaleRecordDtlSalesmanAmount, error) {
	var dtlSalesmanAmount SaleRecordDtlSalesmanAmount
	exist, err := factory.GetSrEngine().Where("order_item_id = ?", orderItemId).
		And("refund_item_id = ?", refundItemId).
		Get(&dtlSalesmanAmount)
	if err != nil {
		return SaleRecordDtlSalesmanAmount{}, err
	}
	if !exist {
		logrus.WithFields(logrus.Fields{
			"orderItemId":  orderItemId,
			"refundItemId": refundItemId,
		}).Error("Fail to GetSaleRecordDtlSalesmanAmount")
		return SaleRecordDtlSalesmanAmount{}, errors.New("SaleRecordDtlSalesmanAmount is not exist!")
	}
	return dtlSalesmanAmount, nil
}

func (saleRecord *AssortedSaleRecord) SplitSaleRecordByBrand(setting *number.Setting) ([]*AssortedSaleRecord, error) {
	mileageSetting := &number.Setting{
		RoundDigit:    0,
		RoundStrategy: "round",
	}

	getBrandMap := func(saleRecord *AssortedSaleRecord) map[string]int64 {
		var brandMap = make(map[string]int64)
		for _, saleRecordItem := range saleRecord.AssortedSaleRecordDtls {
			brandMap[saleRecordItem.BrandCode] = saleRecordItem.BrandId
		}
		return brandMap
	}

	makeSaleRecordDtls := func(saleRecord *AssortedSaleRecord, brandMap map[string]int64) map[string][]AssortedSaleRecordDtl {
		brandSaleRecordDtl := make(map[string][]AssortedSaleRecordDtl)
		for brandCode, _ := range brandMap {
			newSaleRecordDtl := make([]AssortedSaleRecordDtl, 0)
			for _, saleRecordDtl := range saleRecord.AssortedSaleRecordDtls {
				if brandCode != saleRecordDtl.BrandCode {
					continue
				}
				newSaleRecordDtl = append(newSaleRecordDtl, saleRecordDtl)
			}
			brandSaleRecordDtl[brandCode] = newSaleRecordDtl
		}
		return brandSaleRecordDtl
	}

	makeNewSaleRecord := func(saleRecord *AssortedSaleRecord) *AssortedSaleRecord {
		return &AssortedSaleRecord{
			TransactionId:          saleRecord.TransactionId,
			EmpId:                  saleRecord.EmpId,
			ChannelId:              saleRecord.ChannelId,
			Created:                saleRecord.Created,
			CreatedBy:              saleRecord.CreatedBy,
			Modified:               saleRecord.Modified,
			ModifiedBy:             saleRecord.ModifiedBy,
			CustomerId:             saleRecord.CustomerId,
			FreightPrice:           saleRecord.FreightPrice,
			IsOutPaid:              saleRecord.IsOutPaid,
			IsRefund:               saleRecord.IsRefund,
			OrderId:                saleRecord.OrderId,
			OuterOrderNo:           saleRecord.OuterOrderNo,
			RefundId:               saleRecord.RefundId,
			SalesmanId:             saleRecord.SalesmanId,
			StoreId:                saleRecord.StoreId,
			TenantCode:             saleRecord.TenantCode,
			TransactionChannelType: saleRecord.TransactionChannelType,
			TransactionCreateDate:  saleRecord.TransactionCreateDate,
			TransactionStatus:      saleRecord.TransactionStatus,
			TransactionType:        saleRecord.TransactionType,
			TransactionUpdateDate:  saleRecord.TransactionUpdateDate,
			TransactionCreatedId:   saleRecord.TransactionCreatedId,
			BaseTrimCode:           saleRecord.BaseTrimCode,
		}
	}

	calculateSaleRecordPrice := func(newSaleRecord *AssortedSaleRecord, saleRecordDtls []AssortedSaleRecordDtl) {
		for _, saleRecordDtl := range saleRecordDtls {
			newSaleRecord.ObtainMileage = number.ToFixed(newSaleRecord.ObtainMileage+saleRecordDtl.ObtainMileage, mileageSetting)
			newSaleRecord.Mileage = number.ToFixed(newSaleRecord.Mileage+saleRecordDtl.Mileage, mileageSetting)
			newSaleRecord.MileagePrice = number.ToFixed(newSaleRecord.MileagePrice+saleRecordDtl.MileagePrice, mileageSetting)
			newSaleRecord.TotalDiscountPrice = number.ToFixed(newSaleRecord.TotalDiscountPrice+saleRecordDtl.TotalDiscountPrice+saleRecordDtl.TotalDistributedCartOfferPrice+saleRecordDtl.MileagePrice, setting)
			newSaleRecord.TotalListPrice = number.ToFixed(newSaleRecord.TotalListPrice+saleRecordDtl.TotalListPrice, setting)
			newSaleRecord.TotalSalePrice = number.ToFixed(newSaleRecord.TotalSalePrice+saleRecordDtl.TotalDistributedPaymentPrice, setting)
			newSaleRecord.TotalTransactionPrice = number.ToFixed(newSaleRecord.TotalTransactionPrice+saleRecordDtl.TotalTransactionPrice, setting)
			newSaleRecord.CashPrice = number.ToFixed(newSaleRecord.CashPrice+saleRecordDtl.DistributedCashPrice, setting)
		}
	}

	makeCartOffers := func(saleRecordDtls []AssortedSaleRecordDtl, setting *number.Setting) []AppliedSaleRecordCartOffer {
		appliedSaleRecordCartOffers := make([]AppliedSaleRecordCartOffer, 0)
		for _, cartOffer := range saleRecord.AppliedSaleRecordCartOffers {
			var itemCodes, targetItemCodes []string
			var discountPrice float64
			for _, saleRecordDtl := range saleRecordDtls {
				if strings.Index(cartOffer.TargetItemCodes+",", saleRecordDtl.ItemCode+",") > -1 {
					targetItemCodes = append(itemCodes, saleRecordDtl.ItemCode)
					discountPrice = number.ToFixed(discountPrice+saleRecordDtl.TotalDistributedCartOfferPrice, setting)
				} else if strings.Index(cartOffer.ItemCodes+",", saleRecordDtl.ItemCode+",") > -1 {
					itemCodes = append(itemCodes, saleRecordDtl.ItemCode)
					discountPrice = number.ToFixed(discountPrice+saleRecordDtl.TotalDistributedCartOfferPrice, setting)
				}
			}
			if len(itemCodes) == 0 && len(targetItemCodes) == 0 {
				continue
			}
			newCartOffer := AppliedSaleRecordCartOffer{
				TenantCode: cartOffer.TenantCode,
				OfferNo:    cartOffer.OfferNo,
				CouponNo:   cartOffer.CouponNo,
				Type:       cartOffer.Type,
				TargetType: cartOffer.TargetType,
			}
			newCartOffer.ItemCodes = strings.Join(itemCodes, ",")
			newCartOffer.TargetItemCodes = strings.Join(targetItemCodes, ",")
			newCartOffer.Price = discountPrice
			appliedSaleRecordCartOffers = append(appliedSaleRecordCartOffers, cartOffer)
		}
		return appliedSaleRecordCartOffers
	}

	makePayments := func(newSaleRecord *AssortedSaleRecord) []AssortedSaleRecordPayment {
		newPayments := make([]AssortedSaleRecordPayment, 0)

		remainAmt := newSaleRecord.CashPrice

		seqNo := int64(0)
		for _, payment := range saleRecord.AssortedSaleRecordPayments {
			if payment.PayMethod == "MILEAGE" {
				continue
			}
			if remainAmt == 0 {
				break
			}
			seqNo++
			if remainAmt > payment.PayAmt {
				newPayments = append(newPayments, AssortedSaleRecordPayment{
					SeqNo:         seqNo,
					PayMethod:     payment.PayMethod,
					PayAmt:        payment.PayAmt,
					CreatedAt:     payment.CreatedAt,
					TransactionId: payment.TransactionId,
				})
				remainAmt = number.ToFixed(newSaleRecord.CashPrice-payment.PayAmt, setting)
			} else {
				newPayments = append(newPayments, AssortedSaleRecordPayment{
					SeqNo:         seqNo,
					PayMethod:     payment.PayMethod,
					PayAmt:        remainAmt,
					CreatedAt:     payment.CreatedAt,
					TransactionId: payment.TransactionId,
				})
			}
		}
		return newPayments
	}

	store, err := Store{}.GetStore(saleRecord.StoreId)
	if err != nil {
		return nil, err
	}
	getShopCode := func(brandCode string) string {
		for _, elandShop := range store.ElandShops {
			if elandShop.BrandCode == brandCode {
				return elandShop.ShopCode
			}
		}
		return ""
	}

	brandMap := getBrandMap(saleRecord)
	assortedSaleRecords := make([]*AssortedSaleRecord, 0)
	if len(brandMap) == 1 {
		saleRecord.ShopCode = getShopCode(saleRecord.AssortedSaleRecordDtls[0].BrandCode)
		assortedSaleRecords = append(assortedSaleRecords, saleRecord)
	} else {
		brandSaleRecordDtlMap := makeSaleRecordDtls(saleRecord, brandMap)
		for brandCode, saleRecordDtls := range brandSaleRecordDtlMap {
			newSaleRecord := makeNewSaleRecord(saleRecord)
			newSaleRecord.ShopCode = getShopCode(brandCode)
			newSaleRecord.AppliedSaleRecordCartOffers = makeCartOffers(saleRecordDtls, setting)
			newSaleRecord.AssortedSaleRecordDtls = saleRecordDtls
			calculateSaleRecordPrice(newSaleRecord, saleRecordDtls)
			newSaleRecord.AssortedSaleRecordPayments = makePayments(newSaleRecord)
			assortedSaleRecords = append(assortedSaleRecords, newSaleRecord)
		}
	}

	return assortedSaleRecords, nil
}

func GetToken(ctx context.Context, tenantId int64) (string, error) {
	resultToken := ResultToken{}
	body := RequestTokenBody{
		AppId:        config.Config().GetTokenUser.AppId,
		AppSecretKey: config.Config().GetTokenUser.AppSecretKey,
	}
	url := fmt.Sprintf("%s/v1/sso/app-secret-key-token?tenantId=%d", config.Config().Services.GetTokenApi, tenantId)
	if _, err := httpreq.New(http.MethodPost, url, body).
		WithBehaviorLogContext(behaviorlog.FromCtx(ctx)).
		Call(&resultToken); err != nil {
		return "", err
	}
	if resultToken.Success && resultToken.Result.Token != "" {
		return resultToken.Result.Token, nil
	}
	return "", errors.New("Get token error!")
}
