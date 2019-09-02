package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type SaleDtl struct {
	SaleNo                            string    `query:"saleNo" json:"saleNo" xorm:"pk"`
	DtSeq                             int64     `query:"dtSeq" json:"dtSeq" xorm:"pk"`
	BrandCode                         string    `query:"brandCode" json:"brandCode"`
	ShopCode                          string    `query:"shopCode" json:"shopCode"`
	Dates                             string    `query:"dates" json:"dates"`
	PosNo                             string    `query:"posNo" json:"posNo"`
	SeqNo                             int64     `query:"seqNo" json:"seqNo"`
	NormalSaleTypeCode                string    `query:"normalSaleTypeCode" json:"normalSaleTypeCode"`
	CustMileagePolicyNo               int64     `query:"custMileagePolicyNo" json:"custMileagePolicyNo"`
	PrimaryCustEventNo                int64     `query:"primaryCustEventNo" json:"primaryCustEventNo"`
	PrimaryEventTypeCode              string    `query:"primaryEventTypeCode" json:"primaryEventTypeCode"`
	PrimaryEventSettleTypeCode        string    `query:"primaryEventSettleTypeCode" json:"primaryEventSettleTypeCode"`
	SecondaryCustEventNo              int64     `query:"secondaryCustEventNo" json:"secondaryCustEventNo"`
	SecondaryEventTypeCode            string    `query:"secondaryEventTypeCode" json:"secondaryEventTypeCode"`
	SecondaryEventSettleTypeCode      string    `query:"secondaryEventSettleTypeCode" json:"secondaryEventSettleTypeCode"`
	SaleEventNo                       int64     `query:"saleEventNo" json:"saleEventNo"`
	SaleEventTypeCode                 string    `query:"saleEventTypeCode" json:"saleEventTypeCode"`
	SaleReturnReasonCode              string    `query:"saleReturnReasonCode" json:"saleReturnReasonCode"`
	ProdCode                          string    `query:"prodCode" json:"prodCode"`
	EANCode                           string    `query:"eANCode" json:"eANCode"`
	PriceTypeCode                     string    `query:"priceTypeCode" json:"priceTypeCode"`
	SupGroupCode                      string    `query:"supGroupCode" json:"supGroupCode"`
	SaipType                          string    `query:"saipType" json:"saipType"`
	NormalPrice                       float64   `query:"normalPrice" json:"normalPrice"`
	Price                             float64   `query:"price" json:"price"`
	PriceDecisionDate                 string    `query:"priceDecisionDate" json:"priceDecisionDate"`
	SaleQty                           int64     `query:"saleQty" json:"saleQty"`
	SaleAmt                           float64   `query:"saleAmt" json:"saleAmt"`
	EventAutoDiscountAmt              float64   `query:"eventAutoDiscountAmt" json:"eventAutoDiscountAmt"`
	EventDecisionDiscountAmt          float64   `query:"eventDecisionDiscountAmt" json:"eventDecisionDiscountAmt"`
	SaleEventSaleBaseAmt              float64   `query:"saleEventSaleBaseAmt" json:"saleEventSaleBaseAmt"`
	SaleEventDiscountBaseAmt          float64   `query:"saleEventDiscountBaseAmt" json:"saleEventDiscountBaseAmt"`
	SaleEventNormalSaleRecognitionChk bool      `query:"saleEventNormalSaleRecognitionChk" json:"saleEventNormalSaleRecognitionChk"`
	SaleEventInterShopSalePermitChk   bool      `query:"saleEventInterShopSalePermitChk" json:"saleEventInterShopSalePermitChk"`
	SaleEventAutoDiscountAmt          float64   `query:"saleEventAutoDiscountAmt" json:"saleEventAutoDiscountAmt"`
	SaleEventManualDiscountAmt        float64   `query:"saleEventManualDiscountAmt" json:"saleEventManualDiscountAmt"`
	SaleVentDecisionDiscountAmt       float64   `query:"saleVentDecisionDiscountAmt" json:"saleVentDecisionDiscountAmt"`
	ChinaFISaleAmt                    float64   `query:"chinaFISaleAmt" json:"chinaFISaleAmt"`
	EstimateSaleAmt                   float64   `query:"estimateSaleAmt" json:"estimateSaleAmt"`
	SellingAmt                        float64   `query:"sellingAmt" json:"sellingAmt"`
	NormalFee                         float64   `query:"normalFee" json:"normalFee"`
	SaleEventFee                      float64   `query:"saleEventFee" json:"saleEventFee"`
	ActualSaleAmt                     float64   `query:"actualSaleAmt" json:"actualSaleAmt"`
	UseMileage                        float64   `query:"useMileage" json:"useMileage"`
	PreSaleNo                         string    `query:"preSaleNo" json:"preSaleNo"`
	PreSaleDtSeq                      int64     `query:"preSaleDtSeq" json:"preSaleDtSeq"`
	NormalFeeRate                     float64   `query:"normalFeeRate" json:"normalFeeRate"`
	SaleEventFeeRate                  float64   `query:"saleEventFeeRate" json:"saleEventFeeRate"`
	InUserID                          string    `query:"inUserID" json:"inUserID"`
	InDateTime                        time.Time `query:"inDateTime" json:"inDateTime"`
	ModiUserID                        string    `query:"modiUserID" json:"modiUserID"`
	ModiDateTime                      time.Time `query:"modiDateTime" json:"modiDateTime"`
	SendState                         string    `query:"sendState" json:"sendState"`
	SendFlag                          string    `query:"sendFlag" json:"sendFlag"`
	// SendSeqNo                         int64     `query:"sendSeqNo" json:"sendSeqNo"`
	SendDateTime                    time.Time `query:"sendDateTime" json:"sendDateTime"`
	DiscountAmt                     float64   `query:"discountAmt" json:"discountAmt"`
	DiscountAmtAsCost               float64   `query:"discountAmtAsCost" json:"discountAmtAsCost"`
	UseMileageSettleType            string    `query:"useMileageSettleType" json:"useMileageSettleType"`
	EstimateSaleAmtForConsumer      float64   `query:"estimateSaleAmtForConsumer" json:"estimateSaleAmtForConsumer"`
	SaleEventDiscountAmtForConsumer float64   `query:"saleEventDiscountAmtForConsumer" json:"saleEventDiscountAmtForConsumer"`
	ShopEmpEstimateSaleAmt          float64   `query:"shopEmpEstimateSaleAmt" json:"shopEmpEstimateSaleAmt"`
	PromotionID                     int64     `query:"promotionID" json:"promotionID"`
	TMallEventID                    int64     `query:"tMallEventID" json:"tMallEventID"`
	TMall_ObtainMileage             float64   `query:"tMall_ObtainMileage" json:"tMall_ObtainMileage"`
	SaleOfficeCode                  string    `query:"saleOfficeCode" json:"saleOfficeCode"`
}

type SaleMst struct {
	SaleNo               string    `query:"saleNo" json:"saleNo" xorm:"pk"`
	BrandCode            string    `query:"brandCode" json:"brandCode"`
	ShopCode             string    `query:"shopCode" json:"shopCode"`
	Dates                string    `query:"dates" json:"dates"`
	PosNo                string    `query:"posNo" json:"posNo"`
	SeqNo                int64     `query:"seqNo" json:"seqNo"`
	SaleMode             string    `query:"saleMode" json:"saleMode"`
	CustNo               string    `query:"custNo" json:"custNo"`
	CustCardNo           string    `query:"custCardNo" json:"custCardNo"`
	CustMileagePolicyNo  int64     `query:"custMileagePolicyNo" json:"custMileagePolicyNo"`
	PrimaryCustEventNo   int64     `query:"primaryCustEventNo" json:"primaryCustEventNo"`
	SecondaryCustEventNo int64     `query:"secondaryCustEventNo" json:"secondaryCustEventNo"`
	DepartStoreReceiptNo string    `query:"departStoreReceiptNo" json:"departStoreReceiptNo"`
	SaleQty              int64     `query:"saleQty" json:"saleQty"`
	SaleAmt              float64   `query:"saleAmt" json:"saleAmt"`
	DiscountAmt          float64   `query:"discountAmt" json:"discountAmt"`
	ChinaFISaleAmt       float64   `query:"chinaFISaleAmt" json:"chinaFISaleAmt"`
	EstimateSaleAmt      float64   `query:"estimateSaleAmt" json:"estimateSaleAmt"`
	SellingAmt           float64   `query:"sellingAmt" json:"sellingAmt"`
	FeeAmt               float64   `query:"feeAmt" json:"feeAmt"`
	ActualSaleAmt        float64   `query:"actualSaleAmt" json:"actualSaleAmt"`
	UseMileage           float64   `query:"useMileage" json:"useMileage"`
	ObtainMileage        float64   `query:"obtainMileage" json:"obtainMileage"`
	InUserID             string    `query:"inUserID" json:"inUserID"`
	InDateTime           time.Time `query:"inDateTime" json:"inDateTime"`
	ModiUserID           string    `query:"modiUserID" json:"modiUserID"`
	ModiDateTime         time.Time `query:"modiDateTime" json:"modiDateTime"`
	SendState            string    `query:"sendState" json:"sendState"`
	SendFlag             string    `query:"sendFlag" json:"sendFlag"`
	// SendSeqNo                   int64     `query:"sendSeqNo" json:"sendSeqNo"`
	SendDateTime                time.Time `query:"sendDateTime" json:"sendDateTime"`
	DiscountAmtAsCost           float64   `query:"discountAmtAsCost" json:"discountAmtAsCost"`
	CustDivisionCode            string    `query:"custDivisionCode" json:"custDivisionCode"`
	MileageCustChangeStatusCode string    `query:"mileageCustChangeStatusCode" json:"mileageCustChangeStatusCode"`
	CustGradeCode               string    `query:"custGradeCode" json:"custGradeCode"`
	PreSaleNo                   string    `query:"preSaleNo" json:"preSaleNo"`
	ActualSellingAmt            float64   `query:"actualSellingAmt" json:"actualSellingAmt"`
	EstimateSaleAmtForConsumer  float64   `query:"estimateSaleAmtForConsumer" json:"estimateSaleAmtForConsumer"`
	ShopEmpEstimateSaleAmt      float64   `query:"shopEmpEstimateSaleAmt" json:"shopEmpEstimateSaleAmt"`
	ComplexShopSeqNo            string    `query:"complexShopSeqNo" json:"complexShopSeqNo"`
	CustBrandCode               string    `query:"custBrandCode" json:"custBrandCode"`
	Freight                     float64   `query:"freight" json:"freight"`
	TMall_UseMileage            float64   `query:"tMall_UseMileage" json:"tMall_UseMileage"`
	TMall_ObtainMileage         float64   `query:"tMall_ObtainMileage" json:"tMall_ObtainMileage"`
	SaleOfficeCode              string    `query:"saleOfficeCode" json:"saleOfficeCode" xorm:""`
}

type SalePayment struct {
	SaleNo             string    `query:"saleNo" json:"saleNo" xorm:"pk"`
	SeqNo              int64     `query:"seqNo" json:"seqNo" xorm:"pk"`
	PaymentCode        string    `query:"paymentCode" json:"paymentCode"`
	PaymentAmt         float64   `query:"paymentAmt" json:"paymentAmt"`
	InUserID           string    `query:"inUserID" json:"inUserID"`
	InDateTime         time.Time `query:"inDateTime" json:"inDateTime"`
	ModiUserID         string    `query:"modiUserID" json:"modiUserID"`
	ModiDateTime       time.Time `query:"modiDateTime" json:"modiDateTime"`
	SendState          string    `query:"sendState" json:"sendState"`
	SendFlag           string    `query:"sendFlag" json:"sendFlag"`
	SendSeqNo          int64     `query:"sendSeqNo" json:"sendSeqNo"`
	SendDateTime       time.Time `query:"sendDateTime" json:"sendDateTime"`
	CreditCardFirmCode string    `query:"creditCardFirmCode" json:"creditCardFirmCode"`
}

type SaleMstsAndSaleDtls struct {
	SaleMsts []SaleMst `query:"saleMsts" json:"saleMsts" `
	SaleDtls []SaleDtl `query:"saleDtls" json:"saleDtls" `
}

type ResultShop struct {
	Success bool `json:"success"`
	Result  []struct {
		Id   int64  `json:"id"`
		Code string `json:"code"`
	} `json:"result"`
	Error struct{} `json:"error"`
}

type ResultToken struct {
	Success bool `json:"success"`
	Result  struct {
		Token string `json:"token"`
	}
	Error struct{} `json:"error"`
}

type RequestTokenBody struct {
	UserName string `json:"userName"`
	PassWord string `json:"passWord"`
}

// func (SaleMst) GetShopCode(ctx context.Context, storeId int64, token string) (string, error) {
// 	resultShop := ResultShop{}
// 	if _, err := httpreq.New(http.MethodGet, config.Config().Services.PlaceManagementApi+"/outside/v1/store/getbystoreids?ids="+strconv.FormatInt(storeId, 10), nil).
// 		WithBehaviorLogContext(behaviorlog.FromCtx(ctx)).WithToken(token).
// 		Call(&resultShop); err != nil {
// 		return "", err
// 	}
// 	if len(resultShop.Result) != 0 {
// 		if shopCode := resultShop.Result[0].Code; shopCode != "" {
// 			return shopCode, nil
// 		}
// 	}
// 	return "", errors.New("Request PlaceManagementApi failed : Get shopCode error")
// }

// func (SaleMst) GetToken(ctx context.Context) (string, error) {
// 	resultToken := ResultToken{}
// 	body := RequestTokenBody{
// 		UserName: config.Config().GetTokenUser.UserName,
// 		PassWord: config.Config().GetTokenUser.Password,
// 	}
// 	if _, err := httpreq.New(http.MethodPost, config.Config().Services.GetTokenApi, body).
// 		WithBehaviorLogContext(behaviorlog.FromCtx(ctx)).
// 		Call(&resultToken); err != nil {
// 		return "", err
// 	}
// 	if resultToken.Success && resultToken.Result.Token != "" {
// 		return resultToken.Result.Token, nil
// 	}
// 	return "", errors.New("Get token error!")
// }

func (SaleMst) GetlastSeq(shopCode, saleDate string) (string, error) {
	var saleNos []string
	if err := factory.GetCSLEngine().
		Table("dbo.SaleMst").
		Select("SaleNo").
		Where("shopCode = ?", shopCode).
		And("dates = ?", saleDate).
		Desc("SaleNo").Find(&saleNos); err != nil {
		return "", err
	}
	if len(saleNos) != 0 {
		return saleNos[0], nil
	}
	return "", nil
}

func (SaleMst) GetMileage(customerId int64) (*PostMileage, error) {
	var mileage PostMileage
	exist, err := factory.GetSrEngine().Where("customer_id = ?", customerId).Get(&mileage)
	if err != nil {
		return nil, err
	} else if !exist {
		logrus.WithFields(logrus.Fields{
			"customer_id": customerId,
		}).Error("Fail to get Mileage")
		return nil, errors.New("Mileage is not exist")
	}
	return &mileage, nil
}

func (SaleMst) GetSequenceNumber(seq int, str string) (string, int, string, error) {
	startStrs := []string{"A", "B", "C", "D", "E", "F", "G"}
	nextSeq := seq + 1
	strSeq := strconv.Itoa(seq)

	switch {
	//str not null , seq 最大999  so 当str不是空 seq < 999的情况
	case str != "" && seq <= 999:
	BreakA:
		if len(strSeq) != 3 {
			strSeq = "0" + strSeq
			goto BreakA
		}
		return str + strSeq, nextSeq, str, nil
	//seq == 999 && str != "" 下次循环将从str的下一个，seq从1开始
	case str != "" && seq == 1000:
		for i, _ := range startStrs {
			if str == startStrs[i] {
				str = startStrs[i+1]
				break
			}
		}
		return str + "001", 2, str, nil
	//str 为空
	case str == "" && seq <= 9999:
	BreakB:
		if len(strSeq) != 4 {
			strSeq = "0" + strSeq
			goto BreakB
		}
		return strSeq, nextSeq, str, nil
	//str 为空  seq等于9999  下次加前缀，从开始，seq 从1 开始
	case str == "" && seq == 10000:
		str = startStrs[0]
		return str + "001", 2, str, nil
	}
	return "", 0, "", errors.New("GetSequenceNumber EROR")
}

func (SaleMst) GetSeqAndStartStr(lastSeq string) (int, string, error) {
	startStrs := []string{"A", "B", "C", "D", "E", "F", "G"}
	var startStr string
	var seq int
	if lastSeq != "" {
		lastFour := lastSeq[len(lastSeq)-4 : len(lastSeq)]
		for i, _ := range startStrs {
			if strings.HasPrefix(lastFour, startStrs[i]) {
				lastThree := lastFour[len(lastFour)-3 : len(lastFour)]
				intLastThree, err := strconv.Atoi(lastThree)
				if err != nil {
					return 0, "", err
				}
				if intLastThree != 999 {
					seq = intLastThree + 1
					startStr = startStrs[i]
				} else {
					seq = 1
					startStr = startStrs[i+1]
				}
				return seq, startStr, nil
			}
		}
		intLastFour, err := strconv.Atoi(lastFour)
		if err != nil {
			return 0, "", err
		}
		if intLastFour < 9999 {
			seq = intLastFour + 1
		} else {
			seq = 1
			startStr = startStrs[0]
		}
		return seq, startStr, nil
	}
	seq = 1
	return seq, startStr, nil
}
