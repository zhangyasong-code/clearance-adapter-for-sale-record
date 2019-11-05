package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type SaleDtl struct {
	SaleNo                            string         `query:"saleNo" json:"saleNo" xorm:"pk"`
	DtSeq                             int64          `query:"dtSeq" json:"dtSeq" xorm:"pk"`
	BrandCode                         string         `query:"brandCode" json:"brandCode"`
	ShopCode                          string         `query:"shopCode" json:"shopCode"`
	Dates                             string         `query:"dates" json:"dates"`
	PosNo                             string         `query:"posNo" json:"posNo"`
	SeqNo                             int64          `query:"seqNo" json:"seqNo"`
	NormalSaleTypeCode                string         `query:"normalSaleTypeCode" json:"normalSaleTypeCode"`
	CustMileagePolicyNo               sql.NullInt64  `query:"custMileagePolicyNo" json:"custMileagePolicyNo"`
	PrimaryCustEventNo                sql.NullInt64  `query:"primaryCustEventNo" json:"primaryCustEventNo"`
	PrimaryEventTypeCode              sql.NullString `query:"primaryEventTypeCode" json:"primaryEventTypeCode"`
	PrimaryEventSettleTypeCode        sql.NullString `query:"primaryEventSettleTypeCode" json:"primaryEventSettleTypeCode"`
	SecondaryCustEventNo              sql.NullInt64  `query:"secondaryCustEventNo" json:"secondaryCustEventNo"`
	SecondaryEventTypeCode            sql.NullString `query:"secondaryEventTypeCode" json:"secondaryEventTypeCode"`
	SecondaryEventSettleTypeCode      sql.NullString `query:"secondaryEventSettleTypeCode" json:"secondaryEventSettleTypeCode"`
	SaleEventNo                       sql.NullInt64  `query:"saleEventNo" json:"saleEventNo"`
	SaleEventTypeCode                 sql.NullString `query:"saleEventTypeCode" json:"saleEventTypeCode"`
	SaleReturnReasonCode              sql.NullString `query:"saleReturnReasonCode" json:"saleReturnReasonCode"`
	ProdCode                          string         `query:"prodCode" json:"prodCode"`
	EANCode                           string         `query:"eANCode" json:"eANCode"`
	PriceTypeCode                     string         `query:"priceTypeCode" json:"priceTypeCode"`
	SupGroupCode                      string         `query:"supGroupCode" json:"supGroupCode"`
	SaipType                          string         `query:"saipType" json:"saipType"`
	NormalPrice                       float64        `query:"normalPrice" json:"normalPrice"`
	Price                             float64        `query:"price" json:"price"`
	PriceDecisionDate                 string         `query:"priceDecisionDate" json:"priceDecisionDate"`
	SaleQty                           int64          `query:"saleQty" json:"saleQty"`
	SaleAmt                           float64        `query:"saleAmt" json:"saleAmt"`
	EventAutoDiscountAmt              float64        `query:"eventAutoDiscountAmt" json:"eventAutoDiscountAmt"`
	EventDecisionDiscountAmt          float64        `query:"eventDecisionDiscountAmt" json:"eventDecisionDiscountAmt"`
	SaleEventSaleBaseAmt              float64        `query:"saleEventSaleBaseAmt" json:"saleEventSaleBaseAmt"`
	SaleEventDiscountBaseAmt          float64        `query:"saleEventDiscountBaseAmt" json:"saleEventDiscountBaseAmt"`
	SaleEventNormalSaleRecognitionChk bool           `query:"saleEventNormalSaleRecognitionChk" json:"saleEventNormalSaleRecognitionChk"`
	SaleEventInterShopSalePermitChk   bool           `query:"saleEventInterShopSalePermitChk" json:"saleEventInterShopSalePermitChk"`
	SaleEventAutoDiscountAmt          float64        `query:"saleEventAutoDiscountAmt" json:"saleEventAutoDiscountAmt"`
	SaleEventManualDiscountAmt        float64        `query:"saleEventManualDiscountAmt" json:"saleEventManualDiscountAmt"`
	SaleVentDecisionDiscountAmt       float64        `query:"saleVentDecisionDiscountAmt" json:"saleVentDecisionDiscountAmt"`
	ChinaFISaleAmt                    float64        `query:"chinaFISaleAmt" json:"chinaFISaleAmt"`
	EstimateSaleAmt                   float64        `query:"estimateSaleAmt" json:"estimateSaleAmt"`
	SellingAmt                        float64        `query:"sellingAmt" json:"sellingAmt"`
	NormalFee                         float64        `query:"normalFee" json:"normalFee"`
	SaleEventFee                      float64        `query:"saleEventFee" json:"saleEventFee"`
	ActualSaleAmt                     float64        `query:"actualSaleAmt" json:"actualSaleAmt"`
	UseMileage                        float64        `query:"useMileage" json:"useMileage"`
	PreSaleNo                         sql.NullString `query:"preSaleNo" json:"preSaleNo"`
	PreSaleDtSeq                      sql.NullInt64  `query:"preSaleDtSeq" json:"preSaleDtSeq"`
	NormalFeeRate                     float64        `query:"normalFeeRate" json:"normalFeeRate"`
	SaleEventFeeRate                  float64        `query:"saleEventFeeRate" json:"saleEventFeeRate"`
	InUserID                          string         `query:"inUserID" json:"inUserID"`
	InDateTime                        time.Time      `query:"inDateTime" json:"inDateTime"`
	ModiUserID                        string         `query:"modiUserID" json:"modiUserID"`
	ModiDateTime                      time.Time      `query:"modiDateTime" json:"modiDateTime"`
	SendState                         string         `query:"sendState" json:"sendState"`
	SendFlag                          string         `query:"sendFlag" json:"sendFlag"`
	// SendSeqNo                         int64     `query:"sendSeqNo" json:"sendSeqNo"`
	SendDateTime                    time.Time       `query:"sendDateTime" json:"sendDateTime"`
	DiscountAmt                     float64         `query:"discountAmt" json:"discountAmt"`
	DiscountAmtAsCost               float64         `query:"discountAmtAsCost" json:"discountAmtAsCost"`
	UseMileageSettleType            string          `query:"useMileageSettleType" json:"useMileageSettleType"`
	EstimateSaleAmtForConsumer      float64         `query:"estimateSaleAmtForConsumer" json:"estimateSaleAmtForConsumer"`
	SaleEventDiscountAmtForConsumer float64         `query:"saleEventDiscountAmtForConsumer" json:"saleEventDiscountAmtForConsumer"`
	ShopEmpEstimateSaleAmt          float64         `query:"shopEmpEstimateSaleAmt" json:"shopEmpEstimateSaleAmt"`
	PromotionID                     sql.NullInt64   `query:"promotionID" json:"promotionID"`
	TMallEventID                    sql.NullInt64   `query:"tMallEventID" json:"tMallEventID"`
	TMall_ObtainMileage             sql.NullFloat64 `query:"tMall_ObtainMileage" json:"tMall_ObtainMileage"`
	SaleOfficeCode                  string          `query:"saleOfficeCode" json:"saleOfficeCode"`
	OrderItemId                     int64           `json:"orderItemId" xorm:"-"`
	RefundItemId                    int64           `json:"refundItemId" xorm:"-"`
	TransactionDtlId                int64           `json:"transactionDtlId" xorm:"-"`
	StyleCode                       string          `json:"styleCode" xorm:"-"`
}

type SaleMst struct {
	SaleNo               string         `query:"saleNo" json:"saleNo" xorm:"pk"`
	BrandCode            string         `query:"brandCode" json:"brandCode"`
	ShopCode             string         `query:"shopCode" json:"shopCode"`
	Dates                string         `query:"dates" json:"dates"`
	PosNo                string         `query:"posNo" json:"posNo"`
	SeqNo                int64          `query:"seqNo" json:"seqNo"`
	SaleMode             string         `query:"saleMode" json:"saleMode"`
	CustNo               sql.NullString `query:"custNo" json:"custNo"`
	CustCardNo           sql.NullString `query:"custCardNo" json:"custCardNo"`
	CustMileagePolicyNo  sql.NullInt64  `query:"custMileagePolicyNo" json:"custMileagePolicyNo"`
	PrimaryCustEventNo   sql.NullInt64  `query:"primaryCustEventNo" json:"primaryCustEventNo"`
	SecondaryCustEventNo sql.NullInt64  `query:"secondaryCustEventNo" json:"secondaryCustEventNo"`
	DepartStoreReceiptNo string         `query:"departStoreReceiptNo" json:"departStoreReceiptNo"`
	SaleQty              int64          `query:"saleQty" json:"saleQty"`
	SaleAmt              float64        `query:"saleAmt" json:"saleAmt"`
	DiscountAmt          float64        `query:"discountAmt" json:"discountAmt"`
	ChinaFISaleAmt       float64        `query:"chinaFISaleAmt" json:"chinaFISaleAmt"`
	EstimateSaleAmt      float64        `query:"estimateSaleAmt" json:"estimateSaleAmt"`
	SellingAmt           float64        `query:"sellingAmt" json:"sellingAmt"`
	FeeAmt               float64        `query:"feeAmt" json:"feeAmt"`
	ActualSaleAmt        float64        `query:"actualSaleAmt" json:"actualSaleAmt"`
	UseMileage           float64        `query:"useMileage" json:"useMileage"`
	ObtainMileage        float64        `query:"obtainMileage" json:"obtainMileage"`
	InUserID             string         `query:"inUserID" json:"inUserID"`
	InDateTime           time.Time      `query:"inDateTime" json:"inDateTime"`
	ModiUserID           string         `query:"modiUserID" json:"modiUserID"`
	ModiDateTime         time.Time      `query:"modiDateTime" json:"modiDateTime"`
	SendState            string         `query:"sendState" json:"sendState"`
	SendFlag             string         `query:"sendFlag" json:"sendFlag"`
	// SendSeqNo                   int64     `query:"sendSeqNo" json:"sendSeqNo"`
	SendDateTime                time.Time       `query:"sendDateTime" json:"sendDateTime"`
	DiscountAmtAsCost           float64         `query:"discountAmtAsCost" json:"discountAmtAsCost"`
	CustDivisionCode            sql.NullString  `query:"custDivisionCode" json:"custDivisionCode"`
	MileageCustChangeStatusCode sql.NullString  `query:"mileageCustChangeStatusCode" json:"mileageCustChangeStatusCode"`
	CustGradeCode               sql.NullString  `query:"custGradeCode" json:"custGradeCode"`
	PreSaleNo                   sql.NullString  `query:"preSaleNo" json:"preSaleNo"`
	ActualSellingAmt            float64         `query:"actualSellingAmt" json:"actualSellingAmt"`
	EstimateSaleAmtForConsumer  float64         `query:"estimateSaleAmtForConsumer" json:"estimateSaleAmtForConsumer"`
	ShopEmpEstimateSaleAmt      float64         `query:"shopEmpEstimateSaleAmt" json:"shopEmpEstimateSaleAmt"`
	ComplexShopSeqNo            sql.NullString  `query:"complexShopSeqNo" json:"complexShopSeqNo"`
	CustBrandCode               string          `query:"custBrandCode" json:"custBrandCode"`
	Freight                     sql.NullFloat64 `query:"freight" json:"freight"`
	TMall_UseMileage            sql.NullFloat64 `query:"tMall_UseMileage" json:"tMall_UseMileage"`
	TMall_ObtainMileage         sql.NullFloat64 `query:"tMall_ObtainMileage" json:"tMall_ObtainMileage"`
	SaleOfficeCode              string          `query:"saleOfficeCode" json:"saleOfficeCode"`
	TransactionId               int64           `json:"transactionId" xorm:"-"`
	StoreId                     int64           `json:"storeId" xorm:"-"`
	OrderId                     int64           `json:"orderId" xorm:"-"`
}

type SalePayment struct {
	SaleNo       string    `query:"saleNo" json:"saleNo" xorm:"pk"`
	SeqNo        int64     `query:"seqNo" json:"seqNo" xorm:"pk"`
	PaymentCode  string    `query:"paymentCode" json:"paymentCode"`
	PaymentAmt   float64   `query:"paymentAmt" json:"paymentAmt"`
	InUserID     string    `query:"inUserID" json:"inUserID"`
	InDateTime   time.Time `query:"inDateTime" json:"inDateTime"`
	ModiUserID   string    `query:"modiUserID" json:"modiUserID"`
	ModiDateTime time.Time `query:"modiDateTime" json:"modiDateTime"`
	// SendState    sql.NullString `query:"sendState" json:"sendState"`
	SendFlag string `query:"sendFlag" json:"sendFlag"`
	// SendSeqNo          int64          `query:"sendSeqNo" json:"sendSeqNo"`
	SendDateTime       time.Time      `query:"sendDateTime" json:"sendDateTime"`
	CreditCardFirmCode sql.NullString `query:"creditCardFirmCode" json:"creditCardFirmCode"`
	TransactionId      int64          `json:"transactionId" xorm:"-"`
}

type CustMileagePolicy struct {
	CustMileagePolicyNo int64  `json:"custMileagePolicyNo"`
	BrandCode           string `json:"brandCode"`
}

type StaffSaleRecord struct {
	Dates      string    `json:"dates"`
	HREmpNo    string    `json:"hREmpNo"`
	SaleNo     string    `json:"saleNo"`
	BrandCode  string    `json:"brandCode"`
	ShopCode   string    `json:"shopCode"`
	InUserID   string    `json:"inUserID"`
	InDateTime time.Time `json:"inDateTime"`
}

type SaleMstsAndSaleDtls struct {
	SaleMsts         []SaleMst         `json:"saleMsts"`
	SaleDtls         []SaleDtl         `json:"saleDtls"`
	SalePayments     []SalePayment     `json:"salePayments"`
	StaffSaleRecords []StaffSaleRecord `json:"staffSaleRecords"`
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

func (SaleMst) GetPriceTypeCode(brandCode, productCode string) (string, error) {
	var priceTypeCodes []string
	if err := factory.GetCSLEngine().Table("dbo.BrandPrice").
		Select("PriceTypeCode").Distinct("PriceTypeCode").
		Where("BrandCode = ?", brandCode).
		And("StyleCode = ?", productCode).Find(&priceTypeCodes); err != nil {
		return "", err
	}
	if len(priceTypeCodes) != 0 {
		return priceTypeCodes[0], nil
	} else {
		logrus.WithFields(logrus.Fields{
			"brandCode":   brandCode,
			"productCode": productCode,
		}).Error("Fail to GetPriceTypeCode")
		return "", errors.New("GetPriceTypeCode error!")
	}
}

func (SaleMst) GetSupGroupCode(brandCode, productCode string) (string, error) {
	var SupGroupCodes []string
	if err := factory.GetCSLEngine().Table("dbo.Style").
		Select("SupGroupCode").Distinct("SupGroupCode").
		Where("BrandCode = ?", brandCode).
		And("StyleCode = ?", productCode).Find(&SupGroupCodes); err != nil {
		return "", err
	}
	if len(SupGroupCodes) != 0 {
		return SupGroupCodes[0], nil
	} else {
		logrus.WithFields(logrus.Fields{
			"brandCode":   brandCode,
			"productCode": productCode,
		}).Error("Fail to GetSupGroupCode")
		return "", errors.New("GetSupGroupCode error!")
	}
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

func (SaleMst) CheckShop(brandCode, shopCode string) error {
	engine := factory.GetCSLEngine()
	row, err := engine.Query(`EXEC up_CSLK_IF_PPPos_CHECK_Shop @StoreGroupCode = ?,@StoreCode = ?`, brandCode, shopCode)
	if err != nil {
		return err
	}

	//"PPPos100" -- true
	resultText := string((row[0])[""])
	if resultText != "PPPos100" {
		return errors.New("Shop is not exist!")
	}
	return nil
}

func (SaleMst) CheckStock(brandCode, shopCode, prodCode, styleCode string) error {
	engine := factory.GetCSLEngine()
	row, err := engine.Query(`EXEC up_MSL_SaleUpload_CheckMinusStock 
	@BrandCode = ?,
	@ShopCode = ?,
	@ProdCode = ?, 
	@StyleCode = ?`, brandCode, shopCode, prodCode, styleCode)
	if err != nil {
		return err
	}

	// PPPos101-负库存不允许销售
	// PPPos000-商品库存为正，允许销售
	// PPPos100-商品不存在
	resultText := string((row[0])["ResultCode"])
	switch resultText {
	case "PPPos000":
		return nil
	case "PPPos101":
		return errors.New("负库存不允许销售!")
	case "PPPos100":
		return errors.New("商品不存在！")
	}
	return nil
}

func (CustMileagePolicy) GetCustMileagePolicy(brandCode string) (CustMileagePolicy, error) {
	custMileagePolicy := CustMileagePolicy{}
	if _, err := factory.GetCSLEngine().Table("dbo.CustMileagePolicy").
		Where("BrandCode = ?", brandCode).
		And("GETDATE() BETWEEN purchasestartdate AND purchaseenddate").
		And("UseChk = 1").
		Get(&custMileagePolicy); err != nil {
		return CustMileagePolicy{}, err
	}
	return custMileagePolicy, nil
}
