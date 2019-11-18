package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	"xorm.io/core"

	"github.com/go-xorm/xorm"
	"github.com/pangpanglabs/goutils/number"
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
	OrderItemId                     int64           `json:"-" xorm:"-"`
	RefundItemId                    int64           `json:"-" xorm:"-"`
	TransactionDtlId                int64           `json:"-" xorm:"-"`
	StyleCode                       string          `json:"-" xorm:"-"`
	SaleTransactionId               int64           `json:"-" xorm:"-"`
	SaleTransactionDtlId            int64           `json:"-" xorm:"-"`
	TransactionId                   int64           `json:"-" xorm:"-"`
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
	SendDateTime                time.Time         `query:"sendDateTime" json:"sendDateTime"`
	DiscountAmtAsCost           float64           `query:"discountAmtAsCost" json:"discountAmtAsCost"`
	CustDivisionCode            sql.NullString    `query:"custDivisionCode" json:"custDivisionCode"`
	MileageCustChangeStatusCode sql.NullString    `query:"mileageCustChangeStatusCode" json:"mileageCustChangeStatusCode"`
	CustGradeCode               sql.NullString    `query:"custGradeCode" json:"custGradeCode"`
	PreSaleNo                   sql.NullString    `query:"preSaleNo" json:"preSaleNo"`
	ActualSellingAmt            float64           `query:"actualSellingAmt" json:"actualSellingAmt"`
	EstimateSaleAmtForConsumer  float64           `query:"estimateSaleAmtForConsumer" json:"estimateSaleAmtForConsumer"`
	ShopEmpEstimateSaleAmt      float64           `query:"shopEmpEstimateSaleAmt" json:"shopEmpEstimateSaleAmt"`
	ComplexShopSeqNo            sql.NullString    `query:"complexShopSeqNo" json:"complexShopSeqNo"`
	CustBrandCode               string            `query:"custBrandCode" json:"custBrandCode"`
	Freight                     sql.NullFloat64   `query:"freight" json:"freight"`
	TMall_UseMileage            sql.NullFloat64   `query:"tMall_UseMileage" json:"tMall_UseMileage"`
	TMall_ObtainMileage         sql.NullFloat64   `query:"tMall_ObtainMileage" json:"tMall_ObtainMileage"`
	SaleOfficeCode              string            `query:"saleOfficeCode" json:"saleOfficeCode"`
	TransactionId               int64             `json:"-" xorm:"-"`
	StoreId                     int64             `json:"-" xorm:"-"`
	OrderId                     int64             `json:"-" xorm:"-"`
	RefundId                    int64             `json:"-" xorm:"-"`
	SaleTransactionId           int64             `json:"-" xorm:"-"`
	SaleDtls                    []SaleDtl         `json:"saleDtls" xorm:"-"`
	SalePayments                []SalePayment     `json:"salePayments" xorm:"-"`
	StaffSaleRecords            []StaffSaleRecord `json:"staffSaleRecords" xorm:"-"`
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
	TransactionId      int64          `json:"-" xorm:"-"`
	SaleTransactionId  int64          `json:"-" xorm:"-"`
}

type CustMileagePolicy struct {
	CustMileagePolicyNo int64  `json:"custMileagePolicyNo"`
	BrandCode           string `json:"brandCode"`
}

type StaffSaleRecord struct {
	Dates             string    `json:"dates"`
	HREmpNo           string    `json:"hREmpNo"`
	SaleNo            string    `json:"saleNo"`
	BrandCode         string    `json:"brandCode"`
	ShopCode          string    `json:"shopCode"`
	InUserID          string    `json:"inUserID"`
	InDateTime        time.Time `json:"inDateTime"`
	SaleTransactionId int64     `json:"-" xorm:"-"`
	TransactionId     int64     `json:"-" xorm:"-"`
}

type SaleMstsAndSaleDtls struct {
	SaleMsts         []SaleMst         `json:"saleMsts"`
	SaleDtls         []SaleDtl         `json:"saleDtls"`
	SalePayments     []SalePayment     `json:"salePayments"`
	StaffSaleRecords []StaffSaleRecord `json:"staffSaleRecords"`
}

type TargetReturnSale struct {
	SaleDate             string         `query:"saleDate" json:"saleDate"`
	SaleNo               string         `query:"saleNo" json:"saleNo"`
	SaleDtlSeqNo         int64          `query:"saleDtlSeqNo" json:"saleDtlSeqNo,omitempty"`
	CustomerNo           sql.NullString `query:"customerNo" json:"customerNo"`
	CustomerName         sql.NullString `query:"customerName" json:"customerName"`
	CustomerCardNo       sql.NullString `query:"customerCardNo" json:"customerCardNo"`
	DepartStoreReceiptNo string         `query:"departStoreReceiptNo" json:"departStoreReceiptNo"`
	NormalSaleTypeName   string         `query:"normalSaleTypeName" json:"normalSaleTypeName"`
	BrandCode            string         `query:"brandCode" json:"brandCode,omitempty"`
	ShopCode             string         `query:"styleCode" json:"styleCode,omitempty"`
	StyleCode            string         `query:"shopCode" json:"shopCode,omitempty"`
	ColorName            string         `query:"colorName" json:"colorName,omitempty"`
	SizeCode             string         `query:"sizeCode" json:"sizeCode,omitempty"`
	ProdCode             string         `query:"prodCode" json:"prodCode,omitempty"`
	ProdName             string         `query:"prodName" json:"prodName,omitempty"`
	SalePrice            float64        `query:"salePrice" json:"salePrice"`
	SaleQty              int64          `query:"saleQty" json:"saleQty"`
	SaleAmt              float64        `query:"saleAmt" json:"saleAmt"`
	SellingAmt           float64        `query:"sellingAmt" json:"sellingAmt"`
	DiscountAmt          float64        `query:"discountAmt" json:"discountAmt"`
	OperatorName         string         `query:"operatorName" json:"operatorName"`
	OperationDate        string         `query:"operationDate" json:"operationDate"`
	OldShopSaleChk       bool           `query:"oldShopSaleChk" json:"oldShopSaleChk,omitempty"`
	CustBrandCode        sql.NullString `query:"custBrandCode" json:"custBrandCode"`
	RefundedQty          int64          `query:"refundedQty" json:"refundedQty"`
	InUserID             string         `query:"inUserID" json:"inUserID"`
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
	AppId        string `json:"appId"`
	AppSecretKey string `json:"appSecretKey"`
}

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
	engine := factory.GetCSLEngine()
	engine.SetMapper(core.SameMapper{})

	if _, err := engine.Table("dbo.CustMileagePolicy").
		Where("BrandCode = ?", brandCode).
		And("GETDATE() BETWEEN purchasestartdate AND purchaseenddate").
		And("UseChk = 1").
		Get(&custMileagePolicy); err != nil {
		return CustMileagePolicy{}, err
	}
	return custMileagePolicy, nil
}

func (SaleMst) GetCslSaleDetailForReturn(brandCode, shopCode, startSaleDate, endSaleDate, saleNo, deptStoreReceiptNo, customerNo, productCode string) (interface{}, error) {
	var targetReturnSales []TargetReturnSale
	var targetReturnDtailSaleMap []map[string][]byte
	engine := factory.GetCSLEngine()
	targetReturnDtailSaleMap, err := engine.Query(`select  
	A.Dates          				AS Dates            
    , A.SaleNo    					AS SaleNo                               
    , B.DtSeq 						AS DtSeq                              
    , A.CustNo 						AS CustomerNo                            
    --, C.CustName 					AS CustomerName                    
    , A.CustCardNo 					AS CustomerCardNo                
    , A.DepartStoreReceiptNo 		AS DepartStoreReceiptNo            
	, D.NormalSaleTypeName 			AS NormalSaleTypeName
	, B.BrandCode   				AS BrandCode 
	, B.ShopCode   					AS ShopCode      
    , C.StyleCode 					AS StyleCode                             
    , C.ColorName 					AS ColorName                         
    , C.SizeCode 					AS SizeCode                     
	, C.ProdCode 					AS ProdCode 
	, C.ProdName 					AS ProdName             
    , B.Price 						AS Price                                  
    , B.SaleQty 					AS SaleQty                                
	, B.SaleAmt 					AS SaleAmt
	, B.SellingAmt     				AS SellingAmt                                   
    , B.SaleAmt - B.SellingAmt 	 	AS DiscountAmt                    
	, B.InDateTime 					AS OperationDate     
	, 0 							AS OldShopSaleChk 
	, B.InUserID 					AS InUserID
    , CASE WHEN A.CustNo IS NULL THEN NULL ELSE  A.CustBrandCode END AS CustBrandCode  
		from salemst A
		inner join saledtl b 
		on A.saleno=b.saleno
		inner join product c
		on b.prodcode=c.prodcode
		inner JOIN NormalSaleType AS D ON b.NormalSaleTypeCode = D.NormalSaleTypeCode   
		where A.saleno = ?`, saleNo)
	if err != nil {
		return nil, err
	}
	for _, value := range targetReturnDtailSaleMap {
		var targetReturnSale TargetReturnSale
		targetReturnSale.SaleNo = string(value["SaleNo"])
		targetReturnSale.SaleDate = string(value["Dates"])
		targetReturnSale.SaleDtlSeqNo, _ = strconv.ParseInt(string(value["DtSeq"]), 10, 64)
		targetReturnSale.CustomerNo.String = string(value["CustomerNo"])
		targetReturnSale.CustomerName.String = string(value["CustomerName"])
		targetReturnSale.CustomerCardNo.String = string(value["CustomerCardNo"])
		targetReturnSale.DepartStoreReceiptNo = string(value["DepartStoreReceiptNo"])
		targetReturnSale.NormalSaleTypeName = string(value["NormalSaleTypeName"])
		targetReturnSale.StyleCode = string(value["StyleCode"])
		targetReturnSale.ColorName = string(value["ColorName"])
		targetReturnSale.ProdCode = string(value["ProdCode"])
		targetReturnSale.ProdName = string(value["ProdName"])
		salePrice, _ := strconv.ParseFloat(string(value["SalePrice"]), 64)
		targetReturnSale.SalePrice = number.ToFixed(salePrice, nil)
		targetReturnSale.SaleQty, _ = strconv.ParseInt(string(value["SaleQty"]), 10, 64)
		saleAmt, _ := strconv.ParseFloat(string(value["SaleAmt"]), 64)
		targetReturnSale.SaleAmt = number.ToFixed(saleAmt, nil)
		discountAmt, _ := strconv.ParseFloat(string(value["DiscountAmt"]), 64)
		targetReturnSale.DiscountAmt = number.ToFixed(discountAmt, nil)
		sellingAmt, _ := strconv.ParseFloat(string(value["SellingAmt"]), 64)
		targetReturnSale.SellingAmt = number.ToFixed(sellingAmt, nil)
		targetReturnSale.OperatorName = string(value["OperatorName"])
		targetReturnSale.OperationDate = string(value["OperationDate"])
		//targetReturnSale.OldShopSaleChk = bool(value["OldShopSaleChk"])
		targetReturnSale.CustBrandCode.String = string(value["CustBrandCode"])
		targetReturnSale.InUserID = string(value["InUserID"])
		targetReturnSales = append(targetReturnSales, targetReturnSale)
	}
	if len(targetReturnSales) > 0 && targetReturnSales[0].SaleNo != "" {
		SaleIsReturnedMap, err := engine.Query(`select 
		* from saledtl 
		where PreSaleNo=?`,
			targetReturnSales[0].SaleNo)
		for _, saleIsReturned := range SaleIsReturnedMap {
			for key, targetReturnSale := range targetReturnSales {
				if string(saleIsReturned["ProdCode"]) == targetReturnSale.ProdCode {
					returnedQty, _ := strconv.ParseInt(string(saleIsReturned["SaleQty"]), 10, 64)
					targetReturnSales[key].RefundedQty = returnedQty * -1
				}
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return targetReturnSales, nil
}

func (SaleMst) GetCslSaleForReturn(brandCode, shopCode, startSaleDate, endSaleDate, saleNo, deptStoreReceiptNo, customerNo, productCode string) (interface{}, error) {
	var targetReturnSales []TargetReturnSale
	var targetReturnSaleMap []map[string][]byte
	var has = false
	engine := factory.GetCSLEngine()
	targetReturnSaleMap, err := engine.Query(`EXEC up_CSLK_SMM_SearchTargetReturnSale_SaleDtl_R1 @BrandCode = ?,@ShopCode = ?,@StartSaleDate = ?,@EndSaleDate = ?,@SaleNo = ?,@DeptStoreReceiptNo = ?,@CustomerNo = ?,@ProductCode = ?`,
		brandCode, shopCode, startSaleDate, endSaleDate, saleNo, deptStoreReceiptNo, customerNo, productCode)
	if err != nil {
		return nil, err
	}
	for _, value := range targetReturnSaleMap {
		var targetReturnSale TargetReturnSale
		targetReturnSale.SaleNo = string(value["SaleNo"])
		targetReturnSale.SaleDate = string(value["SaleDate"])
		targetReturnSale.SaleDtlSeqNo, _ = strconv.ParseInt(string(value["SaleDtlSeqNo"]), 10, 64)
		targetReturnSale.CustomerNo.String = string(value["CustomerNo"])
		targetReturnSale.CustomerName.String = string(value["CustomerName"])
		targetReturnSale.CustomerCardNo.String = string(value["CustomerCardNo"])
		targetReturnSale.DepartStoreReceiptNo = string(value["DeptStoreReceiptNo"])
		targetReturnSale.NormalSaleTypeName = string(value["NormalSaleTypeName"])
		targetReturnSale.BrandCode = string(value["BrandCode"])
		targetReturnSale.ShopCode = string(value["ShopCode"])
		targetReturnSale.StyleCode = string(value["StyleCode"])
		targetReturnSale.ColorName = string(value["ColorName"])
		salePrice, _ := strconv.ParseFloat(string(value["SalePrice"]), 64)
		targetReturnSale.SalePrice = number.ToFixed(salePrice, nil)
		targetReturnSale.SaleQty, _ = strconv.ParseInt(string(value["SaleQty"]), 10, 64)
		saleAmt, _ := strconv.ParseFloat(string(value["SaleAmt"]), 64)
		targetReturnSale.SaleAmt = number.ToFixed(saleAmt, nil)
		discountAmt, _ := strconv.ParseFloat(string(value["DiscountAmt"]), 64)
		targetReturnSale.DiscountAmt = number.ToFixed(discountAmt, nil)
		targetReturnSale.OperatorName = string(value["OperatorName"])
		targetReturnSale.OperationDate = string(value["OperationDate"])
		//targetReturnSale.OldShopSaleChk = bool(value["OldShopSaleChk"])
		targetReturnSale.CustBrandCode.String = string(value["CustBrandCode"])
		if len(targetReturnSales) == 0 {
			targetReturnSale.SellingAmt = targetReturnSale.SaleAmt - targetReturnSale.DiscountAmt
			targetReturnSale.SellingAmt = number.ToFixed(targetReturnSale.SellingAmt, nil)
			targetReturnSales = append(targetReturnSales, targetReturnSale)
		} else {
			for key, targetReturnSalefor := range targetReturnSales {
				if targetReturnSalefor.SaleNo == targetReturnSale.SaleNo {
					has = true
					targetReturnSalefor.SaleQty += targetReturnSale.SaleQty
					targetReturnSalefor.SaleAmt += targetReturnSale.SaleAmt
					targetReturnSalefor.DiscountAmt += targetReturnSale.DiscountAmt
					targetReturnSalefor.SellingAmt = targetReturnSalefor.SaleAmt - targetReturnSalefor.DiscountAmt
					targetReturnSalefor.SaleAmt = number.ToFixed(targetReturnSalefor.SaleAmt, nil)
					targetReturnSalefor.DiscountAmt = number.ToFixed(targetReturnSalefor.DiscountAmt, nil)
					targetReturnSalefor.SellingAmt = number.ToFixed(targetReturnSalefor.SellingAmt, nil)
					targetReturnSales[key] = targetReturnSalefor
				}
			}
			if has == false {
				targetReturnSales = append(targetReturnSales, targetReturnSale)
			}
			has = false
		}
	}
	return targetReturnSales, nil
}

func (SaleMst) GetCslSales(ctx context.Context, requestInput RequestInput) (int64, []SaleMst, error) {
	queryBuilder := func() xorm.Interface {
		engine := factory.GetCSLEngine()
		engine.SetMapper(core.SameMapper{})
		q := engine.Where("1=1")
		if len(requestInput.SaleNos) != 0 {
			q.In("SaleNo", requestInput.SaleNos)
		}
		return q
	}
	query := queryBuilder()

	if requestInput.MaxResultCount > 0 {
		query.Limit(requestInput.MaxResultCount, requestInput.SkipCount)
	}

	query.Desc("SaleNo")

	var saleMsts []SaleMst
	totalCount, err := query.FindAndCount(&saleMsts)
	if err != nil {
		return 0, nil, err
	}
	if len(saleMsts) == 0 {
		return 0, nil, nil
	}

	saleDtls, err := SaleDtl{}.GetCslDtlBySaleNos(ctx, requestInput.SaleNos)
	if err != nil {
		return 0, nil, err
	}
	salePayments, err := SalePayment{}.GetCslSalePaymentBySaleNos(ctx, requestInput.SaleNos)
	if err != nil {
		return 0, nil, err
	}
	staffSaleRecords, err := StaffSaleRecord{}.GetCslStaffSaleRecordBySaleNos(ctx, requestInput.SaleNos)
	if err != nil {
		return 0, nil, err
	}

	for i, saleMst := range saleMsts {
		for _, saleDtl := range saleDtls {
			if saleDtl.SaleNo == saleMst.SaleNo {
				saleMsts[i].SaleDtls = append(saleMsts[i].SaleDtls, saleDtl)
			}
		}
		for _, salePayment := range salePayments {
			if salePayment.SaleNo == saleMst.SaleNo {
				saleMsts[i].SalePayments = append(saleMsts[i].SalePayments, salePayment)
			}
		}
		for _, staffSaleRecord := range staffSaleRecords {
			if staffSaleRecord.SaleNo == saleMst.SaleNo {
				saleMsts[i].StaffSaleRecords = append(saleMsts[i].StaffSaleRecords, staffSaleRecord)
			}
		}
	}
	return totalCount, saleMsts, nil
}

func (SaleDtl) GetCslDtlBySaleNos(ctx context.Context, saleNos []string) ([]SaleDtl, error) {
	var saleDtls []SaleDtl
	engine := factory.GetCSLEngine()
	engine.SetMapper(core.SameMapper{})
	if err := engine.Where("1=1").In("SaleNo", saleNos).Find(&saleDtls); err != nil {
		return nil, err
	}
	return saleDtls, nil
}

func (SalePayment) GetCslSalePaymentBySaleNos(ctx context.Context, saleNos []string) ([]SalePayment, error) {
	var salePayments []SalePayment
	engine := factory.GetCSLEngine()
	engine.SetMapper(core.SameMapper{})
	if err := engine.Where("1=1").In("SaleNo", saleNos).Find(&salePayments); err != nil {
		return nil, err
	}
	return salePayments, nil
}

func (StaffSaleRecord) GetCslStaffSaleRecordBySaleNos(ctx context.Context, saleNos []string) ([]StaffSaleRecord, error) {
	var staffSaleRecords []StaffSaleRecord
	engine := factory.GetCSLEngine()
	engine.SetMapper(core.SameMapper{})
	if err := engine.Where("1=1").In("SaleNo", saleNos).Find(&staffSaleRecords); err != nil {
		return nil, err
	}
	return staffSaleRecords, nil
}
