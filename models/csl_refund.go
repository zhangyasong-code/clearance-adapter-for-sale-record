package models

import (
	"clearance/clearance-adapter-for-sale-record/config"
	"clearance/clearance-adapter-for-sale-record/factory"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"xorm.io/core"

	"github.com/pangpanglabs/goutils/behaviorlog"
	"github.com/pangpanglabs/goutils/httpreq"
	"github.com/pangpanglabs/goutils/number"
)

type CslRefundDtl struct {
	Id                    int64   `query:"id" json:"id"`
	SaleMstId             int64   `query:"saleMstId" json:"saleMstId"`
	SaleNo                string  `query:"saleNo" json:"saleNo"`
	PreSaleNo             string  `query:"preSaleNo" json:"preSaleNo"`
	PreSaleDtSeq          int64   `query:"preSaleDtSeq" json:"preSaleDtSeq"`
	SaleDate              string  `query:"saleDate" json:"saleDate"`
	SaleDtlSeqNo          int64   `query:"saleDtlSeqNo" json:"saleDtlSeqNo,omitempty"`
	CustomerNo            string  `query:"customerNo" json:"customerNo"`
	CustomerName          string  `query:"customerName" json:"customerName"`
	CustomerCardNo        string  `query:"customerCardNo" json:"customerCardNo"`
	DepartStoreReceiptNo  string  `query:"departStoreReceiptNo" json:"departStoreReceiptNo"`
	NormalSaleTypeName    string  `query:"normalSaleTypeName" json:"normalSaleTypeName"`
	BrandCode             string  `query:"brandCode" json:"brandCode,omitempty"`
	ShopCode              string  `query:"shopCode" json:"shopCode,omitempty"`
	StyleCode             string  `query:"styleCode" json:"styleCode,omitempty"`
	EanCode               string  `query:"eanCode" json:"eanCode"`
	ColorName             string  `query:"colorName" json:"colorName,omitempty"`
	SizeCode              string  `query:"sizeCode" json:"sizeCode,omitempty"`
	ProdCode              string  `query:"prodCode" json:"prodCode,omitempty"`
	ProdName              string  `query:"prodName" json:"prodName,omitempty"`
	SalePrice             float64 `query:"salePrice" json:"salePrice"`
	SaleQty               int64   `query:"saleQty" json:"saleQty"`
	SaleAmt               float64 `query:"saleAmt" json:"saleAmt"`
	SellingAmt            float64 `query:"sellingAmt" json:"sellingAmt"`
	DiscountAmt           float64 `query:"discountAmt" json:"discountAmt"`
	OperatorName          string  `query:"operatorName" json:"operatorName"`
	OperationDate         string  `query:"operationDate" json:"operationDate"`
	OldShopSaleChk        bool    `query:"oldShopSaleChk" json:"oldShopSaleChk,omitempty"`
	CustBrandCode         string  `query:"custBrandCode" json:"custBrandCode"`
	InUserID              string  `query:"inUserID" json:"inUserID"`
	RefundQty             int64   `query:"refundQty" json:"refundQty"`
	RefundAmt             float64 `query:"refundAmt" json:"refundAmt"`
	UseMileage            float64 `query:"useMileage" json:"useMileage"`
	ObtainMileage         float64 `query:"obtainMileage" json:"obtainMileage"`
	SaleMstSaleAmt        float64 `query:"saleMstSaleAmt" json:"saleMstSaleAmt"`
	SaleMstSaleQty        float64 `query:"saleMstSaleQty" json:"saleMstSaleQty"`
	NormalSaleTypeCode    string  `query:"normalSaleTypeCode" json:"normalSaleTypeCode"`
	SaleEventNo           int64   `query:"saleEventNo" json:"saleEventNo"`
	RefundedQty           int64   `query:"refundedQty" json:"refundedQty"`
	ReturnedAmt           float64 `query:"returnedAmt" json:"returnedAmt"`
	ReturnedDiscountAmt   float64 `query:"returnedDiscountAmt" json:"returnedDiscountAmt"`
	ReturnedSellingAmt    float64 `query:"returnedSellingAmt" json:"returnedSellingAmt"`
	ReturnedUseMileage    float64 `query:"returnedSeMileage" json:"returnedSeMileage"`
	ReturnedObtainMileage float64 `query:"returnedObtainMileage" json:"returnedObtainMileage"`
}

type CslRefundMst struct {
	Id            int64   `query:"id" json:"id"`
	SaleManId     int64   `query:"saleManId" json:"saleManId"`
	PreSaleNo     string  `query:"preSaleNo" json:"preSaleNo"`
	RefundReason  string  `query:"refundReason" json:"refundReason"`
	RefundQty     int64   `query:"refundQty" json:"refundQty"`
	RefundAmt     float64 `query:"refundAmt" json:"refundAmt"`
	UseMileage    float64 `query:"useMileage" json:"useMileage"`
	ObtainMileage float64 `query:"obtainMileage" json:"obtainMileage"`
	SaleAmt       float64 `query:"saleAmt" json:"saleAmt"`
	SaleQty       float64 `query:"saleQty" json:"saleQty"`
	SaleDate      string  `query:"saleDate" json:"saleDate"`
}
type CslRefundInput struct {
	CslRefundDtls []CslRefundDtl `json:"cslRefundDtls"`
	CslRefundMst  CslRefundMst   `json:"cslRefundMst"`
}

func (CslRefundDtl) GetCslSaleDetailForReturn(brandCode, shopCode, startSaleDate, endSaleDate, saleNo, deptStoreReceiptNo, customerNo, productCode string) (interface{}, error) {
	var cslRefundDtls []CslRefundDtl
	var targetReturnDtailSaleMap []map[string][]byte
	fmt.Println("creat engine------->", time.Now())
	engine := factory.GetCSLEngine()
	fmt.Println("select A------->", time.Now())
	targetReturnDtailSaleMap, err := engine.Query(`
	declare @saleNo char(15)
	set @saleNo = cast(? as char(15))
	select  
	A.Dates          				AS Dates            
    , A.SaleNo    					AS SaleNo                               
    , B.DtSeq 						AS DtSeq                              
    , A.CustNo 						AS CustomerNo                            
    --, C.CustName 					AS CustomerName                    
    , A.CustCardNo 					AS CustomerCardNo                
    , A.DepartStoreReceiptNo 		AS DepartStoreReceiptNo            
	, D.NormalSaleTypeName 			AS NormalSaleTypeName
	, D.NormalSaleTypeCode 			AS NormalSaleTypeCode
	, B.BrandCode   				AS BrandCode 
	, B.ShopCode   					AS ShopCode      
    , C.StyleCode 					AS StyleCode                             
    , C.ColorName 					AS ColorName                         
    , C.SizeCode 					AS SizeCode                     
	, C.ProdCode 					AS ProdCode 
	, C.ProdName 					AS ProdName             
    , B.Price 						AS SalePrice                                  
    , B.SaleQty 					AS SaleQty                                
	, B.SaleAmt 					AS SaleAmt
	, B.SellingAmt     				AS SellingAmt                                   
    , B.SaleAmt - B.SellingAmt 	 	AS DiscountAmt                    
	, B.InDateTime 					AS OperationDate     
	, 0 							AS OldShopSaleChk 
	, A.InUserID 					AS InUserID
	, B.UseMileage					AS UseMileage
	, B.SaleEventNo 				AS SaleEventNo
	, A.ObtainMileage 				AS ObtainMileage
	, A.SaleAmt 					AS SaleMstSaleAmt
	, B.EANCode 					AS EANCode
    , CASE WHEN A.CustNo IS NULL THEN NULL ELSE  A.CustBrandCode END AS CustBrandCode  
		from salemst A WITH(NOLOCK)
		inner join saledtl b WITH(NOLOCK)
		on A.saleno=b.saleno
		inner join product c WITH(NOLOCK)
		on b.prodcode=c.prodcode and b.brandcode=c.brandcode
		inner JOIN NormalSaleType AS D WITH(NOLOCK)
		ON b.NormalSaleTypeCode = D.NormalSaleTypeCode   
		where A.saleno = @saleNo`, saleNo)
	if err != nil {
		return nil, err
	}
	fmt.Println("select B------->", time.Now())
	SaleIsReturnedMap, err := engine.Query(`
	declare @saleNo char(15)
	set @saleNo = cast(? as char(15))
	SELECT 
	B.ProdCode 					AS ProdCode 
	,B.SaleQty 					AS SaleQty 
	,B.SaleAmt 					AS SaleAmt 
	,B.DiscountAmt 				AS DiscountAmt 
	,B.SellingAmt 				AS SellingAmt 
	,B.UseMileage 				AS UseMileage 
	FROM SaleMst A WITH(NOLOCK)
	INNER JOIN SaleDtl B WITH(NOLOCK) on A.SaleNo=B.SaleNo
	WHERE A.PreSaleNo= @saleNo`, saleNo)
	if err != nil {
		return nil, err
	}
	fmt.Println("for ------->", time.Now())
	for _, value := range targetReturnDtailSaleMap {
		salePrice, _ := strconv.ParseFloat(string(value["SalePrice"]), 64)
		saleAmt, _ := strconv.ParseFloat(string(value["SaleAmt"]), 64)
		discountAmt, _ := strconv.ParseFloat(string(value["DiscountAmt"]), 64)
		sellingAmt, _ := strconv.ParseFloat(string(value["SellingAmt"]), 64)
		useMileage, _ := strconv.ParseFloat(string(value["UseMileage"]), 64)
		obtainMileage, _ := strconv.ParseFloat(string(value["ObtainMileage"]), 64)
		saleMstSaleAmt, _ := strconv.ParseFloat(string(value["SaleMstSaleAmt"]), 64)
		var cslRefundDtl CslRefundDtl
		cslRefundDtl.SaleNo = string(value["SaleNo"])
		cslRefundDtl.SaleDate = string(value["Dates"])
		cslRefundDtl.SaleDtlSeqNo, _ = strconv.ParseInt(string(value["DtSeq"]), 10, 64)
		cslRefundDtl.CustomerNo = string(value["CustomerNo"])
		cslRefundDtl.CustomerName = string(value["CustomerName"])
		cslRefundDtl.CustomerCardNo = string(value["CustomerCardNo"])
		cslRefundDtl.DepartStoreReceiptNo = string(value["DepartStoreReceiptNo"])
		cslRefundDtl.NormalSaleTypeName = string(value["NormalSaleTypeName"])
		cslRefundDtl.NormalSaleTypeCode = string(value["NormalSaleTypeCode"])
		cslRefundDtl.BrandCode = string(value["BrandCode"])
		cslRefundDtl.ShopCode = string(value["ShopCode"])
		cslRefundDtl.StyleCode = string(value["StyleCode"])
		cslRefundDtl.ColorName = string(value["ColorName"])
		cslRefundDtl.ProdCode = string(value["ProdCode"])
		cslRefundDtl.ProdName = string(value["ProdName"])
		cslRefundDtl.SalePrice = number.ToFixed(salePrice, nil)
		cslRefundDtl.SaleQty, _ = strconv.ParseInt(string(value["SaleQty"]), 10, 64)
		cslRefundDtl.SaleAmt = number.ToFixed(saleAmt, nil)
		cslRefundDtl.DiscountAmt = number.ToFixed(discountAmt, nil)
		cslRefundDtl.SellingAmt = number.ToFixed(sellingAmt, nil)
		cslRefundDtl.UseMileage = number.ToFixed(useMileage, nil)
		cslRefundDtl.ObtainMileage = number.ToFixed(obtainMileage, nil)
		cslRefundDtl.SaleMstSaleAmt = number.ToFixed(saleMstSaleAmt, nil)
		cslRefundDtl.OperatorName = string(value["OperatorName"])
		cslRefundDtl.OperationDate = string(value["OperationDate"])
		cslRefundDtl.CustBrandCode = string(value["CustBrandCode"])
		cslRefundDtl.InUserID = string(value["InUserID"])
		cslRefundDtl.SaleEventNo, _ = strconv.ParseInt(string(value["SaleEventNo"]), 10, 64)
		cslRefundDtl.EanCode = string(value["EANCode"])
		cslRefundDtls = append(cslRefundDtls, cslRefundDtl)
	}
	if len(cslRefundDtls) > 0 && cslRefundDtls[0].SaleNo != "" {
		for key, targetReturnSale := range cslRefundDtls {
			var returnedQtyAll int64
			var returnedAmtAll float64
			var returnedDiscountAmtAll float64
			var returnedSellingAmtAll float64
			var returnedUseMileageAll float64
			var returnedObtainMileageAll float64
			for _, saleIsReturned := range SaleIsReturnedMap {
				if string(saleIsReturned["ProdCode"]) == targetReturnSale.ProdCode {
					returnedQty, _ := strconv.ParseInt(string(saleIsReturned["SaleQty"]), 10, 64)
					returnedAmt, _ := strconv.ParseFloat(string(saleIsReturned["SaleAmt"]), 64)
					returnedDiscountAmt, _ := strconv.ParseFloat(string(saleIsReturned["DiscountAmt"]), 64)
					returnedSellingAmt, _ := strconv.ParseFloat(string(saleIsReturned["SellingAmt"]), 64)
					returnedUesMileage, _ := strconv.ParseFloat(string(saleIsReturned["UseMileage"]), 64)
					returnedObtainMileage, _ := strconv.ParseFloat(string(saleIsReturned["ObtainMileage"]), 64)
					returnedQtyAll += returnedQty
					returnedAmtAll += returnedAmt
					returnedDiscountAmtAll += returnedDiscountAmt
					returnedSellingAmtAll += returnedSellingAmt
					returnedUseMileageAll += returnedUesMileage
					returnedObtainMileageAll += returnedObtainMileage
				}
			}
			cslRefundDtls[key].RefundedQty = returnedQtyAll * -1
			cslRefundDtls[key].ReturnedAmt = number.ToFixed(returnedAmtAll, nil) * -1
			cslRefundDtls[key].ReturnedDiscountAmt = number.ToFixed(returnedDiscountAmtAll, nil) * -1
			cslRefundDtls[key].ReturnedSellingAmt = number.ToFixed(returnedSellingAmtAll, nil) * -1
			cslRefundDtls[key].ReturnedUseMileage = number.ToFixed(returnedUseMileageAll, nil) * -1
			cslRefundDtls[key].ReturnedObtainMileage = number.ToFixed(returnedObtainMileageAll, nil) * -1
		}
	}
	fmt.Println("end ------->", time.Now())
	return cslRefundDtls, nil
}

func (CslRefundDtl) GetCslSaleForReturn(brandCode, shopCode, startSaleDate, endSaleDate, saleNo, deptStoreReceiptNo, customerNo, productCode string) (interface{}, error) {
	var cslRefundDtls []CslRefundDtl
	var targetReturnSaleMap []map[string][]byte
	saleNo = deptStoreReceiptNo
	engine := factory.GetCSLEngine()
	sql := fmt.Sprintf(
		"	declare @startDate char(8)"+
			"	declare @endDate char(8)"+
			"	declare @shopCode char(4)"+
			"	declare @brandCode varchar(4)"+
			"	declare @deptStoreReceiptNo varchar(20)"+
			"	declare @saleNo varchar(15)"+
			"	set @brandCode = cast('%v' as varchar(15))"+
			"	set @shopCode = cast('%v' as char(4))"+
			"	set @startDate = cast('%v' as char(8))"+
			"	set @endDate = cast('%v' as char(8))"+
			"	set @deptStoreReceiptNo = cast('%v' as varchar(20))"+
			"	set @saleNo = cast('%v' as varchar(15))"+
			"	select"+
			"	Dates          					AS Dates"+
			"	, SaleNo    					AS SaleNo"+
			"	, DepartStoreReceiptNo 			AS DepartStoreReceiptNo"+
			"	, BrandCode   					AS BrandCode"+
			"	, ShopCode   					AS ShopCode"+
			"	, SaleQty 						AS SaleQty"+
			"	, SaleAmt 						AS SaleAmt"+
			"	, SellingAmt     				AS SellingAmt"+
			"	, SaleAmt - SellingAmt 	 		AS DiscountAmt"+
			"	, InDateTime 					AS OperationDate"+
			"	, InUserID 						AS InUserID"+
			"	FROM SaleMst WITH(NOLOCK)"+
			"	WHERE ShopCode = @shopCode"+
			"	AND BrandCode = @brandCode"+
			"	AND (SaleOfficeCode is null or SaleOfficeCode<>'P009')",
		brandCode, shopCode, startSaleDate, endSaleDate, deptStoreReceiptNo, saleNo)
	if deptStoreReceiptNo != "" {
		sql = sql + "	AND (DepartStoreReceiptNo = @deptStoreReceiptNo OR SaleNo = @saleNo)"
	}
	if startSaleDate != "" && endSaleDate != "" {
		sql = sql + "	AND Dates BETWEEN @startDate AND @endDate"
	}
	targetReturnSaleMap, err := engine.Query(sql)
	if err != nil {
		return nil, err
	}
	for _, value := range targetReturnSaleMap {
		var cslRefundDtl CslRefundDtl
		saleAmt, _ := strconv.ParseFloat(string(value["SaleAmt"]), 64)
		sellingAmt, _ := strconv.ParseFloat(string(value["SellingAmt"]), 64)
		discountAmt, _ := strconv.ParseFloat(string(value["DiscountAmt"]), 64)
		cslRefundDtl.SaleNo = string(value["SaleNo"])
		cslRefundDtl.SaleDate = string(value["Dates"])
		cslRefundDtl.DepartStoreReceiptNo = string(value["DepartStoreReceiptNo"])
		cslRefundDtl.BrandCode = string(value["BrandCode"])
		cslRefundDtl.ShopCode = string(value["ShopCode"])
		cslRefundDtl.SaleQty, _ = strconv.ParseInt(string(value["SaleQty"]), 10, 64)
		cslRefundDtl.SaleAmt = number.ToFixed(saleAmt, nil)
		cslRefundDtl.SellingAmt = number.ToFixed(sellingAmt, nil)
		cslRefundDtl.DiscountAmt = number.ToFixed(discountAmt, nil)
		cslRefundDtl.OperatorName = string(value["InUserID"])
		cslRefundDtl.OperationDate = string(value["OperationDate"])

		cslRefundDtls = append(cslRefundDtls, cslRefundDtl)
	}
	return cslRefundDtls, nil
}

const (
	MSLV1_REFUND_POS = "9"
	MILEAGE_CUSTOMER = "M"
	NEW_CUSTOMER     = "N"
	MSLv2_0          = "P009"
	Refund           = "R"
	Sale             = "S"
	NotSynChronized  = "R" // R 未同步
	SaipType         = "00"
	InUserID         = "MSLV2"
)

func (CslRefundInput) CslRefundInput(ctx context.Context, cslRefundInput CslRefundInput) error {
	var endSeq int
	var dtSeq, saleQty int64 //colleaguesId
	var saleEventNormalSaleRecognitionChk bool
	var refundedObtainMileage float64
	var startStr, strSeqNo, saleMode, eANCode, normalSaleTypeCode, useMileageSettleType,
		inUserID, paymentCode string //itemIds, baseTrimCode,payCreditCardFirmCode, offerNo, couponNo
	var custMileagePolicyNo, primaryCustEventNo, eventNo, secondaryCustEventNo, preSaleDtSeq sql.NullInt64
	var primaryEventTypeCode, secondaryEventTypeCode, eventTypeCode, primaryEventSettleTypeCode,
		secondaryEventSettleTypeCode, preSaleNo, custNo, custCardNo,
		custGradeCode, complexShopSeqNo sql.NullString
	var saleEventSaleBaseAmt, saleEventDiscountBaseAmt, saleEventAutoDiscountAmt, saleEventManualDiscountAmt, saleVentDecisionDiscountAmt,
		discountAmt, actualSaleAmt, saleEventFee, normalFee, normalFeeRate, saleEventFeeRate, eventAutoDiscountAmt,
		eventDecisionDiscountAmt, chinaFISaleAmt, estimateSaleAmt, useMileage, sellingAmt, discountAmtAsCost, saleAmt, normalPrice, shopEmpEstimateSaleAmt, paymentAmt float64

	saleMsts := make([]SaleMst, 0)
	saleDtls := make([]SaleDtl, 0)
	salePayments := make([]SalePayment, 0)
	saleDate := time.Now().Format("20060102")
	inUserID = cslRefundInput.CslRefundDtls[0].InUserID
	saleMstForQty, err := SaleMst{}.GetCslMstBySaleNo(ctx, cslRefundInput.CslRefundMst.PreSaleNo)
	if err != nil {
		return err
	}
	lastSeq, err := SaleMst{}.GetlastSeq(cslRefundInput.CslRefundDtls[0].ShopCode, saleDate)
	if err != nil {
		return err
	}
	seq, str, err := SaleMst{}.GetSeqAndStartStr(lastSeq)
	if err != nil {
		return err
	}
	endSeq = seq
	startStr = str
	roundSetting := &number.Setting{
		RoundDigit:    0,
		RoundStrategy: "round",
	}
	//Get SequenceNumber
	sequenceNumber, nextSeq, str, err := SaleMst{}.GetSequenceNumber(endSeq, startStr)
	if err != nil {
		return err
	}
	endSeq = nextSeq
	startStr = str
	saleNo := cslRefundInput.CslRefundDtls[0].ShopCode + saleDate[len(saleDate)-6:len(saleDate)] + MSLV1_REFUND_POS + sequenceNumber

	//get SeqNo
	strSeqNo = ""
	startStrs := []string{"A", "B", "C", "D", "E", "F", "G"}
	for _, startStr := range startStrs {
		if strings.HasPrefix(sequenceNumber, startStr) {
			strSeqNo = sequenceNumber[len(sequenceNumber)-3 : len(sequenceNumber)]
			break
		} else {
			strSeqNo = sequenceNumber
		}
	}
	seqNo, err := strconv.ParseInt(strSeqNo, 10, 64)
	if err != nil {
		return err
	}
	saleMode = ""
	complexShopSeqNo = sql.NullString{"", false}

	preSaleNo = sql.NullString{"", false}

	saleMode = Refund
	preSaleNo = sql.NullString{cslRefundInput.CslRefundMst.PreSaleNo, true}
	// get mileage
	if cslRefundInput.CslRefundDtls[0].ObtainMileage != 0 {
		for _, cslRefundDtl := range cslRefundInput.CslRefundDtls {
			refundDtlProportion := number.ToFixed(float64(cslRefundDtl.RefundQty)/float64(saleMstForQty[0].SaleQty), nil)
			refundDtlObtainMileage := number.ToFixed(refundDtlProportion*cslRefundDtl.ObtainMileage, roundSetting)
			refundedObtainMileage += refundDtlObtainMileage
		}
		refundedObtainMileage = number.ToFixed(refundedObtainMileage, nil)
	}
	custGradeCode = sql.NullString{"", false}
	salesPerson, err := Employee{}.GetEmployee(cslRefundInput.CslRefundMst.SaleManId)
	if err != nil {
		return err
	}
	userInfo, err := UserInfo{}.GetUserInfo(salesPerson.EmpId)
	if err != nil {
		return err
	}
	custNo = sql.NullString{"", false}
	custCardNo = sql.NullString{"", false}
	if cslRefundInput.CslRefundDtls[0].CustomerNo != "" {
		custNo = sql.NullString{cslRefundInput.CslRefundDtls[0].CustomerNo, true}
	}
	if cslRefundInput.CslRefundDtls[0].CustomerCardNo != "" {
		custCardNo = sql.NullString{cslRefundInput.CslRefundDtls[0].CustomerCardNo, true}
	}
	saleAmt = cslRefundInput.CslRefundMst.RefundAmt
	saleQty = cslRefundInput.CslRefundMst.RefundQty
	saleAmt = saleAmt * -1
	saleQty = saleQty * -1
	saleMst := SaleMst{
		SaleNo:                      saleNo,
		SeqNo:                       seqNo,
		PosNo:                       MSLV1_REFUND_POS,
		Dates:                       saleDate,
		ShopCode:                    cslRefundInput.CslRefundDtls[0].ShopCode,
		SaleMode:                    saleMode,
		CustNo:                      custNo,
		CustCardNo:                  custCardNo,
		PrimaryCustEventNo:          sql.NullInt64{0, false},
		SecondaryCustEventNo:        sql.NullInt64{0, false},
		DepartStoreReceiptNo:        cslRefundInput.CslRefundDtls[0].DepartStoreReceiptNo,
		CustDivisionCode:            sql.NullString{"", false},
		MileageCustChangeStatusCode: sql.NullString{"", false},
		CustGradeCode:               custGradeCode,
		CustBrandCode:               cslRefundInput.CslRefundDtls[0].CustBrandCode,
		PreSaleNo:                   preSaleNo,
		SaleQty:                     saleQty,
		SaleAmt:                     saleAmt,
		ObtainMileage:               refundedObtainMileage * -1,
		InUserID:                    inUserID,
		ModiUserID:                  inUserID,
		SendState:                   "",
		SendFlag:                    NotSynChronized,
		DiscountAmtAsCost:           0,
		ComplexShopSeqNo:            complexShopSeqNo,
		SaleOfficeCode:              MSLv2_0,
		Freight:                     sql.NullFloat64{0, false},
		TMall_UseMileage:            sql.NullFloat64{0, false},
		TMall_ObtainMileage:         sql.NullFloat64{0, false},
		TransactionId:               0,
		StoreId:                     0,
		OrderId:                     0,
		RefundId:                    0,
		SaleTransactionId:           0,
	}
	dtSeq = 0
	for _, cslRefundDtl := range cslRefundInput.CslRefundDtls {
		dtSeq += 1
		saleMst.BrandCode = cslRefundDtl.BrandCode
		custMileagePolicy, err := CustMileagePolicy{}.GetCustMileagePolicy(cslRefundDtl.BrandCode)
		if err != nil {
			return err
		}
		if custMileagePolicy.CustMileagePolicyNo != 0 {
			custMileagePolicyNo = sql.NullInt64{custMileagePolicy.CustMileagePolicyNo, true}
		}
		eventNo = sql.NullInt64{cslRefundDtl.SaleEventNo, false}
		primaryCustEventNo = sql.NullInt64{0, false}
		primaryEventTypeCode = sql.NullString{"", false}
		secondaryCustEventNo = sql.NullInt64{0, false}
		secondaryEventTypeCode = sql.NullString{"", false}
		eventTypeCode = sql.NullString{"", false}
		saleEventSaleBaseAmt = 0
		saleEventDiscountBaseAmt = 0
		normalSaleTypeCode = cslRefundDtl.NormalSaleTypeCode
		saleEventAutoDiscountAmt = 0
		saleEventManualDiscountAmt = 0
		saleVentDecisionDiscountAmt = 0
		discountAmt = 0
		primaryEventSettleTypeCode = sql.NullString{"", false}
		secondaryEventSettleTypeCode = sql.NullString{"", false}
		useMileageSettleType = "1"
		// offerNo = ""
		// couponNo = ""
		saleEventFee = 0
		normalFee = 0
		normalFeeRate = 0
		saleEventFeeRate = 0
		eventAutoDiscountAmt = 0
		eventDecisionDiscountAmt = 0
		chinaFISaleAmt = 0
		estimateSaleAmt = 0
		useMileage = 0
		sellingAmt = 0
		discountAmtAsCost = 0
		saleQty = 0
		saleAmt = 0
		saleEventNormalSaleRecognitionChk = false
		if cslRefundDtl.UseMileage != 0 {
			refundDtlProportion := number.ToFixed(float64(cslRefundDtl.RefundQty)/float64(cslRefundDtl.SaleQty), nil)
			useMileage = number.ToFixed(refundDtlProportion*cslRefundDtl.UseMileage, roundSetting)
		}
		eANCode = cslRefundDtl.EanCode
		priceTypeCode, err := SaleMst{}.GetPriceTypeCode(cslRefundDtl.BrandCode, cslRefundDtl.StyleCode)
		if err != nil {
			return err
		}
		supGroupCode, err := SaleMst{}.GetSupGroupCode(cslRefundDtl.BrandCode, cslRefundDtl.StyleCode)
		if err != nil {
			return err
		}
		preSaleDtSeq = sql.NullInt64{cslRefundDtl.PreSaleDtSeq, true}
		discountAmt = cslRefundDtl.DiscountAmt
		estimateSaleAmt = cslRefundDtl.RefundAmt
		sellingAmt = cslRefundDtl.RefundAmt
		chinaFISaleAmt = cslRefundDtl.RefundAmt
		normalPrice = cslRefundDtl.SalePrice
		saleQty = cslRefundDtl.RefundQty
		saleAmt = cslRefundDtl.RefundAmt

		normalPrice = normalPrice * -1
		saleQty = saleQty * -1
		saleAmt = saleAmt * -1
		eventAutoDiscountAmt = eventAutoDiscountAmt * -1
		eventDecisionDiscountAmt = eventDecisionDiscountAmt * -1
		saleEventAutoDiscountAmt = saleEventAutoDiscountAmt * -1
		saleEventManualDiscountAmt = saleEventManualDiscountAmt * -1
		saleVentDecisionDiscountAmt = saleVentDecisionDiscountAmt * -1
		chinaFISaleAmt = chinaFISaleAmt * -1
		estimateSaleAmt = estimateSaleAmt * -1
		sellingAmt = sellingAmt * -1
		normalFee = normalFee * -1
		saleEventFee = saleEventFee * -1
		actualSaleAmt = actualSaleAmt * -1
		useMileage = useMileage * -1
		discountAmt = discountAmt * -1
		saleVentDecisionDiscountAmt = saleVentDecisionDiscountAmt * -1
		shopEmpEstimateSaleAmt = shopEmpEstimateSaleAmt * -1
		saleDtl := SaleDtl{
			SaleNo:                            saleNo,
			ShopCode:                          cslRefundDtl.ShopCode,
			BrandCode:                         cslRefundDtl.BrandCode,
			DtSeq:                             dtSeq,
			CustMileagePolicyNo:               custMileagePolicyNo,
			SeqNo:                             seqNo,
			Dates:                             saleDate,
			PosNo:                             MSLV1_REFUND_POS,
			NormalSaleTypeCode:                normalSaleTypeCode,
			PrimaryCustEventNo:                primaryCustEventNo,
			PrimaryEventTypeCode:              primaryEventTypeCode,
			PrimaryEventSettleTypeCode:        primaryEventSettleTypeCode,
			SecondaryCustEventNo:              secondaryCustEventNo,
			SecondaryEventTypeCode:            secondaryEventTypeCode,
			SecondaryEventSettleTypeCode:      secondaryEventSettleTypeCode,
			SaleEventNo:                       eventNo,
			SaleEventTypeCode:                 eventTypeCode,
			SaleReturnReasonCode:              sql.NullString{"", false},
			ProdCode:                          cslRefundDtl.ProdCode,
			EANCode:                           eANCode,
			PriceTypeCode:                     priceTypeCode,
			SupGroupCode:                      supGroupCode,
			SaipType:                          SaipType,
			NormalPrice:                       normalPrice,
			Price:                             normalPrice,
			PriceDecisionDate:                 saleDate,
			SaleQty:                           saleQty,
			SaleAmt:                           saleAmt + useMileage,
			EventAutoDiscountAmt:              eventAutoDiscountAmt,
			EventDecisionDiscountAmt:          eventDecisionDiscountAmt,
			SaleEventSaleBaseAmt:              saleEventSaleBaseAmt,
			SaleEventDiscountBaseAmt:          saleEventDiscountBaseAmt,
			SaleEventNormalSaleRecognitionChk: saleEventNormalSaleRecognitionChk,
			SaleEventInterShopSalePermitChk:   false,
			SaleEventAutoDiscountAmt:          saleEventAutoDiscountAmt,
			SaleEventManualDiscountAmt:        saleEventManualDiscountAmt,
			SaleVentDecisionDiscountAmt:       saleVentDecisionDiscountAmt,
			ChinaFISaleAmt:                    chinaFISaleAmt,
			EstimateSaleAmt:                   estimateSaleAmt,
			SellingAmt:                        sellingAmt,
			NormalFee:                         normalFee,
			SaleEventFee:                      saleEventFee,
			ActualSaleAmt:                     actualSaleAmt,
			UseMileage:                        useMileage,
			PreSaleNo:                         preSaleNo,
			PreSaleDtSeq:                      preSaleDtSeq,
			NormalFeeRate:                     normalFeeRate,
			SaleEventFeeRate:                  saleEventFeeRate,
			InUserID:                          userInfo.UserName,
			ModiUserID:                        userInfo.UserName,
			SendState:                         "",
			SendFlag:                          NotSynChronized,
			DiscountAmt:                       useMileage,
			DiscountAmtAsCost:                 discountAmtAsCost,
			UseMileageSettleType:              useMileageSettleType,
			EstimateSaleAmtForConsumer:        estimateSaleAmt,
			SaleEventDiscountAmtForConsumer:   saleVentDecisionDiscountAmt,
			ShopEmpEstimateSaleAmt:            shopEmpEstimateSaleAmt,
			PromotionID:                       sql.NullInt64{0, false},
			TMallEventID:                      sql.NullInt64{0, false},
			TMall_ObtainMileage:               sql.NullFloat64{0, false},
			SaleOfficeCode:                    MSLv2_0,
			OrderItemId:                       0,
			RefundItemId:                      0,
			TransactionDtlId:                  0,
			StyleCode:                         cslRefundDtl.StyleCode,
			SaleTransactionId:                 0,
			SaleTransactionDtlId:              0,
			TransactionId:                     0,
		}
		saleDtls = append(saleDtls, saleDtl)

	}
	//set value for saleMst "UseMileage", "SellingAmt","ChinaFISaleAmt","ActualSaleAmt"
	saleMst.CustMileagePolicyNo = custMileagePolicyNo
	saleMst.UseMileage = 0
	saleMst.SellingAmt = 0
	saleMst.DiscountAmt = 0
	saleMst.ChinaFISaleAmt = 0
	saleMst.ActualSaleAmt = 0
	saleMst.EstimateSaleAmt = 0
	saleMst.ShopEmpEstimateSaleAmt = 0
	saleMst.ShopEmpEstimateSaleAmt = 0
	saleMst.SaleAmt = 0
	for _, saleDtl := range saleDtls {
		if saleMst.SaleNo == saleDtl.SaleNo {
			saleMst.UseMileage += saleDtl.UseMileage
			saleMst.SellingAmt += saleDtl.SellingAmt
			saleMst.ChinaFISaleAmt += saleDtl.SellingAmt
			saleMst.ActualSaleAmt += saleDtl.SellingAmt
			saleMst.EstimateSaleAmt += saleDtl.SellingAmt
			saleMst.DiscountAmt += saleDtl.DiscountAmt
			saleMst.ShopEmpEstimateSaleAmt += saleDtl.SellingAmt
			saleMst.SaleAmt += saleDtl.SaleAmt
		}
	}
	saleMst.UseMileage = number.ToFixed(saleMst.UseMileage, nil)
	saleMst.SellingAmt = number.ToFixed(saleMst.SellingAmt, nil)
	saleMst.DiscountAmt = number.ToFixed(saleMst.DiscountAmt, nil)
	saleMst.ChinaFISaleAmt = number.ToFixed(saleMst.ChinaFISaleAmt, nil)
	saleMst.ActualSaleAmt = number.ToFixed(saleMst.ActualSaleAmt, nil)
	saleMst.EstimateSaleAmt = number.ToFixed(saleMst.EstimateSaleAmt, nil)
	saleMst.ShopEmpEstimateSaleAmt = number.ToFixed(saleMst.ShopEmpEstimateSaleAmt, nil)
	saleMst.FeeAmt = number.ToFixed(saleMst.FeeAmt, nil)
	saleMst.EstimateSaleAmtForConsumer = saleMst.EstimateSaleAmt
	saleMst.ActualSellingAmt = saleMst.SellingAmt
	paymentCode = "11"
	paymentAmt = cslRefundInput.CslRefundMst.RefundAmt * -1
	// }
	salePayment := SalePayment{
		SaleNo:             saleMst.SaleNo,
		SeqNo:              1,
		PaymentCode:        paymentCode,
		PaymentAmt:         paymentAmt,
		InUserID:           inUserID,
		ModiUserID:         inUserID,
		SendFlag:           "R",
		CreditCardFirmCode: sql.NullString{"", false},
		TransactionId:      saleMst.TransactionId,
		SaleTransactionId:  saleMst.SaleTransactionId,
	}
	salePayments = append(salePayments, salePayment)
	saleMsts = append(saleMsts, saleMst)
	engine := factory.GetCSLEngine()
	engine.SetMapper(core.SameMapper{})
	//create session
	session := engine.NewSession()
	defer session.Close()
	if err := session.Begin(); err != nil {
		return err
	}
	local, _ := time.ParseDuration("8h")
	createTime := (time.Now()).Add(local)
	for _, saleMstFor := range saleMsts {
		saleMstFor.InDateTime = createTime
		saleMstFor.ModiDateTime = createTime
		if _, err := session.Table("dbo.SaleMst").Insert(&saleMstFor); err != nil {
			session.Rollback()
			return err
		}
	}
	for _, saleDtlFor := range saleDtls {
		saleDtlFor.InDateTime = createTime
		saleDtlFor.ModiDateTime = createTime
		if _, err := session.Table("dbo.SaleDtl").Insert(&saleDtlFor); err != nil {
			session.Rollback()
			return err
		}
	}
	for _, salePaymentFor := range salePayments {
		salePaymentFor.InDateTime = createTime
		salePaymentFor.ModiDateTime = createTime
		if _, err := session.Table("dbo.SalePayment").Insert(&salePaymentFor); err != nil {
			session.Rollback()
			return err
		}
	}
	if cslRefundInput.CslRefundDtls[0].CustomerCardNo != "" {
		if _, err := session.Query(`EXEC up_CSLK_SMM_UpdateCustomerStateBySale_CustMileageInfo_U1 @SaleNo = ?`, saleMst.SaleNo); err != nil {
			return err
		}
	}
	if err := session.Commit(); err != nil {
		return err
	}
	if cslRefundInput.CslRefundDtls[0].CustomerCardNo != "" {
		s := strings.Split(cslRefundInput.CslRefundDtls[0].CustomerName, ",")
		mallId, err := strconv.ParseInt(s[0], 10, 64)
		if err != nil {
			return err
		}
		if err := (Mileage{}).SetMslv2Mileage(ctx, Mileage{
			// MemberId:        2,
			// Mobile:          "",
			MallId:          mallId,
			TenantCode:      s[1],
			CardNo:          cslRefundInput.CslRefundDtls[0].CustomerCardNo,
			Type:            "A",
			TradeDate:       time.Now(),
			Point:           saleMst.UseMileage,
			CalculateAmount: saleMst.ObtainMileage,
			Remark:          "CSL1.0退货",
			CreateBy:        "CSL1.0退货",
		}); err != nil {
			fmt.Println(err)
		}
	}
	return nil
}

type Mileage struct {
	TenantCode      string    `json:"tenantCode,omitempty" xorm:"" validate:"required"` /*租户代码*/
	MallId          int64     `json:"mallId,omitempty" xorm:"" validate:"required"`     /*购物中心代码*/
	MemberId        int64     `json:"memberId" xorm:"" validate:"gt=0"`                 /*会员id*/
	CardNo          string    `json:"cardNo,omitempty"`                                 /*会员卡号*/
	Mobile          string    `json:"mobile,omitempty"`                                 /*手机号*/
	Type            string    `json:"type,omitempty" xorm:"" validate:"required"`       /*类型*/   /*渠道*/
	TradeDate       time.Time `json:"tradeDate,omitempty" xorm:""`                      /*交易日期*/ /*累计积分的金额*/
	Point           float64   `json:"point" xorm:"decimal(19,2)"`                       /*积分数量*/
	CalculateAmount float64   `json:"point" xorm:"decimal(19,2)"`                       /*积分抵扣金额*/
	Remark          string    `json:"remark,omitempty" xorm:""`                         /*备注*/ /*变动是否推送给顾客*/
	CreateBy        string    `json:"createBy,omitempty" xorm:""`
}
type ResultMileage struct {
	Success bool `json:"success"`
	Result  struct {
		Token string `json:"token"`
	}
	Error struct{} `json:"error"`
}

func (Mileage) SetMslv2Mileage(ctx context.Context, mileage Mileage) error {
	resultMileage := ResultMileage{}
	url := fmt.Sprintf("%s/v1/mileage", config.Config().Services.Membership)
	if _, err := httpreq.New(http.MethodPost, url, mileage).
		WithBehaviorLogContext(behaviorlog.FromCtx(ctx)).
		Call(&resultMileage); err != nil {
		return err
	}
	return nil
}
