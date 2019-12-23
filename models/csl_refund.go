package models

import (
	"clearance/clearance-adapter-for-sale-record/config"
	"clearance/clearance-adapter-for-sale-record/factory"
	"context"
	"database/sql"
	"errors"
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
	ShopName              string  `query:"shopName" json:"shopName"`
	BranchCode            string  `query:"branchCode" json:"branchCode"`
	BranchName            string  `query:"branchName" json:"branchName"`
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

type ReturnSaleDtl struct {
	ReturnSaleNo                      string         `query:"returnSaleNo" json:"returnSaleNo" xorm:"pk"`
	DtSeq                             int64          `query:"dtSeq" json:"dtSeq" xorm:"pk"`
	BrandCode                         string         `query:"brandCode" json:"brandCode"`
	ShopCode                          string         `query:"shopCode" json:"shopCode"`
	Dates                             string         `query:"dates" json:"dates"`
	PosNo                             string         `query:"posNo" json:"posNo"`
	SeqNo                             int64          `query:"seqNo" json:"seqNo"`
	NormalSaleTypeCode                string         `query:"normalSaleTypeCode" json:"normalSaleTypeCode"`
	CustMileagePolicyNo               string         `query:"custMileagePolicyNo" json:"custMileagePolicyNo"`
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
	DiscountAmt                       float64        `query:"discountAmt" json:"discountAmt"`
	DiscountAmtAsCost                 float64        `query:"discountAmtAsCost" json:"discountAmtAsCost"`
	UseMileageSettleType              string         `query:"useMileageSettleType" json:"useMileageSettleType"`
	EstimateSaleAmtForConsumer        float64        `query:"estimateSaleAmtForConsumer" json:"estimateSaleAmtForConsumer"`
	SaleEventDiscountAmtForConsumer   float64        `query:"saleEventDiscountAmtForConsumer" json:"saleEventDiscountAmtForConsumer"`
	ShopEmpEstimateSaleAmt            float64        `query:"shopEmpEstimateSaleAmt" json:"shopEmpEstimateSaleAmt"`

	ObtainMileage float64 `query:"obtainMileage" json:"obtainMileage"`
}

type ReturnSaleMst struct {
	ReturnSaleNo                string         `query:"returnSaleNo" json:"returnSaleNo" xorm:"pk"`
	BrandCode                   string         `query:"brandCode" json:"brandCode"`
	ShopCode                    string         `query:"shopCode" json:"shopCode"`
	Dates                       string         `query:"dates" json:"dates"`
	PosNo                       string         `query:"posNo" json:"posNo"`
	SeqNo                       int64          `query:"seqNo" json:"seqNo"`
	SaleMode                    string         `query:"saleMode" json:"saleMode"`
	PolicyNo                    int64          `query:"policyNo" json:"policyNo"`
	CustNo                      sql.NullString `query:"custNo" json:"custNo"`
	CustCardNo                  sql.NullString `query:"custCardNo" json:"custCardNo"`
	CustMileagePolicyNo         sql.NullInt64  `query:"custMileagePolicyNo" json:"custMileagePolicyNo"`
	PrimaryCustEventNo          sql.NullInt64  `query:"primaryCustEventNo" json:"primaryCustEventNo"`
	SecondaryCustEventNo        sql.NullInt64  `query:"secondaryCustEventNo" json:"secondaryCustEventNo"`
	DepartStoreReceiptNo        string         `query:"departStoreReceiptNo" json:"departStoreReceiptNo"`
	SaleQty                     int64          `query:"saleQty" json:"saleQty"`
	SaleAmt                     float64        `query:"saleAmt" json:"saleAmt"`
	DiscountAmt                 float64        `query:"discountAmt" json:"discountAmt"`
	ChinaFISaleAmt              float64        `query:"chinaFISaleAmt" json:"chinaFISaleAmt"`
	EstimateSaleAmt             float64        `query:"estimateSaleAmt" json:"estimateSaleAmt"`
	SellingAmt                  float64        `query:"sellingAmt" json:"sellingAmt"`
	FeeAmt                      float64        `query:"feeAmt" json:"feeAmt"`
	ActualSaleAmt               float64        `query:"actualSaleAmt" json:"actualSaleAmt"`
	UseMileage                  float64        `query:"useMileage" json:"useMileage"`
	ObtainMileage               float64        `query:"obtainMileage" json:"obtainMileage"`
	InUserID                    string         `query:"inUserID" json:"inUserID"`
	InDateTime                  time.Time      `query:"inDateTime" json:"inDateTime"`
	DiscountAmtAsCost           float64        `query:"discountAmtAsCost" json:"discountAmtAsCost"`
	CustDivisionCode            sql.NullString `query:"custDivisionCode" json:"custDivisionCode"`
	MileageCustChangeStatusCode sql.NullString `query:"mileageCustChangeStatusCode" json:"mileageCustChangeStatusCode"`
	CustGradeCode               sql.NullString `query:"custGradeCode" json:"custGradeCode"`
	PreSaleNo                   sql.NullString `query:"preSaleNo" json:"preSaleNo"`
	ActualSellingAmt            float64        `query:"actualSellingAmt" json:"actualSellingAmt"`
	EstimateSaleAmtForConsumer  float64        `query:"estimateSaleAmtForConsumer" json:"estimateSaleAmtForConsumer"`
	ShopEmpEstimateSaleAmt      float64        `query:"shopEmpEstimateSaleAmt" json:"shopEmpEstimateSaleAmt"`
	ComplexShopSeqNo            sql.NullString `query:"complexShopSeqNo" json:"complexShopSeqNo"`
	CustBrandCode               string         `query:"custBrandCode" json:"custBrandCode"`
	FirstApprovalStatus         string         `query:"firstApprovalStatus" json:"firstApprovalStatus"`
	FirstApprovalUserID         string         `query:"firstApprovalUserID" json:"firstApprovalUserID"`
	FirstApprovalDateTime       time.Time      `query:"firstApprovalDateTime" json:"firstApprovalDateTime"`
	SecondApprovalStatus        string         `query:"secondApprovalStatus" json:"secondApprovalStatus"`
	SecondApprovalUserID        string         `query:"secondApprovalUserID" json:"secondApprovalUserID"`
	SecondApprovalDateTime      time.Time      `query:"secondApprovalDateTime" json:"secondApprovalDateTime"`
	SaleSeqNo                   string         `query:"saleSeqNo" json:"saleSeqNo"`
	SaleOfficeCode              string         `query:"saleOfficeCode" json:"saleOfficeCode"`
}

type ReturnSalePayment struct {
	ReturnSaleNo       string         `query:"returnSaleNo" json:"returnSaleNo" xorm:"pk"`
	SeqNo              int64          `query:"seqNo" json:"seqNo" xorm:"pk"`
	PaymentCode        string         `query:"paymentCode" json:"paymentCode"`
	PaymentAmt         float64        `query:"paymentAmt" json:"paymentAmt"`
	InUserID           string         `query:"inUserID" json:"inUserID"`
	InDateTime         time.Time      `query:"inDateTime" json:"inDateTime"`
	CreditCardFirmCode sql.NullString `query:"creditCardFirmCode" json:"creditCardFirmCode"`
}

func (CslRefundDtl) GetCslSaleDetailForReturn(brandCode, shopCode, startSaleDate, endSaleDate, saleNo, deptStoreReceiptNo, customerNo, productCode string) (interface{}, error) {
	var cslRefundDtls []CslRefundDtl
	var targetReturnDtailSaleMap []map[string][]byte
	engine := factory.GetCSLEngine()
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
	, E.ShopName					AS ShopName
	, F.BranchCode					AS BranchCode
	, F.BranchName					AS BranchName
    , CASE WHEN A.CustNo IS NULL THEN NULL ELSE  A.CustBrandCode END AS CustBrandCode  
		from salemst A WITH(NOLOCK)
		inner join saledtl b WITH(NOLOCK)
		on A.saleno=b.saleno
		inner join product c WITH(NOLOCK)
		on b.prodcode=c.prodcode and b.brandcode=c.brandcode
		inner JOIN NormalSaleType AS D WITH(NOLOCK)
		ON b.NormalSaleTypeCode = D.NormalSaleTypeCode   
		inner JOIN Shop E
		ON b.brandcode=E.brandcode and b.shopcode=E.shopcode
		left join branch F on E.BranchCode =F.BranchCode
		where A.saleno = @saleNo`, saleNo)
	if err != nil {
		return nil, err
	}
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
		cslRefundDtl.ShopName = string(value["ShopName"])
		cslRefundDtl.BranchCode = string(value["BranchCode"])
		cslRefundDtl.BranchName = string(value["BranchName"])
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
	sql = sql + "	ORDER BY SaleNo DESC"
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
	Approval_Status  = "N"
)

func (CslRefundInput) CslRefundInput(ctx context.Context, cslRefundInput CslSaleMstStruct) error {
	saleDate := time.Now().Format("20060102")
	var saleDtls []SaleDtl
	var salePayments []SalePayment
	var saleMst SaleMst
	engine := factory.GetCSLEngine()
	engine.SetMapper(core.SameMapper{})
	//create session
	session := engine.NewSession()
	defer session.Close()
	if err := session.Begin(); err != nil {
		return err
	}
	getPreSaleSql := "SELECT * from SaleMst WHERE SaleMode = 'R' AND PreSaleNo ='" + cslRefundInput.PreSaleNo + "'"
	var refundedSaleMsts []SaleMst
	if err := session.SQL(getPreSaleSql).Find(&refundedSaleMsts); err != nil {
		return err
	}
	if len(refundedSaleMsts) > 0 {
		return errors.New("此单已经退货：" + cslRefundInput.PreSaleNo)
	}
	preSaleMst := SaleMst{}
	preSaleMstList, err := SaleMst{}.GetCslMstBySaleNo(ctx, cslRefundInput.PreSaleNo)
	if err != nil {
		return err
	}
	if len(preSaleMstList) > 0 {
		preSaleMst = preSaleMstList[0]
	} else {
		return errors.New("没有找到CSL原单编号：" + cslRefundInput.PreSaleNo)
	}
	preSaleDtls, err := SaleDtl{}.GetCslDtlBySaleNos(ctx, "'"+cslRefundInput.PreSaleNo+"'")
	if err != nil {
		return err
	}
	preSalePayments, err := SalePayment{}.GetCslSalePaymentBySaleNos(ctx, "'"+cslRefundInput.PreSaleNo+"'")
	if err != nil {
		return err
	}
	//----------------------------。
	lastSeq, err := SaleMst{}.GetlastSeq(cslRefundInput.ShopCode, saleDate, MSLV1_REFUND_POS)
	if err != nil {
		return err
	}
	seq, str, err := SaleMst{}.GetSeqAndStartStr(lastSeq)
	if err != nil {
		return err
	}
	endSeq := seq
	startStr := str
	//Get SequenceNumber
	sequenceNumber, nextSeq, str, err := SaleMst{}.GetSequenceNumber(endSeq, startStr)
	if err != nil {
		return err
	}
	endSeq = nextSeq
	startStr = str
	saleNo := cslRefundInput.ShopCode + saleDate[len(saleDate)-6:len(saleDate)] + MSLV1_REFUND_POS + sequenceNumber

	//get SeqNo
	strSeqNo := ""
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
	//获取业绩分配人
	salesPerson, err := Employee{}.GetEmployee(cslRefundInput.SaleManId)
	if err != nil {
		return err
	}
	userInfo, err := UserInfo{}.GetUserInfo(salesPerson.EmpId)
	if err != nil {
		return err
	}
	colleaguetUserID := ""
	colleagues, err := Colleagues{}.GetColleaguesAuth(cslRefundInput.UserId, "")
	if colleagues.UserName != "" {
		colleaguetUserID = colleagues.UserName
	} else {
		colleaguetUserID = InUserID
	}
	//----------------------------》
	saleMode := ""
	complexShopSeqNo := sql.NullString{"", false}
	preSaleNo := sql.NullString{"", false}

	saleMode = Refund
	preSaleNo = sql.NullString{cslRefundInput.PreSaleNo, true}

	saleMst.UseMileage = number.ToFixed(saleMst.UseMileage, nil)
	saleMst.SellingAmt = number.ToFixed(saleMst.SellingAmt, nil)
	saleMst.DiscountAmt = number.ToFixed(saleMst.DiscountAmt, nil)
	saleMst.ChinaFISaleAmt = number.ToFixed(saleMst.ChinaFISaleAmt, nil)
	saleMst.ActualSaleAmt = number.ToFixed(saleMst.ActualSaleAmt, nil)
	saleMst.EstimateSaleAmt = number.ToFixed(saleMst.EstimateSaleAmt, nil)
	saleMst.ShopEmpEstimateSaleAmt = number.ToFixed(saleMst.ShopEmpEstimateSaleAmt, nil)
	saleMst.FeeAmt = number.ToFixed(saleMst.FeeAmt, nil)
	saleMst.EstimateSaleAmtForConsumer = number.ToFixed(saleMst.EstimateSaleAmt, nil)
	saleMst.ActualSellingAmt = number.ToFixed(saleMst.ActualSellingAmt, nil)

	saleMst = SaleMst{
		SaleNo:                      saleNo,
		SeqNo:                       seqNo,
		PosNo:                       MSLV1_REFUND_POS,
		Dates:                       saleDate,
		InUserID:                    colleaguetUserID,
		ModiUserID:                  colleaguetUserID,
		SaleMode:                    saleMode,
		PreSaleNo:                   preSaleNo,
		SaleOfficeCode:              cslRefundInput.SaleOfficeCode,
		SendFlag:                    NotSynChronized,
		SendState:                   "",
		BrandCode:                   preSaleMst.BrandCode,
		ShopCode:                    preSaleMst.ShopCode,
		CustNo:                      preSaleMst.CustNo,
		CustCardNo:                  preSaleMst.CustCardNo,
		CustGradeCode:               preSaleMst.CustGradeCode,
		CustBrandCode:               preSaleMst.CustBrandCode,
		SaleQty:                     preSaleMst.SaleQty * -1,
		SaleAmt:                     preSaleMst.SaleAmt * -1,
		ObtainMileage:               preSaleMst.ObtainMileage * -1,
		EstimateSaleAmt:             preSaleMst.EstimateSaleAmt * -1,
		ActualSellingAmt:            preSaleMst.ActualSellingAmt * -1,
		UseMileage:                  preSaleMst.UseMileage * -1,
		SellingAmt:                  preSaleMst.SellingAmt * -1,
		DiscountAmt:                 preSaleMst.DiscountAmt * -1,
		ChinaFISaleAmt:              preSaleMst.ChinaFISaleAmt * -1,
		ActualSaleAmt:               preSaleMst.ActualSaleAmt * -1,
		FeeAmt:                      preSaleMst.FeeAmt * -1,
		DepartStoreReceiptNo:        preSaleMst.DepartStoreReceiptNo,
		EstimateSaleAmtForConsumer:  preSaleMst.EstimateSaleAmtForConsumer * -1,
		ShopEmpEstimateSaleAmt:      preSaleMst.ShopEmpEstimateSaleAmt * -1,
		ComplexShopSeqNo:            complexShopSeqNo,
		DiscountAmtAsCost:           0,
		TransactionId:               0,
		StoreId:                     0,
		OrderId:                     0,
		RefundId:                    0,
		SaleTransactionId:           0,
		Freight:                     sql.NullFloat64{0, false},
		TMall_UseMileage:            sql.NullFloat64{0, false},
		TMall_ObtainMileage:         sql.NullFloat64{0, false},
		PrimaryCustEventNo:          sql.NullInt64{0, false},
		SecondaryCustEventNo:        sql.NullInt64{0, false},
		CustDivisionCode:            sql.NullString{"", false},
		MileageCustChangeStatusCode: sql.NullString{"", false},
	}
	var dtSeq int64 = 0
	for _, preSaleDtl := range preSaleDtls {
		dtSeq += 1
		saleDtl := SaleDtl{
			SaleNo:            saleNo,
			DtSeq:             dtSeq,
			SeqNo:             seqNo,
			Dates:             saleDate,
			InUserID:          userInfo.UserName,
			ModiUserID:        userInfo.UserName,
			PosNo:             MSLV1_REFUND_POS,
			SaleOfficeCode:    cslRefundInput.SaleOfficeCode,
			SendState:         "",
			SendFlag:          NotSynChronized,
			PriceDecisionDate: saleDate,
			PreSaleNo:         preSaleNo,
			PreSaleDtSeq:      sql.NullInt64{preSaleDtl.DtSeq, true},

			StyleCode:                         preSaleDtl.StyleCode,
			ShopCode:                          preSaleDtl.ShopCode,
			BrandCode:                         preSaleDtl.BrandCode,
			ProdCode:                          preSaleDtl.ProdCode,
			EANCode:                           preSaleDtl.EANCode,
			CustMileagePolicyNo:               preSaleDtl.CustMileagePolicyNo,
			NormalSaleTypeCode:                preSaleDtl.NormalSaleTypeCode,
			PrimaryCustEventNo:                preSaleDtl.PrimaryCustEventNo,
			PrimaryEventTypeCode:              preSaleDtl.PrimaryEventTypeCode,
			PrimaryEventSettleTypeCode:        preSaleDtl.PrimaryEventSettleTypeCode,
			SecondaryCustEventNo:              preSaleDtl.SecondaryCustEventNo,
			SecondaryEventTypeCode:            preSaleDtl.SecondaryEventTypeCode,
			SecondaryEventSettleTypeCode:      preSaleDtl.SecondaryEventSettleTypeCode,
			SaleEventNo:                       preSaleDtl.SaleEventNo,
			SaleEventTypeCode:                 preSaleDtl.SaleEventTypeCode,
			PriceTypeCode:                     preSaleDtl.PriceTypeCode,
			SupGroupCode:                      preSaleDtl.SupGroupCode,
			SaipType:                          preSaleDtl.SaipType,
			UseMileageSettleType:              preSaleDtl.UseMileageSettleType,
			SaleEventNormalSaleRecognitionChk: preSaleDtl.SaleEventNormalSaleRecognitionChk,

			NormalPrice:                     preSaleDtl.NormalPrice,
			Price:                           preSaleDtl.Price,
			SaleQty:                         preSaleDtl.SaleQty * -1,
			SaleAmt:                         preSaleDtl.SaleAmt * -1,
			EventAutoDiscountAmt:            preSaleDtl.EventAutoDiscountAmt * -1,
			EventDecisionDiscountAmt:        preSaleDtl.EventDecisionDiscountAmt * -1,
			SaleEventSaleBaseAmt:            preSaleDtl.SaleEventSaleBaseAmt,
			SaleEventDiscountBaseAmt:        preSaleDtl.SaleEventDiscountBaseAmt,
			SaleEventAutoDiscountAmt:        preSaleDtl.SaleEventAutoDiscountAmt * -1,
			SaleEventManualDiscountAmt:      preSaleDtl.SaleEventManualDiscountAmt * -1,
			SaleVentDecisionDiscountAmt:     preSaleDtl.SaleVentDecisionDiscountAmt * -1,
			ChinaFISaleAmt:                  preSaleDtl.ChinaFISaleAmt * -1,
			EstimateSaleAmt:                 preSaleDtl.EstimateSaleAmt * -1,
			SellingAmt:                      preSaleDtl.SellingAmt * -1,
			NormalFee:                       preSaleDtl.NormalFee * -1,
			SaleEventFee:                    preSaleDtl.SaleEventFee * -1,
			ActualSaleAmt:                   preSaleDtl.ActualSaleAmt * -1,
			UseMileage:                      preSaleDtl.UseMileage * -1,
			NormalFeeRate:                   preSaleDtl.NormalFeeRate,
			SaleEventFeeRate:                preSaleDtl.SaleEventFeeRate,
			DiscountAmt:                     preSaleDtl.DiscountAmt * -1,
			DiscountAmtAsCost:               preSaleDtl.DiscountAmtAsCost * -1,
			EstimateSaleAmtForConsumer:      preSaleDtl.EstimateSaleAmtForConsumer * -1,
			SaleEventDiscountAmtForConsumer: preSaleDtl.SaleEventDiscountAmtForConsumer * -1,
			ShopEmpEstimateSaleAmt:          preSaleDtl.ShopEmpEstimateSaleAmt * -1,
			SaleEventInterShopSalePermitChk: false,
			SaleReturnReasonCode:            sql.NullString{"", false},
			PromotionID:                     sql.NullInt64{0, false},
			TMallEventID:                    sql.NullInt64{0, false},
			TMall_ObtainMileage:             sql.NullFloat64{0, false},
			OrderItemId:                     0,
			RefundItemId:                    0,
			TransactionDtlId:                0,
			SaleTransactionId:               0,
			SaleTransactionDtlId:            0,
			TransactionId:                   0,
		}
		saleDtls = append(saleDtls, saleDtl)
	}
	for _, preSalePayment := range preSalePayments {
		salePayment := SalePayment{
			SaleNo:             saleNo,
			SeqNo:              preSalePayment.SeqNo,
			PaymentCode:        preSalePayment.PaymentCode,
			PaymentAmt:         preSalePayment.PaymentAmt * -1,
			CreditCardFirmCode: preSalePayment.CreditCardFirmCode,
			InUserID:           colleaguetUserID,
			ModiUserID:         colleaguetUserID,
			SendFlag:           "R",
		}
		salePayments = append(salePayments, salePayment)
	}
	local, _ := time.ParseDuration("8h")
	createTime := (time.Now()).Add(local)
	saleMst.InDateTime = createTime
	saleMst.ModiDateTime = createTime
	if _, err := session.Table("dbo.SaleMst").Insert(&saleMst); err != nil {
		session.Rollback()
		return err
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
	if _, err := session.Query(`update returnsalemst set FirstApprovalStatus='A', SecondApprovalStatus='A' where PreSaleNo = ?`, cslRefundInput.PreSaleNo); err != nil {
		session.Rollback()
		return err
	}
	if saleMst.CustNo.String != "" {
		if _, err := session.Query(`EXEC up_CSLK_SMM_UpdateCustomerStateBySale_CustMileageInfo_U1 @SaleNo = ?`, saleMst.SaleNo); err != nil {
			return err
		}
	}
	if err := session.Commit(); err != nil {
		return err
	}
	// if saleMst.CustNo.String != "" {
	// 	msl2Message := strings.Split(cslRefundInput.Msl2Message, ",")
	// 	mallId, err := strconv.ParseInt(msl2Message[0], 10, 64)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	telNo, err := (SaleMst{}).GetCslCustomer(ctx, saleMst.BrandCode, saleMst.CustNo.String)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	mileage, err := (Mileage{}).GetMileage(ctx, telNo, msl2Message[1], msl2Message[0])
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	if err := (Mileage{}).SetMslv2Mileage(ctx, Mileage{
	// 		MemberId:   mileage.MemberId,
	// 		CardNo:     mileage.CardNo,
	// 		Mobile:     mileage.Mobile,
	// 		MallId:     mallId,
	// 		TenantCode: msl2Message[1],
	// 		Type:       "A",
	// 		TradeDate:  time.Now(),
	// 		Point:      number.ToFixed(saleMst.UseMileage*-1+saleMst.ObtainMileage, nil),
	// 		Remark:     "CSL1.0退货",
	// 		CreateBy:   "CSL1.0退货",
	// 	}); err != nil {
	// 		fmt.Println(err)
	// 	}
	// }
	return nil
}

func (CslRefundInput) CslReturnSaleInput(ctx context.Context, cslRefundInput CslSaleMstStruct) error {
	saleDate := time.Now().Format("20060102")
	var saleDtls []ReturnSaleDtl
	var salePayments []ReturnSalePayment
	var saleMst ReturnSaleMst
	var policyNo int64
	engine := factory.GetCSLEngine()
	engine.SetMapper(core.SameMapper{})
	//create session
	session := engine.NewSession()
	defer session.Close()
	if err := session.Begin(); err != nil {
		return err
	}
	getPolicyNoSql := "SELECT * from SaleReturnControlPolicy WHERE TypeCode = 'D' AND BrandCode ='" + cslRefundInput.BrandCode + "'"
	policyMap, err := session.Query(getPolicyNoSql)
	if err != nil {
		return err
	}
	for _, value := range policyMap {
		policyNo, _ = strconv.ParseInt(string(value["value"]), 10, 64)
	}
	getPreSaleSql := "SELECT * from ReturnSaleMst WHERE SaleMode = 'R' AND SecondApprovalStatus IN ('A','N') AND PreSaleNo ='" + cslRefundInput.PreSaleNo + "'"
	var refundedSaleMsts []ReturnSaleMst
	if err := session.SQL(getPreSaleSql).Find(&refundedSaleMsts); err != nil {
		return err
	}
	if len(refundedSaleMsts) > 0 {
		return errors.New("此单已有审批中的退货：" + cslRefundInput.PreSaleNo)
	}
	preSaleMst := SaleMst{}
	preSaleMstList, err := SaleMst{}.GetCslMstBySaleNo(ctx, cslRefundInput.PreSaleNo)
	if err != nil {
		return err
	}
	if len(preSaleMstList) > 0 {
		preSaleMst = preSaleMstList[0]
	} else {
		return errors.New("没有找到CSL原单编号：" + cslRefundInput.PreSaleNo)
	}
	preSaleDtls, err := SaleDtl{}.GetCslDtlBySaleNos(ctx, "'"+cslRefundInput.PreSaleNo+"'")
	if err != nil {
		return err
	}
	preSalePayments, err := SalePayment{}.GetCslSalePaymentBySaleNos(ctx, "'"+cslRefundInput.PreSaleNo+"'")
	if err != nil {
		return err
	}
	//----------------------------。
	lastSeq, err := SaleMst{}.GetlastSeq(cslRefundInput.ShopCode, saleDate, MSLV1_REFUND_POS)
	if err != nil {
		return err
	}
	seq, str, err := SaleMst{}.GetSeqAndStartStr(lastSeq)
	if err != nil {
		return err
	}
	endSeq := seq
	startStr := str
	//Get SequenceNumber
	sequenceNumber, nextSeq, str, err := SaleMst{}.GetSequenceNumber(endSeq, startStr)
	if err != nil {
		return err
	}
	endSeq = nextSeq
	startStr = str
	saleNo := "R" + cslRefundInput.ShopCode + saleDate[len(saleDate)-6:len(saleDate)] + MSLV1_REFUND_POS + sequenceNumber

	//get SeqNo
	strSeqNo := ""
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
	//获取业绩分配人
	salesPerson, err := Employee{}.GetEmployee(cslRefundInput.SaleManId)
	if err != nil {
		return err
	}
	userInfo, err := UserInfo{}.GetUserInfo(salesPerson.EmpId)
	if err != nil {
		return err
	}
	colleaguetUserID := ""
	colleagues, err := Colleagues{}.GetColleaguesAuth(cslRefundInput.UserId, "")
	if colleagues.UserName != "" {
		colleaguetUserID = colleagues.UserName
	} else {
		colleaguetUserID = InUserID
	}
	//----------------------------》
	saleMode := ""
	complexShopSeqNo := sql.NullString{"", false}
	preSaleNo := sql.NullString{"", false}

	saleMode = Refund
	preSaleNo = sql.NullString{cslRefundInput.PreSaleNo, true}

	saleMst.UseMileage = number.ToFixed(saleMst.UseMileage, nil)
	saleMst.SellingAmt = number.ToFixed(saleMst.SellingAmt, nil)
	saleMst.DiscountAmt = number.ToFixed(saleMst.DiscountAmt, nil)
	saleMst.ChinaFISaleAmt = number.ToFixed(saleMst.ChinaFISaleAmt, nil)
	saleMst.ActualSaleAmt = number.ToFixed(saleMst.ActualSaleAmt, nil)
	saleMst.EstimateSaleAmt = number.ToFixed(saleMst.EstimateSaleAmt, nil)
	saleMst.ShopEmpEstimateSaleAmt = number.ToFixed(saleMst.ShopEmpEstimateSaleAmt, nil)
	saleMst.FeeAmt = number.ToFixed(saleMst.FeeAmt, nil)
	saleMst.EstimateSaleAmtForConsumer = number.ToFixed(saleMst.EstimateSaleAmt, nil)
	saleMst.ActualSellingAmt = number.ToFixed(saleMst.ActualSellingAmt, nil)

	saleMst = ReturnSaleMst{
		ReturnSaleNo:               saleNo,
		SeqNo:                      seqNo,
		PosNo:                      MSLV1_REFUND_POS,
		Dates:                      saleDate,
		InUserID:                   colleaguetUserID,
		SaleMode:                   saleMode,
		PreSaleNo:                  preSaleNo,
		SaleOfficeCode:             MSLv2_0,
		BrandCode:                  preSaleMst.BrandCode,
		ShopCode:                   preSaleMst.ShopCode,
		CustNo:                     preSaleMst.CustNo,
		CustCardNo:                 preSaleMst.CustCardNo,
		CustGradeCode:              preSaleMst.CustGradeCode,
		CustBrandCode:              preSaleMst.CustBrandCode,
		SaleQty:                    preSaleMst.SaleQty * -1,
		SaleAmt:                    preSaleMst.SaleAmt * -1,
		ObtainMileage:              preSaleMst.ObtainMileage * -1,
		EstimateSaleAmt:            preSaleMst.EstimateSaleAmt * -1,
		ActualSellingAmt:           preSaleMst.ActualSellingAmt * -1,
		UseMileage:                 preSaleMst.UseMileage * -1,
		SellingAmt:                 preSaleMst.SellingAmt * -1,
		DiscountAmt:                preSaleMst.DiscountAmt * -1,
		ChinaFISaleAmt:             preSaleMst.ChinaFISaleAmt * -1,
		ActualSaleAmt:              preSaleMst.ActualSaleAmt * -1,
		FeeAmt:                     preSaleMst.FeeAmt * -1,
		DepartStoreReceiptNo:       preSaleMst.DepartStoreReceiptNo,
		EstimateSaleAmtForConsumer: preSaleMst.EstimateSaleAmtForConsumer * -1,
		ShopEmpEstimateSaleAmt:     preSaleMst.ShopEmpEstimateSaleAmt * -1,
		ComplexShopSeqNo:           complexShopSeqNo,
		FirstApprovalStatus:        Approval_Status,
		FirstApprovalUserID:        colleaguetUserID,
		SecondApprovalStatus:       Approval_Status,
		SecondApprovalUserID:       colleaguetUserID,
		DiscountAmtAsCost:          0,

		PolicyNo:  policyNo,
		SaleSeqNo: strings.TrimLeft(saleNo, "R"),
	}
	var dtSeq int64 = 0
	for _, preSaleDtl := range preSaleDtls {
		dtSeq += 1
		saleDtl := ReturnSaleDtl{
			ReturnSaleNo:      saleNo,
			DtSeq:             dtSeq,
			SeqNo:             seqNo,
			Dates:             saleDate,
			InUserID:          userInfo.UserName,
			PosNo:             MSLV1_REFUND_POS,
			PriceDecisionDate: saleDate,
			PreSaleNo:         preSaleNo,
			PreSaleDtSeq:      sql.NullInt64{preSaleDtl.DtSeq, true},

			ShopCode:                          preSaleDtl.ShopCode,
			BrandCode:                         preSaleDtl.BrandCode,
			ProdCode:                          preSaleDtl.ProdCode,
			EANCode:                           preSaleDtl.EANCode,
			NormalSaleTypeCode:                preSaleDtl.NormalSaleTypeCode,
			PrimaryCustEventNo:                preSaleDtl.PrimaryCustEventNo,
			PrimaryEventTypeCode:              preSaleDtl.PrimaryEventTypeCode,
			PrimaryEventSettleTypeCode:        preSaleDtl.PrimaryEventSettleTypeCode,
			SecondaryCustEventNo:              preSaleDtl.SecondaryCustEventNo,
			SecondaryEventTypeCode:            preSaleDtl.SecondaryEventTypeCode,
			SecondaryEventSettleTypeCode:      preSaleDtl.SecondaryEventSettleTypeCode,
			SaleEventNo:                       preSaleDtl.SaleEventNo,
			SaleEventTypeCode:                 preSaleDtl.SaleEventTypeCode,
			PriceTypeCode:                     preSaleDtl.PriceTypeCode,
			SupGroupCode:                      preSaleDtl.SupGroupCode,
			SaipType:                          preSaleDtl.SaipType,
			UseMileageSettleType:              preSaleDtl.UseMileageSettleType,
			SaleEventNormalSaleRecognitionChk: preSaleDtl.SaleEventNormalSaleRecognitionChk,

			NormalPrice:                     preSaleDtl.NormalPrice,
			Price:                           preSaleDtl.Price,
			SaleQty:                         preSaleDtl.SaleQty * -1,
			SaleAmt:                         preSaleDtl.SaleAmt * -1,
			EventAutoDiscountAmt:            preSaleDtl.EventAutoDiscountAmt * -1,
			EventDecisionDiscountAmt:        preSaleDtl.EventDecisionDiscountAmt * -1,
			SaleEventSaleBaseAmt:            preSaleDtl.SaleEventSaleBaseAmt,
			SaleEventDiscountBaseAmt:        preSaleDtl.SaleEventDiscountBaseAmt,
			SaleEventAutoDiscountAmt:        preSaleDtl.SaleEventAutoDiscountAmt * -1,
			SaleEventManualDiscountAmt:      preSaleDtl.SaleEventManualDiscountAmt * -1,
			SaleVentDecisionDiscountAmt:     preSaleDtl.SaleVentDecisionDiscountAmt * -1,
			ChinaFISaleAmt:                  preSaleDtl.ChinaFISaleAmt * -1,
			EstimateSaleAmt:                 preSaleDtl.EstimateSaleAmt * -1,
			SellingAmt:                      preSaleDtl.SellingAmt * -1,
			NormalFee:                       preSaleDtl.NormalFee * -1,
			SaleEventFee:                    preSaleDtl.SaleEventFee * -1,
			ActualSaleAmt:                   preSaleDtl.ActualSaleAmt * -1,
			UseMileage:                      preSaleDtl.UseMileage * -1,
			NormalFeeRate:                   preSaleDtl.NormalFeeRate,
			SaleEventFeeRate:                preSaleDtl.SaleEventFeeRate,
			DiscountAmt:                     preSaleDtl.DiscountAmt * -1,
			DiscountAmtAsCost:               preSaleDtl.DiscountAmtAsCost * -1,
			EstimateSaleAmtForConsumer:      preSaleDtl.EstimateSaleAmtForConsumer * -1,
			SaleEventDiscountAmtForConsumer: preSaleDtl.SaleEventDiscountAmtForConsumer * -1,
			ShopEmpEstimateSaleAmt:          preSaleDtl.ShopEmpEstimateSaleAmt * -1,
			SaleEventInterShopSalePermitChk: false,
			SaleReturnReasonCode:            sql.NullString{"", false},

			ObtainMileage: 0,
		}
		saleDtls = append(saleDtls, saleDtl)
	}
	for _, preSalePayment := range preSalePayments {
		salePayment := ReturnSalePayment{
			ReturnSaleNo:       saleNo,
			SeqNo:              preSalePayment.SeqNo,
			PaymentCode:        preSalePayment.PaymentCode,
			PaymentAmt:         preSalePayment.PaymentAmt * -1,
			CreditCardFirmCode: preSalePayment.CreditCardFirmCode,
			InUserID:           colleaguetUserID,
		}
		salePayments = append(salePayments, salePayment)
	}
	local, _ := time.ParseDuration("8h")
	createTime := (time.Now()).Add(local)
	saleMst.InDateTime = createTime
	saleMst.FirstApprovalDateTime = createTime
	saleMst.SecondApprovalDateTime = createTime
	if _, err := session.Table("dbo.ReturnSaleMst").Insert(&saleMst); err != nil {
		session.Rollback()
		return err
	}
	for _, saleDtlFor := range saleDtls {
		saleDtlFor.InDateTime = createTime
		if _, err := session.Table("dbo.ReturnSaleDtl").Insert(&saleDtlFor); err != nil {
			session.Rollback()
			return err
		}
	}
	for _, salePaymentFor := range salePayments {
		salePaymentFor.InDateTime = createTime
		if _, err := session.Table("dbo.ReturnSalePayment").Insert(&salePaymentFor); err != nil {
			session.Rollback()
			return err
		}
	}
	if err := session.Commit(); err != nil {
		return err
	}
	// if saleMst.CustNo.String != "" {
	// 	msl2Message := strings.Split(cslRefundInput.Msl2Message, ",")
	// 	mallId, err := strconv.ParseInt(msl2Message[0], 10, 64)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	telNo, err := (SaleMst{}).GetCslCustomer(ctx, saleMst.BrandCode, saleMst.CustNo.String)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	mileage, err := (Mileage{}).GetMileage(ctx, telNo, msl2Message[1], msl2Message[0])
	// 	if err != nil {
	// 		fmt.Println(err)
	// 	}
	// 	if err := (Mileage{}).SetMslv2Mileage(ctx, Mileage{
	// 		MemberId:   mileage.MemberId,
	// 		CardNo:     mileage.CardNo,
	// 		Mobile:     mileage.Mobile,
	// 		MallId:     mallId,
	// 		TenantCode: msl2Message[1],
	// 		Type:       "A",
	// 		TradeDate:  time.Now(),
	// 		Point:      number.ToFixed(saleMst.UseMileage*-1+saleMst.ObtainMileage, nil),
	// 		Remark:     "CSL1.0退货",
	// 		CreateBy:   "CSL1.0退货",
	// 	}); err != nil {
	// 		fmt.Println(err)
	// 	}
	// }
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
	Success bool    `json:"success"`
	Result  Mileage `json:"token"`
	Error   struct {
		Code    int         `json:"code,omitempty"`
		Message string      `json:"message,omitempty"`
		Details interface{} `json:"details,omitempty"`
	} `json:"error"`
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

func (Mileage) GetMileage(ctx context.Context, mobile string, tenantCode string, mallIds string) (Mileage, error) {
	resultMileage := ResultMileage{}
	url := fmt.Sprintf("%s/v1/mileage?mobile=%s&tenantCode=%s&mallIds=%s", config.Config().Services.Membership, mobile, tenantCode, mallIds)
	if _, err := httpreq.New(http.MethodGet, url, nil).
		WithBehaviorLogContext(behaviorlog.FromCtx(ctx)).
		Call(&resultMileage); err != nil {
		return Mileage{}, err
	}
	if resultMileage.Success != true {
		return Mileage{}, errors.New(fmt.Sprintf("%v|%v|%v", resultMileage.Error.Code, resultMileage.Error.Message, resultMileage.Error.Details))
	}
	return resultMileage.Result, nil
}
