package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pangpanglabs/goutils/number"
)

type CslSellStruct struct {
	SaleQtyAll    int64              `json:"saleQtyAll"`
	RefundQtyAll  int64              `json:"refundQtyAll"`
	SaleAmtAll    float64            `json:"saleAmtAll"`
	RefundAmtAll  float64            `json:"refundAmtAll"`
	SellingQtyAll int64              `json:"sellingQtyAll"`
	SellingAmtAll float64            `json:"sellingAmtAll"`
	SaleMsts      []CslSaleMstStruct `json:"saleMsts"`
}

type CslSaleMstStruct struct {
	SaleNo               string             `json:"saleNo" xorm:"index"`
	BranchCode           string             `json:"branchCode"`
	ShopName             string             `json:"shopName"`
	DepartStoreReceiptNo string             `json:"departStoreReceiptNo"`
	SaleMode             string             `json:"saleMode"`
	BrandCode            string             `json:"brandCode"`
	ShopCode             string             `json:"shopCode"`
	Dates                string             `json:"dates"`
	SaleQty              int64              `json:"saleQty"`
	RefundedQty          int64              `json:"refundedQty"`
	SaleAmt              float64            `json:"saleAmt"`
	DiscountAmt          float64            `json:"discountAmt"`
	SellingAmt           float64            `json:"sellingAmt"`
	UserId               int64              `json:"userId"`
	InUserID             string             `json:"inUserID"`
	InDateTime           string             `json:"inDateTime"`
	SaleOfficeCode       string             `json:"saleOfficeCode"`
	UserName             string             `json:"userName"`
	CustNo               string             `json:"custNo"`
	CustName             string             `json:"custName"`
	UseMileage           float64            `json:"useMileage"`
	PaymentName          string             `json:"paymentName"`
	PaymentCode          string             `json:"paymentCode"`
	PrimaryEventName     string             `json:"primaryEventName"` //积分Event
	SaleManId            int64              `query:"saleManId" json:"saleManId"`
	PreSaleNo            string             `query:"preSaleNo" json:"preSaleNo"`
	RefundReason         string             `query:"refundReason" json:"refundReason"`
	Msl2Message          string             `query:"msl2Message" json:"msl2Message"`
	Status               string             `query:"status" json:"status"`
	SaleDtls             []CslSaleDtlStruct `json:"saleDtls"`
}

type CslSaleDtlStruct struct {
	SaleNo             string  `json:"saleNo"`
	SaleQty            int64   `json:"saleQty"`
	Price              float64 `json:"price"`
	SaleAmt            float64 `json:"saleAmt"`
	SellingAmt         float64 `json:"sellingAmt"`
	DiscountAmt        float64 `json:"discountAmt"`
	DtSeq              int64   `json:"dtSeq"`
	ProdCode           string  `json:"prodCode"`
	ProdName           string  `json:"prodName"`
	PrimaryEventName   string  `json:"primaryEventName"`   //基本Event
	SecondaryEventName string  `json:"secondaryEventName"` //附加Event
	SaleEventName      string  `json:"saleEventName"`      //活动Event
	RefundedQty        int64   `json:"refundedQty"`
	RefundedSellingAmt float64 `json:"refundedSellingAmt"`
	IsGift             bool    `json:"isGift"`
	IsCouponItem       bool    `json:"isCouponItem"`
}

func (CslSaleDtlStruct) GetCslSaleDtl(saleNo string) (interface{}, error) {
	if saleNo == "" {
		return nil, errors.New("saleNo不能为空")
	}
	var cslSaleDtlStructs []CslSaleDtlStruct
	var cslSaleMstStructs []CslSaleMstStruct
	engine := factory.GetCSLEngine()
	saleMstMap, err := engine.Query(`
		declare @saleNo char(15)
		set @saleNo = cast(? as char(15))
		declare @refundSaleNo char(16)
		set @refundSaleNo = cast(? as char(16))
		SELECT  
		A.SaleNo    					AS SaleNo  
		, B.BranchCode					AS BranchCode
		, B.ShopName					AS ShopName
		, C.UserName 					AS UserName 
		, D.CustName					AS CustName 
		, A.SaleMode          			AS SaleMode
		, A.Dates    					AS Dates
		, A.DepartStoreReceiptNo 		AS DepartStoreReceiptNo
		, A.BrandCode   				AS BrandCode
		, A.ShopCode   					AS ShopCode
		, A.SaleQty 					AS SaleQty
		, A.SaleAmt 					AS SaleAmt
		, A.SellingAmt     				AS SellingAmt
		, A.DiscountAmt	 				AS DiscountAmt
		, A.InDateTime 					AS InDateTime
		, A.InUserID 					AS InUserID
		, A.SaleOfficeCode 				AS SaleOfficeCode
		, A.UseMileage 					AS UseMileage
		, F.PaymentName 				AS PaymentName
		, F.PaymentCode 				AS PaymentCode
		, A.CustNo 						AS CustNo
		, G.EventName 					AS PrimaryEventName
		FROM SaleMst A WITH(NOLOCK)
			INNER JOIN Shop B
			ON A.BrandCode=B.BrandCode AND A.ShopCode=B.ShopCode
			LEFT JOIN UserInfo C WITH(NOLOCK)
			ON A.InUserID=C.UserID
			LEFT JOIN Customer D WITH(NOLOCK)
			ON A.BrandCode=D.BrandCode AND A.CustNo=D.CustNo
			LEFT JOIN SalePayment E
			ON A.SaleNo = E.SaleNo AND E.SeqNo = 1
			LEFT JOIN Payment F
			ON E.PaymentCode = F.PaymentCode
			LEFT JOIN CustEvent G
			ON A.PrimaryCustEventNo = G.CustEventNo AND  G.EventTypeCode  = 'C'
		WHERE A.SaleNo = @saleNo
		UNION All
		SELECT  
		A.ReturnSaleNo    				AS SaleNo  
		, B.BranchCode					AS BranchCode
		, B.ShopName					AS ShopName
		, C.UserName 					AS UserName 
		, D.CustName					AS CustName 
		, A.SaleMode          			AS SaleMode
		, A.Dates    					AS Dates
		, A.DepartStoreReceiptNo 		AS DepartStoreReceiptNo
		, A.BrandCode   				AS BrandCode
		, A.ShopCode   					AS ShopCode
		, A.SaleQty 					AS SaleQty
		, A.SaleAmt 					AS SaleAmt
		, A.SellingAmt     				AS SellingAmt
		, A.DiscountAmt	 				AS DiscountAmt
		, A.InDateTime 					AS InDateTime
		, A.InUserID 					AS InUserID
		, A.SaleOfficeCode 				AS SaleOfficeCode
		, A.UseMileage 					AS UseMileage
		, F.PaymentName 				AS PaymentName
		, F.PaymentCode 				AS PaymentCode
		, A.CustNo 						AS CustNo
		, G.EventName 					AS PrimaryEventName
		FROM ReturnSaleMst A WITH(NOLOCK)
			INNER JOIN Shop B
			ON A.BrandCode=B.BrandCode AND A.ShopCode=B.ShopCode
			LEFT JOIN UserInfo C WITH(NOLOCK)
			ON A.InUserID=C.UserID
			LEFT JOIN Customer D WITH(NOLOCK)
			ON A.BrandCode=D.BrandCode AND A.CustNo=D.CustNo
			LEFT JOIN ReturnSalePayment E
			ON A.ReturnSaleNo = E.ReturnSaleNo AND E.SeqNo = 1
			LEFT JOIN Payment F
			ON E.PaymentCode = F.PaymentCode
			LEFT JOIN CustEvent G
			ON A.PrimaryCustEventNo = G.CustEventNo AND  G.EventTypeCode  = 'C'
		WHERE A.ReturnSaleNo = @refundSaleNo
		`, saleNo, saleNo)
	if err != nil {
		return nil, err
	}
	for _, value := range saleMstMap {
		var cslSaleMstStruct CslSaleMstStruct
		saleAmt, _ := strconv.ParseFloat(string(value["SaleAmt"]), 64)
		sellingAmt, _ := strconv.ParseFloat(string(value["SellingAmt"]), 64)
		discountAmt, _ := strconv.ParseFloat(string(value["DiscountAmt"]), 64)
		useMileage, _ := strconv.ParseFloat(string(value["UseMileage"]), 64)
		saleQty, _ := strconv.ParseInt(string(value["SaleQty"]), 10, 64)
		if saleQty < 0 {
			cslSaleMstStruct.SaleMode = "R"
			cslSaleMstStruct.SaleQty = saleQty * -1
			cslSaleMstStruct.SaleAmt = number.ToFixed(saleAmt, nil) * -1
			cslSaleMstStruct.SellingAmt = number.ToFixed(sellingAmt, nil) * -1
			cslSaleMstStruct.DiscountAmt = number.ToFixed(discountAmt, nil) * -1
			cslSaleMstStruct.UseMileage = number.ToFixed(useMileage, nil) * -1
		} else {
			cslSaleMstStruct.SaleMode = "S"
			cslSaleMstStruct.SaleQty = saleQty
			cslSaleMstStruct.SaleAmt = number.ToFixed(saleAmt, nil)
			cslSaleMstStruct.SellingAmt = number.ToFixed(sellingAmt, nil)
			cslSaleMstStruct.DiscountAmt = number.ToFixed(discountAmt, nil)
			cslSaleMstStruct.UseMileage = number.ToFixed(useMileage, nil)
		}
		cslSaleMstStruct.SaleNo = string(value["SaleNo"])
		cslSaleMstStruct.BranchCode = string(value["BranchCode"])
		cslSaleMstStruct.ShopName = string(value["ShopName"])
		cslSaleMstStruct.Dates = string(value["Dates"])
		cslSaleMstStruct.DepartStoreReceiptNo = string(value["DepartStoreReceiptNo"])
		cslSaleMstStruct.BrandCode = string(value["BrandCode"])
		cslSaleMstStruct.ShopCode = string(value["ShopCode"])
		cslSaleMstStruct.InUserID = string(value["InUserID"])
		cslSaleMstStruct.InDateTime = string(value["InDateTime"])
		cslSaleMstStruct.SaleOfficeCode = string(value["SaleOfficeCode"])
		cslSaleMstStruct.UserName = string(value["UserName"])
		cslSaleMstStruct.CustName = string(value["CustName"])
		cslSaleMstStruct.CustNo = string(value["CustNo"])
		cslSaleMstStruct.PaymentName = string(value["PaymentName"])
		cslSaleMstStruct.PaymentCode = string(value["PaymentCode"])
		cslSaleMstStruct.PrimaryEventName = string(value["PrimaryEventName"])
		cslSaleMstStruct.Status = "registered"
		cslSaleMstStructs = append(cslSaleMstStructs, cslSaleMstStruct)
	}
	saleDtlMap, err := engine.Query(`
		declare @saleNo char(15)
		set @saleNo = cast(? as char(15))
		declare @refundSaleNo char(16)
		set @refundSaleNo = cast(? as char(16))
		SELECT  
		A.SaleNo    					AS SaleNo 
		, A.SaleQty 					AS SaleQty
		, A.Price 						AS Price  
		, A.SaleAmt 					AS SaleAmt
		, A.SellingAmt     				AS SellingAmt
		, A.DiscountAmt	 				AS DiscountAmt
		, A.DtSeq						AS DtSeq
		, A.ProdCode 					AS ProdCode 
		, B.ProdName 					AS ProdName
		, C.EventName 					AS PrimaryEventName
		, D.EventName 					AS SecondaryEventName  
		, E.SaleEventName 				AS SaleEventName  
		, CASE WHEN A.SellingAmt = 0 THEN 'true' ELSE 'false' END AS IsGift 
		FROM SaleDtl A WITH(NOLOCK)
		INNER JOIN Product B WITH(NOLOCK)
		ON A.ProdCode=B.ProdCode AND A.BrandCode=B.BrandCode
		LEFT JOIN CustEvent C
		ON A.PrimaryCustEventNo = C.CustEventNo AND A.PrimaryEventTypeCode = C.EventTypeCode
		LEFT JOIN CustEvent D
		ON A.SecondaryCustEventNo = D.CustEventNo AND A.SecondaryEventTypeCode = D.EventTypeCode
		LEFT JOIN SaleEvent E
		ON A.SaleEventNo = E.SaleEventNo AND A.SaleEventTypeCode = E.SaleEventTypeCode
		WHERE A.SaleNo = @saleNo
		UNION
		SELECT  
		A.ReturnSaleNo    					AS SaleNo 
		, A.SaleQty 					AS SaleQty
		, A.Price 						AS Price  
		, A.SaleAmt 					AS SaleAmt
		, A.SellingAmt     				AS SellingAmt
		, A.DiscountAmt	 				AS DiscountAmt
		, A.DtSeq						AS DtSeq
		, A.ProdCode 					AS ProdCode 
		, B.ProdName 					AS ProdName
		, C.EventName 					AS PrimaryEventName
		, D.EventName 					AS SecondaryEventName  
		, E.SaleEventName 				AS SaleEventName  
		, CASE WHEN A.SellingAmt = 0 THEN 'true' ELSE 'false' END AS IsGift 
		FROM ReturnSaleDtl A WITH(NOLOCK)
			INNER JOIN Product B WITH(NOLOCK)
			ON A.ProdCode=B.ProdCode AND A.BrandCode=B.BrandCode
			LEFT JOIN CustEvent C
			ON A.PrimaryCustEventNo = C.CustEventNo AND A.PrimaryEventTypeCode = C.EventTypeCode
			LEFT JOIN CustEvent D
			ON A.SecondaryCustEventNo = D.CustEventNo AND A.SecondaryEventTypeCode = D.EventTypeCode
			LEFT JOIN SaleEvent E
			ON A.SaleEventNo = E.SaleEventNo AND A.SaleEventTypeCode = E.SaleEventTypeCode
		WHERE A.ReturnSaleNo = @refundSaleNo
		`, saleNo, saleNo)
	if err != nil {
		return nil, err
	}
	var isFloat64 float64 = 1
	var isInt64 int64 = 1
	if len(cslSaleMstStructs) > 0 && cslSaleMstStructs[0].SaleMode == "R" {
		isFloat64 = -1
		isInt64 = -1
	}
	for _, value := range saleDtlMap {
		var cslSaleDtlStruct CslSaleDtlStruct
		price, _ := strconv.ParseFloat(string(value["Price"]), 64)
		saleAmt, _ := strconv.ParseFloat(string(value["SaleAmt"]), 64)
		discountAmt, _ := strconv.ParseFloat(string(value["DiscountAmt"]), 64)
		sellingAmt, _ := strconv.ParseFloat(string(value["SellingAmt"]), 64)
		saleQty, _ := strconv.ParseInt(string(value["SaleQty"]), 10, 64)
		cslSaleDtlStruct.SaleNo = string(value["SaleNo"])
		cslSaleDtlStruct.SaleQty = saleQty * isInt64
		cslSaleDtlStruct.Price = number.ToFixed(price, nil) * isFloat64
		cslSaleDtlStruct.SaleAmt = number.ToFixed(saleAmt, nil) * isFloat64
		cslSaleDtlStruct.SellingAmt = number.ToFixed(sellingAmt, nil) * isFloat64
		cslSaleDtlStruct.DiscountAmt = number.ToFixed(discountAmt, nil) * isFloat64
		cslSaleDtlStruct.DtSeq, _ = strconv.ParseInt(string(value["DtSeq"]), 10, 64)
		cslSaleDtlStruct.ProdCode = string(value["ProdCode"])
		cslSaleDtlStruct.ProdName = string(value["ProdName"])
		cslSaleDtlStruct.PrimaryEventName = string(value["PrimaryEventName"])
		cslSaleDtlStruct.SecondaryEventName = string(value["SecondaryEventName"])
		cslSaleDtlStruct.SaleEventName = string(value["SaleEventName"])
		cslSaleDtlStruct.IsGift, _ = ParseBool(string(value["IsGift"]))
		cslSaleDtlStructs = append(cslSaleDtlStructs, cslSaleDtlStruct)
	}
	SaleIsReturnedMap, err := engine.Query(`
		declare @saleNo char(15)
		set @saleNo = cast(? as char(15))
		SELECT
			A.SaleQty			AS SaleMstQty
			,B.SaleQty 			AS SaleQty
			,B.SellingAmt 		AS SellingAmt
			,B.PreSaleNo		AS PreSaleNo
			,B.PreSaleDtSeq		AS PreSaleDtSeq
		FROM SaleMst A WITH(NOLOCK)
			INNER JOIN SaleDtl B WITH(NOLOCK) on A.SaleNo=B.SaleNo
		WHERE A.PreSaleNo = @saleNo AND A.SaleMode = 'R'
		UNION
		SELECT
			A.SaleQty			AS SaleMstQty
			,B.SaleQty 			AS SaleQty
			,B.SellingAmt 		AS SellingAmt
			,B.PreSaleNo		AS PreSaleNo
			,B.PreSaleDtSeq		AS PreSaleDtSeq
		FROM ReturnSaleMst A WITH(NOLOCK)
			INNER JOIN ReturnSaleDtl B WITH(NOLOCK) on A.ReturnSaleNo=B.ReturnSaleNo
		WHERE A.PreSaleNo = @saleNo AND A.SaleMode = 'R' AND A.SecondApprovalStatus = 'N'
		`, saleNo)
	if err != nil {
		return nil, err
	}
	var saleMstQty int64 = 0
	if len(cslSaleDtlStructs) > 0 && cslSaleDtlStructs[0].SaleNo != "" {
		for key, targetReturnSale := range cslSaleDtlStructs {
			var returnedQtyAll int64
			var returnedSellingAmtAll float64
			for _, saleIsReturned := range SaleIsReturnedMap {
				saleMstQty, _ = strconv.ParseInt(string(saleIsReturned["SaleMstQty"]), 10, 64)
				isReturnedPreSaleDtSeq, _ := strconv.ParseInt(string(saleIsReturned["PreSaleDtSeq"]), 10, 64)
				if isReturnedPreSaleDtSeq == targetReturnSale.DtSeq &&
					string(saleIsReturned["PreSaleNo"]) == targetReturnSale.SaleNo {
					returnedQty, _ := strconv.ParseInt(string(saleIsReturned["SaleQty"]), 10, 64)
					returnedSellingAmt, _ := strconv.ParseFloat(string(saleIsReturned["SellingAmt"]), 64)
					returnedQtyAll += returnedQty
					returnedSellingAmtAll += returnedSellingAmt
				}
			}
			cslSaleDtlStructs[key].RefundedQty = returnedQtyAll * -1
			cslSaleDtlStructs[key].RefundedSellingAmt = number.ToFixed(returnedSellingAmtAll, nil) * -1
		}
	}
	if len(cslSaleMstStructs) > 0 && len(cslSaleDtlStructs) > 0 {
		saleMst := cslSaleMstStructs[0]
		saleMst.RefundedQty = saleMstQty * -1
		saleMst.SaleDtls = cslSaleDtlStructs
		return saleMst, nil
	}
	return nil, nil
}

func (CslSaleMstStruct) GetCslSaleMst(brandCode, shopCode, startSaleDate, endSaleDate, saleNo, deptStoreReceiptNo string) (interface{}, error) {
	if brandCode == "" || shopCode == "" || (startSaleDate == "" && startSaleDate == "" && saleNo == "" && deptStoreReceiptNo == "") {
		return nil, errors.New("参数不全：" +
			"brandCode:" + brandCode +
			"|shopCode:" + shopCode +
			"|deptStoreReceiptNo:" + deptStoreReceiptNo +
			"|startSaleDate:" + startSaleDate +
			"|endSaleDate:" + endSaleDate)
	}
	if err := DateTimeValidate(startSaleDate, endSaleDate, 31); err != nil {
		return nil, err
	}
	startSaleDate = strings.Replace(startSaleDate, "-", "", 2)
	endSaleDate = strings.Replace(endSaleDate, "-", "", 2)
	var cslSaleMstStructs []CslSaleMstStruct
	var cslSellStruct CslSellStruct
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
			"	,SaleMode          				AS SaleMode"+
			"	,SaleNo    						AS SaleNo"+
			"	,DepartStoreReceiptNo 			AS DepartStoreReceiptNo"+
			"	,BrandCode   					AS BrandCode"+
			"	,ShopCode   					AS ShopCode"+
			"	,SaleQty 						AS SaleQty"+
			"	,SaleAmt 						AS SaleAmt"+
			"	,SellingAmt     				AS SellingAmt"+
			"	,DiscountAmt	 				AS DiscountAmt"+
			"	,InDateTime 					AS InDateTime"+
			"	,InUserID 						AS InUserID"+
			"	,SaleOfficeCode 				AS SaleOfficeCode"+
			"	FROM SaleMst WITH(NOLOCK)"+
			"	WHERE 1 = 1"+
			"	AND SaleQty<>0"+
			"	AND (SaleOfficeCode IS NULL OR SaleOfficeCode<>'P009')",
		brandCode, shopCode, startSaleDate, endSaleDate, deptStoreReceiptNo, saleNo)
	aprSql := fmt.Sprintf(
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
			"	,SaleMode          				AS SaleMode"+
			"	,ReturnSaleNo    				AS SaleNo"+
			"	,DepartStoreReceiptNo 			AS DepartStoreReceiptNo"+
			"	,BrandCode   					AS BrandCode"+
			"	,ShopCode   					AS ShopCode"+
			"	,SaleQty 						AS SaleQty"+
			"	,SaleAmt 						AS SaleAmt"+
			"	,SellingAmt     				AS SellingAmt"+
			"	,DiscountAmt	 				AS DiscountAmt"+
			"	,InDateTime 					AS InDateTime"+
			"	,InUserID 						AS InUserID"+
			"	,SaleOfficeCode 				AS SaleOfficeCode"+
			"	FROM ReturnSaleMst WITH(NOLOCK)"+
			"	WHERE SecondApprovalStatus = 'N'"+
			"	AND SaleQty<0"+
			"	AND (SaleOfficeCode IS NULL OR SaleOfficeCode<>'P009')",
		brandCode, shopCode, startSaleDate, endSaleDate, deptStoreReceiptNo, saleNo)
	shopCodeList := ""
	brandCodeList := ""
	for _, v := range strings.Split(shopCode, ",") {
		shopCodeList += "'" + v + "',"
	}
	shopCodeList = strings.TrimRight(shopCodeList, ",")
	for _, v := range strings.Split(brandCode, ",") {
		brandCodeList += "'" + v + "',"
	}
	brandCodeList = strings.TrimRight(brandCodeList, ",")
	if len(shopCode) == 4 {
		sql = sql + "	AND ShopCode = @shopCode"
		aprSql = aprSql + "	AND ShopCode = @shopCode"
	} else {
		sql = sql + "	AND ShopCode IN (" + shopCodeList + ")"
		aprSql = aprSql + "	AND ShopCode IN (" + shopCodeList + ")"
	}
	if len(brandCode) == 2 {
		sql = sql + "	AND BrandCode = @brandCode"
		aprSql = aprSql + "	AND BrandCode = @brandCode"
	} else {
		sql = sql + "	AND BrandCode IN (" + brandCodeList + ")"
		aprSql = aprSql + "	AND BrandCode IN (" + brandCodeList + ")"
	}
	if deptStoreReceiptNo != "" {
		sql = sql + "	AND (DepartStoreReceiptNo = @deptStoreReceiptNo OR SaleNo = @saleNo)"
		aprSql = aprSql + "	AND (DepartStoreReceiptNo = @deptStoreReceiptNo OR ReturnSaleNo = @saleNo)"
	}
	if startSaleDate != "" && endSaleDate != "" {
		sql = sql + "	AND Dates BETWEEN @startDate AND @endDate"
		aprSql = aprSql + "	AND Dates BETWEEN @startDate AND @endDate"
	}
	sql = sql + "	ORDER BY SaleNo DESC"
	aprSql = aprSql + "	ORDER BY ReturnSaleNo DESC"
	targetReturnSaleMap, err := engine.Query(sql)
	if err != nil {
		return nil, err
	}
	aprSaleMap, err := engine.Query(aprSql)
	if err != nil {
		return nil, err
	}
	for _, value := range targetReturnSaleMap {
		var cslSaleMstStruct CslSaleMstStruct
		saleAmt, _ := strconv.ParseFloat(string(value["SaleAmt"]), 64)
		sellingAmt, _ := strconv.ParseFloat(string(value["SellingAmt"]), 64)
		discountAmt, _ := strconv.ParseFloat(string(value["DiscountAmt"]), 64)
		saleQty, _ := strconv.ParseInt(string(value["SaleQty"]), 10, 64)
		if saleQty < 0 {
			cslSaleMstStruct.SaleMode = "R"
			cslSaleMstStruct.SaleQty = saleQty * -1
			cslSaleMstStruct.SaleAmt = number.ToFixed(saleAmt, nil) * -1
			cslSaleMstStruct.SellingAmt = number.ToFixed(sellingAmt, nil) * -1
			cslSaleMstStruct.DiscountAmt = number.ToFixed(discountAmt, nil) * -1
			cslSellStruct.RefundQtyAll += saleQty * -1
			cslSellStruct.RefundAmtAll = cslSellStruct.RefundAmtAll + sellingAmt*-1
		} else {
			cslSaleMstStruct.SaleMode = "S"
			cslSaleMstStruct.SaleQty = saleQty
			cslSaleMstStruct.SaleAmt = number.ToFixed(saleAmt, nil)
			cslSaleMstStruct.SellingAmt = number.ToFixed(sellingAmt, nil)
			cslSaleMstStruct.DiscountAmt = number.ToFixed(discountAmt, nil)
			cslSellStruct.SaleQtyAll += saleQty
			cslSellStruct.SaleAmtAll = cslSellStruct.SaleAmtAll + sellingAmt
		}
		cslSaleMstStruct.SaleNo = string(value["SaleNo"])
		cslSaleMstStruct.Dates = string(value["Dates"])
		cslSaleMstStruct.DepartStoreReceiptNo = string(value["DepartStoreReceiptNo"])
		cslSaleMstStruct.BrandCode = string(value["BrandCode"])
		cslSaleMstStruct.ShopCode = string(value["ShopCode"])
		cslSaleMstStruct.InUserID = string(value["InUserID"])
		cslSaleMstStruct.InDateTime = string(value["InDateTime"])
		cslSaleMstStruct.SaleOfficeCode = string(value["SaleOfficeCode"])

		cslSaleMstStructs = append(cslSaleMstStructs, cslSaleMstStruct)
		cslSellStruct.SaleAmtAll = number.ToFixed(cslSellStruct.SaleAmtAll, nil)
		cslSellStruct.RefundAmtAll = number.ToFixed(cslSellStruct.RefundAmtAll, nil)
		cslSellStruct.SellingQtyAll = cslSellStruct.SaleQtyAll - cslSellStruct.RefundQtyAll
		cslSellStruct.SellingAmtAll = number.ToFixed(cslSellStruct.SaleAmtAll-cslSellStruct.RefundAmtAll, nil)
	}
	for _, value := range aprSaleMap {
		var cslSaleMstStruct CslSaleMstStruct
		saleAmt, _ := strconv.ParseFloat(string(value["SaleAmt"]), 64)
		sellingAmt, _ := strconv.ParseFloat(string(value["SellingAmt"]), 64)
		discountAmt, _ := strconv.ParseFloat(string(value["DiscountAmt"]), 64)
		saleQty, _ := strconv.ParseInt(string(value["SaleQty"]), 10, 64)
		if saleQty < 0 {
			cslSaleMstStruct.SaleMode = "R"
			cslSaleMstStruct.SaleQty = saleQty * -1
			cslSaleMstStruct.SaleAmt = number.ToFixed(saleAmt, nil) * -1
			cslSaleMstStruct.SellingAmt = number.ToFixed(sellingAmt, nil) * -1
			cslSaleMstStruct.DiscountAmt = number.ToFixed(discountAmt, nil) * -1
			cslSellStruct.RefundQtyAll += saleQty * -1
			cslSellStruct.RefundAmtAll = cslSellStruct.RefundAmtAll + sellingAmt*-1
		} else {
			cslSaleMstStruct.SaleMode = "S"
			cslSaleMstStruct.SaleQty = saleQty
			cslSaleMstStruct.SaleAmt = number.ToFixed(saleAmt, nil)
			cslSaleMstStruct.SellingAmt = number.ToFixed(sellingAmt, nil)
			cslSaleMstStruct.DiscountAmt = number.ToFixed(discountAmt, nil)
			cslSellStruct.SaleQtyAll += saleQty
			cslSellStruct.SaleAmtAll = cslSellStruct.SaleAmtAll + sellingAmt
		}
		cslSaleMstStruct.SaleNo = string(value["SaleNo"])
		cslSaleMstStruct.Dates = string(value["Dates"])
		cslSaleMstStruct.DepartStoreReceiptNo = string(value["DepartStoreReceiptNo"])
		cslSaleMstStruct.BrandCode = string(value["BrandCode"])
		cslSaleMstStruct.ShopCode = string(value["ShopCode"])
		cslSaleMstStruct.InUserID = string(value["InUserID"])
		cslSaleMstStruct.InDateTime = string(value["InDateTime"])
		cslSaleMstStruct.SaleOfficeCode = string(value["SaleOfficeCode"])
		cslSaleMstStruct.Status = "registered"

		cslSaleMstStructs = append(cslSaleMstStructs, cslSaleMstStruct)
		cslSellStruct.SaleAmtAll = number.ToFixed(cslSellStruct.SaleAmtAll, nil)
		cslSellStruct.RefundAmtAll = number.ToFixed(cslSellStruct.RefundAmtAll, nil)
		cslSellStruct.SellingQtyAll = cslSellStruct.SaleQtyAll - cslSellStruct.RefundQtyAll
		cslSellStruct.SellingAmtAll = number.ToFixed(cslSellStruct.SaleAmtAll-cslSellStruct.RefundAmtAll, nil)
	}
	cslSellStruct.SaleMsts = cslSaleMstStructs
	return cslSellStruct, nil
}

func ParseBool(str string) (bool, error) {
	switch str {
	case "1", "t", "T", "true", "TRUE", "True":
		return true, nil
	case "0", "f", "F", "false", "FALSE", "False":
		return false, nil
	}
	return false, errors.New("参数不正确")
}

func DateTimeValidate(startDate, endDate string, term int) error {
	timeLayout := "2006-01-02"
	if startDate == "" && endDate == "" {
		return errors.New("查询条件必须有日期")
	}
	if startDate == "" && endDate != "" {
		return errors.New("结束日期为空")
	}
	if startDate != "" && endDate == "" {
		return errors.New("开始日期为空")
	}

	startTime, err := time.Parse(timeLayout, startDate)
	if err != nil {
		return err
	}
	endTime, err := time.Parse(timeLayout, endDate)
	if err != nil {
		return err
	}

	if startTime.After(endTime) {
		return err
	}
	if startTime.AddDate(0, 0, term-1).Before(endTime) {
		return errors.New(fmt.Sprintf("查询期间不能大于%d天", term))
	}
	return nil
}
