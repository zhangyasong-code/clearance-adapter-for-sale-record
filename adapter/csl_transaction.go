package adapter

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/go-xorm/xorm"
	"github.com/pangpanglabs/goetl"
	"xorm.io/core"
)

const (
	MSLV2_POS        = "8"
	MILEAGE_CUSTOMER = "M"
	NEW_CUSTOMER     = "N"
	MSLv2_0          = "P009"
	Refund           = "R"
	Sale             = "S"
	NotSynChronized  = "R" // R 未同步
	SaipType         = "00"
	InUserID         = "MSLV2"
	Exchange         = "C"
)

// Clearance到CSL
type ClearanceToCslETL struct{}

func buildClearanceToCslETL() *goetl.ETL {
	etl := goetl.New(ClearanceToCslETL{})
	return etl
}

// Extract ...
func (etl ClearanceToCslETL) Extract(ctx context.Context) (interface{}, error) {
	saleTransactions := []models.SaleTransaction{}
	saleTransactionDtls := []models.SaleTransactionDtl{}
	//分页查询   一次查1000条
	skipCount := 0
	data := ctx.Value("data")
	dataInput := data.(models.RequestInput)
	for {
		var stsAndStds []struct {
			SaleTransaction    models.SaleTransaction    `xorm:"extends"`
			SaleTransactionDtl models.SaleTransactionDtl `xorm:"extends"`
		}
		query := func() xorm.Interface {
			q := factory.GetCfsrEngine().Table("sale_transaction").
				Select("sale_transaction.*,sale_transaction_dtl.*").
				Join("INNER", "sale_transaction_dtl",
					"sale_transaction_dtl.transaction_id = sale_transaction.transaction_id and sale_transaction_dtl.sale_transaction_id = sale_transaction.id").
				Where("sale_transaction.whether_send = ?", false)
			if dataInput.TransactionId != 0 {
				q.And("sale_transaction.transaction_id = ?", dataInput.TransactionId)
			}
			if dataInput.TransactionChannelType != "" {
				q.And("sale_transaction.transaction_channel_type = ?", dataInput.TransactionChannelType)
			}
			return q
		}
		if err := query().Limit(maxResultCount, skipCount).Find(&stsAndStds); err != nil {
			return nil, err
		}
		for _, stsAndStd := range stsAndStds {
			check := true
			for _, saleTransaction := range saleTransactions {
				if stsAndStd.SaleTransaction.Id == saleTransaction.Id && stsAndStd.SaleTransaction.OrderId == saleTransaction.OrderId && stsAndStd.SaleTransaction.RefundId == saleTransaction.RefundId {
					check = false
				}
			}
			if len(saleTransactions) == 0 || check {
				saleTransactions = append(saleTransactions, stsAndStd.SaleTransaction)
			}
			saleTransactionDtls = append(saleTransactionDtls, stsAndStd.SaleTransactionDtl)
		}
		if len(stsAndStds) < maxResultCount {
			break
		} else {
			skipCount += maxResultCount
		}
	}
	return models.SaleTAndSaleTDtls{
		SaleTransactions:    saleTransactions,
		SaleTransactionDtls: saleTransactionDtls,
	}, nil
}

// Transform ...
func (etl ClearanceToCslETL) Transform(ctx context.Context, source interface{}) (interface{}, error) {
	saleTAndSaleTDtls, ok := source.(models.SaleTAndSaleTDtls)
	if !ok {
		return nil, errors.New("Convert Failed")
	}
	saleMsts := make([]models.SaleMst, 0)
	saleDtls := make([]models.SaleDtl, 0)
	salePayments := make([]models.SalePayment, 0)
	staffSaleRecords := make([]models.StaffSaleRecord, 0)
	for _, saleTransaction := range saleTAndSaleTDtls.SaleTransactions {
		baseTrimCode := "A"
		saleDate := models.GetSaleDate(saleTransaction.UpdatedAt)
		if saleTransaction.ShopCode == "" || saleDate == "" {
			return nil, errors.New("ShopCode or saleDate is null")
		}
		checkSaleNo, seqNo, isThatNewCheckSaleNo, err := models.GetCheckSaleNoWithSeqNo(saleTransaction, saleDate, MSLV2_POS)
		if err != nil {
			return nil, err
		}
		//new checkSaleNo not need to check
		if !isThatNewCheckSaleNo {
			if checkSaleNo.Processing == true || checkSaleNo.Whthersend == true {
				continue
			}
		}
		saleNo := checkSaleNo.SaleNo

		//Sale S 销售,  Refund R 退货, EXCHANGE C 交换
		saleMode := models.GetSaleMode(saleTransaction)
		preSaleNo, err := models.GetPreSaleNo(saleTransaction)
		if err != nil {
			return nil, err
		}
		custNo, custGradeCode, custBrandCode, err := models.GetCustNoAndGradeCodeAndBrandCode(saleTransaction)
		if err != nil {
			return nil, err
		}
		inUserID, err := models.GetInUserID(saleTransaction)
		if err != nil {
			return nil, err
		}
		inUserName, err := models.GetInUserName(saleTransaction)
		if err != nil {
			return nil, err
		}
		obtainMileage := saleTransaction.ObtainMileage
		saleAmt := saleTransaction.TotalListPrice
		if saleTransaction.RefundId != 0 {
			saleAmt = saleAmt * -1
			obtainMileage = obtainMileage * -1
		}
		saleMst := models.SaleMst{
			SaleNo:                      saleNo,
			SeqNo:                       seqNo,
			PosNo:                       MSLV2_POS,
			Dates:                       saleDate,
			ShopCode:                    saleTransaction.ShopCode,
			SaleMode:                    saleMode,
			CustNo:                      custNo,
			CustCardNo:                  sql.NullString{"", false},
			PrimaryCustEventNo:          sql.NullInt64{0, false},
			SecondaryCustEventNo:        sql.NullInt64{0, false},
			DepartStoreReceiptNo:        saleTransaction.OuterOrderNo,
			CustDivisionCode:            sql.NullString{"", false},
			MileageCustChangeStatusCode: sql.NullString{"", false},
			CustGradeCode:               custGradeCode,
			CustBrandCode:               custBrandCode,
			PreSaleNo:                   preSaleNo,
			SaleAmt:                     saleAmt,
			ObtainMileage:               obtainMileage,
			InUserID:                    inUserID,
			ModiUserID:                  inUserID,
			SendState:                   "",
			SendFlag:                    NotSynChronized,
			DiscountAmtAsCost:           0,
			ComplexShopSeqNo:            sql.NullString{"", false},
			SaleOfficeCode:              MSLv2_0,
			Freight:                     sql.NullFloat64{0, false},
			TMall_UseMileage:            sql.NullFloat64{0, false},
			TMall_ObtainMileage:         sql.NullFloat64{0, false},
			TransactionId:               saleTransaction.TransactionId,
			StoreId:                     saleTransaction.StoreId,
			OrderId:                     saleTransaction.OrderId,
			RefundId:                    saleTransaction.RefundId,
			SaleTransactionId:           saleTransaction.Id,
			TransactionChannelType:      saleTransaction.TransactionChannelType,
			TransactionType:             saleTransaction.TransactionType,
			SalesmanId:                  saleTransaction.SalesmanId,
		}
		appliedSaleRecordCartOffers, err := models.AppliedSaleRecordCartOffer{}.GetAppliedSaleRecordCartOffers(saleTransaction.TransactionId)
		if err != nil {
			return nil, err
		}
		staffSaleRecord := models.GetStaffSaleRecord(saleTransaction, saleDate, saleMst)
		dtSeq := int64(0)
		for _, saleTransactionDtl := range saleTAndSaleTDtls.SaleTransactionDtls {
			if saleTransactionDtl.TransactionId == saleTransaction.TransactionId && saleTransactionDtl.SaleTransactionId == saleTransaction.Id {
				dtSeq += 1
				saleMst.BrandCode = saleTransactionDtl.BrandCode
				staffSaleRecord.BrandCode = saleTransactionDtl.BrandCode
				custMileagePolicyNo, err := models.GetCustMileagePolicyNo(saleTransactionDtl.BrandCode)
				if err != nil {
					return nil, err
				}
				saleMst.CustMileagePolicyNo = custMileagePolicyNo
				discountAmtAsCost := float64(0)
				saleAmt = 0

				promotionEvent, couponNo, err := models.GetPromotionEventAndCouponNo(appliedSaleRecordCartOffers, saleTransaction, saleTransactionDtl)
				if err != nil {
					return nil, err
				}
				normalSaleTypeCode, err := models.GetNormalSaleTypeCode(promotionEvent, couponNo)
				if err != nil {
					return nil, err
				}
				eventNo, err := models.GetEventNo(promotionEvent)
				if err != nil {
					return nil, err
				}
				useMileageSettleType, eventTypeCode := models.GetUseMileageSettleTypeAndEventTypeCode(promotionEvent)
				saleEventNormalSaleRecognitionChk := models.GetSaleEventNormalSaleRecognitionChk(promotionEvent)

				saleEventAutoDiscountAmt := models.GetSaleEventAutoDiscountAmt(promotionEvent, saleTransactionDtl, baseTrimCode)
				saleEventManualDiscountAmt := saleEventAutoDiscountAmt
				saleVentDecisionDiscountAmt := saleEventAutoDiscountAmt
				saleEventSaleBaseAmt, saleEventDiscountBaseAmt := models.GetSaleEventSaleBaseAmt_SaleEventDiscountBaseAmt(promotionEvent)

				primaryCustEventNo, primaryEventTypeCode, primaryEventSettleTypeCode,
					err := models.GetPrimaryCustEventNo_PrimaryEventTypeCode_PrimaryEventSettleTypeCode(promotionEvent, couponNo, saleTransaction, saleTransactionDtl)
				if err != nil {
					return nil, err
				}
				if err := models.ValidCustomerCustNo(saleMst, promotionEvent, saleTransaction, saleTransactionDtl); err != nil {
					return nil, err
				}
				secondaryCustEventNo, secondaryEventTypeCode, secondaryEventSettleTypeCode, err := models.GetSecondaryCustEventNo_SecondaryEventTypeCode_SecondaryEventSettleTypeCode(promotionEvent)
				if err != nil {
					return nil, err
				}
				eventAutoDiscountAmt, eventDecisionDiscountAmt := models.GetEventAutoDiscountAmt_EventDecisionDiscountAmt(normalSaleTypeCode, baseTrimCode, saleTransactionDtl)
				eANCode, skuCode, err := models.GetEANCodeAndSkuCode(saleTransaction, saleTransactionDtl)
				if err != nil {
					return nil, err
				}
				product, err := models.GetProduct(saleTransaction, saleTransactionDtl)
				if err != nil {
					return nil, err
				}
				priceTypeCode, supGroupCode, err := models.GetPriceTypeCode_SupGroupCode(product, saleTransaction, saleTransactionDtl)
				if err != nil {
					return nil, err
				}
				postSaleRecordFee, err := models.GetPostSaleRecordFee(saleTransaction, saleTransactionDtl)
				if err != nil {
					return nil, err
				}
				preSaleDtSeq, err := models.GetPreSaleDtSeq(saleTransaction, saleTransactionDtl)
				if err != nil {
					return nil, err
				}
				useMileage := models.GetUseMileage(normalSaleTypeCode, saleTransactionDtl)
				discountAmt := GetToFixedPrice(eventAutoDiscountAmt+useMileage+saleEventAutoDiscountAmt, baseTrimCode)
				estimateSaleAmt := GetToFixedPrice(saleTransactionDtl.TotalListPrice-discountAmt, baseTrimCode)
				sellingAmt := GetToFixedPrice(estimateSaleAmt-discountAmtAsCost, baseTrimCode)
				chinaFISaleAmt := GetToFixedPrice(estimateSaleAmt+saleEventAutoDiscountAmt, baseTrimCode)
				saleEventFee, saleEventFeeRate, err := models.GetSaleEventFee_SaleEventFeeRate(postSaleRecordFee, normalSaleTypeCode, baseTrimCode, sellingAmt)
				if err != nil {
					return nil, err
				}
				normalFee, actualSaleAmt, err := models.GetNormalFee_ActualSaleAmt(postSaleRecordFee, normalSaleTypeCode, baseTrimCode, sellingAmt, saleEventFee)
				if err != nil {
					return nil, err
				}
				normalFeeRate := postSaleRecordFee.ItemFeeRate
				normalPrice := saleTransactionDtl.ListPrice
				saleQty := saleTransactionDtl.Quantity
				saleAmt := saleTransactionDtl.TotalListPrice

				shopEmpEstimateSaleAmt, err := models.GetShopEmpEstimateSaleAmt(saleTransaction, saleTransactionDtl, baseTrimCode)
				if err != nil {
					return nil, err
				}

				if saleTransactionDtl.RefundItemId != 0 {
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
					shopEmpEstimateSaleAmt = shopEmpEstimateSaleAmt * -1
				}
				saleDtl := models.SaleDtl{
					SaleNo:                            saleNo,
					ShopCode:                          saleTransaction.ShopCode,
					BrandCode:                         saleTransactionDtl.BrandCode,
					DtSeq:                             dtSeq,
					CustMileagePolicyNo:               custMileagePolicyNo,
					SeqNo:                             seqNo,
					Dates:                             saleDate,
					PosNo:                             MSLV2_POS,
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
					ProdCode:                          skuCode,
					EANCode:                           eANCode,
					PriceTypeCode:                     priceTypeCode,
					SupGroupCode:                      supGroupCode,
					SaipType:                          SaipType,
					NormalPrice:                       normalPrice,
					Price:                             normalPrice,
					PriceDecisionDate:                 saleDate,
					SaleQty:                           saleQty,
					SaleAmt:                           saleAmt,
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
					InUserID:                          inUserName,
					ModiUserID:                        inUserName,
					SendState:                         "",
					SendFlag:                          NotSynChronized,
					DiscountAmt:                       discountAmt,
					DiscountAmtAsCost:                 discountAmtAsCost,
					UseMileageSettleType:              useMileageSettleType,
					EstimateSaleAmtForConsumer:        estimateSaleAmt,
					SaleEventDiscountAmtForConsumer:   saleVentDecisionDiscountAmt,
					ShopEmpEstimateSaleAmt:            shopEmpEstimateSaleAmt,
					PromotionID:                       sql.NullInt64{0, false},
					TMallEventID:                      sql.NullInt64{0, false},
					TMall_ObtainMileage:               sql.NullFloat64{0, false},
					SaleOfficeCode:                    MSLv2_0,
					OrderItemId:                       saleTransactionDtl.OrderItemId,
					RefundItemId:                      saleTransactionDtl.RefundItemId,
					TransactionDtlId:                  saleTransactionDtl.TransactionDtlId,
					StyleCode:                         product.Code,
					SaleTransactionId:                 saleTransaction.Id,
					SaleTransactionDtlId:              saleTransactionDtl.Id,
					TransactionId:                     saleTransaction.TransactionId,
				}
				saleDtls = append(saleDtls, saleDtl)
			}
		}
		//set value for saleMst "UseMileage", "SellingAmt","ChinaFISaleAmt","ActualSaleAmt"
		saleMst.UseMileage = 0
		saleMst.SellingAmt = 0
		saleMst.DiscountAmt = 0
		saleMst.ChinaFISaleAmt = 0
		saleMst.ActualSaleAmt = 0
		saleMst.EstimateSaleAmt = 0
		saleMst.ShopEmpEstimateSaleAmt = 0
		saleMst.FeeAmt = 0
		saleMst.SaleQty = 0
		for _, saleDtl := range saleDtls {
			if saleMst.SaleNo == saleDtl.SaleNo {
				saleMst.UseMileage += saleDtl.UseMileage
				saleMst.SellingAmt += saleDtl.SellingAmt
				saleMst.DiscountAmt += saleDtl.DiscountAmt
				saleMst.ChinaFISaleAmt += saleDtl.ChinaFISaleAmt
				saleMst.ActualSaleAmt += saleDtl.ActualSaleAmt
				saleMst.EstimateSaleAmt += saleDtl.EstimateSaleAmt
				saleMst.ShopEmpEstimateSaleAmt += saleDtl.ShopEmpEstimateSaleAmt
				saleMst.FeeAmt += (saleDtl.SaleEventFee + saleDtl.NormalFee)
				saleMst.SaleQty += saleDtl.SaleQty
			}
		}
		saleMst.UseMileage = GetToFixedPrice(saleMst.UseMileage, baseTrimCode)
		saleMst.SellingAmt = GetToFixedPrice(saleMst.SellingAmt, baseTrimCode)
		saleMst.DiscountAmt = GetToFixedPrice(saleMst.DiscountAmt, baseTrimCode)
		saleMst.ChinaFISaleAmt = GetToFixedPrice(saleMst.ChinaFISaleAmt, baseTrimCode)
		saleMst.ActualSaleAmt = GetToFixedPrice(saleMst.ActualSaleAmt, baseTrimCode)
		saleMst.EstimateSaleAmt = GetToFixedPrice(saleMst.EstimateSaleAmt, baseTrimCode)
		saleMst.ShopEmpEstimateSaleAmt = GetToFixedPrice(saleMst.ShopEmpEstimateSaleAmt, baseTrimCode)
		saleMst.FeeAmt = GetToFixedPrice(saleMst.FeeAmt, baseTrimCode)

		//set value for saleMst "EstimateSaleAmtForConsumer","ShopEmpEstimateSaleAmt"
		saleMst.EstimateSaleAmtForConsumer = saleMst.EstimateSaleAmt
		saleMst.ActualSellingAmt = saleMst.SellingAmt
		generatedSalePayments, err := models.GetGeneratedSalePayments(saleTransaction, inUserID, baseTrimCode, saleMst)
		if err != nil {
			return nil, err
		}
		salePayments = generatedSalePayments
		boolAppendValid := models.GetAppendValid(saleMst, saleDtls, salePayments)
		if boolAppendValid {
			staffSaleRecords = append(staffSaleRecords, staffSaleRecord)
			saleMsts = append(saleMsts, saleMst)
		} else {
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
				SaleTransactionId:      saleTransaction.Id,
				TransactionChannelType: saleTransaction.TransactionChannelType,
				OrderId:                saleTransaction.OrderId,
				RefundId:               saleTransaction.RefundId,
				StoreId:                saleTransaction.StoreId,
				TransactionId:          saleMst.TransactionId,
				CreatedBy:              "API",
				Error:                  "SaleMst、SaleDtl、SalePayment数据不一致" + strconv.FormatInt(saleMst.TransactionId, 10),
				Details:                "数据异常！",
			}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return nil, err
			}
			continue
		}
	}
	return models.SaleMstsAndSaleDtls{
		SaleMsts:         saleMsts,
		SaleDtls:         saleDtls,
		SalePayments:     salePayments,
		StaffSaleRecords: staffSaleRecords,
	}, nil
}

// ReadyToLoad ...
func (etl ClearanceToCslETL) ReadyToLoad(ctx context.Context, source interface{}) error {
	var paymentAmt float64
	if source == nil {
		return errors.New("source is nil")
	}
	saleMstsAndSaleDtls, ok := source.(models.SaleMstsAndSaleDtls)
	if !ok {
		return errors.New("Convert Failed")
	}

	for _, saleMst := range saleMstsAndSaleDtls.SaleMsts {
		paymentAmt = 0
		//Check Shop
		if err := models.ValidShop(saleMst); err != nil {
			return err
		}
		//Check PaymentAmt
		if err := models.ValidPaymentAmt(saleMstsAndSaleDtls.SalePayments, saleMst, paymentAmt); err != nil {
			return err
		}
		//Check NormalFeeRate
		if err := models.ValidNormalFeeRate(saleMst, saleMstsAndSaleDtls.SaleDtls); err != nil {
			return err
		}
	}
	err := Clearance{}.TransformToClearance(saleMstsAndSaleDtls)
	if err != nil {
		return err
	}
	return nil
}

// Load ...
func (etl ClearanceToCslETL) Load(ctx context.Context, source interface{}) error {
	if source == nil {
		return errors.New("source is nil")
	}
	saleMstsAndSaleDtls, ok := source.(models.SaleMstsAndSaleDtls)
	if !ok {
		return errors.New("Convert Failed")
	}
	//get engine
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
	for _, saleMst := range saleMstsAndSaleDtls.SaleMsts {
		//check saleNo Whether it exists or not
		successes, err := models.SaleRecordIdSuccessMapping{}.GetBySaleNo("", saleMst.SaleTransactionId)
		if err != nil {
			return err
		}
		//exists
		if len(successes) != 0 {
			continue
		}
		//not exists
		saleMst.InDateTime = createTime
		saleMst.ModiDateTime = createTime
		if _, err := session.Table("dbo.SaleMst").Insert(&saleMst); err != nil {
			str, _ := json.Marshal(saleMst)
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
				SaleTransactionId:      saleMst.SaleTransactionId,
				TransactionChannelType: saleMst.TransactionChannelType,
				OrderId:                saleMst.OrderId,
				RefundId:               saleMst.RefundId,
				StoreId:                saleMst.StoreId,
				TransactionId:          saleMst.TransactionId,
				CreatedBy:              "API",
				Error:                  err.Error() + " TransactionId:" + strconv.FormatInt(saleMst.TransactionId, 10),
				Details:                "数据插入异常！",
				Data:                   string(str),
			}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return err
			}
			session.Rollback()
			return err
		}

		// insert staffSaleRecord > 内购销售上传
		for _, staffSaleRecord := range saleMstsAndSaleDtls.StaffSaleRecords {
			if staffSaleRecord.SaleNo == saleMst.SaleNo {
				staffSaleRecord.InDateTime = createTime
				if _, err := session.Table("dbo.StaffSaleRecord").Insert(&staffSaleRecord); err != nil {
					str, _ := json.Marshal(staffSaleRecord)
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
						SaleTransactionId:      saleMst.SaleTransactionId,
						TransactionChannelType: saleMst.TransactionChannelType,
						OrderId:                saleMst.OrderId,
						RefundId:               saleMst.RefundId,
						StoreId:                saleMst.StoreId,
						TransactionId:          saleMst.TransactionId,
						CreatedBy:              "API",
						Error:                  err.Error() + " TransactionId:" + strconv.FormatInt(saleMst.TransactionId, 10),
						Details:                "数据插入异常!",
						Data:                   string(str),
					}
					if err := SaleRecordIdFailMapping.Save(); err != nil {
						return err
					}
					session.Rollback()
					return err
				}
			}
		}

		//insert saleDtl
		orderItemIds := ""
		for _, saleDtl := range saleMstsAndSaleDtls.SaleDtls {
			if saleDtl.SaleNo == saleMst.SaleNo {
				saleDtl.InDateTime = createTime
				saleDtl.ModiDateTime = createTime
				orderItemIds += strconv.FormatInt(saleDtl.OrderItemId, 10) + ","
				if _, err := session.Table("dbo.SaleDtl").Insert(&saleDtl); err != nil {
					str, _ := json.Marshal(saleDtl)
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
						SaleTransactionId:      saleMst.SaleTransactionId,
						TransactionChannelType: saleMst.TransactionChannelType,
						OrderId:                saleMst.OrderId,
						RefundId:               saleMst.RefundId,
						StoreId:                saleMst.StoreId,
						TransactionId:          saleMst.TransactionId,
						TransactionDtlId:       saleDtl.TransactionDtlId,
						CreatedBy:              "API",
						Error:                  err.Error() + " TransactionId:" + strconv.FormatInt(saleMst.TransactionId, 10),
						Details:                "数据插入异常!",
						Data:                   string(str),
					}
					if err := SaleRecordIdFailMapping.Save(); err != nil {
						return err
					}
					session.Rollback()
					return err
				}
			}
		}

		//insert salePayMent
		for _, salePayment := range saleMstsAndSaleDtls.SalePayments {
			if saleMst.SaleNo == salePayment.SaleNo {
				salePayment.InDateTime = createTime
				salePayment.ModiDateTime = createTime
				if _, err := session.Table("dbo.SalePayment").Insert(&salePayment); err != nil {
					str, _ := json.Marshal(salePayment)
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
						SaleTransactionId:      saleMst.SaleTransactionId,
						TransactionChannelType: saleMst.TransactionChannelType,
						OrderId:                saleMst.OrderId,
						RefundId:               saleMst.RefundId,
						StoreId:                saleMst.StoreId,
						TransactionId:          saleMst.TransactionId,
						CreatedBy:              "API",
						Error:                  err.Error() + " SalePaymentTransactionId:" + strconv.FormatInt(salePayment.TransactionId, 10),
						Details:                "数据插入异常！",
						Data:                   string(str),
					}
					if err := SaleRecordIdFailMapping.Save(); err != nil {
						return err
					}
					session.Rollback()
					return err
				}
			}
		}

		//CheckIfTheExchange when refund.
		if strings.ToUpper(saleMst.TransactionType) != "EXCHANGE" && saleMst.RefundId != 0 {
			if orderItemIds != "" {
				orderItemIds = strings.TrimSuffix(orderItemIds, ",")
			}
			if err := models.CheckIfTheExchange(ctx, saleMst.OrderId, saleMst.SalesmanId, orderItemIds); err != nil {
				session.Rollback()
				return err
			}
		}

		if err := models.SaveAndUpdateLog(ctx, saleMst, saleMstsAndSaleDtls); err != nil {
			return err
		}
	}
	//commit session
	if err := session.Commit(); err != nil {
		return err
	}
	return nil
}
