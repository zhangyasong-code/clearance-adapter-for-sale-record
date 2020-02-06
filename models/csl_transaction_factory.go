package models

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

const (
	Exchange = "C"
)

//	保存插入成功log并更新插入错误log
func SaveAndUpdateLog(ctx context.Context, saleMstInput SaleMst, saleMstsAndSaleDtls SaleMstsAndSaleDtls) error {
	g := errgroup.Group{}

	g.Go(func() error {
		//insert success table
		for _, saleMst := range saleMstsAndSaleDtls.SaleMsts {
			if saleMst.SaleNo == saleMstInput.SaleNo {
				for _, saleDtl := range saleMstsAndSaleDtls.SaleDtls {
					if saleDtl.SaleNo == saleMst.SaleNo {
						for _, salePayment := range saleMstsAndSaleDtls.SalePayments {
							if salePayment.SaleNo == saleDtl.SaleNo {
								saleRecordIdSuccessMapping := &SaleRecordIdSuccessMapping{
									SaleTransactionId:      saleMst.SaleTransactionId,
									TransactionChannelType: saleMst.TransactionChannelType,
									SaleNo:                 saleMst.SaleNo,
									CreatedBy:              "API",
									TransactionId:          saleMst.TransactionId,
									OrderId:                saleMst.OrderId,
									RefundId:               saleMst.RefundId,
									OrderItemId:            saleDtl.OrderItemId,
									RefundItemId:           saleDtl.RefundItemId,
									DtlSeq:                 saleDtl.DtSeq,
								}
								if err := saleRecordIdSuccessMapping.CheckAndSave(); err != nil {
									return err
								}
							}
						}
					}
				}
			}
		}
		return nil
	})

	g.Go(func() error {
		//To update "WhetherSend" field in clearance db
		saleTransaction, err := SaleTransaction{}.Get(saleMstInput.SaleTransactionId, saleMstInput.TransactionId)
		if err != nil {
			return err
		}
		saleTransaction.WhetherSend = true
		saleTransaction.InDateTime = saleMstInput.InDateTime
		if err := saleTransaction.Update(); err != nil {
			return err
		}
		return nil
	})

	g.Go(func() error {
		// update saleRecordIdFailMappings when send to csl success
		_, saleRecordIdFailMappings, err := SaleRecordIdFailMapping{}.GetSaleFailDataLog(ctx, RequestInput{SaleTransactionId: saleMstInput.SaleTransactionId})
		if err != nil {
			return err
		}
		for _, saleRecordIdFailMapping := range saleRecordIdFailMappings {
			saleRecordIdFailMapping.IsCreate = true
			if err := saleRecordIdFailMapping.Update(); err != nil {
				return err
			}
		}
		return nil
	})

	g.Go(func() error {
		checkSaleNo, err := CheckSaleNo{}.GetCheckSaleNoBySaleTransactionid(saleMstInput.SaleTransactionId)
		if err != nil {
			return err
		}
		checkSaleNo.Processing = false
		checkSaleNo.Whthersend = true
		if err := checkSaleNo.Update(); err != nil {
			return err
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

//	查询CheckSaleNo和seqNo
func GetCheckSaleNoWithSeqNo(saleTransaction SaleTransaction, saleDate, posNo string) (CheckSaleNo, int64, bool, error) {
	queryCheckSaleNo, err := CheckSaleNo{}.GetCheckSaleNoBySaleTransactionid(saleTransaction.Id)
	if err != nil {
		return CheckSaleNo{}, 0, false, err
	}
	seqNumber, sequenceNumber, err := getSeqNoAndSequenceNumber(saleTransaction.ShopCode, saleDate, posNo)
	if err != nil {
		return CheckSaleNo{}, 0, false, err
	}
	if queryCheckSaleNo.SaleNo == "" {
		saleNo := saleTransaction.ShopCode + saleDate[len(saleDate)-6:len(saleDate)] + posNo + sequenceNumber
		checkSaleNo := &CheckSaleNo{
			TransactionId:          saleTransaction.TransactionId,
			SaleTransactionId:      saleTransaction.Id,
			TransactionChannelType: saleTransaction.TransactionChannelType,
			OrderId:                saleTransaction.OrderId,
			RefundId:               saleTransaction.RefundId,
			ShopCode:               saleTransaction.ShopCode,
			Dates:                  saleDate,
			SaleNo:                 saleNo,
			PosNo:                  posNo,
			Processing:             true,
			Whthersend:             false,
		}
		if err = checkSaleNo.Save(); err != nil {
			return CheckSaleNo{}, 0, false, err
		}
		return makeCheckSaleNoEntity(checkSaleNo), seqNumber, true, nil
	}
	return queryCheckSaleNo, seqNumber, false, nil
}

//	获取seqNo和SequenceNumber
func getSeqNoAndSequenceNumber(shopCode, saleDate, posNo string) (int64, string, error) {
	lastSaleNo, err := CheckSaleNo{}.GetLastSaleNo(shopCode, saleDate, posNo)
	if err != nil {
		return 0, "", err
	}
	seq, str, err := SaleMst{}.GetSeqAndStartStr(lastSaleNo)
	if err != nil {
		return 0, "", err
	}
	//Get SequenceNumber
	sequenceNumber, _, _, err := SaleMst{}.GetSequenceNumber(seq, str)
	if err != nil {
		return 0, "", err
	}
	//get SeqNo
	seqNumber, err := SaleMst{}.GetSeqNo(sequenceNumber)
	if err != nil {
		return 0, "", err
	}
	return seqNumber, sequenceNumber, nil
}

func makeCheckSaleNoEntity(checkSaleNo *CheckSaleNo) CheckSaleNo {
	return CheckSaleNo{
		Id:                     checkSaleNo.Id,
		TransactionId:          checkSaleNo.TransactionId,
		SaleTransactionId:      checkSaleNo.Id,
		TransactionChannelType: checkSaleNo.TransactionChannelType,
		OrderId:                checkSaleNo.OrderId,
		RefundId:               checkSaleNo.RefundId,
		ShopCode:               checkSaleNo.ShopCode,
		Dates:                  checkSaleNo.Dates,
		SaleNo:                 checkSaleNo.SaleNo,
		PosNo:                  checkSaleNo.PosNo,
		Processing:             checkSaleNo.Processing,
		Whthersend:             checkSaleNo.Whthersend,
	}
}

func GetSaleMode(saleTransaction SaleTransaction) (saleMode string) {
	if saleTransaction.RefundId == 0 {
		saleMode = Sale
	} else {
		saleMode = Refund
	}
	if strings.ToUpper(saleTransaction.TransactionType) == "EXCHANGE" {
		saleMode = Exchange
	}
	return saleMode
}

func GetPreSaleNo(saleTransaction SaleTransaction) (sql.NullString, error) {
	//SuccessOrderId and SuccessRefundId are parameters used when querying successful data
	successOrderId := saleTransaction.OrderId
	successRefundId := int64(0)
	details := ""
	boolPreSaleNoCheck := false
	if saleTransaction.RefundId != 0 {
		details = "退货处理必须有之前的销售数据！"
		boolPreSaleNoCheck = true
	}
	if strings.ToUpper(saleTransaction.TransactionType) == "EXCHANGE" {
		boolPreSaleNoCheck = false
		if saleTransaction.RefundId == 0 {
			//SuccessRefundId = saleTransaction.OrderId and successOrderId = 0 when TransactionType is EXCHANGE and sales after return
			successOrderId = 0
			successRefundId = saleTransaction.OrderId
			details = "换货处理必须有之前的退货数据！"
			boolPreSaleNoCheck = true
		}
	}
	if boolPreSaleNoCheck {
		successDtls, err := SaleRecordIdSuccessMapping{}.GetSaleSuccessData(0, successOrderId, successRefundId, 0, 0, saleTransaction.TransactionChannelType)
		if err != nil {
			SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
				SaleTransactionId:      saleTransaction.Id,
				TransactionChannelType: saleTransaction.TransactionChannelType,
				OrderId:                saleTransaction.OrderId,
				RefundId:               saleTransaction.RefundId,
				StoreId:                saleTransaction.StoreId,
				TransactionId:          saleTransaction.TransactionId,
				CreatedBy:              "API",
				Error:                  err.Error() + " OrderId:" + strconv.FormatInt(saleTransaction.OrderId, 10) + " RefundId:" + strconv.FormatInt(saleTransaction.RefundId, 10),
				Details:                details,
			}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return sql.NullString{"", false}, err
			}
		}
		return sql.NullString{successDtls[0].SaleNo, true}, nil
	}
	return sql.NullString{"", false}, nil
}

func GetCustNoAndGradeCodeAndBrandCode(saleTransaction SaleTransaction) (sql.NullString, sql.NullString, string, error) {
	custNo := sql.NullString{"", false}
	custGradeCode := sql.NullString{"", false}
	custBrandCode := ""
	if saleTransaction.CustomerId != 0 {
		custNo = sql.NullString{strconv.FormatInt(saleTransaction.CustomerId, 10), true}
		//get mileage
		mileage, err := PostMileage{}.GetMileage(saleTransaction.CustomerId, saleTransaction.TransactionId)
		if err != nil {
			SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
				SaleTransactionId:      saleTransaction.Id,
				TransactionChannelType: saleTransaction.TransactionChannelType,
				OrderId:                saleTransaction.OrderId,
				RefundId:               saleTransaction.RefundId,
				StoreId:                saleTransaction.StoreId,
				TransactionId:          saleTransaction.TransactionId,
				CreatedBy:              "API",
				Error:                  err.Error() + " TransactionId:" + strconv.FormatInt(saleTransaction.TransactionId, 10),
				Details:                "查询PostMileage失败！",
			}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return sql.NullString{"", false}, sql.NullString{"", false}, "", err
			}
		}
		custBrandCode = mileage.BrandCode
		if mileage.GradeId != 0 {
			custGradeCode = sql.NullString{strconv.FormatInt(mileage.GradeId, 10), true}
		}
		return custNo, custGradeCode, custBrandCode, nil
	}
	return custNo, custGradeCode, custBrandCode, nil
}

func GetInUserID(saleTransaction SaleTransaction) (string, error) {
	if strings.ToUpper(saleTransaction.TransactionChannelType) == "POS" && saleTransaction.TransactionCreatedId != 0 {
		colleagues, err := Colleagues{}.GetColleaguesAuth(saleTransaction.TransactionCreatedId, "")
		if err != nil {
			SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
				SaleTransactionId:      saleTransaction.Id,
				TransactionChannelType: saleTransaction.TransactionChannelType,
				OrderId:                saleTransaction.OrderId,
				RefundId:               saleTransaction.RefundId,
				StoreId:                saleTransaction.StoreId,
				TransactionId:          saleTransaction.TransactionId,
				CreatedBy:              "API",
				Error:                  err.Error() + " TransactionCreatedId:" + strconv.FormatInt(saleTransaction.TransactionCreatedId, 10),
				Details:                "Colleague信息不存在！",
			}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return "", err
			}
		}
		if colleagues.UserName != "" {
			return colleagues.UserName, nil
		}
		return InUserID, nil
	}
	return InUserID, nil
}

func GetInUserName(saleTransaction SaleTransaction) (string, error) {
	if saleTransaction.SalesmanId != 0 {
		salesPerson, err := Employee{}.GetEmployee(saleTransaction.SalesmanId)
		if err != nil {
			SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
				SaleTransactionId:      saleTransaction.Id,
				TransactionChannelType: saleTransaction.TransactionChannelType,
				OrderId:                saleTransaction.OrderId,
				RefundId:               saleTransaction.RefundId,
				StoreId:                saleTransaction.StoreId,
				TransactionId:          saleTransaction.TransactionId,
				CreatedBy:              "API",
				Error:                  err.Error() + " SalesmanId:" + strconv.FormatInt(saleTransaction.SalesmanId, 10),
				Details:                "销售员信息不存在！",
			}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return "", err
			}
		}
		// colleague, err := models.Colleagues{}.GetColleaguesAuth(0, salesPerson.EmpId)
		userInfo, err := UserInfo{}.GetUserInfo(salesPerson.EmpId)
		if err != nil {
			SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
				SaleTransactionId:      saleTransaction.Id,
				TransactionChannelType: saleTransaction.TransactionChannelType,
				OrderId:                saleTransaction.OrderId,
				RefundId:               saleTransaction.RefundId,
				StoreId:                saleTransaction.StoreId,
				TransactionId:          saleTransaction.TransactionId,
				CreatedBy:              "API",
				Error:                  err.Error() + " EmpId:" + salesPerson.EmpId,
				Details:                "UserInfo信息不存在！",
			}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return "", err
			}
		}
		return userInfo.UserName, nil
	}
	return "", nil
}

func GetSaleDate(updatedAt time.Time) string {
	local, _ := time.ParseDuration("8h")
	localSaleDate := (updatedAt).Add(local)
	return localSaleDate.Format("20060102")
}

func GetStaffSaleRecord(saleTransaction SaleTransaction, saleDate string, saleMst SaleMst) StaffSaleRecord {
	// 是否上传内购到CSL Parameters : empId
	if saleTransaction.EmpId != "" {
		return StaffSaleRecord{
			Dates:             saleDate,
			HREmpNo:           saleTransaction.EmpId,
			SaleNo:            saleMst.SaleNo,
			ShopCode:          saleMst.ShopCode,
			InUserID:          saleMst.InUserID,
			SaleTransactionId: saleTransaction.Id,
			TransactionId:     saleTransaction.TransactionId,
		}
	}
	return StaffSaleRecord{}
}

func GetCustMileagePolicyNo(brandCode string) (sql.NullInt64, error) {
	custMileagePolicy, err := CustMileagePolicy{}.GetCustMileagePolicy(brandCode)
	if err != nil {
		return sql.NullInt64{0, false}, err
	}
	if custMileagePolicy.CustMileagePolicyNo != 0 {
		return sql.NullInt64{custMileagePolicy.CustMileagePolicyNo, true}, nil
	}
	return sql.NullInt64{0, false}, err
}

func GetEANCodeAndSkuCode(saleTransaction SaleTransaction, saleTransactionDtl SaleTransactionDtl) (string, string, error) {
	sku, err := Product{}.GetSkuBySkuId(saleTransactionDtl.SkuId)
	if err != nil {
		SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
			SaleTransactionId:      saleTransaction.Id,
			TransactionChannelType: saleTransaction.TransactionChannelType,
			OrderId:                saleTransaction.OrderId,
			RefundId:               saleTransaction.RefundId,
			StoreId:                saleTransaction.StoreId,
			TransactionId:          saleTransactionDtl.TransactionId,
			TransactionDtlId:       saleTransactionDtl.TransactionDtlId,
			CreatedBy:              "API",
			Error:                  err.Error() + " SkuId:" + strconv.FormatInt(saleTransactionDtl.SkuId, 10),
			Details:                "商品不存在！",
		}
		if err := SaleRecordIdFailMapping.Save(); err != nil {
			return "", "", err
		}
		return "", "", err
	}

	if len(sku.Identifiers) == 0 || sku.Identifiers[0].Uid == "" {
		SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
			SaleTransactionId:      saleTransaction.Id,
			TransactionChannelType: saleTransaction.TransactionChannelType,
			OrderId:                saleTransaction.OrderId,
			RefundId:               saleTransaction.RefundId,
			StoreId:                saleTransaction.StoreId,
			TransactionId:          saleTransaction.TransactionId,
			CreatedBy:              "API",
			Error:                  "Sku.Identifiers not exist.  SkuID : " + strconv.FormatInt(saleTransactionDtl.SkuId, 10),
			Details:                "商品UID不存在！",
		}
		if err := SaleRecordIdFailMapping.Save(); err != nil {
			return "", "", err
		}
		return "", "", errors.New("Sku.Identifiers not exist")
	}
	return sku.Identifiers[0].Uid, sku.Code, nil
}

func GetPreSaleDtSeq(saleTransaction SaleTransaction, saleTransactionDtl SaleTransactionDtl) (sql.NullInt64, error) {
	if strings.ToUpper(saleTransaction.TransactionType) == "EXCHANGE" {
		//Sale order need refund saleNo
		if saleTransaction.RefundId == 0 {
			//when TransactionType="EXCHANGE".change orderId = Refund RefunId
			refundId := saleTransaction.OrderId
			refundItemId := saleTransactionDtl.OrderItemId
			successDtls, err := SaleRecordIdSuccessMapping{}.GetSaleSuccessData(0, 0, refundId, 0, refundItemId, saleTransaction.TransactionChannelType)
			if err != nil {
				SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
					SaleTransactionId:      saleTransaction.Id,
					TransactionChannelType: saleTransaction.TransactionChannelType,
					OrderId:                saleTransaction.OrderId,
					RefundId:               saleTransaction.RefundId,
					StoreId:                saleTransaction.StoreId,
					TransactionId:          saleTransaction.TransactionId,
					CreatedBy:              "API",
					Error:                  err.Error() + " OrderId:" + strconv.FormatInt(saleTransaction.OrderId, 10),
					Details:                "换货处理必须有之前的退货数据！",
				}
				if err := SaleRecordIdFailMapping.Save(); err != nil {
					return sql.NullInt64{0, false}, err
				}
				return sql.NullInt64{0, false}, err
			}
			return sql.NullInt64{successDtls[0].DtlSeq, true}, nil
		}
	} else {
		if saleTransaction.RefundId != 0 {
			successDtls, err := SaleRecordIdSuccessMapping{}.GetSaleSuccessData(0, saleTransaction.OrderId, 0, saleTransactionDtl.OrderItemId, 0, saleTransaction.TransactionChannelType)
			if err != nil {
				SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
					SaleTransactionId:      saleTransaction.Id,
					TransactionChannelType: saleTransaction.TransactionChannelType,
					OrderId:                saleTransaction.OrderId,
					RefundId:               saleTransaction.RefundId,
					StoreId:                saleTransaction.StoreId,
					TransactionId:          saleTransactionDtl.TransactionId,
					TransactionDtlId:       saleTransactionDtl.TransactionDtlId,
					CreatedBy:              "API",
					Error:                  err.Error() + " OrderId:" + strconv.FormatInt(saleTransaction.OrderId, 10) + " OrderItemId:" + strconv.FormatInt(saleTransactionDtl.OrderItemId, 10),
					Details:                "退货处理必须有之前的销售数据！",
				}
				if err := SaleRecordIdFailMapping.Save(); err != nil {
					return sql.NullInt64{0, false}, err
				}
				return sql.NullInt64{0, false}, err
			}
			return sql.NullInt64{successDtls[0].DtlSeq, true}, nil
		}
	}
	return sql.NullInt64{0, false}, nil
}

func GetCouponNoAndOfferNo(appliedSaleRecordCartOffers []AppliedSaleRecordCartOffer, orderItemId int64) (string, string) {
	for _, appliedSaleRecordCartOffer := range appliedSaleRecordCartOffers {
		itemIds := ""
		if appliedSaleRecordCartOffer.TargetItemIds != "" {
			itemIds = appliedSaleRecordCartOffer.TargetItemIds
		} else {
			itemIds = appliedSaleRecordCartOffer.ItemIds
		}
		result := strings.Index(itemIds+",", strconv.FormatInt(orderItemId, 10)+",")
		if result != -1 {
			return appliedSaleRecordCartOffer.CouponNo, appliedSaleRecordCartOffer.OfferNo
		}
	}
	return "", ""
}

func GetShopEmpEstimateSaleAmt(saleTransaction SaleTransaction, saleTransactionDtl SaleTransactionDtl, baseTrimCode string) (float64, error) {
	dtlSalesmanAmount, err := SaleRecordDtlSalesmanAmount{}.GetSaleRecordDtlSalesmanAmount(saleTransactionDtl.OrderItemId, saleTransactionDtl.RefundItemId)
	if err != nil {
		SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
			SaleTransactionId:      saleTransaction.Id,
			TransactionChannelType: saleTransaction.TransactionChannelType,
			OrderId:                saleTransaction.OrderId,
			RefundId:               saleTransaction.RefundId,
			StoreId:                saleTransaction.StoreId,
			TransactionId:          saleTransactionDtl.TransactionId,
			TransactionDtlId:       saleTransactionDtl.TransactionDtlId,
			CreatedBy:              "API",
			Error:                  err.Error() + " OrderItemId:" + strconv.FormatInt(saleTransactionDtl.OrderItemId, 10) + " RefundItemId:" + strconv.FormatInt(saleTransactionDtl.RefundItemId, 10),
			Details:                "营业员销售业绩不存在！",
		}
		if err := SaleRecordIdFailMapping.Save(); err != nil {
			return 0, err
		}
		return 0, err
	}
	return GetToFixedPrice(dtlSalesmanAmount.SalesmanSaleAmount, baseTrimCode), nil
}

func GetGeneratedSalePayments(saleTransaction SaleTransaction, inUserID, baseTrimCode string, saleMst SaleMst) ([]SalePayment, error) {
	var salePayments []SalePayment
	saleTransactionPayments, err := SaleTransactionPayment{}.GetSaleTransactionPayment(saleTransaction.Id)
	if err != nil {
		SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
			SaleTransactionId:      saleTransaction.Id,
			TransactionChannelType: saleTransaction.TransactionChannelType,
			OrderId:                saleTransaction.OrderId,
			RefundId:               saleTransaction.RefundId,
			StoreId:                saleTransaction.StoreId,
			TransactionId:          saleTransaction.TransactionId,
			CreatedBy:              "API",
			Error:                  err.Error() + " TransactionId:" + strconv.FormatInt(saleTransaction.TransactionId, 10),
			Details:                "支付信息不存在！",
		}
		if err := SaleRecordIdFailMapping.Save(); err != nil {
			return nil, err
		}
		return nil, err
	}
	for _, stp := range saleTransactionPayments {
		if stp.PayMethod == "MILEAGE" {
			continue
		}
		paymentCode, payCreditCardFirmCode, err := getPaymentCodeAndPayCreditCardFirmCode(stp.PayMethod)
		if err != nil {
			return nil, err
		}
		creditCardFirmCode := sql.NullString{"", false}
		if payCreditCardFirmCode != "" {
			creditCardFirmCode = sql.NullString{payCreditCardFirmCode, true}
		}
		paymentAmt := GetToFixedPrice(stp.PayAmt, baseTrimCode)
		if saleTransaction.RefundId != 0 {
			paymentAmt = GetToFixedPrice(stp.PayAmt, baseTrimCode) * -1
		}
		salePayment := SalePayment{
			SaleNo:             saleMst.SaleNo,
			SeqNo:              stp.SeqNo,
			PaymentCode:        paymentCode,
			PaymentAmt:         paymentAmt,
			InUserID:           inUserID,
			ModiUserID:         inUserID,
			SendFlag:           "R",
			CreditCardFirmCode: creditCardFirmCode,
			TransactionId:      saleMst.TransactionId,
			SaleTransactionId:  saleMst.SaleTransactionId,
		}
		salePayments = append(salePayments, salePayment)
	}
	return salePayments, nil
}

func getPromotionEventByOfferNo(offerNo string, saleTransaction SaleTransaction, saleTransactionDtl SaleTransactionDtl) (*PromotionEvent, error) {
	promotionEvent, err := PromotionEvent{}.GetPromotionEvent(offerNo)
	if promotionEvent == nil || promotionEvent.EventNo == "" {
		err = errors.New("PromotionEvent的EventNo为空!")
	}
	if err != nil {
		eventNo := ""
		if promotionEvent != nil {
			eventNo = promotionEvent.EventNo
		}
		SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
			SaleTransactionId:      saleTransaction.Id,
			TransactionChannelType: saleTransaction.TransactionChannelType,
			OrderId:                saleTransaction.OrderId,
			RefundId:               saleTransaction.RefundId,
			StoreId:                saleTransaction.StoreId,
			TransactionId:          saleTransactionDtl.TransactionId,
			TransactionDtlId:       saleTransactionDtl.TransactionDtlId,
			CreatedBy:              "API",
			Error:                  err.Error() + " OfferNo:" + offerNo + " EventNo:" + eventNo,
			Details:                "商品参加的活动不存在！",
		}
		if err := SaleRecordIdFailMapping.Save(); err != nil {
			return nil, err
		}
		return nil, err
	}
	return promotionEvent, nil
}

func GetPromotionEventAndCouponNo(appliedSaleRecordCartOffers []AppliedSaleRecordCartOffer, saleTransaction SaleTransaction, saleTransactionDtl SaleTransactionDtl) (*PromotionEvent, string, error) {
	couponNo, offerNo := GetCouponNoAndOfferNo(appliedSaleRecordCartOffers, saleTransactionDtl.OrderItemId)
	if couponNo != "" {
		return nil, couponNo, nil
	}
	if offerNo != "" {
		//if offerNo exist,promotionEvent can not be nil.
		promotionEvent, err := getPromotionEventByOfferNo(offerNo, saleTransaction, saleTransactionDtl)
		if err != nil {
			return nil, "", err
		}
		return promotionEvent, "", err
	}
	return nil, couponNo, nil
}

func GetNormalSaleTypeCode(promotionEvent *PromotionEvent, couponNo string) (string, error) {
	if couponNo != "" {
		return "2", nil
	}
	if promotionEvent != nil && couponNo == "" {
		if promotionEvent.EventTypeCode == "01" || promotionEvent.EventTypeCode == "02" || promotionEvent.EventTypeCode == "03" {
			return "1", nil
		} else if promotionEvent.EventTypeCode == "B" || promotionEvent.EventTypeCode == "C" ||
			promotionEvent.EventTypeCode == "G" || promotionEvent.EventTypeCode == "M" || promotionEvent.EventTypeCode == "P" ||
			promotionEvent.EventTypeCode == "R" || promotionEvent.EventTypeCode == "V" {
			return "2", nil
		}
	}
	return "0", nil
}

func GetEventNo(promotionEvent *PromotionEvent) (sql.NullInt64, error) {
	if promotionEvent != nil {
		eventNumber, err := strconv.ParseInt(promotionEvent.EventNo, 10, 64)
		if err != nil {
			return sql.NullInt64{0, false}, err
		}
		if promotionEvent.EventTypeCode == "01" || promotionEvent.EventTypeCode == "02" || promotionEvent.EventTypeCode == "03" {
			if eventNumber != 0 {
				return sql.NullInt64{eventNumber, true}, nil
			}
		}
	}
	return sql.NullInt64{0, false}, nil
}

//GetUseMileageSettleTypeAndEventTypeCode return  UseMileageSettleType and EventTypeCode, UseMileageSettleType default "1"
func GetUseMileageSettleTypeAndEventTypeCode(promotionEvent *PromotionEvent) (string, sql.NullString) {
	if promotionEvent != nil {
		if promotionEvent.EventTypeCode == "01" || promotionEvent.EventTypeCode == "02" || promotionEvent.EventTypeCode == "03" {
			return "0", sql.NullString{promotionEvent.EventTypeCode, true}
		}
	}
	return "1", sql.NullString{"", false}
}

//GetSaleEventNormalSaleRecognitionChk default false
func GetSaleEventNormalSaleRecognitionChk(promotionEvent *PromotionEvent) bool {
	if promotionEvent != nil && promotionEvent.EventTypeCode == "01" {
		return true
	}
	return false
}

//GetSaleEventAutoDiscountAmt return SaleEventAutoDiscountAmt
func GetSaleEventAutoDiscountAmt(promotionEvent *PromotionEvent, saleTransactionDtl SaleTransactionDtl, baseTrimCode string) float64 {
	if promotionEvent != nil {
		if promotionEvent.EventTypeCode == "02" || promotionEvent.EventTypeCode == "03" {
			return GetToFixedPrice(saleTransactionDtl.TotalDistributedCartOfferPrice+saleTransactionDtl.TotalDistributedItemOfferPrice, baseTrimCode)
		}
	}
	return 0
}

//GetSaleEventSaleBaseAmt_SaleEventDiscountBaseAmt return SaleEventSaleBaseAmt and SaleEventDiscountBaseAmt
func GetSaleEventSaleBaseAmt_SaleEventDiscountBaseAmt(promotionEvent *PromotionEvent) (float64, float64) {
	if promotionEvent != nil {
		if promotionEvent.EventTypeCode == "01" || promotionEvent.EventTypeCode == "02" {
			return promotionEvent.SaleBaseAmt, promotionEvent.DiscountBaseAmt
		}
	}
	return 0, 0
}

//GetPrimaryCustEventNo_PrimaryEventTypeCode_PrimaryEventSettleTypeCode return PrimaryCustEventNo and PrimaryEventTypeCode and PrimaryEventSettleTypeCodes
func GetPrimaryCustEventNo_PrimaryEventTypeCode_PrimaryEventSettleTypeCode(promotionEvent *PromotionEvent, couponNo string, saleTransaction SaleTransaction, saleTransactionDtl SaleTransactionDtl) (sql.NullInt64, sql.NullString, sql.NullString, error) {
	if couponNo == "" && promotionEvent != nil {
		eventN, err := strconv.ParseInt(promotionEvent.EventNo, 10, 64)
		if err != nil {
			return sql.NullInt64{0, false}, sql.NullString{"", false}, sql.NullString{"", false}, err
		}
		if eventN != 0 && (promotionEvent.EventTypeCode == "B" || promotionEvent.EventTypeCode == "C" || promotionEvent.EventTypeCode == "P" || promotionEvent.EventTypeCode == "V") {
			return sql.NullInt64{eventN, true}, sql.NullString{promotionEvent.EventTypeCode, true}, sql.NullString{"1", true}, nil
		}
	}
	if couponNo != "" {
		coupenEvent, err := PostCouponEvent{}.GetPostCoupenEvent(saleTransactionDtl.BrandCode)
		if err != nil {
			saleRecordIdFailMapping := &SaleRecordIdFailMapping{
				SaleTransactionId:      saleTransaction.Id,
				TransactionChannelType: saleTransaction.TransactionChannelType,
				OrderId:                saleTransaction.OrderId,
				RefundId:               saleTransaction.RefundId,
				StoreId:                saleTransaction.StoreId,
				TransactionId:          saleTransactionDtl.TransactionId,
				TransactionDtlId:       saleTransactionDtl.TransactionDtlId,
				CreatedBy:              "API",
				Error:                  err.Error() + " BrandCode:" + saleTransactionDtl.BrandCode,
				Details:                "优惠券信息不存在！",
			}
			if err := saleRecordIdFailMapping.Save(); err != nil {
				return sql.NullInt64{0, false}, sql.NullString{"", false}, sql.NullString{"", false}, err
			}
			return sql.NullInt64{0, false}, sql.NullString{"", false}, sql.NullString{"", false}, err
		}
		return sql.NullInt64{coupenEvent.EventNo, true}, sql.NullString{"C", true}, sql.NullString{"1", true}, nil
	}
	return sql.NullInt64{0, false}, sql.NullString{"", false}, sql.NullString{"", false}, nil
}

// ValidCustomerCustNo Check Customer Information
func ValidCustomerCustNo(saleMst SaleMst, promotionEvent *PromotionEvent, saleTransaction SaleTransaction, saleTransactionDtl SaleTransactionDtl) error {
	if promotionEvent != nil {
		if promotionEvent.EventTypeCode == "B" || promotionEvent.EventTypeCode == "C" || promotionEvent.EventTypeCode == "P" {
			if !saleMst.CustNo.Valid {
				saleRecordIdFailMapping := &SaleRecordIdFailMapping{
					SaleTransactionId:      saleTransaction.Id,
					TransactionChannelType: saleTransaction.TransactionChannelType,
					OrderId:                saleTransaction.OrderId,
					RefundId:               saleTransaction.RefundId,
					StoreId:                saleTransaction.StoreId,
					TransactionId:          saleTransactionDtl.TransactionId,
					TransactionDtlId:       saleTransactionDtl.TransactionDtlId,
					CreatedBy:              "API",
					Error:                  promotionEvent.EventTypeCode + "类型必须要有顾客信息!",
					Details:                promotionEvent.EventTypeCode + "类型必须要有顾客信息!",
				}
				if err := saleRecordIdFailMapping.Save(); err != nil {
					return err
				}
				return errors.New("百货店VIP,打折劵Event,品牌折扣型,优秀顾客型,积分型 必须要有顾客信息!")
			}
		}
	}
	return nil
}

//GetSecondaryCustEventNo_SecondaryEventTypeCode_SecondaryEventSettleTypeCode return SecondaryCustEventNo and SecondaryEventTypeCode and SecondaryEventSettleTypeCode
func GetSecondaryCustEventNo_SecondaryEventTypeCode_SecondaryEventSettleTypeCode(promotionEvent *PromotionEvent) (sql.NullInt64, sql.NullString, sql.NullString, error) {
	if promotionEvent != nil {
		eventNumber, err := strconv.ParseInt(promotionEvent.EventNo, 10, 64)
		if err != nil {
			return sql.NullInt64{0, false}, sql.NullString{"", false}, sql.NullString{"", false}, err
		}
		if eventNumber != 0 && (promotionEvent.EventTypeCode == "G" || promotionEvent.EventTypeCode == "M" || promotionEvent.EventTypeCode == "R") {
			return sql.NullInt64{eventNumber, true}, sql.NullString{promotionEvent.EventTypeCode, true}, sql.NullString{"1", true}, nil
		}
	}
	return sql.NullInt64{0, false}, sql.NullString{"", false}, sql.NullString{"", false}, nil
}

//GetEventAutoDiscountAmt_EventDecisionDiscountAmt: EventAutoDiscountAmt==EventDecisionDiscountAmt
func GetEventAutoDiscountAmt_EventDecisionDiscountAmt(normalSaleTypeCode, baseTrimCode string, saleTransactionDtl SaleTransactionDtl) (float64, float64) {
	if normalSaleTypeCode == "2" {
		eventAutoDiscountAmt := GetToFixedPrice(saleTransactionDtl.TotalDistributedCartOfferPrice+saleTransactionDtl.TotalDistributedItemOfferPrice, baseTrimCode)
		eventDecisionDiscountAmt := eventAutoDiscountAmt
		return eventAutoDiscountAmt, eventDecisionDiscountAmt
	}
	return 0, 0
}

//GetProduct by ProductId
func GetProduct(saleTransaction SaleTransaction, saleTransactionDtl SaleTransactionDtl) (Product, error) {
	product, err := Product{}.GetProductById(saleTransactionDtl.ProductId)
	if err != nil {
		SaleRecordIdFailMapping := &SaleRecordIdFailMapping{
			SaleTransactionId:      saleTransaction.Id,
			TransactionChannelType: saleTransaction.TransactionChannelType,
			OrderId:                saleTransaction.OrderId,
			RefundId:               saleTransaction.RefundId,
			StoreId:                saleTransaction.StoreId,
			TransactionId:          saleTransactionDtl.TransactionId,
			TransactionDtlId:       saleTransactionDtl.TransactionDtlId,
			CreatedBy:              "API",
			Error:                  err.Error() + " ProductId:" + strconv.FormatInt(saleTransactionDtl.ProductId, 10),
			Details:                "商品款式不存在!",
		}
		if err := SaleRecordIdFailMapping.Save(); err != nil {
			return Product{}, err
		}
		return Product{}, err
	}
	return product, nil
}

//GetUseMileage return UseMileage
func GetUseMileage(normalSaleTypeCode string, saleTransactionDtl SaleTransactionDtl) float64 {
	if normalSaleTypeCode != "1" {
		return math.Abs(saleTransactionDtl.MileagePrice)
	}
	return 0
}
