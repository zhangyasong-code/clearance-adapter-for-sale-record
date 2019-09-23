package adapter

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/go-xorm/core"
	"github.com/go-xorm/xorm"
	"github.com/pangpanglabs/goetl"
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
	// start, _ := time.Parse("2006-01-02", "2019-08-08")
	// end, _ := time.Parse("2006-01-02", "2019-08-09")
	//分页查询   一次查1000条
	skipCount := 0
	data := ctx.Value("data")
	dataMap := data.(map[string]string)
	brandCode := dataMap["brandCode"]
	transactionChannelType := dataMap["channelType"]
	startAt := dataMap["startAt"]
	endAt := dataMap["endAt"]
	for {
		var stsAndStds []struct {
			SaleTransaction    models.SaleTransaction    `xorm:"extends"`
			SaleTransactionDtl models.SaleTransactionDtl `xorm:"extends"`
		}
		query := func() xorm.Interface {
			q := factory.GetCfsrEngine().Table("sale_transaction").
				Select("sale_transaction.*,sale_transaction_dtl.*").
				Join("INNER", "sale_transaction_dtl", "sale_transaction_dtl.transaction_id = sale_transaction.transaction_id").
				Where("1 = 1")
			if brandCode != "" {
				q.And("sale_transaction_dtl.brand_code = ?", brandCode)
			}
			if transactionChannelType != "" {
				q.And("sale_transaction.transaction_channel_type = ?", transactionChannelType)
			}
			if startAt != "" && endAt != "" {
				st, _ := time.Parse("2006-01-02 15:04:05", startAt)
				et, _ := time.Parse("2006-01-02 15:04:05", endAt)
				h, _ := time.ParseDuration("-8h")
				q.And("sale_transaction.sale_date >= ?", st.Add(h)).And("sale_transaction.sale_date < ?", et.Add(h))
			}
			return q
		}
		if err := query().Limit(maxResultCount, skipCount).Find(&stsAndStds); err != nil {
			return nil, err
		}
		for _, stsAndStd := range stsAndStds {
			check := true
			for _, saleTransaction := range saleTransactions {
				if stsAndStd.SaleTransaction.OrderId == saleTransaction.OrderId && stsAndStd.SaleTransaction.RefundId == saleTransaction.RefundId {
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
	var endSeq int
	var dtSeq int64
	var saleEventNormalSaleRecognitionChk bool
	var startStr, strSeqNo, saleMode, eANCode, normalSaleTypeCode, useMileageSettleType, offerNo string
	var custMileagePolicyNo, primaryCustEventNo, eventNo, secondaryCustEventNo, preSaleDtSeq sql.NullInt64
	var primaryEventTypeCode, secondaryEventTypeCode, eventTypeCode, primaryEventSettleTypeCode, secondaryEventSettleTypeCode, preSaleNo, creditCardFirmCode sql.NullString
	var saleEventSaleBaseAmt, saleEventDiscountBaseAmt, saleEventAutoDiscountAmt, saleEventManualDiscountAmt, saleVentDecisionDiscountAmt,
		discountAmt, saleEventDiscountAmtForConsumer, actualSaleAmt, saleEventFee, normalFee, normalFeeRate, saleEventFeeRate float64

	saleTAndSaleTDtls, ok := source.(models.SaleTAndSaleTDtls)
	if !ok {
		return nil, errors.New("Convert Failed")
	}
	saleMsts := make([]models.SaleMst, 0)
	saleDtls := make([]models.SaleDtl, 0)
	salePayments := make([]models.SalePayment, 0)
	for i, saleTransaction := range saleTAndSaleTDtls.SaleTransactions {
		saleDate := saleTransaction.SaleDate.Format("20060102")

		//get store
		store, err := models.Store{}.GetStore(saleTransaction.StoreId)
		if err != nil {
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransaction.TransactionId, CreatedBy: "batch-job", Error: err.Error() + " StoreId:" + strconv.FormatInt(saleTransaction.StoreId, 10)}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return nil, err
			}
			continue
		}

		//get last endSeq and startStr in csl SaleMst
		if i == 0 {
			lastSeq, err := models.SaleMst{}.GetlastSeq(store.Code, saleDate)
			if err != nil {
				return nil, err
			}
			seq, str, err := models.SaleMst{}.GetSeqAndStartStr(lastSeq)
			if err != nil {
				return nil, err
			}
			endSeq = seq
			startStr = str
		}

		//Get SequenceNumber
		sequenceNumber, nextSeq, str, err := models.SaleMst{}.GetSequenceNumber(endSeq, startStr)
		if err != nil {
			return nil, err
		}
		endSeq = nextSeq
		startStr = str
		saleNo := store.Code + saleDate[len(saleDate)-6:len(saleDate)] + MSLV2_POS + sequenceNumber

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
			return nil, err
		}
		//sum quantity , total_sale_price , total_discount_price
		res, err := models.AssortedSaleRecordDtl{}.GetSumsFields(saleTransaction.TransactionId)
		if err != nil {
			return nil, err
		}
		//Sale S 销售  Refund R 退货
		saleMode = ""
		use_type := models.UseTypeEarn
		complexShopSeqNo := ""

		preSaleNo = sql.NullString{"", false}
		if saleTransaction.RefundId == 0 {
			saleMode = Sale
			complexShopSeqNo = strconv.FormatInt(saleTransaction.OrderId, 10)
		} else {
			saleMode = Refund
			use_type = models.UseTypeEarnCancel
			complexShopSeqNo = strconv.FormatInt(saleTransaction.RefundId, 10)
			successDtls, err := models.SaleRecordIdSuccessMapping{}.Get(saleTransaction.OrderId, 0)
			if err != nil {
				SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransaction.TransactionId, CreatedBy: "batch-job",
					Error: err.Error() + " OrderId:" + strconv.FormatInt(saleTransaction.OrderId, 10) + " RefundId:" + strconv.FormatInt(saleTransaction.RefundId, 10)}
				if err := SaleRecordIdFailMapping.Save(); err != nil {
					return nil, err
				}
				continue
			}
			preSaleNo = sql.NullString{successDtls[0].SaleNo, true}
		}
		//get mileage
		mileage, err := models.PostMileage{}.GetMileage(saleTransaction.CustomerId, saleTransaction.TransactionId, use_type)
		if err != nil {
			return nil, err
		}
		if mileage.CustMileagePolicyNo != 0 {
			custMileagePolicyNo = sql.NullInt64{mileage.CustMileagePolicyNo, true}
		}
		var brand models.Brand
		if mileage.BrandId != 0 {
			brand, err = models.Product{}.GetBrandById(mileage.BrandId)
			if err != nil {
				SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransaction.TransactionId, CreatedBy: "batch-job", Error: err.Error() + " BrandId:" + strconv.FormatInt(mileage.BrandId, 10)}
				if err := SaleRecordIdFailMapping.Save(); err != nil {
					return nil, err
				}
				continue
			}
		}

		feeAmt, err := models.PostSaleRecordFee{}.GetSumFeeAmount(saleTransaction.TransactionId)
		if err != nil {
			return nil, err
		}

		colleagues, err := models.Colleagues{}.GetColleaguesAuth(saleTransaction.SalesmanId)
		if err != nil {
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransaction.TransactionId, CreatedBy: "batch-job", Error: err.Error() + " SalesmanId:" + strconv.FormatInt(saleTransaction.SalesmanId, 10)}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return nil, err
			}
			continue
		}

		saleMst := models.SaleMst{
			SaleNo:                      saleNo,
			SeqNo:                       seqNo,
			PosNo:                       MSLV2_POS,
			Dates:                       saleDate,
			ShopCode:                    store.Code,
			SaleMode:                    saleMode,
			CustNo:                      strconv.FormatInt(saleTransaction.CustomerId, 10),
			CustCardNo:                  sql.NullString{"", false},
			CustMileagePolicyNo:         custMileagePolicyNo,
			PrimaryCustEventNo:          sql.NullInt64{0, false},
			SecondaryCustEventNo:        sql.NullInt64{0, false},
			DepartStoreReceiptNo:        saleTransaction.OuterOrderNo,
			CustDivisionCode:            sql.NullString{"", false},
			MileageCustChangeStatusCode: sql.NullString{"", false},
			CustGradeCode:               sql.NullString{"", false},
			CustBrandCode:               brand.Code,
			PreSaleNo:                   preSaleNo,
			SaleQty:                     int64(res[0]),
			SaleAmt:                     res[1],
			DiscountAmt:                 saleTransaction.TotalDiscountPrice,
			ChinaFISaleAmt:              saleTransaction.TotalSalePrice,
			EstimateSaleAmt:             saleTransaction.TotalTransactionPrice,
			SellingAmt:                  saleTransaction.TotalTransactionPrice,
			FeeAmt:                      feeAmt,
			ActualSaleAmt:               saleTransaction.TotalTransactionPrice - feeAmt,
			UseMileage:                  saleTransaction.Mileage,
			ObtainMileage:               mileage.PointAmount,
			InUserID:                    colleagues.UserName,
			InDateTime:                  saleTransaction.SaleDate,
			ModiUserID:                  colleagues.UserName,
			ModiDateTime:                saleTransaction.SaleDate,
			SendState:                   "",
			SendFlag:                    NotSynChronized,
			ActualSellingAmt:            saleTransaction.TotalTransactionPrice,
			EstimateSaleAmtForConsumer:  saleTransaction.TotalTransactionPrice,
			ShopEmpEstimateSaleAmt:      saleTransaction.TotalTransactionPrice,
			DiscountAmtAsCost:           0,
			ComplexShopSeqNo:            complexShopSeqNo,
			SaleOfficeCode:              MSLv2_0,
			Freight:                     sql.NullFloat64{0, false},
			TMall_UseMileage:            sql.NullFloat64{0, false},
			TMall_ObtainMileage:         sql.NullFloat64{0, false},
			TransactionId:               saleTransaction.TransactionId,
		}
		dtSeq = 0
		for _, saleTransactionDtl := range saleTAndSaleTDtls.SaleTransactionDtls {
			if saleTransactionDtl.TransactionId == saleTransaction.TransactionId {
				dtSeq += 1
				saleMst.BrandCode = saleTransactionDtl.BrandCode
				eventNo = sql.NullInt64{0, false}
				primaryCustEventNo = sql.NullInt64{0, false}
				primaryEventTypeCode = sql.NullString{"", false}
				secondaryCustEventNo = sql.NullInt64{0, false}
				secondaryEventTypeCode = sql.NullString{"", false}
				eventTypeCode = sql.NullString{"", false}
				saleEventSaleBaseAmt = 0
				saleEventDiscountBaseAmt = 0
				normalSaleTypeCode = "0"
				saleEventAutoDiscountAmt = 0
				saleEventManualDiscountAmt = 0
				saleVentDecisionDiscountAmt = 0
				discountAmt = 0
				saleEventDiscountAmtForConsumer = 0
				primaryEventSettleTypeCode = sql.NullString{"", false}
				secondaryEventSettleTypeCode = sql.NullString{"", false}
				useMileageSettleType = "1"
				custMileagePolicyNo = sql.NullInt64{0, false}
				offerNo = ""
				saleEventFee = 0
				normalFee = 0
				normalFeeRate = 0
				saleEventFeeRate = 0
				saleEventNormalSaleRecognitionChk = false
				if saleTransactionDtl.TotalDiscountPrice != 0 || saleTransactionDtl.TotalDistributedItemOfferPrice != 0 || saleTransactionDtl.TotalDistributedCartOfferPrice != 0 {
					if saleTransactionDtl.TotalDistributedItemOfferPrice != 0 {
						appliedOrderItemOffer, err := models.AppliedOrderItemOffer{}.GetAppliedOrderItemOffer(saleTransactionDtl.OrderItemId)
						if err != nil {
							SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransactionDtl.TransactionId, TransactionDtlId: saleTransactionDtl.Id, CreatedBy: "batch-job", Error: err.Error() + " OrderItemId:" + strconv.FormatInt(saleTransactionDtl.OrderItemId, 10)}
							if err := SaleRecordIdFailMapping.Save(); err != nil {
								return nil, err
							}
							continue
						}
						offerNo = appliedOrderItemOffer.OfferNo
					}
					if saleTransactionDtl.TotalDistributedCartOfferPrice != 0 {
						appliedOrderCartOffer, err := models.AppliedOrderCartOffer{}.GetAppliedOrderCartOffer(saleTransaction.OrderId)
						if err != nil {
							SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransactionDtl.TransactionId, TransactionDtlId: saleTransactionDtl.Id, CreatedBy: "batch-job", Error: err.Error() + " OrderId:" + strconv.FormatInt(saleTransaction.OrderId, 10)}
							if err := SaleRecordIdFailMapping.Save(); err != nil {
								return nil, err
							}
							continue
						}
						offerNo = appliedOrderCartOffer.OfferNo
					}
					if offerNo != "" {
						promotionEvent, err := models.PromotionEvent{}.GetPromotionEvent(offerNo)
						if err != nil {
							SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransactionDtl.TransactionId, TransactionDtlId: saleTransactionDtl.Id, CreatedBy: "batch-job", Error: err.Error() + " OfferNo:" + offerNo}
							if err := SaleRecordIdFailMapping.Save(); err != nil {
								return nil, err
							}
							continue
						}
						eventN, err := strconv.ParseInt(promotionEvent.EventNo, 10, 64)
						if err != nil {
							return nil, err
						}
						if promotionEvent.EventTypeCode == "01" || promotionEvent.EventTypeCode == "02" || promotionEvent.EventTypeCode == "03" {
							normalSaleTypeCode = "1"
							useMileageSettleType = "0"
							saleEventSaleBaseAmt = promotionEvent.SaleBaseAmt
							saleEventDiscountBaseAmt = promotionEvent.DiscountBaseAmt
							saleEventNormalSaleRecognitionChk = true
							if eventN != 0 {
								eventNo = sql.NullInt64{eventN, true}
							}
							if promotionEvent.EventTypeCode != "" {
								eventTypeCode = sql.NullString{promotionEvent.EventTypeCode, true}
							}
						} else if promotionEvent.EventTypeCode == "B" || promotionEvent.EventTypeCode == "C" ||
							promotionEvent.EventTypeCode == "G" || promotionEvent.EventTypeCode == "M" || promotionEvent.EventTypeCode == "P" ||
							promotionEvent.EventTypeCode == "R" || promotionEvent.EventTypeCode == "V" {
							normalSaleTypeCode = "2"
							if eventN != 0 && (promotionEvent.EventTypeCode == "B" || promotionEvent.EventTypeCode == "C" || promotionEvent.EventTypeCode == "P" || promotionEvent.EventTypeCode == "V") {
								primaryCustEventNo = sql.NullInt64{eventN, true}
								primaryEventTypeCode = sql.NullString{promotionEvent.EventTypeCode, true}
								primaryEventSettleTypeCode = sql.NullString{"1", true}
							}
							if eventN != 0 && (promotionEvent.EventTypeCode == "G" || promotionEvent.EventTypeCode == "M" || promotionEvent.EventTypeCode == "R") {
								secondaryCustEventNo = sql.NullInt64{eventN, true}
								secondaryEventTypeCode = sql.NullString{promotionEvent.EventTypeCode, true}
								secondaryEventSettleTypeCode = sql.NullString{"1", true}
							}
						}
					}
				}

				sku, err := models.Product{}.GetSkuBySkuId(saleTransactionDtl.SkuId)
				if err != nil {
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransactionDtl.TransactionId, TransactionDtlId: saleTransactionDtl.Id, CreatedBy: "batch-job", Error: err.Error() + " SkuId:" + strconv.FormatInt(saleTransactionDtl.SkuId, 10)}
					if err := SaleRecordIdFailMapping.Save(); err != nil {
						return nil, err
					}
					continue
				}

				eANCode = ""
				if len(sku.Identifiers) != 0 {
					if sku.Identifiers[0].Uid == "" {
						eANCode = sku.Code
					} else {
						eANCode = sku.Identifiers[0].Uid
					}
				}
				product, err := models.Product{}.GetProductById(saleTransactionDtl.ProductId)
				if err != nil {
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransactionDtl.TransactionId, TransactionDtlId: saleTransactionDtl.Id, CreatedBy: "batch-job", Error: err.Error() + " ProductId:" + strconv.FormatInt(saleTransactionDtl.ProductId, 10)}
					if err := SaleRecordIdFailMapping.Save(); err != nil {
						return nil, err
					}
					continue
				}
				priceTypeCode, err := models.SaleMst{}.GetPriceTypeCode(saleTransactionDtl.BrandCode, product.Code)
				if err != nil {
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransactionDtl.TransactionId, TransactionDtlId: saleTransactionDtl.Id, CreatedBy: "batch-job", Error: err.Error() + " BrandCode:" + saleTransactionDtl.BrandCode + " productCode:" + product.Code}
					if err := SaleRecordIdFailMapping.Save(); err != nil {
						return nil, err
					}
					continue
				}
				supGroupCode, err := models.SaleMst{}.GetSupGroupCode(saleTransactionDtl.BrandCode, product.Code)
				if err != nil {
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransactionDtl.TransactionId, TransactionDtlId: saleTransactionDtl.Id, CreatedBy: "batch-job", Error: err.Error() + " BrandCode:" + saleTransactionDtl.BrandCode + " productCode:" + product.Code}
					if err := SaleRecordIdFailMapping.Save(); err != nil {
						return nil, err
					}
					continue
				}
				discountAmt = saleTransactionDtl.TotalTransactionPrice - saleTransactionDtl.TotalDistributedPaymentPrice
				postMileageDtl, err := models.PostMileage{}.GetPostMileageDtl(saleTransactionDtl.Id, models.UseTypeUsed)
				if err != nil {
					return nil, err
				}
				if postMileageDtl.CustMileagePolicyNo != 0 {
					custMileagePolicyNo = sql.NullInt64{postMileageDtl.CustMileagePolicyNo, true}
				}
				postSaleRecordFee, err := models.PostSaleRecordFee{}.GetPostSaleRecordFee(saleTransactionDtl.OrderItemId, saleTransactionDtl.RefundItemId)
				if err != nil {
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransactionDtl.TransactionId,
						TransactionDtlId: saleTransactionDtl.Id, CreatedBy: "batch-job", Error: err.Error() + " OrderItemId:" + strconv.FormatInt(saleTransactionDtl.OrderItemId, 10) + " RefundItemId:" + strconv.FormatInt(saleTransactionDtl.RefundItemId, 10)}
					if err := SaleRecordIdFailMapping.Save(); err != nil {
						return nil, err
					}
					continue
				}
				if normalSaleTypeCode == "1" {
					saleEventFee = postSaleRecordFee.FeeAmount
					saleEventAutoDiscountAmt = saleTransactionDtl.TotalDistributedCartOfferPrice
					saleEventManualDiscountAmt = saleTransactionDtl.TotalDistributedCartOfferPrice
					saleVentDecisionDiscountAmt = saleTransactionDtl.TotalDistributedCartOfferPrice
					saleEventDiscountAmtForConsumer = saleTransactionDtl.TotalDistributedCartOfferPrice
					saleEventFeeRate = postSaleRecordFee.AppliedFeeRate
				} else {
					normalFee = postSaleRecordFee.FeeAmount
					normalFeeRate = postSaleRecordFee.AppliedFeeRate
				}
				actualSaleAmt = saleTransactionDtl.TotalDistributedPaymentPrice - (postSaleRecordFee.AppliedFeeRate)/100*saleTransactionDtl.TotalDistributedPaymentPrice
				if saleTransaction.RefundId != 0 {
					successDtls, err := models.SaleRecordIdSuccessMapping{}.Get(saleTransaction.OrderId, saleTransactionDtl.RefundItemId)
					if err != nil {
						SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransactionDtl.TransactionId,
							TransactionDtlId: saleTransactionDtl.Id, CreatedBy: "batch-job", Error: err.Error() + " OrderId:" + strconv.FormatInt(saleTransaction.OrderId, 10) + " RefundItemId:" + strconv.FormatInt(saleTransactionDtl.RefundItemId, 10)}
						if err := SaleRecordIdFailMapping.Save(); err != nil {
							return nil, err
						}
						continue
					}
					preSaleDtSeq = sql.NullInt64{successDtls[0].DtlSeq, false}
				}
				saleDtl := models.SaleDtl{
					SaleNo:                            saleNo,
					ShopCode:                          store.Code,
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
					ProdCode:                          sku.Code,
					EANCode:                           eANCode,
					PriceTypeCode:                     priceTypeCode,
					SupGroupCode:                      supGroupCode,
					SaipType:                          SaipType,
					NormalPrice:                       saleTransactionDtl.ListPrice,
					Price:                             saleTransactionDtl.SalePrice,
					PriceDecisionDate:                 saleDate,
					SaleQty:                           saleTransactionDtl.Quantity,
					SaleAmt:                           saleTransactionDtl.TotalTransactionPrice,
					EventAutoDiscountAmt:              saleTransactionDtl.TotalDistributedCartOfferPrice + saleTransactionDtl.TotalDistributedItemOfferPrice,
					EventDecisionDiscountAmt:          saleTransactionDtl.TotalDistributedCartOfferPrice + saleTransactionDtl.TotalDistributedItemOfferPrice,
					SaleEventSaleBaseAmt:              saleEventSaleBaseAmt,
					SaleEventDiscountBaseAmt:          saleEventDiscountBaseAmt,
					SaleEventNormalSaleRecognitionChk: saleEventNormalSaleRecognitionChk,
					SaleEventInterShopSalePermitChk:   false,
					SaleEventAutoDiscountAmt:          saleEventAutoDiscountAmt,
					SaleEventManualDiscountAmt:        saleEventManualDiscountAmt,
					SaleVentDecisionDiscountAmt:       saleVentDecisionDiscountAmt,
					ChinaFISaleAmt:                    saleTransactionDtl.TotalDistributedPaymentPrice,
					EstimateSaleAmt:                   saleTransactionDtl.TotalDistributedPaymentPrice,
					SellingAmt:                        saleTransactionDtl.TotalDistributedPaymentPrice,
					NormalFee:                         normalFee,
					SaleEventFee:                      saleEventFee,
					ActualSaleAmt:                     actualSaleAmt,
					UseMileage:                        postMileageDtl.PointPrice,
					PreSaleNo:                         preSaleNo,
					PreSaleDtSeq:                      preSaleDtSeq,
					NormalFeeRate:                     normalFeeRate,
					SaleEventFeeRate:                  saleEventFeeRate,
					InUserID:                          colleagues.UserName,
					InDateTime:                        saleTransaction.SaleDate,
					ModiUserID:                        colleagues.UserName,
					ModiDateTime:                      saleTransaction.SaleDate,
					SendState:                         "",
					SendFlag:                          NotSynChronized,
					DiscountAmt:                       discountAmt,
					DiscountAmtAsCost:                 0,
					UseMileageSettleType:              useMileageSettleType,
					EstimateSaleAmtForConsumer:        saleTransactionDtl.TotalDistributedPaymentPrice,
					SaleEventDiscountAmtForConsumer:   saleEventDiscountAmtForConsumer,
					ShopEmpEstimateSaleAmt:            saleTransactionDtl.TotalDistributedPaymentPrice,
					PromotionID:                       sql.NullInt64{0, false},
					TMallEventID:                      sql.NullInt64{0, false},
					TMall_ObtainMileage:               sql.NullFloat64{0, false},
					SaleOfficeCode:                    MSLv2_0,
					OrderItemId:                       saleTransactionDtl.OrderItemId,
					RefundItemId:                      saleTransactionDtl.RefundItemId,
					TransactionDtlId:                  saleTransactionDtl.Id,
				}
				saleDtls = append(saleDtls, saleDtl)
			}
		}
		postOrderPayments, err := models.PostPayment{}.GetPostPayment(saleTransaction.TransactionId)
		if err != nil {
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleTransaction.TransactionId, CreatedBy: "batch-job", Error: err.Error() + " TransactionId:" + strconv.FormatInt(saleTransaction.TransactionId, 10)}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return nil, err
			}
			continue
		}
		for _, pop := range postOrderPayments {
			creditCardFirmCode = sql.NullString{"", false}
			if pop.CreditCardFirmCode != "" {
				creditCardFirmCode = sql.NullString{pop.CreditCardFirmCode, true}
			}
			salePayment := models.SalePayment{
				SaleNo:             saleNo,
				SeqNo:              pop.SeqNo,
				PaymentCode:        pop.PaymentCode,
				PaymentAmt:         pop.PaymentAmt,
				InUserID:           colleagues.UserName,
				InDateTime:         pop.InDateTime,
				ModiUserID:         colleagues.UserName,
				ModiDateTime:       pop.ModiDateTime,
				SendFlag:           "R",
				CreditCardFirmCode: creditCardFirmCode,
				TransactionId:      saleMst.TransactionId,
			}
			salePayments = append(salePayments, salePayment)
		}

		check := false
		for _, saleDtl := range saleDtls {
			if saleNo == saleDtl.SaleNo {
				for _, salePayment := range salePayments {
					if saleNo == salePayment.SaleNo {
						check = true
					}
				}
			}
		}
		if check {
			saleMsts = append(saleMsts, saleMst)
		} else {
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleMst.TransactionId, CreatedBy: "batch-job", Error: "SaleMst、SaleDtl、SalePayment数据不一致" + strconv.FormatInt(saleMst.TransactionId, 10)}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return nil, err
			}
			continue
		}
	}
	return models.SaleMstsAndSaleDtls{
		SaleMsts:     saleMsts,
		SaleDtls:     saleDtls,
		SalePayments: salePayments,
	}, nil
}

// ReadyToLoad ...
func (etl ClearanceToCslETL) ReadyToLoad(ctx context.Context, source interface{}) error {
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

	for _, saleMst := range saleMstsAndSaleDtls.SaleMsts {
		if _, err := session.Table("dbo.SaleMst").Insert(&saleMst); err != nil {
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleMst.TransactionId, CreatedBy: "batch-job", Error: err.Error() + " TransactionId:" + strconv.FormatInt(saleMst.TransactionId, 10)}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return err
			}
			session.Rollback()
			return err
		}
		//insert saleDtl
		for _, saleDtl := range saleMstsAndSaleDtls.SaleDtls {
			if saleDtl.SaleNo == saleMst.SaleNo {
				if _, err := session.Table("dbo.SaleDtl").Insert(&saleDtl); err != nil {
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleMst.TransactionId, TransactionDtlId: saleDtl.TransactionDtlId, CreatedBy: "batch-job", Error: err.Error() + " TransactionId:" + strconv.FormatInt(saleMst.TransactionId, 10)}
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
				if _, err := session.Table("dbo.SalePayment").Insert(&salePayment); err != nil {
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{TransactionId: saleMst.TransactionId, CreatedBy: "batch-job", Error: err.Error() + " SalePaymentTransactionId:" + strconv.FormatInt(salePayment.TransactionId, 10)}
					if err := SaleRecordIdFailMapping.Save(); err != nil {
						return err
					}
					session.Rollback()
					return err
				}
			}
		}
		//insert success table
		for _, salDtl := range saleMstsAndSaleDtls.SaleDtls {
			saleRecordIdSuccessMapping := &models.SaleRecordIdSuccessMapping{SaleNo: saleMst.SaleNo, CreatedBy: "batch-job",
				TransactionId: saleMst.TransactionId, OrderItemId: salDtl.OrderItemId, RefundItemId: salDtl.RefundItemId, DtlSeq: salDtl.DtSeq}
			if err := saleRecordIdSuccessMapping.CheckAndSave(); err != nil {
				return err
			}
		}
	}
	//commit session
	if err := session.Commit(); err != nil {
		return err
	}
	return nil
}
