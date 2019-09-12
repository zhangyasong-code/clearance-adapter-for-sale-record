package adapter

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/go-xorm/core"
	"github.com/pangpanglabs/goetl"
)

const (
	MSLV2_POS        = "8"
	MILEAGE_CUSTOMER = "M"
	NEW_CUSTOMER     = "N"
	MSLv2_0          = "P009"
	Refund           = "R"
	Sale             = "S"
	InUserID         = "MSLV2"
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
	for {
		var stsAndStds []struct {
			SaleTransaction    models.SaleTransaction    `xorm:"extends"`
			SaleTransactionDtl models.SaleTransactionDtl `xorm:"extends"`
		}
		if err := factory.GetCfsrEngine().Table("sale_transaction").
			Select("sale_transaction.*,sale_transaction_dtl.*").
			Join("INNER", "sale_transaction_dtl", "sale_transaction_dtl.transaction_id = sale_transaction.transaction_id").
			// Where("sale_transaction.sale_date > ?", start).
			// And("sale_transaction.sale_date < ?", end).
			Limit(maxResultCount, skipCount).Find(&stsAndStds); err != nil {
			return nil, err
		}
		for _, stsAndStd := range stsAndStds {
			check := true
			for _, saleTransaction := range saleTransactions {
				if stsAndStd.SaleTransaction.OrderId == saleTransaction.OrderId {
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
	var startStr, strSeqNo, saleMode, eventTypeCode, eANCode, normalSaleTypeCode, primaryEventTypeCode, secondaryEventTypeCode string
	var eventNo, primaryCustEventNo, secondaryCustEventNo int64
	var saleEventSaleBaseAmt, saleEventDiscountBaseAmt, saleEventAutoDiscountAmt, saleEventManualDiscountAmt, saleVentDecisionDiscountAmt,
		discountAmt, saleEventDiscountAmtForConsumer, actualSaleAmt float64

	saleTAndSaleTDtls, ok := source.(models.SaleTAndSaleTDtls)
	if !ok {
		return nil, errors.New("Convert Failed")
	}
	saleMsts := make([]models.SaleMst, 0)
	saleDtls := make([]models.SaleDtl, 0)
	for i, saleTransaction := range saleTAndSaleTDtls.SaleTransactions {
		saleDate := saleTransaction.SaleDate.Format("20060102")

		//get store
		store, err := models.Store{}.GetStore(saleTransaction.StoreId)
		if err != nil {
			return nil, err
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
		if saleTransaction.RefundId == 0 {
			saleMode = Sale
			complexShopSeqNo = strconv.FormatInt(saleTransaction.OrderId, 10)
		} else {
			saleMode = Refund
			use_type = models.UseTypeEarnCancel
			complexShopSeqNo = strconv.FormatInt(saleTransaction.RefundId, 10)
		}
		//get mileage
		mileage, err := models.PostMileage{}.GetMileage(saleTransaction.CustomerId, saleTransaction.TransactionId, use_type)
		if err != nil {
			return nil, err
		}
		brand, err := models.Product{}.GetBrandById(mileage.BrandId)
		if err != nil {
			return nil, err
		}
		feeAmt, err := models.PostSaleRecordFee{}.GetSumFeeAmount(saleTransaction.TransactionId)
		if err != nil {
			return nil, err
		}
		saleMst := models.SaleMst{
			SaleNo:                     saleNo,
			SeqNo:                      seqNo,
			PosNo:                      MSLV2_POS,
			Dates:                      saleDate,
			ShopCode:                   store.Code,
			SaleMode:                   saleMode,
			CustNo:                     strconv.FormatInt(saleTransaction.CustomerId, 10),
			CustCardNo:                 "",
			CustMileagePolicyNo:        mileage.CustMileagePolicyNo,
			DepartStoreReceiptNo:       saleTransaction.OuterOrderNo,
			CustDivisionCode:           MILEAGE_CUSTOMER,
			CustGradeCode:              strconv.FormatInt(mileage.GradeId, 10),
			CustBrandCode:              brand.Code,
			SaleQty:                    int64(res[0]),
			SaleAmt:                    res[1],
			DiscountAmt:                res[2],
			ChinaFISaleAmt:             saleTransaction.TotalSalePrice,
			EstimateSaleAmt:            saleTransaction.TotalTransactionPrice,
			SellingAmt:                 saleTransaction.TotalTransactionPrice,
			FeeAmt:                     feeAmt,
			ActualSaleAmt:              saleTransaction.TotalTransactionPrice - feeAmt,
			UseMileage:                 saleTransaction.Mileage,
			ObtainMileage:              mileage.PointAmount,
			InUserID:                   InUserID,
			InDateTime:                 saleTransaction.SaleDate,
			ModiUserID:                 InUserID,
			ModiDateTime:               saleTransaction.SaleDate,
			SendState:                  "",
			SendFlag:                   NotSynChronized,
			ActualSellingAmt:           saleTransaction.TotalTransactionPrice,
			EstimateSaleAmtForConsumer: saleTransaction.TotalTransactionPrice,
			ShopEmpEstimateSaleAmt:     saleTransaction.TotalTransactionPrice,
			DiscountAmtAsCost:          0,
			ComplexShopSeqNo:           complexShopSeqNo,
			SaleOfficeCode:             MSLv2_0,
		}
		for _, saleTransactionDtl := range saleTAndSaleTDtls.SaleTransactionDtls {
			if saleTransactionDtl.TransactionId == saleTransaction.TransactionId {
				saleMst.BrandCode = saleTransactionDtl.BrandCode
				eventNo = 0
				primaryCustEventNo = 0
				primaryEventTypeCode = ""
				secondaryCustEventNo = 0
				secondaryEventTypeCode = ""
				eventTypeCode = ""
				saleEventSaleBaseAmt = 0
				saleEventDiscountBaseAmt = 0
				normalSaleTypeCode = "0"
				saleEventAutoDiscountAmt = 0
				saleEventManualDiscountAmt = 0
				saleVentDecisionDiscountAmt = 0
				discountAmt = 0
				saleEventDiscountAmtForConsumer = 0
				if saleTransactionDtl.TotalDiscountPrice != 0 {
					appliedOrderItemOffer, err := models.AppliedOrderItemOffer{}.GetAppliedOrderItemOffer(saleTransactionDtl.OrderItemId)
					if err != nil {
						return nil, err
					}
					if appliedOrderItemOffer.OfferNo != "" {
						promotionEvent, err := models.PromotionEvent{}.GetPromotionEvent(appliedOrderItemOffer.OfferNo)
						if err != nil {
							return nil, err
						}
						eventN, err := strconv.ParseInt(promotionEvent.EventNo, 10, 64)
						if err != nil {
							return nil, err
						}
						saleEventSaleBaseAmt = promotionEvent.SaleBaseAmt
						saleEventDiscountBaseAmt = promotionEvent.DiscountBaseAmt
						if promotionEvent.EventTypeCode == "01" || promotionEvent.EventTypeCode == "02" || promotionEvent.EventTypeCode == "03" {
							normalSaleTypeCode = "1"
							eventNo = eventN
							eventTypeCode = promotionEvent.EventTypeCode
						} else if promotionEvent.EventTypeCode == "B" || promotionEvent.EventTypeCode == "C" ||
							promotionEvent.EventTypeCode == "G" || promotionEvent.EventTypeCode == "M" || promotionEvent.EventTypeCode == "P" ||
							promotionEvent.EventTypeCode == "R" || promotionEvent.EventTypeCode == "V" {
							normalSaleTypeCode = "2"
							primaryCustEventNo = eventN
							primaryEventTypeCode = promotionEvent.EventTypeCode
							secondaryCustEventNo = eventN
							secondaryEventTypeCode = promotionEvent.EventTypeCode
						}
					}
				}

				sku, err := models.Product{}.GetSkuBySkuId(saleTransactionDtl.SkuId)
				if err != nil {
					return nil, err
				}

				eANCode = ""
				if len(sku.Identifiers) != 0 {
					eANCode = sku.Identifiers[0].Uid
				}
				product, err := models.Product{}.GetProductById(saleTransactionDtl.ProductId)
				if err != nil {
					return nil, err
				}
				priceTypeCode, err := models.SaleMst{}.GetPriceTypeCode(saleTransactionDtl.BrandCode, product.Code)
				if err != nil {
					return nil, err
				}
				supGroupCode, err := models.SaleMst{}.GetSupGroupCode(saleTransactionDtl.BrandCode, product.Code)
				if err != nil {
					return nil, err
				}
				if normalSaleTypeCode == "1" {
					saleEventAutoDiscountAmt = saleTransactionDtl.TotalDistributedCartOfferPrice
					saleEventManualDiscountAmt = saleTransactionDtl.TotalDistributedCartOfferPrice
					saleVentDecisionDiscountAmt = saleTransactionDtl.TotalDistributedCartOfferPrice
					discountAmt = saleTransactionDtl.TotalDistributedCartOfferPrice
					saleEventDiscountAmtForConsumer = saleTransactionDtl.TotalDistributedCartOfferPrice
				}
				postMileageDtl, err := models.PostMileage{}.GetPostMileageDtl(saleTransactionDtl.Id, models.UseTypeUsed)
				if err != nil {
					return nil, err
				}
				postSaleRecordFee, err := models.PostSaleRecordFee{}.GetPostSaleRecordFee(saleTransactionDtl.OrderItemId, saleTransactionDtl.RefundItemId)
				if err != nil {
					return nil, err
				}
				actualSaleAmt = 0
				if postSaleRecordFee.EventFeeRate != 0 {
					//SellingAmt-SaleEventFee
					actualSaleAmt = saleTransactionDtl.TotalTransactionPrice - postSaleRecordFee.AppliedFeeRate*saleTransactionDtl.TotalSalePrice
				} else {
					//SellingAmt - NormalFee
					actualSaleAmt = saleTransactionDtl.TotalTransactionPrice - saleTransactionDtl.ItemFee
				}
				saleDtl := models.SaleDtl{
					SaleNo:                            saleNo,
					ShopCode:                          store.Code,
					BrandCode:                         saleTransactionDtl.BrandCode,
					DtSeq:                             int64(len(saleDtls) + 1),
					SeqNo:                             seqNo,
					Dates:                             saleDate,
					PosNo:                             MSLV2_POS,
					NormalSaleTypeCode:                normalSaleTypeCode,
					PrimaryCustEventNo:                primaryCustEventNo,
					PrimaryEventTypeCode:              primaryEventTypeCode,
					SecondaryCustEventNo:              secondaryCustEventNo,
					SecondaryEventTypeCode:            secondaryEventTypeCode,
					SaleEventNo:                       eventNo,
					SaleEventTypeCode:                 eventTypeCode,
					SaleReturnReasonCode:              "",
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
					EventAutoDiscountAmt:              0,
					EventDecisionDiscountAmt:          0,
					SaleEventSaleBaseAmt:              saleEventSaleBaseAmt,
					SaleEventDiscountBaseAmt:          saleEventDiscountBaseAmt,
					SaleEventNormalSaleRecognitionChk: false,
					SaleEventInterShopSalePermitChk:   false,
					SaleEventAutoDiscountAmt:          saleEventAutoDiscountAmt,
					SaleEventManualDiscountAmt:        saleEventManualDiscountAmt,
					SaleVentDecisionDiscountAmt:       saleVentDecisionDiscountAmt,
					ChinaFISaleAmt:                    saleTransactionDtl.TotalSalePrice,
					EstimateSaleAmt:                   saleTransactionDtl.TotalTransactionPrice,
					SellingAmt:                        saleTransactionDtl.TotalTransactionPrice,
					NormalFee:                         saleTransactionDtl.ItemFee,
					SaleEventFee:                      postSaleRecordFee.EventFeeRate * saleTransactionDtl.TotalSalePrice,
					ActualSaleAmt:                     actualSaleAmt,
					UseMileage:                        postMileageDtl.PointAmount,
					NormalFeeRate:                     postSaleRecordFee.ItemFeeRate,
					SaleEventFeeRate:                  postSaleRecordFee.EventFeeRate,
					InUserID:                          InUserID,
					InDateTime:                        saleTransaction.SaleDate,
					ModiUserID:                        InUserID,
					ModiDateTime:                      saleTransaction.SaleDate,
					SendState:                         "",
					SendFlag:                          NotSynChronized,
					DiscountAmt:                       discountAmt,
					DiscountAmtAsCost:                 0,
					EstimateSaleAmtForConsumer:        saleTransactionDtl.TotalTransactionPrice,
					SaleEventDiscountAmtForConsumer:   saleEventDiscountAmtForConsumer,
					ShopEmpEstimateSaleAmt:            saleTransactionDtl.TotalTransactionPrice,
					SaleOfficeCode:                    MSLv2_0,
				}
				saleDtls = append(saleDtls, saleDtl)
			}
		}
		saleMsts = append(saleMsts, saleMst)
	}
	return models.SaleMstsAndSaleDtls{
		SaleMsts: saleMsts,
		SaleDtls: saleDtls,
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
			session.Rollback()
			return err
		}
	}
	for _, saleDtl := range saleMstsAndSaleDtls.SaleDtls {
		if _, err := session.Table("dbo.SaleDtl").Insert(&saleDtl); err != nil {
			session.Rollback()
			return err
		}
	}
	//commit session
	if err := session.Commit(); err != nil {
		return err
	}
	return nil
}
