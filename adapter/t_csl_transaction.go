package adapter

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/go-xorm/xorm"
	"github.com/pangpanglabs/goetl"
	"xorm.io/core"
)

const (
	MSLV2_EMALL = "1"
	ChannelCode = "MSL2"
)

// Clearance到CSL_T_sale
type ClearanceToCslTSaleETL struct{}

func buildClearanceToCslTtableETL() *goetl.ETL {
	etl := goetl.New(ClearanceToCslTSaleETL{})
	return etl
}

// Extract ...
func (etl ClearanceToCslTSaleETL) Extract(ctx context.Context) (interface{}, error) {
	saleTransactions := []models.SaleTransaction{}
	saleTransactionDtls := []models.SaleTransactionDtl{}
	dataInput := ctx.Value("data").(models.RequestInput)
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
	if err := query().Find(&stsAndStds); err != nil {
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

	return models.SaleTAndSaleTDtls{
		SaleTransactions:    saleTransactions,
		SaleTransactionDtls: saleTransactionDtls,
	}, nil
}

// Transform ...
func (etl ClearanceToCslTSaleETL) Transform(ctx context.Context, source interface{}) (interface{}, error) {
	var dtSeq, saleQty, seqNo, colleaguesId int64
	var saleMode, eANCode, normalSaleTypeCode, offerNo, couponNo,
		inUserID, itemIds, baseTrimCode, saleNo, departStoreReceiptNo string
	var preSaleDtSeq sql.NullInt64
	var preSaleNo, tMall_ID, offlineShopCode sql.NullString
	var freight sql.NullFloat64
	var discountAmt, estimateSaleAmt, saleAmt, normalPrice, paymentAmt float64

	saleTAndSaleTDtls, ok := source.(models.SaleTAndSaleTDtls)
	if !ok {
		return nil, errors.New("Convert Failed")
	}
	tSaleMsts := make([]models.T_SaleMst, 0)
	tSaleDtls := make([]models.T_SaleDtl, 0)
	tSalePayments := make([]models.T_SalePayment, 0)
	local, _ := time.ParseDuration("8h")
	for _, saleTransaction := range saleTAndSaleTDtls.SaleTransactions {
		baseTrimCode = "A"
		localSaleDate := (saleTransaction.UpdatedAt).Add(local)
		saleDate := localSaleDate.Format("20060102")
		saleNo = ""
		seqNo = 0
		if saleTransaction.ShopCode == "" || saleDate == "" {
			return nil, errors.New("ShopCode or saleDate is null")
		}
		checkSaleNo, err := models.CheckSaleNo{}.GetCheckSaleNoBySaleTransactionid(saleTransaction.Id)
		if err != nil {
			return nil, err
		}
		saleNo = checkSaleNo.SaleNo
		if saleNo == "" {
			lastSaleNo, err := models.CheckSaleNo{}.GetLastSaleNo(saleTransaction.ShopCode, saleDate, MSLV2_EMALL)
			if err != nil {
				return nil, err
			}
			seq, str, err := models.SaleMst{}.GetSeqAndStartStr(lastSaleNo)
			if err != nil {
				return nil, err
			}
			//Get SequenceNumber
			sequenceNumber, _, _, err := models.SaleMst{}.GetSequenceNumber(seq, str)
			if err != nil {
				return nil, err
			}
			//get SeqNo
			seqNumber, err := models.SaleMst{}.GetSeqNo(sequenceNumber)
			if err != nil {
				return nil, err
			}
			seqNo = seqNumber
			saleNo = saleTransaction.ShopCode + saleDate[len(saleDate)-6:len(saleDate)] + MSLV2_EMALL + sequenceNumber
			checkSaleNo := &models.CheckSaleNo{
				TransactionId:          saleTransaction.TransactionId,
				SaleTransactionId:      saleTransaction.Id,
				TransactionChannelType: saleTransaction.TransactionChannelType,
				OrderId:                saleTransaction.OrderId,
				RefundId:               saleTransaction.RefundId,
				ShopCode:               saleTransaction.ShopCode,
				Dates:                  saleDate,
				SaleNo:                 saleNo,
				PosNo:                  MSLV2_EMALL,
				Processing:             true,
				Whthersend:             false,
			}
			if err = checkSaleNo.Save(); err != nil {
				return nil, err
			}
		} else {
			if checkSaleNo.Processing == true || checkSaleNo.Whthersend == true {
				continue
			}
		}

		//Sale S 销售  Refund R 退货
		saleMode = ""

		preSaleNo = sql.NullString{"", false}
		if saleTransaction.RefundId == 0 {
			saleMode = Sale
		} else {
			saleMode = Refund
			if saleTransaction.OrderId != 0 {
				successDtls, err := models.SaleRecordIdSuccessMapping{}.GetSaleSuccessData(0, saleTransaction.OrderId, 0, saleTransaction.TransactionChannelType)
				if err != nil {
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
						SaleTransactionId:      saleTransaction.Id,
						TransactionChannelType: saleTransaction.TransactionChannelType,
						OrderId:                saleTransaction.OrderId,
						RefundId:               saleTransaction.RefundId,
						StoreId:                saleTransaction.StoreId,
						TransactionId:          saleTransaction.TransactionId,
						CreatedBy:              "API",
						Error:                  err.Error() + " OrderId:" + strconv.FormatInt(saleTransaction.OrderId, 10) + " RefundId:" + strconv.FormatInt(saleTransaction.RefundId, 10),
						Details:                "退货处理必须有之前的销售数据！",
					}
					if err := SaleRecordIdFailMapping.Save(); err != nil {
						return nil, err
					}
					continue
				}
				preSaleNo = sql.NullString{successDtls[0].SaleNo, true}
			}
		}
		if saleTransaction.TransactionCreatedId != 0 {
			colleaguesId = saleTransaction.TransactionCreatedId
		}
		colleagues, err := models.Colleagues{}.GetColleaguesAuth(colleaguesId, "")
		if err != nil {
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
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
				return nil, err
			}
			continue
		}
		if colleagues.UserName != "" {
			inUserID = colleagues.UserName
		}
		saleAmt = saleTransaction.TotalListPrice
		if saleTransaction.RefundId != 0 {
			saleAmt = saleAmt * -1
		}
		freight = sql.NullFloat64{0, false}
		if saleTransaction.FreightPrice != 0 {
			freight = sql.NullFloat64{saleTransaction.FreightPrice, true}
		}
		tMall_ID = sql.NullString{"", false}
		if saleTransaction.OuterOrderNo != "" {
			tMall_ID = sql.NullString{saleTransaction.OuterOrderNo, true}
		}
		departStoreReceiptNo = "Tmall_" + strconv.FormatInt(seqNo, 10)
		offlineShopCode = sql.NullString{"", false}
		if saleTransaction.SalesmanShopCode != "" || saleTransaction.SalesmanEmpId != "" {
			offlineShopCode = sql.NullString{saleTransaction.SalesmanShopCode + "," + saleTransaction.SalesmanEmpId, true}
		}
		tSaleMst := models.T_SaleMst{
			SaleNo:                 saleNo,
			ShopCode:               saleTransaction.ShopCode,
			Dates:                  saleDate,
			SeqNo:                  seqNo,
			SaleMode:               saleMode,
			DepartStoreReceiptNo:   departStoreReceiptNo,
			TMall_ID:               tMall_ID,
			SaleAmt:                saleAmt,
			Freight:                freight,
			TMall_UseMileage:       sql.NullFloat64{0, false},
			TMall_ObtainMileage:    sql.NullFloat64{0, false},
			PreSaleNo:              preSaleNo,
			InUserID:               inUserID,
			ModiUserID:             inUserID,
			Tran_status:            "N",
			ErrorMessage:           sql.NullString{"", false},
			SaleEventName:          "",
			OfflineShopCode:        offlineShopCode,
			SaleMan:                sql.NullString{"", false},
			ChannelCode:            ChannelCode,
			TransactionId:          saleTransaction.TransactionId,
			StoreId:                saleTransaction.StoreId,
			OrderId:                saleTransaction.OrderId,
			RefundId:               saleTransaction.RefundId,
			SaleTransactionId:      saleTransaction.Id,
			TransactionChannelType: saleTransaction.TransactionChannelType,
		}
		appliedSaleRecordCartOffers, err := models.AppliedSaleRecordCartOffer{}.GetAppliedSaleRecordCartOffers(saleTransaction.TransactionId)
		if err != nil {
			return nil, err
		}
		dtSeq = 0
		for _, saleTransactionDtl := range saleTAndSaleTDtls.SaleTransactionDtls {
			if saleTransactionDtl.TransactionId == saleTransaction.TransactionId && saleTransactionDtl.SaleTransactionId == saleTransaction.Id {
				dtSeq += 1
				tSaleMst.BrandCode = saleTransactionDtl.BrandCode
				tSaleMst.SaleEventNo = sql.NullInt64{0, false}
				normalSaleTypeCode = "0"
				discountAmt = 0
				offerNo = ""
				couponNo = ""
				estimateSaleAmt = 0
				saleQty = 0
				saleAmt = 0

				for _, appliedSaleRecordCartOffer := range appliedSaleRecordCartOffers {
					itemIds = ""
					if appliedSaleRecordCartOffer.TargetItemIds != "" {
						itemIds = appliedSaleRecordCartOffer.TargetItemIds
					} else {
						itemIds = appliedSaleRecordCartOffer.ItemIds
					}
					result := strings.Index(itemIds+",", strconv.FormatInt(saleTransactionDtl.OrderItemId, 10)+",")
					if result != -1 {
						if appliedSaleRecordCartOffer.OfferNo == "" {
							SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
								SaleTransactionId:      saleTransaction.Id,
								TransactionChannelType: saleTransaction.TransactionChannelType,
								OrderId:                saleTransaction.OrderId,
								RefundId:               saleTransaction.RefundId,
								StoreId:                saleTransaction.StoreId,
								TransactionId:          saleTransactionDtl.TransactionId,
								TransactionDtlId:       saleTransactionDtl.TransactionDtlId,
								CreatedBy:              "API",
								Error:                  "OfferNo can not be null!",
								Details:                "OfferNo不能为空!",
							}
							if err := SaleRecordIdFailMapping.Save(); err != nil {
								return nil, err
							}
							return nil, errors.New("OfferNo can not be null!")
						}
						couponNo = appliedSaleRecordCartOffer.CouponNo
						offerNo = appliedSaleRecordCartOffer.OfferNo
						break
					}
				}

				if offerNo != "" && couponNo == "" {
					promotionEvent, err := models.PromotionEvent{}.GetPromotionEvent(offerNo)
					if promotionEvent == nil || promotionEvent.EventNo == "" {
						err = errors.New("PromotionEvent的EventNo为空!")
					}
					if err != nil {
						eventNo := ""
						if promotionEvent != nil {
							eventNo = promotionEvent.EventNo
						}
						SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
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
					eventN, err := strconv.ParseInt(promotionEvent.EventNo, 10, 64)
					if err != nil {
						return nil, err
					}
					if promotionEvent.EventTypeCode == "01" || promotionEvent.EventTypeCode == "02" || promotionEvent.EventTypeCode == "03" {
						tSaleMst.SaleEventName = promotionEvent.EventName
						if eventN != 0 {
							tSaleMst.SaleEventNo = sql.NullInt64{eventN, true}
						}
						normalSaleTypeCode = "1"
					}
				}
				if couponNo != "" {
					normalSaleTypeCode = "2"
				}
				sku, err := models.Product{}.GetSkuBySkuId(saleTransactionDtl.SkuId)
				if err != nil {
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
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
						return nil, err
					}
					return nil, err
				}

				if len(sku.Identifiers) == 0 || sku.Identifiers[0].Uid == "" {
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
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
						return nil, err
					}
					return nil, errors.New("Sku.Identifiers not exist")
				}
				eANCode = sku.Identifiers[0].Uid

				product, err := models.Product{}.GetProductById(saleTransactionDtl.ProductId)
				if err != nil {
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
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
						return nil, err
					}
					return nil, err
				}

				if saleTransaction.RefundId != 0 && saleTransaction.OrderId != 0 {
					successDtls, err := models.SaleRecordIdSuccessMapping{}.GetSaleSuccessData(0, saleTransaction.OrderId, saleTransactionDtl.OrderItemId, saleTransaction.TransactionChannelType)
					if err != nil {
						SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
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
							return nil, err
						}
						return nil, err
					}
					preSaleDtSeq = sql.NullInt64{successDtls[0].DtlSeq, true}
				}

				discountAmt = GetToFixedPrice(saleTransactionDtl.TotalDistributedCartOfferPrice, baseTrimCode)
				estimateSaleAmt = GetToFixedPrice(saleTransactionDtl.TotalListPrice-discountAmt, baseTrimCode)
				normalPrice = saleTransactionDtl.ListPrice
				saleQty = saleTransactionDtl.Quantity
				saleAmt = saleTransactionDtl.TotalListPrice

				if saleTransactionDtl.RefundItemId != 0 {
					saleQty = saleQty * -1
					saleAmt = saleAmt * -1
					estimateSaleAmt = estimateSaleAmt * -1
					discountAmt = discountAmt * -1
				}
				tSaleDtl := models.T_SaleDtl{
					SaleNo:                     saleNo,
					DtSeq:                      dtSeq,
					TMall_ID:                   tMall_ID,
					TMall_DtlNo:                dtSeq,
					NormalSaleTypeCode:         normalSaleTypeCode,
					TMallEventID:               sql.NullInt64{0, false},
					TMallEventDesc:             sql.NullString{"", false},
					ProdCode:                   sku.Code,
					EANCode:                    eANCode,
					NormalPrice:                normalPrice,
					Price:                      saleTransactionDtl.SalePrice,
					SaleQty:                    saleQty,
					SaleAmt:                    saleAmt,
					DiscountAmt:                discountAmt,
					EstimateSaleAmt:            estimateSaleAmt,
					EstimateSaleAmtForConsumer: estimateSaleAmt,
					TMall_ObtainMileage:        sql.NullFloat64{0, false},
					PreSaleNo:                  preSaleNo,
					PreSaleDtSeq:               preSaleDtSeq,
					InUserID:                   inUserID,
					ModiUserID:                 inUserID,
					OrderItemId:                saleTransactionDtl.OrderItemId,
					RefundItemId:               saleTransactionDtl.RefundItemId,
					TransactionDtlId:           saleTransactionDtl.TransactionDtlId,
					StyleCode:                  product.Code,
					SaleTransactionId:          saleTransaction.Id,
					SaleTransactionDtlId:       saleTransactionDtl.Id,
					TransactionId:              saleTransaction.TransactionId,
				}
				tSaleDtls = append(tSaleDtls, tSaleDtl)
			}
		}
		tSaleMst.DiscountAmt = 0
		tSaleMst.SaleQty = 0
		tSaleMst.EstimateSaleAmt = 0
		tSaleMst.EstimateSaleAmtForConsumer = 0
		for _, tSaleDtl := range tSaleDtls {
			if tSaleMst.SaleNo == tSaleDtl.SaleNo {
				tSaleMst.SaleQty += tSaleDtl.SaleQty
				tSaleMst.DiscountAmt += tSaleDtl.DiscountAmt
				tSaleMst.EstimateSaleAmt += tSaleDtl.EstimateSaleAmt
				tSaleMst.EstimateSaleAmtForConsumer += tSaleDtl.EstimateSaleAmtForConsumer
			}
		}
		tSaleMst.DiscountAmt = GetToFixedPrice(tSaleMst.DiscountAmt, baseTrimCode)
		tSaleMst.EstimateSaleAmt = GetToFixedPrice(tSaleMst.EstimateSaleAmt, baseTrimCode)
		tSaleMst.EstimateSaleAmtForConsumer = GetToFixedPrice(tSaleMst.EstimateSaleAmtForConsumer, baseTrimCode)

		saleTransactionPayments, err := models.SaleTransactionPayment{}.GetSaleTransactionPayment(saleTransaction.Id)
		if err != nil {
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
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
			paymentCode, _, err := getPaymentCodeAndPayCreditCardFirmCode(stp.PayMethod)
			if err != nil {
				return nil, err
			}
			paymentAmt = GetToFixedPrice(stp.PayAmt, baseTrimCode)
			if saleTransaction.RefundId != 0 {
				paymentAmt = GetToFixedPrice(stp.PayAmt, baseTrimCode) * -1
			}
			tSalePayment := models.T_SalePayment{
				SaleNo:            saleNo,
				SeqNo:             stp.SeqNo,
				TMall_ID:          tMall_ID,
				PaymentCode:       paymentCode,
				PaymentAmt:        paymentAmt,
				InUserID:          inUserID,
				ModiUserID:        inUserID,
				TransactionId:     tSaleMst.TransactionId,
				SaleTransactionId: tSaleMst.SaleTransactionId,
			}
			tSalePayments = append(tSalePayments, tSalePayment)
		}

		check := false
		for _, tSaleDtl := range tSaleDtls {
			if saleNo == tSaleDtl.SaleNo {
				for _, tSalePayment := range tSalePayments {
					if saleNo == tSalePayment.SaleNo {
						check = true
					}
				}
			}
		}
		if check {
			tSaleMsts = append(tSaleMsts, tSaleMst)
		} else {
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
				SaleTransactionId:      saleTransaction.Id,
				TransactionChannelType: saleTransaction.TransactionChannelType,
				OrderId:                saleTransaction.OrderId,
				RefundId:               saleTransaction.RefundId,
				StoreId:                saleTransaction.StoreId,
				TransactionId:          tSaleMst.TransactionId,
				CreatedBy:              "API",
				Error:                  "TSaleMst、TSaleDtl、TSalePayment数据不一致" + strconv.FormatInt(tSaleMst.TransactionId, 10),
				Details:                "数据异常！",
			}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return nil, err
			}
			continue
		}
	}
	return models.T_SaleMstsAndSaleDtls{
		T_SaleMsts:     tSaleMsts,
		T_SaleDtls:     tSaleDtls,
		T_SalePayments: tSalePayments,
	}, nil
}

// ReadyToLoad ...
func (etl ClearanceToCslTSaleETL) ReadyToLoad(ctx context.Context, source interface{}) error {
	var paymentAmt float64
	if source == nil {
		return errors.New("source is nil")
	}
	tSaleMstsAndSaleDtls, ok := source.(models.T_SaleMstsAndSaleDtls)
	if !ok {
		return errors.New("Convert Failed")
	}

	for _, tSaleMst := range tSaleMstsAndSaleDtls.T_SaleMsts {
		paymentAmt = 0

		//Check Shop
		err := models.SaleMst{}.CheckShop(tSaleMst.BrandCode, tSaleMst.ShopCode)
		if err != nil {
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
				SaleTransactionId:      tSaleMst.SaleTransactionId,
				TransactionChannelType: tSaleMst.TransactionChannelType,
				OrderId:                tSaleMst.OrderId,
				RefundId:               tSaleMst.RefundId,
				StoreId:                tSaleMst.StoreId,
				TransactionId:          tSaleMst.TransactionId,
				CreatedBy:              "API",
				Error:                  err.Error() + " BrandCode:" + tSaleMst.BrandCode + " ShopCode:" + tSaleMst.ShopCode,
				Details:                "卖场信息不存在!",
			}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return err
			}
			return err
		}

		//Check PaymentAmt
		for _, tSalePayment := range tSaleMstsAndSaleDtls.T_SalePayments {
			if tSaleMst.SaleNo == tSalePayment.SaleNo {
				paymentAmt += tSalePayment.PaymentAmt
			}
		}
		if tSaleMst.EstimateSaleAmtForConsumer != paymentAmt {
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
				SaleTransactionId:      tSaleMst.SaleTransactionId,
				TransactionChannelType: tSaleMst.TransactionChannelType,
				OrderId:                tSaleMst.OrderId,
				RefundId:               tSaleMst.RefundId,
				StoreId:                tSaleMst.StoreId,
				TransactionId:          tSaleMst.TransactionId,
				CreatedBy:              "API",
				Error:                  "支付金额:" + fmt.Sprintf("%g", paymentAmt) + "和TSaleMst实际销售金额:" + fmt.Sprintf("%g", tSaleMst.SaleAmt) + "不一致！",
				Details:                "支付金额:" + fmt.Sprintf("%g", paymentAmt) + "和TSaleMst实际销售金额:" + fmt.Sprintf("%g", tSaleMst.SaleAmt) + "不一致！",
			}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return err
			}
			return errors.New("支付金额和TSaleMst实际销售金额不一致！")
		}
	}
	err := TSaleClearance{}.TSaleTransformToClearance(tSaleMstsAndSaleDtls)
	if err != nil {
		return err
	}
	return nil
}

// Load ...
func (etl ClearanceToCslTSaleETL) Load(ctx context.Context, source interface{}) error {
	if source == nil {
		return errors.New("source is nil")
	}
	tSaleMstsAndSaleDtls, ok := source.(models.T_SaleMstsAndSaleDtls)
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
	for _, tSaleMst := range tSaleMstsAndSaleDtls.T_SaleMsts {
		//check saleNo Whether it exists or not
		successes, err := models.SaleRecordIdSuccessMapping{}.GetBySaleNo("", tSaleMst.SaleTransactionId)
		if err != nil {
			return err
		}
		//exists
		if len(successes) != 0 {
			continue
		}
		//not exists
		tSaleMst.InDateTime = createTime
		tSaleMst.ModiDateTime = createTime
		if _, err := session.Table("dbo.T_SaleMst").Insert(&tSaleMst); err != nil {
			str, _ := json.Marshal(tSaleMst)
			SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
				SaleTransactionId:      tSaleMst.SaleTransactionId,
				TransactionChannelType: tSaleMst.TransactionChannelType,
				OrderId:                tSaleMst.OrderId,
				RefundId:               tSaleMst.RefundId,
				StoreId:                tSaleMst.StoreId,
				TransactionId:          tSaleMst.TransactionId,
				CreatedBy:              "API",
				Error:                  err.Error() + " TransactionId:" + strconv.FormatInt(tSaleMst.TransactionId, 10),
				Details:                "数据插入异常！",
				Data:                   string(str),
			}
			if err := SaleRecordIdFailMapping.Save(); err != nil {
				return err
			}
			session.Rollback()
			return err
		}
		//insert tSaleDtl
		for _, tSaleDtl := range tSaleMstsAndSaleDtls.T_SaleDtls {
			if tSaleDtl.SaleNo == tSaleMst.SaleNo {
				tSaleDtl.InDateTime = createTime
				tSaleDtl.ModiDateTime = createTime
				if _, err := session.Table("dbo.T_SaleDtl").Insert(&tSaleDtl); err != nil {
					str, _ := json.Marshal(tSaleDtl)
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
						SaleTransactionId:      tSaleMst.SaleTransactionId,
						TransactionChannelType: tSaleMst.TransactionChannelType,
						OrderId:                tSaleMst.OrderId,
						RefundId:               tSaleMst.RefundId,
						StoreId:                tSaleMst.StoreId,
						TransactionId:          tSaleMst.TransactionId,
						TransactionDtlId:       tSaleDtl.TransactionDtlId,
						CreatedBy:              "API",
						Error:                  err.Error() + " TransactionId:" + strconv.FormatInt(tSaleMst.TransactionId, 10),
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
		for _, tSalePayment := range tSaleMstsAndSaleDtls.T_SalePayments {
			if tSaleMst.SaleNo == tSalePayment.SaleNo {
				tSalePayment.InDateTime = createTime
				tSalePayment.ModiDateTime = createTime
				if _, err := session.Table("dbo.T_SalePayment").Insert(&tSalePayment); err != nil {
					str, _ := json.Marshal(tSalePayment)
					SaleRecordIdFailMapping := &models.SaleRecordIdFailMapping{
						SaleTransactionId:      tSaleMst.SaleTransactionId,
						TransactionChannelType: tSaleMst.TransactionChannelType,
						OrderId:                tSaleMst.OrderId,
						RefundId:               tSaleMst.RefundId,
						StoreId:                tSaleMst.StoreId,
						TransactionId:          tSaleMst.TransactionId,
						CreatedBy:              "API",
						Error:                  err.Error() + " SalePaymentTransactionId:" + strconv.FormatInt(tSalePayment.TransactionId, 10),
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

		if err := SaveAndUpdateTSaleLog(ctx, tSaleMst, tSaleMstsAndSaleDtls); err != nil {
			return err
		}
	}
	//commit session
	if err := session.Commit(); err != nil {
		return err
	}
	return nil
}

func SaveAndUpdateTSaleLog(ctx context.Context, saleMstInput models.T_SaleMst, tSaleMstsAndSaleDtls models.T_SaleMstsAndSaleDtls) error {
	g := errgroup.Group{}

	g.Go(func() error {
		//insert success table
		for _, tSaleMst := range tSaleMstsAndSaleDtls.T_SaleMsts {
			if tSaleMst.SaleNo == saleMstInput.SaleNo {
				for _, tSalDtl := range tSaleMstsAndSaleDtls.T_SaleDtls {
					if tSalDtl.SaleNo == tSaleMst.SaleNo {
						for _, tSalePayment := range tSaleMstsAndSaleDtls.T_SalePayments {
							if tSalePayment.SaleNo == tSalDtl.SaleNo {
								saleRecordIdSuccessMapping := &models.SaleRecordIdSuccessMapping{
									SaleTransactionId:      tSaleMst.SaleTransactionId,
									TransactionChannelType: tSaleMst.TransactionChannelType,
									SaleNo:                 tSaleMst.SaleNo,
									CreatedBy:              "API",
									TransactionId:          tSaleMst.TransactionId,
									OrderId:                tSaleMst.OrderId,
									RefundId:               tSaleMst.RefundId,
									OrderItemId:            tSalDtl.OrderItemId,
									RefundItemId:           tSalDtl.RefundItemId,
									DtlSeq:                 tSalDtl.DtSeq,
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
		saleTransaction, err := models.SaleTransaction{}.Get(saleMstInput.SaleTransactionId, saleMstInput.TransactionId)
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
		_, saleRecordIdFailMappings, err := models.SaleRecordIdFailMapping{}.GetAll(ctx, models.RequestInput{SaleTransactionId: saleMstInput.SaleTransactionId})
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
		checkSaleNo, err := models.CheckSaleNo{}.GetCheckSaleNoBySaleTransactionid(saleMstInput.SaleTransactionId)
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
