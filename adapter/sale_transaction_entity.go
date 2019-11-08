package adapter

import (
	"clearance/clearance-adapter-for-sale-record/models"

	"golang.org/x/sync/errgroup"
)

type Clearance struct{}

func (Clearance) TransformToClearance(saleMstsAndSaleDtls models.SaleMstsAndSaleDtls) error {
	for _, saleMst := range saleMstsAndSaleDtls.SaleMsts {
		cslSaleMst := makeCslSaleMstEntity(saleMst)
		cslSaleMsts, err := models.CslSaleMst{}.GetAll(models.RequestInput{TransactionId: cslSaleMst.TransactionId})
		if err != nil {
			return err
		}
		if cslSaleMsts != nil {
			err := models.CslSaleMst{}.Delete(models.RequestInput{TransactionId: cslSaleMsts[0].TransactionId})
			if err != nil {
				return err
			}
		}
		if err := cslSaleMst.Save(); err != nil {
			return err
		}
		if err := saveOtherData(saleMst.SaleNo, saleMstsAndSaleDtls); err != nil {
			return err
		}
	}
	return nil
}

func saveOtherData(saleNo string, saleMstsAndSaleDtls models.SaleMstsAndSaleDtls) error {
	g := errgroup.Group{}
	g.Go(func() error {
		for _, saleDtl := range saleMstsAndSaleDtls.SaleDtls {
			if saleNo == saleDtl.SaleNo {
				cslSaleDtl := makeCslSaleDtlEntity(saleDtl)
				if err := cslSaleDtl.Save(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	g.Go(func() error {
		for _, salePayment := range saleMstsAndSaleDtls.SalePayments {
			if saleNo == salePayment.SaleNo {
				cslSalePayment := makeCslSalePaymentEntity(salePayment)
				if err := cslSalePayment.Save(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	g.Go(func() error {
		for _, staffSaleRecord := range saleMstsAndSaleDtls.StaffSaleRecords {
			if saleNo == staffSaleRecord.SaleNo {
				cslStaffSaleRecord := makeCslStaffSaleRecordEntity(staffSaleRecord)
				if err := cslStaffSaleRecord.Save(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func makeCslSaleMstEntity(saleMst models.SaleMst) models.CslSaleMst {
	return models.CslSaleMst{
		SaleTransactionId:           saleMst.SaleTransactionId,
		TransactionId:               saleMst.TransactionId,
		StoreId:                     saleMst.StoreId,
		OrderId:                     saleMst.OrderId,
		RefundId:                    saleMst.RefundId,
		SaleNo:                      saleMst.SaleNo,
		BrandCode:                   saleMst.BrandCode,
		ShopCode:                    saleMst.ShopCode,
		Dates:                       saleMst.Dates,
		PosNo:                       saleMst.PosNo,
		SeqNo:                       saleMst.SeqNo,
		SaleMode:                    saleMst.SaleMode,
		CustNo:                      saleMst.CustNo.String,
		CustCardNo:                  saleMst.CustCardNo.String,
		CustMileagePolicyNo:         saleMst.CustMileagePolicyNo.Int64,
		PrimaryCustEventNo:          saleMst.PrimaryCustEventNo.Int64,
		SecondaryCustEventNo:        saleMst.SecondaryCustEventNo.Int64,
		DepartStoreReceiptNo:        saleMst.DepartStoreReceiptNo,
		SaleQty:                     saleMst.SaleQty,
		SaleAmt:                     saleMst.SaleAmt,
		DiscountAmt:                 saleMst.DiscountAmt,
		ChinaFISaleAmt:              saleMst.ChinaFISaleAmt,
		EstimateSaleAmt:             saleMst.EstimateSaleAmt,
		SellingAmt:                  saleMst.SellingAmt,
		FeeAmt:                      saleMst.FeeAmt,
		ActualSaleAmt:               saleMst.ActualSaleAmt,
		UseMileage:                  saleMst.UseMileage,
		ObtainMileage:               saleMst.ObtainMileage,
		InUserID:                    saleMst.InUserID,
		InDateTime:                  saleMst.InDateTime,
		ModiUserID:                  saleMst.ModiUserID,
		ModiDateTime:                saleMst.ModiDateTime,
		SendState:                   saleMst.SendState,
		SendFlag:                    saleMst.SendFlag,
		SendDateTime:                saleMst.SendDateTime,
		DiscountAmtAsCost:           saleMst.DiscountAmtAsCost,
		CustDivisionCode:            saleMst.CustDivisionCode.String,
		MileageCustChangeStatusCode: saleMst.MileageCustChangeStatusCode.String,
		CustGradeCode:               saleMst.CustGradeCode.String,
		PreSaleNo:                   saleMst.PreSaleNo.String,
		ActualSellingAmt:            saleMst.ActualSellingAmt,
		EstimateSaleAmtForConsumer:  saleMst.EstimateSaleAmtForConsumer,
		ShopEmpEstimateSaleAmt:      saleMst.ShopEmpEstimateSaleAmt,
		ComplexShopSeqNo:            saleMst.ComplexShopSeqNo.String,
		CustBrandCode:               saleMst.CustBrandCode,
		Freight:                     saleMst.Freight.Float64,
		TMall_UseMileage:            saleMst.TMall_UseMileage.Float64,
		TMall_ObtainMileage:         saleMst.TMall_ObtainMileage.Float64,
		SaleOfficeCode:              saleMst.SaleOfficeCode,
	}
}

func makeCslSaleDtlEntity(saleDtl models.SaleDtl) models.CslSaleDtl {
	return models.CslSaleDtl{
		SaleTransactionId:                 saleDtl.SaleTransactionId,
		SaleTransactionDtlId:              saleDtl.SaleTransactionDtlId,
		TransactionId:                     saleDtl.TransactionId,
		OrderItemId:                       saleDtl.OrderItemId,
		RefundItemId:                      saleDtl.RefundItemId,
		TransactionDtlId:                  saleDtl.TransactionDtlId,
		SaleNo:                            saleDtl.SaleNo,
		DtSeq:                             saleDtl.DtSeq,
		BrandCode:                         saleDtl.BrandCode,
		ShopCode:                          saleDtl.ShopCode,
		Dates:                             saleDtl.Dates,
		PosNo:                             saleDtl.PosNo,
		SeqNo:                             saleDtl.SeqNo,
		NormalSaleTypeCode:                saleDtl.NormalSaleTypeCode,
		CustMileagePolicyNo:               saleDtl.CustMileagePolicyNo.Int64,
		PrimaryCustEventNo:                saleDtl.PrimaryCustEventNo.Int64,
		PrimaryEventTypeCode:              saleDtl.PrimaryEventTypeCode.String,
		PrimaryEventSettleTypeCode:        saleDtl.PrimaryEventSettleTypeCode.String,
		SecondaryCustEventNo:              saleDtl.SecondaryCustEventNo.Int64,
		SecondaryEventTypeCode:            saleDtl.SecondaryEventTypeCode.String,
		SecondaryEventSettleTypeCode:      saleDtl.SecondaryEventSettleTypeCode.String,
		SaleEventNo:                       saleDtl.SaleEventNo.Int64,
		SaleEventTypeCode:                 saleDtl.SaleEventTypeCode.String,
		SaleReturnReasonCode:              saleDtl.SaleReturnReasonCode.String,
		ProdCode:                          saleDtl.ProdCode,
		EANCode:                           saleDtl.EANCode,
		PriceTypeCode:                     saleDtl.PriceTypeCode,
		SupGroupCode:                      saleDtl.SupGroupCode,
		SaipType:                          saleDtl.SaipType,
		NormalPrice:                       saleDtl.NormalPrice,
		Price:                             saleDtl.Price,
		PriceDecisionDate:                 saleDtl.PriceDecisionDate,
		SaleQty:                           saleDtl.SaleQty,
		SaleAmt:                           saleDtl.SaleAmt,
		EventAutoDiscountAmt:              saleDtl.EventAutoDiscountAmt,
		EventDecisionDiscountAmt:          saleDtl.EventDecisionDiscountAmt,
		SaleEventSaleBaseAmt:              saleDtl.SaleEventSaleBaseAmt,
		SaleEventDiscountBaseAmt:          saleDtl.SaleEventDiscountBaseAmt,
		SaleEventNormalSaleRecognitionChk: saleDtl.SaleEventNormalSaleRecognitionChk,
		SaleEventInterShopSalePermitChk:   saleDtl.SaleEventInterShopSalePermitChk,
		SaleEventAutoDiscountAmt:          saleDtl.SaleEventAutoDiscountAmt,
		SaleEventManualDiscountAmt:        saleDtl.SaleEventManualDiscountAmt,
		SaleVentDecisionDiscountAmt:       saleDtl.SaleVentDecisionDiscountAmt,
		ChinaFISaleAmt:                    saleDtl.ChinaFISaleAmt,
		EstimateSaleAmt:                   saleDtl.EstimateSaleAmt,
		SellingAmt:                        saleDtl.SellingAmt,
		NormalFee:                         saleDtl.NormalFee,
		SaleEventFee:                      saleDtl.SaleEventFee,
		ActualSaleAmt:                     saleDtl.ActualSaleAmt,
		UseMileage:                        saleDtl.UseMileage,
		PreSaleNo:                         saleDtl.PreSaleNo.String,
		PreSaleDtSeq:                      saleDtl.PreSaleDtSeq.Int64,
		NormalFeeRate:                     saleDtl.NormalFeeRate,
		SaleEventFeeRate:                  saleDtl.SaleEventFeeRate,
		InUserID:                          saleDtl.InUserID,
		InDateTime:                        saleDtl.InDateTime,
		ModiUserID:                        saleDtl.ModiUserID,
		ModiDateTime:                      saleDtl.ModiDateTime,
		SendState:                         saleDtl.SendState,
		SendFlag:                          saleDtl.SendFlag,
		SendDateTime:                      saleDtl.SendDateTime,
		DiscountAmt:                       saleDtl.DiscountAmt,
		DiscountAmtAsCost:                 saleDtl.DiscountAmtAsCost,
		UseMileageSettleType:              saleDtl.UseMileageSettleType,
		EstimateSaleAmtForConsumer:        saleDtl.EstimateSaleAmtForConsumer,
		SaleEventDiscountAmtForConsumer:   saleDtl.SaleEventDiscountAmtForConsumer,
		ShopEmpEstimateSaleAmt:            saleDtl.ShopEmpEstimateSaleAmt,
		PromotionID:                       saleDtl.PromotionID.Int64,
		TMallEventID:                      saleDtl.TMallEventID.Int64,
		TMall_ObtainMileage:               saleDtl.TMall_ObtainMileage.Float64,
		SaleOfficeCode:                    saleDtl.SaleOfficeCode,
	}
}

func makeCslSalePaymentEntity(salePayment models.SalePayment) models.CslSalePayment {
	return models.CslSalePayment{
		SaleTransactionId:  salePayment.SaleTransactionId,
		TransactionId:      salePayment.TransactionId,
		SaleNo:             salePayment.SaleNo,
		SeqNo:              salePayment.SeqNo,
		PaymentCode:        salePayment.PaymentCode,
		PaymentAmt:         salePayment.PaymentAmt,
		InUserID:           salePayment.InUserID,
		InDateTime:         salePayment.InDateTime,
		ModiUserID:         salePayment.ModiUserID,
		ModiDateTime:       salePayment.ModiDateTime,
		SendFlag:           salePayment.SendFlag,
		SendDateTime:       salePayment.SendDateTime,
		CreditCardFirmCode: salePayment.CreditCardFirmCode.String,
	}
}

func makeCslStaffSaleRecordEntity(staffSaleRecord models.StaffSaleRecord) models.CslStaffSaleRecord {
	return models.CslStaffSaleRecord{
		SaleTransactionId: staffSaleRecord.SaleTransactionId,
		TransactionId:     staffSaleRecord.TransactionId,
		Dates:             staffSaleRecord.Dates,
		HREmpNo:           staffSaleRecord.HREmpNo,
		SaleNo:            staffSaleRecord.SaleNo,
		BrandCode:         staffSaleRecord.BrandCode,
		ShopCode:          staffSaleRecord.ShopCode,
		InUserID:          staffSaleRecord.InUserID,
		InDateTime:        staffSaleRecord.InDateTime,
	}
}
