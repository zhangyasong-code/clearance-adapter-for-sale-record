package adapter

import (
	"clearance/clearance-adapter-for-sale-record/models"

	"golang.org/x/sync/errgroup"
)

type TSaleClearance struct{}

func (TSaleClearance) TSaleTransformToClearance(tSaleMstsAndSaleDtls models.T_SaleMstsAndSaleDtls) error {
	for _, tSaleMst := range tSaleMstsAndSaleDtls.T_SaleMsts {
		cslTSaleMst := makeCslTSaleMstEntity(tSaleMst)
		tCslSaleMsts, err := models.CslTSaleMst{}.GetAll(models.RequestInput{SaleTransactionId: cslTSaleMst.SaleTransactionId})
		if err != nil {
			return err
		}
		if tCslSaleMsts != nil {
			err := models.CslTSaleMst{}.Delete(models.RequestInput{SaleTransactionId: tCslSaleMsts[0].SaleTransactionId})
			if err != nil {
				return err
			}
		}
		if err := cslTSaleMst.Save(); err != nil {
			return err
		}
		err = TSaleClearance{}.SaveOtherData(tSaleMst.SaleNo, tSaleMstsAndSaleDtls)
		if err != nil {
			return err
		}
	}
	return nil
}

func (TSaleClearance) SaveOtherData(saleNo string, tSaleMstsAndSaleDtls models.T_SaleMstsAndSaleDtls) error {
	g := errgroup.Group{}
	g.Go(func() error {
		for _, tSaleDtl := range tSaleMstsAndSaleDtls.T_SaleDtls {
			if saleNo == tSaleDtl.SaleNo {
				cslTSaleDtl := makeCslTSaleDtlEntity(tSaleDtl)
				if err := cslTSaleDtl.Save(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	g.Go(func() error {
		for _, tSalePayment := range tSaleMstsAndSaleDtls.T_SalePayments {
			if saleNo == tSalePayment.SaleNo {
				cslTSalePayment := makeCslTSalePaymentEntity(tSalePayment)
				if err := cslTSalePayment.Save(); err != nil {
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

func makeCslTSaleMstEntity(tSaleMst models.T_SaleMst) models.CslTSaleMst {
	return models.CslTSaleMst{
		SaleTransactionId:          tSaleMst.SaleTransactionId,
		TransactionId:              tSaleMst.TransactionId,
		StoreId:                    tSaleMst.StoreId,
		OrderId:                    tSaleMst.OrderId,
		RefundId:                   tSaleMst.RefundId,
		SaleNo:                     tSaleMst.SaleNo,
		BrandCode:                  tSaleMst.BrandCode,
		ShopCode:                   tSaleMst.ShopCode,
		Dates:                      tSaleMst.Dates,
		SeqNo:                      tSaleMst.SeqNo,
		SaleMode:                   tSaleMst.SaleMode,
		DepartStoreReceiptNo:       tSaleMst.DepartStoreReceiptNo,
		TMall_ID:                   tSaleMst.TMall_ID,
		SaleQty:                    tSaleMst.SaleQty,
		SaleAmt:                    tSaleMst.SaleAmt,
		Freight:                    tSaleMst.Freight,
		DiscountAmt:                tSaleMst.DiscountAmt,
		EstimateSaleAmt:            tSaleMst.EstimateSaleAmt,
		EstimateSaleAmtForConsumer: tSaleMst.EstimateSaleAmtForConsumer,
		TMall_UseMileage:           tSaleMst.TMall_UseMileage,
		TMall_ObtainMileage:        tSaleMst.TMall_ObtainMileage,
		PreSaleNo:                  tSaleMst.PreSaleNo,
		InUserID:                   tSaleMst.InUserID,
		InDateTime:                 tSaleMst.InDateTime,
		ModiUserID:                 tSaleMst.ModiUserID,
		ModiDateTime:               tSaleMst.ModiDateTime,
		Tran_status:                tSaleMst.Tran_status,
		ErrorMessage:               tSaleMst.ErrorMessage,
		SaleEventNo:                tSaleMst.SaleEventNo,
		SaleEventName:              tSaleMst.SaleEventName,
		OfflineShopCode:            tSaleMst.OfflineShopCode,
		SaleMan:                    tSaleMst.SaleMan,
	}
}

func makeCslTSaleDtlEntity(tSaleDtl models.T_SaleDtl) models.CslTSaleDtl {
	return models.CslTSaleDtl{
		SaleTransactionId:          tSaleDtl.SaleTransactionId,
		SaleTransactionDtlId:       tSaleDtl.SaleTransactionDtlId,
		TransactionId:              tSaleDtl.TransactionId,
		OrderItemId:                tSaleDtl.OrderItemId,
		RefundItemId:               tSaleDtl.RefundItemId,
		TransactionDtlId:           tSaleDtl.TransactionDtlId,
		SaleNo:                     tSaleDtl.SaleNo,
		DtSeq:                      tSaleDtl.DtSeq,
		TMall_ID:                   tSaleDtl.TMall_ID,
		TMall_DtlNo:                tSaleDtl.TMall_DtlNo,
		NormalSaleTypeCode:         tSaleDtl.NormalSaleTypeCode,
		TMallEventID:               tSaleDtl.TMallEventID,
		TMallEventDesc:             tSaleDtl.TMallEventDesc,
		ProdCode:                   tSaleDtl.ProdCode,
		EANCode:                    tSaleDtl.EANCode,
		NormalPrice:                tSaleDtl.NormalPrice,
		Price:                      tSaleDtl.Price,
		SaleQty:                    tSaleDtl.SaleQty,
		SaleAmt:                    tSaleDtl.SaleAmt,
		DiscountAmt:                tSaleDtl.DiscountAmt,
		EstimateSaleAmt:            tSaleDtl.EstimateSaleAmt,
		EstimateSaleAmtForConsumer: tSaleDtl.EstimateSaleAmtForConsumer,
		TMall_ObtainMileage:        tSaleDtl.TMall_ObtainMileage,
		PreSaleNo:                  tSaleDtl.PreSaleNo,
		PreSaleDtSeq:               tSaleDtl.PreSaleDtSeq,
		InUserID:                   tSaleDtl.InUserID,
		InDateTime:                 tSaleDtl.InDateTime,
		ModiUserID:                 tSaleDtl.ModiUserID,
		ModiDateTime:               tSaleDtl.ModiDateTime,
	}
}

func makeCslTSalePaymentEntity(tSalePayment models.T_SalePayment) models.CslTSalePayment {
	return models.CslTSalePayment{
		SaleTransactionId: tSalePayment.SaleTransactionId,
		TransactionId:     tSalePayment.TransactionId,
		SaleNo:            tSalePayment.SaleNo,
		SeqNo:             tSalePayment.SeqNo,
		TMall_ID:          tSalePayment.TMall_ID,
		PaymentCode:       tSalePayment.PaymentCode,
		PaymentAmt:        tSalePayment.PaymentAmt,
		InUserID:          tSalePayment.InUserID,
		InDateTime:        tSalePayment.InDateTime,
		ModiUserID:        tSalePayment.ModiUserID,
		ModiDateTime:      tSalePayment.ModiDateTime,
	}
}
