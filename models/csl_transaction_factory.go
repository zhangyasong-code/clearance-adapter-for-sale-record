package models

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

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
func GetCheckSaleNoWithSeqNo(saleTransaction SaleTransaction, saleDate, MSLV2_POS string) (CheckSaleNo, int64, error) {
	queryCheckSaleNo, err := CheckSaleNo{}.GetCheckSaleNoBySaleTransactionid(saleTransaction.Id)
	if err != nil {
		return CheckSaleNo{}, 0, err
	}
	if queryCheckSaleNo.SaleNo == "" {
		seqNumber, sequenceNumber, err := getSeqNoAndSequenceNumber(saleTransaction.ShopCode, saleDate, MSLV2_POS)
		if err != nil {
			return CheckSaleNo{}, 0, err
		}
		saleNo := saleTransaction.ShopCode + saleDate[len(saleDate)-6:len(saleDate)] + MSLV2_POS + sequenceNumber
		checkSaleNo := &CheckSaleNo{
			TransactionId:          saleTransaction.TransactionId,
			SaleTransactionId:      saleTransaction.Id,
			TransactionChannelType: saleTransaction.TransactionChannelType,
			OrderId:                saleTransaction.OrderId,
			RefundId:               saleTransaction.RefundId,
			ShopCode:               saleTransaction.ShopCode,
			Dates:                  saleDate,
			SaleNo:                 saleNo,
			PosNo:                  MSLV2_POS,
			Processing:             true,
			Whthersend:             false,
		}
		if err = checkSaleNo.Save(); err != nil {
			return CheckSaleNo{}, 0, err
		}
		return makeCheckSaleNoEntity(checkSaleNo), seqNumber, nil
	}
	return queryCheckSaleNo, 0, nil
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
