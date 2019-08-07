package adapter

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"clearance/clearance-adapter-for-sale-record/models"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-xorm/core"
	"github.com/pangpanglabs/goetl"
)

const (
	MSLV2_POS = "8"
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
	for {
		var stsAndStds []struct {
			SaleTransaction    models.SaleTransaction    `xorm:"extends"`
			SaleTransactionDtl models.SaleTransactionDtl `xorm:"extends"`
		}
		if err := factory.GetCfsrEngine().Table("sale_transaction").Select("sale_transaction.*,sale_transaction_dtl.*").
			Join("INNER", "sale_transaction_dtl", "sale_transaction_dtl.order_id = sale_transaction.order_id").
			Where("1 = 1").Limit(maxResultCount, skipCount).Find(&stsAndStds); err != nil {
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
	saleTAndSaleTDtls, ok := source.(models.SaleTAndSaleTDtls)
	if !ok {
		return nil, errors.New("Convert Failed")
	}
	saleMsts := make([]models.SaleMst, 0)
	saleDtls := make([]models.SaleDtl, 0)
	token, err := models.SaleMst{}.GetToken(ctx)
	if err != nil {
		return nil, err
	}
	seq := 0
	for i, saleTransaction := range saleTAndSaleTDtls.SaleTransactions {
		saleDate := saleTransaction.SaleDate.Format("20060102")

		//get shopCode BY StoreId
		shopCode, err := models.SaleMst{}.GetShopCode(ctx, saleTransaction.StoreId, token)
		if err != nil {
			return nil, err
		}

		//get last Seq in csl SaleMst
		if i == 0 {
			lastSeq, err := models.SaleMst{}.GetlastSeq(ctx, shopCode, saleDate)
			if err != nil {
				return nil, err
			}
			if lastSeq != "" {
				lastFour := lastSeq[len(lastSeq)-4 : len(lastSeq)]
				intLastFour, err := strconv.Atoi(lastFour)
				if err != nil {
					//    When err != nil intLastFour = A001-A999
					fmt.Println(err)
				}
				if intLastFour != 9999 {
					seq = intLastFour + 1
				}
			}
		}

		//获取四位seq
		sequenceNumber, err := models.SaleMst{}.GetSequenceNumber(seq)
		if err != nil {
			return nil, err
		}
		saleNo := shopCode + saleDate[len(saleDate)-6:len(saleDate)] + MSLV2_POS + sequenceNumber
		seqNo, err := strconv.ParseInt(sequenceNumber, 10, 64)
		if err != nil {
			return nil, err
		}
		saleMsts = append(saleMsts, models.SaleMst{
			SaleNo:        saleNo,
			SeqNo:         seqNo,
			PosNo:         MSLV2_POS,
			Dates:         saleDate,
			ShopCode:      shopCode,
			InDateTime:    time.Now(),
			ActualSaleAmt: saleTransaction.TotalSalePrice,
		})
		for _, saleTransactionDtl := range saleTAndSaleTDtls.SaleTransactionDtls {
			if saleTransactionDtl.OrderId == saleTransaction.OrderId {
				saleDtls = append(saleDtls, models.SaleDtl{
					SaleNo:     saleNo,
					ShopCode:   shopCode,
					DtSeq:      int64(len(saleDtls)),
					SeqNo:      seqNo,
					Dates:      saleDate,
					ProdCode:   strconv.FormatInt(saleTransactionDtl.SkuId, 10),
					InDateTime: time.Now(),
					SaleQty:    saleTransactionDtl.Quantity,
					SaleAmt:    saleTransactionDtl.SalePrice,
				})
			}
		}
		seq += 1
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
