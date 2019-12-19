package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"context"
	"database/sql"
	"strings"
	"time"

	"xorm.io/core"
)

type T_SaleMst struct {
	SaleNo                     string          `query:"saleNo" json:"saleNo" xorm:"pk"`
	BrandCode                  string          `query:"brandCode" json:"brandCode"`
	ShopCode                   string          `query:"shopCode" json:"shopCode"`
	Dates                      string          `query:"dates" json:"dates"`
	SeqNo                      int64           `query:"seqNo" json:"seqNo"`
	SaleMode                   string          `query:"saleMode" json:"saleMode"`
	DepartStoreReceiptNo       string          `query:"departStoreReceiptNo" json:"departStoreReceiptNo"`
	TMall_ID                   sql.NullString  `query:"tMall_ID" json:"tMall_ID"`
	SaleQty                    int64           `query:"saleQty" json:"saleQty"`
	SaleAmt                    float64         `query:"saleAmt" json:"saleAmt"`
	Freight                    sql.NullFloat64 `query:"freight" json:"freight"`
	DiscountAmt                float64         `query:"discountAmt" json:"discountAmt"`
	EstimateSaleAmt            float64         `query:"estimateSaleAmt" json:"estimateSaleAmt"`
	EstimateSaleAmtForConsumer float64         `query:"estimateSaleAmtForConsumer" json:"estimateSaleAmtForConsumer"`
	TMall_UseMileage           sql.NullFloat64 `query:"tMall_UseMileage" json:"tMall_UseMileage"`
	TMall_ObtainMileage        sql.NullFloat64 `query:"tMall_ObtainMileage" json:"tMall_ObtainMileage"`
	PreSaleNo                  sql.NullString  `query:"preSaleNo" json:"preSaleNo"`
	InUserID                   string          `query:"inUserID" json:"inUserID"`
	InDateTime                 time.Time       `query:"inDateTime" json:"inDateTime"`
	ModiUserID                 string          `query:"modiUserID" json:"modiUserID"`
	ModiDateTime               time.Time       `query:"modiDateTime" json:"modiDateTime"`
	DelChk                     bool            `query:"delChk" json:"delChk"`
	Tran_status                string          `query:"tran_status" json:"tran_status"`
	ErrorMessage               sql.NullString  `query:"errorMessage" json:"errorMessage"`
	SaleEventNo                sql.NullInt64   `query:"saleEventNo" json:"saleEventNo"`
	SaleEventName              string          `query:"saleEventName" json:"saleEventName"`
	OfflineShopCode            sql.NullString  `query:"offlineShopCode" json:"offlineShopCode"`
	SaleMan                    sql.NullString  `query:"saleMan" json:"saleMan"`
	ChannelCode                string          `query:"channelCode" json:"channelCode"`
	TransactionId              int64           `json:"-" xorm:"-"`
	StoreId                    int64           `json:"-" xorm:"-"`
	OrderId                    int64           `json:"-" xorm:"-"`
	RefundId                   int64           `json:"-" xorm:"-"`
	SaleTransactionId          int64           `json:"-" xorm:"-"`
	TransactionChannelType     string          `json:"-" xorm:"-"`
	T_SaleDtls                 []T_SaleDtl     `json:"t_SaleDtls" xorm:"-"`
	T_SalePayments             []T_SalePayment `json:"t_SalePayments" xorm:"-"`
}

type T_SaleDtl struct {
	SaleNo                     string          `query:"saleNo" json:"saleNo" xorm:"pk"`
	DtSeq                      int64           `query:"dtSeq" json:"dtSeq" xorm:"pk"`
	TMall_ID                   sql.NullString  `query:"tMall_ID" json:"tMall_ID"`
	TMall_DtlNo                int64           `query:"tMall_DtlNo" json:"tMall_DtlNo"`
	NormalSaleTypeCode         string          `query:"normalSaleTypeCode" json:"normalSaleTypeCode"`
	TMallEventID               sql.NullInt64   `query:"tMallEventID" json:"tMallEventID"`
	TMallEventDesc             sql.NullString  `query:"tMallEventDesc" json:"tMallEventDesc"`
	ProdCode                   string          `query:"prodCode" json:"prodCode"`
	EANCode                    string          `query:"eANCode" json:"eANCode"`
	NormalPrice                float64         `query:"normalPrice" json:"normalPrice"`
	Price                      float64         `query:"price" json:"price"`
	SaleQty                    int64           `query:"saleQty" json:"saleQty"`
	SaleAmt                    float64         `query:"saleAmt" json:"saleAmt"`
	DiscountAmt                float64         `query:"discountAmt" json:"discountAmt"`
	EstimateSaleAmt            float64         `query:"estimateSaleAmt" json:"estimateSaleAmt"`
	EstimateSaleAmtForConsumer float64         `query:"estimateSaleAmtForConsumer" json:"estimateSaleAmtForConsumer"`
	TMall_ObtainMileage        sql.NullFloat64 `query:"tMall_ObtainMileage" json:"tMall_ObtainMileage"`
	PreSaleNo                  sql.NullString  `query:"preSaleNo" json:"preSaleNo"`
	PreSaleDtSeq               sql.NullInt64   `query:"preSaleDtSeq" json:"preSaleDtSeq"`
	DelChk                     bool            `query:"delChk" json:"delChk"`
	InUserID                   string          `query:"inUserID" json:"inUserID"`
	InDateTime                 time.Time       `query:"inDateTime" json:"inDateTime"`
	ModiUserID                 string          `query:"modiUserID" json:"modiUserID"`
	ModiDateTime               time.Time       `query:"modiDateTime" json:"modiDateTime"`
	OrderItemId                int64           `json:"-" xorm:"-"`
	RefundItemId               int64           `json:"-" xorm:"-"`
	TransactionDtlId           int64           `json:"-" xorm:"-"`
	StyleCode                  string          `json:"-" xorm:"-"`
	SaleTransactionId          int64           `json:"-" xorm:"-"`
	SaleTransactionDtlId       int64           `json:"-" xorm:"-"`
	TransactionId              int64           `json:"-" xorm:"-"`
}

type T_SalePayment struct {
	SaleNo            string         `query:"saleNo" json:"saleNo" xorm:"pk"`
	SeqNo             int64          `query:"seqNo" json:"seqNo" xorm:"pk"`
	TMall_ID          sql.NullString `query:"tMall_ID" json:"tMall_ID"`
	PaymentCode       string         `query:"paymentCode" json:"paymentCode"`
	PaymentAmt        float64        `query:"paymentAmt" json:"paymentAmt"`
	InUserID          string         `query:"inUserID" json:"inUserID"`
	InDateTime        time.Time      `query:"inDateTime" json:"inDateTime"`
	ModiUserID        string         `query:"modiUserID" json:"modiUserID"`
	ModiDateTime      time.Time      `query:"modiDateTime" json:"modiDateTime"`
	TransactionId     int64          `json:"-" xorm:"-"`
	SaleTransactionId int64          `json:"-" xorm:"-"`
}

type T_SaleMstsAndSaleDtls struct {
	T_SaleMsts     []T_SaleMst     `json:"t_SaleMsts"`
	T_SaleDtls     []T_SaleDtl     `json:"t_SaleDtls"`
	T_SalePayments []T_SalePayment `json:"t_SalePayments"`
}

func (T_SaleMst) GetCslTSales(ctx context.Context, requestInput RequestInput) (int64, []T_SaleMst, error) {
	engine := factory.GetCSLEngine()
	engine.SetMapper(core.SameMapper{})

	sns := ""
	for _, sn := range requestInput.SaleNos {
		sns += "'" + sn + "'" + ","
	}

	sql := "SELECT * from T_SaleMst where 1 = 1 "
	if sns != "" {
		sql += "and SaleNo in (" + strings.TrimSuffix(sns, ",") + ")"
	}
	if requestInput.ShopCode != "" {
		sql += "and ShopCode = '" + requestInput.ShopCode + "'"
	}
	if requestInput.Dates != "" {
		sql += "and Dates = '" + requestInput.Dates + "'"
	}
	if requestInput.SaleMode != "" {
		sql += "and SaleMode = '" + requestInput.SaleMode + "'"
	}
	var t_SaleMsts []T_SaleMst
	err := engine.SQL(sql).Find(&t_SaleMsts)
	if err != nil {
		return 0, nil, err
	}
	if len(t_SaleMsts) == 0 {
		return 0, nil, nil
	}
	loadSaleNos := ""
	for _, t_SaleMst := range t_SaleMsts {
		loadSaleNos += "'" + t_SaleMst.SaleNo + "'" + ","
	}
	tSaleDtls, err := T_SaleDtl{}.GetCslTDtlBySaleNos(ctx, strings.TrimSuffix(loadSaleNos, ","))
	if err != nil {
		return 0, nil, err
	}
	tSalePayments, err := T_SalePayment{}.GetCslTSalePaymentBySaleNos(ctx, strings.TrimSuffix(loadSaleNos, ","))
	if err != nil {
		return 0, nil, err
	}

	for i, t_SaleMst := range t_SaleMsts {
		for _, tSaleDtl := range tSaleDtls {
			if tSaleDtl.SaleNo == t_SaleMst.SaleNo {
				t_SaleMsts[i].T_SaleDtls = append(t_SaleMsts[i].T_SaleDtls, tSaleDtl)
			}
		}
		for _, tSalePayment := range tSalePayments {
			if tSalePayment.SaleNo == t_SaleMst.SaleNo {
				t_SaleMsts[i].T_SalePayments = append(t_SaleMsts[i].T_SalePayments, tSalePayment)
			}
		}
	}
	return 0, t_SaleMsts, nil
}

func (T_SaleDtl) GetCslTDtlBySaleNos(ctx context.Context, saleNos string) ([]T_SaleDtl, error) {
	engine := factory.GetCSLEngine()
	engine.SetMapper(core.SameMapper{})

	sql := "SELECT * from T_SaleDtl where SaleNo in (" + saleNos + ")"

	var t_SaleDtls []T_SaleDtl
	if err := engine.SQL(sql).Find(&t_SaleDtls); err != nil {
		return nil, err
	}
	return t_SaleDtls, nil
}

func (T_SalePayment) GetCslTSalePaymentBySaleNos(ctx context.Context, saleNos string) ([]T_SalePayment, error) {
	engine := factory.GetCSLEngine()
	engine.SetMapper(core.SameMapper{})

	sql := "SELECT * from T_SalePayment where SaleNo in (" + saleNos + ")"

	var t_SalePayments []T_SalePayment
	if err := engine.SQL(sql).Find(&t_SalePayments); err != nil {
		return nil, err
	}
	return t_SalePayments, nil
}
