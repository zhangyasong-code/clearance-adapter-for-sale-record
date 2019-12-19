package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"database/sql"
	"time"

	"github.com/go-xorm/xorm"
)

type CslTSaleMst struct {
	Id                         int64           `json:"id"`
	SaleNo                     string          `query:"saleNo" json:"saleNo" xorm:"index"`
	SaleTransactionId          int64           `json:"saleTransactionId" xorm:"index default 0"`
	TransactionId              int64           `json:"transactionId" xorm:"index default 0"`
	StoreId                    int64           `json:"storeId" xorm:"index default 0"`
	OrderId                    int64           `json:"orderId" xorm:"index default 0"`
	RefundId                   int64           `json:"refundId" xorm:"index default 0"`
	BrandCode                  string          `query:"brandCode" json:"brandCode"`
	ShopCode                   string          `query:"shopCode" json:"shopCode"`
	Dates                      string          `query:"dates" json:"dates"`
	SeqNo                      int64           `query:"seqNo" json:"seqNo"`
	SaleMode                   string          `query:"saleMode" json:"saleMode"`
	DepartStoreReceiptNo       string          `query:"departStoreReceiptNo" json:"departStoreReceiptNo"`
	TMall_ID                   sql.NullString  `query:"tMall_ID" json:"tMall_ID"`
	SaleQty                    int64           `query:"saleQty" json:"saleQty"`
	SaleAmt                    float64         `query:"saleAmt" json:"saleAmt"`
	Freight                    float64         `query:"freight" json:"freight"`
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
	T_SaleDtls                 []T_SaleDtl     `json:"t_SaleDtls" xorm:"-"`
	T_SalePayments             []T_SalePayment `json:"t_SalePayments" xorm:"-"`
}

type CslTSaleDtl struct {
	Id                         int64           `json:"id"`
	SaleTransactionId          int64           `json:"saleTransactionId" xorm:"index default 0"`
	SaleTransactionDtlId       int64           `json:"saleTransactionDtlId" xorm:"index default 0"`
	TransactionId              int64           `json:"transactionId" xorm:"index default 0"`
	OrderItemId                int64           `json:"orderItemId" xorm:"index default 0"`
	RefundItemId               int64           `json:"refundItemId" xorm:"index default 0"`
	TransactionDtlId           int64           `json:"transactionDtlId" xorm:"index default 0"`
	SaleNo                     string          `query:"saleNo" json:"saleNo" xorm:"index"`
	DtSeq                      int64           `query:"dtSeq" json:"dtSeq" xorm:"index"`
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
}

type CslTSalePayment struct {
	Id                int64          `json:"id"`
	SaleTransactionId int64          `json:"saleTransactionId" xorm:"index default 0"`
	TransactionId     int64          `json:"transactionId" xorm:"index default 0"`
	SaleNo            string         `query:"saleNo" json:"saleNo" xorm:"index"`
	SeqNo             int64          `query:"seqNo" json:"seqNo" xorm:"index"`
	TMall_ID          sql.NullString `query:"tMall_ID" json:"tMall_ID"`
	PaymentCode       string         `query:"paymentCode" json:"paymentCode"`
	PaymentAmt        float64        `query:"paymentAmt" json:"paymentAmt"`
	InUserID          string         `query:"inUserID" json:"inUserID"`
	InDateTime        time.Time      `query:"inDateTime" json:"inDateTime"`
	ModiUserID        string         `query:"modiUserID" json:"modiUserID"`
	ModiDateTime      time.Time      `query:"modiDateTime" json:"modiDateTime"`
}

func (cslTSaleMst *CslTSaleMst) Save() error {
	if _, err := factory.GetCfsrEngine().Insert(cslTSaleMst); err != nil {
		return err
	}
	return nil
}

func (cslTSaleMst *CslTSaleDtl) Save() error {
	if _, err := factory.GetCfsrEngine().Insert(cslTSaleMst); err != nil {
		return err
	}
	return nil
}

func (cslTSalePayment *CslTSalePayment) Save() error {
	if _, err := factory.GetCfsrEngine().Insert(cslTSalePayment); err != nil {
		return err
	}
	return nil
}

func (CslTSaleMst) GetAll(requestInput RequestInput) ([]CslTSaleMst, error) {
	var cslTSaleMsts []CslTSaleMst
	queryBuilder := func() xorm.Interface {
		q := factory.GetCfsrEngine().Where("1 = 1")
		if requestInput.TransactionId != 0 {
			q.And("transaction_id = ?", requestInput.TransactionId)
		}
		if requestInput.SaleTransactionId != 0 {
			q.And("sale_transaction_id = ?", requestInput.SaleTransactionId)
		}
		return q
	}
	if requestInput.MaxResultCount > 0 {
		queryBuilder().Limit(requestInput.MaxResultCount, requestInput.SkipCount)
	}
	if err := queryBuilder().Find(&cslTSaleMsts); err != nil {
		return nil, err
	}
	if len(cslTSaleMsts) == 0 {
		return nil, nil
	}
	return cslTSaleMsts, nil
}

func (CslTSaleMst) Delete(requestInput RequestInput) error {
	queryBuilder := func() xorm.Interface {
		q := factory.GetCfsrEngine().Where("1 = 1").And("sale_transaction_id = ?", requestInput.SaleTransactionId)
		return q
	}
	if _, err := queryBuilder().Delete(&CslTSaleDtl{}); err != nil {
		return err
	}
	if _, err := queryBuilder().Delete(&CslTSaleMst{}); err != nil {
		return err
	}
	if _, err := queryBuilder().Delete(&CslTSalePayment{}); err != nil {
		return err
	}
	return nil
}
