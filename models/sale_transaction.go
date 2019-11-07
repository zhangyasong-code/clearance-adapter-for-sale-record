package models

import (
	"context"
	"errors"
	"time"

	"clearance/clearance-adapter-for-sale-record/factory"

	"github.com/go-xorm/xorm"
)

type SaleTransaction struct {
	TransactionId          int64                `json:"transactionId" xorm:"index default 0 pk" validate:"required"`
	OrderId                int64                `json:"orderId" xorm:"index default 0" validate:"required"`
	RefundId               int64                `json:"refundId" xorm:"index default 0" validate:"required"`
	EmpId                  string               `json:"empId" xorm:"index VARCHAR(50)"`
	StoreId                int64                `json:"storeId" xorm:"index default 0" validate:"required"`
	SalesmanId             int64                `json:"salesmanId" xorm:"index default 0" validate:"required"`
	CustomerId             int64                `json:"customerId" xorm:"index default 0" validate:"required"`
	TransactionCreatedId   int64                `json:"transactionCreatedId" xorm:"index default 0" validate:"required"`
	TotalListPrice         float64              `json:"totalListPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalSalePrice         float64              `json:"totalSalePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalTransactionPrice  float64              `json:"totalTransactionPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalDiscountPrice     float64              `json:"totalDiscountPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	SaleDate               time.Time            `json:"saleDate"`
	Mileage                float64              `json:"mileage" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	MileagePrice           float64              `json:"mileagePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	OuterOrderNo           string               `json:"outerOrderNo" xorm:"index VARCHAR(30) notnull" validate:"required"`
	TransactionChannelType string               `json:"transactionChannelType" xorm:"index VARCHAR(30) notnull"`
	BaseTrimCode           string               `json:"baseTrimCode" xorm:"index VARCHAR(30)"`
	Dtls                   []SaleTransactionDtl `json:"dtls" xorm:"-"`
	WhetherSend            bool                 `json:"whetherSend" xorm:"index default false"`
}

type SaleTransactionDtl struct {
	Id                             int64   `json:"id"`
	Quantity                       int64   `json:"quantity" xorm:"notnull" validate:"required"`
	SalePrice                      float64 `json:"salePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	ListPrice                      float64 `json:"listPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalDiscountPrice             float64 `json:"totalDiscountPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	OrderItemId                    int64   `json:"orderItemId" xorm:"index notnull" validate:"required"`
	RefundItemId                   int64   `json:"refundItemId" xorm:"index notnull" validate:"required"`
	ProductId                      int64   `json:"productId" xorm:"index notnull" validate:"required"`
	SkuId                          int64   `json:"skuId" xorm:"index notnull" validate:"gte=0"`
	BrandCode                      string  `json:"brandCode" xorm:"index VARCHAR(30) notnull" validate:"required"`
	BrandId                        int64   `json:"brandId" xorm:"index default 0"`
	ItemCode                       string  `json:"itemCode" xorm:"index VARCHAR(60)"`
	ItemFee                        float64 `json:"itemFee" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalListPrice                 float64 `json:"totalListPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalTransactionPrice          float64 `json:"totalTransactionPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalDistributedCartOfferPrice float64 `json:"totalDistributedCartOfferPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalDistributedItemOfferPrice float64 `json:"totalDistributedItemOfferPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalDistributedPaymentPrice   float64 `json:"totalDistributedPaymentPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalSalePrice                 float64 `json:"totalSalePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	DistributedCashPrice           float64 `json:"distributedCashPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TransactionId                  int64   `json:"transactionId" xorm:"index default 0" validate:"required"`
	TransactionDtlId               int64   `json:"transactionDtlId" xorm:"index default 0" validate:"required"`
}

//SaleTransactionAndSaleTransactionDtl
type SaleTAndSaleTDtls struct {
	SaleTransactions    []SaleTransaction    `json:"saleTransactions"`
	SaleTransactionDtls []SaleTransactionDtl `json:"saleTransactionDtls"`
}

type SaleRecordIdSuccessMapping struct {
	Id            int64     `json:"id"`
	SaleNo        string    `json:"saleNo" xorm:"index VARCHAR(30) notnull"`
	TransactionId int64     `json:"transactionId" xorm:"index default 0"`
	OrderId       int64     `json:"orderId" xorm:"index default 0"`
	RefundId      int64     `json:"refundId" xorm:"index default 0"`
	OrderItemId   int64     `json:"orderItemId" xorm:"index default 0"`
	RefundItemId  int64     `json:"refundItemId" xorm:"index default 0"`
	DtlSeq        int64     `json:"dtlSeq" xorm:"index default 0"`
	CreatedAt     time.Time `json:"createdAt" xorm:"created"`
	CreatedBy     string    `json:"createdBy" xorm:"index VARCHAR(30) notnull"`
}

type SaleRecordIdFailMapping struct {
	Id               int64     `json:"id"`
	OrderId          int64     `json:"orderId" xorm:"index default 0"`
	RefundId         int64     `json:"refundId" xorm:"index default 0"`
	StoreId          int64     `json:"storeId" xorm:"index default 0"`
	TransactionId    int64     `json:"transactionId" xorm:"index default 0"`
	TransactionDtlId int64     `json:"transactionDtlId" xorm:"index default 0"`
	Error            string    `json:"error" xorm:"VARCHAR(1000)"`
	Details          string    `json:"details" xorm:"VARCHAR(100)"`
	Data             string    `json:"data" xorm:"TEXT"`
	IsCreate         bool      `json:"isCreate" xorm:"index notnull default false"`
	CreatedAt        time.Time `json:"createdAt" xorm:"created"`
	CreatedBy        string    `json:"createdBy" xorm:"index VARCHAR(30)"`
}

type RequestInput struct {
	BrandCode      string `json:"brandCode"`
	ChannelType    string `json:"channelType"`
	OrderId        int64  `json:"orderId"`
	RefundId       int64  `json:"refundId"`
	StartAt        string `json:"startAt"`
	EndAt          string `json:"endAt"`
	MaxResultCount int    `json:"maxResultCount"`
	SkipCount      int    `json:"skipCount"`
	StoreId        int    `json:"storeId"`
	TransactionId  int64  `json:"transactionId"`
}

func (srsm *SaleRecordIdSuccessMapping) CheckAndSave() error {
	saleRecordIdSuccessMapping := SaleRecordIdSuccessMapping{}
	has, err := factory.GetCfsrEngine().Where("sale_no = ?", srsm.SaleNo).And("order_item_id = ?", srsm.OrderItemId).
		And("refund_item_id = ? ", srsm.RefundItemId).Get(&saleRecordIdSuccessMapping)
	if err != nil {
		return err
	}
	if !has {
		if _, err := factory.GetCfsrEngine().Insert(srsm); err != nil {
			return err
		}
	}
	return nil
}

func (srfm *SaleRecordIdFailMapping) Save() error {
	var saleRecordIdFailMapping SaleRecordIdFailMapping
	has, err := factory.GetCfsrEngine().Where("transaction_id = ?", srfm.TransactionId).And("is_create = ?", false).Get(&saleRecordIdFailMapping)
	if err != nil {
		return err
	}
	if !has {
		if _, err := factory.GetCfsrEngine().Insert(srfm); err != nil {
			return err
		}
	} else {
		if err := srfm.Update(); err != nil {
			return err
		}
	}
	return nil
}

func (SaleRecordIdSuccessMapping) GetSaleSuccessData(orderId int64, itemId int64) ([]SaleRecordIdSuccessMapping, error) {
	var success []SaleRecordIdSuccessMapping
	queryBuilder := func() xorm.Interface {
		q := factory.GetCfsrEngine().Where("1 = 1")
		if orderId != 0 {
			q.And("order_id = ?", orderId)
		}
		if itemId != 0 {
			q.And("order_item_id = ?", itemId)
		}
		return q
	}
	if err := queryBuilder().Find(&success); err != nil {
		return nil, err
	}
	if len(success) == 0 {
		return nil, errors.New("SaleRecordIdSuccessMapping is not exist!")
	}
	return success, nil
}

func (requestInput RequestInput) Validate() error {
	if requestInput.BrandCode == "" {
		return errors.New("BrandCode can not be null!")
	}
	if requestInput.ChannelType == "" {
		return errors.New("ChannelType can not be null!")
	}
	if requestInput.StartAt != "" && requestInput.EndAt != "" {
		_, err := time.Parse("2006-01-02 15:04:05", requestInput.StartAt)
		if err != nil {
			return errors.New("Please input the correct time format!")
		}
		_, err = time.Parse("2006-01-02 15:04:05", requestInput.EndAt)
		if err != nil {
			return errors.New("Please input the correct time format!")
		}
	}
	if requestInput.OrderId == 0 && requestInput.RefundId == 0 && requestInput.StartAt == "" && requestInput.EndAt == "" {
		return errors.New("In orderId and startAt must be have one condition!")
	}
	return nil
}

func (SaleRecordIdFailMapping) GetAll(ctx context.Context, requestInput RequestInput) (int64, []SaleRecordIdFailMapping, error) {
	var failDatas []SaleRecordIdFailMapping
	query := func() xorm.Interface {
		query := factory.GetCfsrEngine().Where("1 = 1").And("is_create = ?", false)
		if requestInput.StoreId != 0 {
			query.And("store_id = ?", requestInput.StoreId)
		}
		if requestInput.TransactionId != 0 {
			query.And("transaction_id = ?", requestInput.TransactionId)
		}
		return query
	}
	totalCount, err := query().Desc("id").Limit(requestInput.MaxResultCount, requestInput.SkipCount).FindAndCount(&failDatas)
	if err != nil {
		return 0, nil, err
	}
	return totalCount, failDatas, nil
}

func (saleTransaction *SaleTransaction) Update() error {
	if _, err := factory.GetCfsrEngine().ID(saleTransaction.TransactionId).AllCols().Update(saleTransaction); err != nil {
		return err
	}
	for _, saleTransactionDtl := range saleTransaction.Dtls {
		if _, err := factory.GetCfsrEngine().Where("order_item_id = ?", saleTransactionDtl.OrderItemId).
			And("refund_item_id = ?", saleTransactionDtl.RefundItemId).AllCols().Update(saleTransactionDtl); err != nil {
			return err
		}
	}
	return nil
}

func (SaleTransaction) Get(transactionId int64) (SaleTransaction, error) {
	var saleTransactions []struct {
		SaleTransaction    SaleTransaction    `xorm:"extends"`
		SaleTransactionDtl SaleTransactionDtl `xorm:"extends"`
	}
	if err := factory.GetCfsrEngine().Table("sale_transaction").
		Join("INNER", "sale_transaction_dtl", "sale_transaction_dtl.transaction_id = sale_transaction.transaction_id").
		Where("sale_transaction.transaction_id = ? ", transactionId).Find(&saleTransactions); err != nil {
		return SaleTransaction{}, err
	}
	var saleTransaction SaleTransaction
	for i, sale := range saleTransactions {
		if i == 0 {
			saleTransaction = sale.SaleTransaction
		}
		saleTransaction.Dtls = append(saleTransaction.Dtls, sale.SaleTransactionDtl)
	}
	return saleTransaction, nil
}

func (saleRecordIdFailMapping *SaleRecordIdFailMapping) Update() error {
	if _, err := factory.GetCfsrEngine().Where("transaction_id = ?", saleRecordIdFailMapping.TransactionId).AllCols().Update(saleRecordIdFailMapping); err != nil {
		return err
	}
	return nil
}

func (SaleRecordIdSuccessMapping) GetBySaleNo(salNo string) ([]SaleRecordIdSuccessMapping, error) {
	var successes []SaleRecordIdSuccessMapping
	queryBuilder := func() xorm.Interface {
		q := factory.GetCfsrEngine().Where("1 = 1")
		if salNo != "" {
			q.And("sale_no = ?", salNo)
		}
		return q
	}
	if err := queryBuilder().Find(&successes); err != nil {
		return nil, err
	}
	return successes, nil
}

func (SaleTransaction) GetSaleTransactions(ctx context.Context, transactionId, orderId, RefundId int64, maxResultCount, skipCount int) (int64, []SaleTransaction, error) {

	queryBuilder := func() xorm.Interface {
		q := factory.GetCfsrEngine().Where("1=1")
		if transactionId > 0 {
			q.And("transaction_id =?", transactionId)
		}
		if orderId > 0 {
			q.And("order_id =?", orderId)
		}
		if RefundId > 0 {
			q.And("refund_id =?", RefundId)
		}
		return q
	}
	query := queryBuilder()

	if maxResultCount > 0 {
		query.Limit(maxResultCount, skipCount)
	}

	query.Desc("transaction_id")

	var saleTransactions []SaleTransaction
	totalCount, err := query.FindAndCount(&saleTransactions)
	if err != nil {
		return 0, nil, err
	}

	var transactionIds []int64
	for _, t := range saleTransactions {
		transactionIds = append(transactionIds, t.TransactionId)
	}

	saleTransactionDtls, err := SaleTransaction{}.GetSaleTransactionDtls(ctx, transactionIds)
	if err != nil {
		return 0, nil, err
	}

	for i, saleTransaction := range saleTransactions {
		for _, saleTransactionDtl := range saleTransactionDtls {
			if saleTransaction.TransactionId == saleTransactionDtl.TransactionId {
				saleTransactions[i].Dtls = append(saleTransactions[i].Dtls, saleTransactionDtl)
			}
		}
	}

	return totalCount, saleTransactions, nil
}

func (SaleTransaction) GetSaleTransactionDtls(ctx context.Context, transactionIds []int64) ([]SaleTransactionDtl, error) {
	queryBuilder := func() xorm.Interface {
		q := factory.GetCfsrEngine().Where("1=1")
		if len(transactionIds) > 0 {
			q.In("transaction_id", transactionIds)
		}
		return q
	}
	query := queryBuilder()
	query.Desc("transaction_id").Desc("transaction_dtl_id")

	var saleTransactionDtls []SaleTransactionDtl
	if err := query.Find(&saleTransactionDtls); err != nil {
		return nil, err
	}

	return saleTransactionDtls, nil
}
