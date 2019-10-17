package models

import (
	"context"
	"errors"
	"time"

	"clearance/clearance-adapter-for-sale-record/factory"

	"github.com/go-xorm/xorm"
)

type SaleTransaction struct {
	TransactionId          int64     `json:"transactionId" xorm:"index default 0 pk" validate:"required"`
	OrderId                int64     `json:"orderId" xorm:"index default 0" validate:"required"`
	RefundId               int64     `json:"refundId" xorm:"index default 0" validate:"required"`
	EmpId                  string    `json:"empId" xorm:"index VARCHAR(50)"`
	StoreId                int64     `json:"storeId" xorm:"index default 0" validate:"required"`
	SalesmanId             int64     `json:"salesmanId" xorm:"index default 0" validate:"required"`
	CustomerId             int64     `json:"customerId" xorm:"index default 0" validate:"required"`
	TransactionCreatedId   int64     `json:"transactionCreatedId" xorm:"index default 0" validate:"required"`
	TotalListPrice         float64   `json:"totalListPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalSalePrice         float64   `json:"totalSalePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalTransactionPrice  float64   `json:"totalTransactionPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	TotalDiscountPrice     float64   `json:"totalDiscountPrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	SaleDate               time.Time `json:"saleDate"`
	Mileage                float64   `json:"mileage" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	MileagePrice           float64   `json:"mileagePrice" xorm:"DECIMAL(18,2) default 0" validate:"gte=0"`
	OuterOrderNo           string    `json:"outerOrderNo" xorm:"index VARCHAR(30) notnull" validate:"required"`
	TransactionChannelType string    `json:"transactionChannelType" xorm:"index VARCHAR(30) notnull"`

	Dtls []SaleTransactionDtl `json:"dtls"`
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
}

//SaleTransactionAndSaleTransactionDtl
type SaleTAndSaleTDtls struct {
	SaleTransactions    []SaleTransaction    `json:"saleTransactions"`
	SaleTransactionDtls []SaleTransactionDtl `json:"saleTransactionDtls"`
}

type SaleRecordIdSuccessMapping struct {
	Id            int64     `json:"id"`
	SaleNo        string    `json:"saleNo" xorm:"index VARCHAR(30) notnull"`
	TransactionId int64     `json:"transactionId" xorm:"index default 0" validate:"required"`
	OrderItemId   int64     `json:"orderItemId" xorm:"index default 0" validate:"required"`
	RefundItemId  int64     `json:"refundItemId" xorm:"index default 0" validate:"required"`
	DtlSeq        int64     `json:"dtlSeq" xorm:"index default 0" validate:"required"`
	CreatedAt     time.Time `json:"createdAt" xorm:"created"`
	CreatedBy     string    `json:"createdBy" xorm:"index VARCHAR(30) notnull"`
}

type SaleRecordIdFailMapping struct {
	Id               int64     `json:"id"`
	StoreId          int64     `json:"storeId" xorm:"index default 0"`
	TransactionId    int64     `json:"transactionId" xorm:"index default 0" validate:"required"`
	TransactionDtlId int64     `json:"transactionDtlId" xorm:"index default 0"`
	Error            string    `json:"error" xorm:"VARCHAR(1000)"`
	Details          string    `json:"details" xorm:"VARCHAR(1000)"`
	IsCreate         bool      `json:"isCreate" xorm:"index notnull default false"`
	CreatedAt        time.Time `json:"createdAt" xorm:"created"`
	CreatedBy        string    `json:"createdBy" xorm:"index VARCHAR(30)"`
}

type RequestInput struct {
	BrandCode   string `json:"brandCode"`
	ChannelType string `json:"channelType"`
	OrderId     int64  `json:"orderId"`
	RefundId    int64  `json:"refundId"`
	StartAt     string `json:"startAt"`
	EndAt       string `json:"endAt"`
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
	if _, err := factory.GetCfsrEngine().Insert(srfm); err != nil {
		return err
	}
	return nil
}

func (SaleRecordIdSuccessMapping) Get(orderId int64, itemId int64) ([]SaleRecordIdSuccessMapping, error) {
	var success []SaleRecordIdSuccessMapping
	queryBuilder := func() xorm.Interface {
		q := factory.GetCfsrEngine().Where("1 = 1")
		if orderId != 0 {
			q.And("transaction_id = ?", orderId)
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
func (SaleTransaction) GetAll(ctx context.Context, maxResultCount, skipCount int) (int64, []SaleTransaction, error) {
	var saleTransactions []SaleTransaction
	totalCount, err := factory.GetCfsrEngine().Desc("transaction_id").Limit(maxResultCount, skipCount).FindAndCount(&saleTransactions)
	if err != nil {
		return 0, nil, err
	}

	var transactionIds []interface{}
	for _, t := range saleTransactions {
		transactionIds = append(transactionIds, t.TransactionId)
	}

	var dtls []SaleTransactionDtl
	if err := factory.GetCfsrEngine().In("transaction_id", transactionIds...).Find(&dtls); err != nil {
		return 0, nil, err
	}

	for i := range saleTransactions {
		for j := range dtls {
			if saleTransactions[i].TransactionId == dtls[j].TransactionId {
				saleTransactions[i].Dtls = append(saleTransactions[i].Dtls, dtls[j])
			}
		}
	}

	return totalCount, saleTransactions, nil
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

func (SaleRecordIdFailMapping) GetAll(ctx context.Context, maxResultCount, skipCount, storeId int) (int64, []SaleRecordIdFailMapping, error) {
	var failDatas []SaleRecordIdFailMapping
	query := func() xorm.Interface {
		query := factory.GetCfsrEngine().Where("1 = 1").And("is_create = ?", false)
		if storeId != 0 {
			query = query.And("store_id = ?", storeId)
		}
		return query
	}
	totalCount, err := query().Limit(maxResultCount, skipCount).FindAndCount(&failDatas)
	if err != nil {
		return 0, nil, err
	}
	return totalCount, failDatas, nil
}
