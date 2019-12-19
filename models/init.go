package models

import "github.com/go-xorm/xorm"

func InitDb(db *xorm.Engine) error {
	// return nil
	return db.Sync(new(SaleTransaction), new(SaleTransactionDtl),
		new(SaleRecordIdSuccessMapping), new(SaleRecordIdFailMapping),
		new(CslSaleMst), new(CslSaleDtl), new(CslSalePayment), new(CslStaffSaleRecord),
		new(SaleTransactionPayment), new(CheckSaleNo), new(CslTSaleMst),
		new(CslTSaleDtl), new(CslTSalePayment))
}
