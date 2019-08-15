package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"errors"

	"github.com/go-xorm/xorm"
)

type Store struct {
	Id       int64  `query:"id" json:"id"`
	TenantId int64  `query:"tenantId" json:"tenantId"`
	Code     string `query:"code" json:"code"`
	Name     string `query:"name" json:"name"`
}

func (Store) GetStore(storeId int64) (*Store, error) {
	var store Store
	queryBuilder := func() xorm.Interface {
		q := factory.GetPmEngine().ID(storeId)
		return q
	}
	has, err := queryBuilder().Get(&store)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, errors.New("Store not find!")
	}
	return &store, nil
}
