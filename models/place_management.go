package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"errors"

	"github.com/go-xorm/xorm"
	"github.com/sirupsen/logrus"
)

type Store struct {
	Id         int64  `query:"id" json:"id"`
	TenantCode string `query:"tenantCode" json:"tenantCode"`
	Code       string `query:"code" json:"code"`
	Name       string `query:"name" json:"name"`
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
		logrus.WithFields(logrus.Fields{
			"storeId": storeId,
		}).Error("Store not find!")
		return nil, errors.New("Store not find!")
	}
	return &store, nil
}
