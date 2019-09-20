package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"errors"

	"github.com/sirupsen/logrus"
)

type Colleagues struct {
	Id         int64  `json:"id"`
	EmployeeId int64  `json:"employeeId"`
	Code       string `json:"code"`
	Name       string `json:"name"`
	UserName   string `json:"userName"`
	Enable     bool   `json:"enable"`
}

func (Colleagues) GetColleaguesAuth(salesmanId int64) (*Colleagues, error) {
	var colleagues Colleagues
	exist, err := factory.GetColleagueAuthEngine().ID(salesmanId).Get(&colleagues)
	if err != nil {
		return nil, err
	} else if !exist {
		logrus.WithFields(logrus.Fields{
			"salesmanId": salesmanId,
		}).Error("Fail to GetColleaguesAuth")
		return nil, errors.New("Colleagues is not exist")
	}
	return &colleagues, nil
}
