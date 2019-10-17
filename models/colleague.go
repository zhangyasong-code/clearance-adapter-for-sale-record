package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"errors"

	"github.com/go-xorm/xorm"
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

type Employee struct {
	Id      int64  `json:"id"`
	EmpId   int64  `json:"empId"`
	EmpName string `json:"empName"`
}

func (Colleagues) GetColleaguesAuth(colleaguesId, empId int64) (Colleagues, error) {
	var colleagues Colleagues
	if colleaguesId == 0 && empId == 0 {
		return colleagues, nil
	}
	query := func() xorm.Interface {
		q := factory.GetColleagueAuthEngine().Where("1 = 1")
		if colleaguesId != 0 {
			q.And("id = ? ", colleaguesId)
		}
		if empId != 0 {
			q.And("emp_id = ?", empId)
		}
		return q
	}
	exist, err := query().Get(&colleagues)
	if err != nil {
		return colleagues, err
	} else if !exist {
		logrus.WithFields(logrus.Fields{
			"colleaguesId": colleaguesId,
		}).Error("Fail to GetColleaguesAuth")
		return colleagues, errors.New("Colleagues is not exist")
	}
	return colleagues, nil
}

func (Employee) GetEmployee(salesmanId int64) (*Employee, error) {
	var employee Employee
	exist, err := factory.GetShopEmployeeEngine().ID(salesmanId).Get(&employee)
	if err != nil {
		return nil, err
	} else if !exist {
		logrus.WithFields(logrus.Fields{
			"salesmanId": salesmanId,
		}).Error("Fail to GetEmployee")
		return nil, errors.New("Employee is not exist")
	}
	return &employee, nil
}
