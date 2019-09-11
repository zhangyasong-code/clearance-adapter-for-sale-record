package models

import (
	"clearance/clearance-adapter-for-sale-record/factory"
	"errors"

	"github.com/sirupsen/logrus"
)

type Sku struct {
	Id          int64           `json:"id"`
	ProductId   int64           `json:"productId"`
	Code        string          `json:"code"`
	Name        string          `json:"name"`
	Image       string          `json:"image"`
	Identifiers []SkuIdentifier `json:"identifiers"`
}

type SkuIdentifier struct {
	Id        int64  `json:"id,omitempty"`
	SkuId     int64  `json:"skuId,omitempty" xorm:"index"`
	ProductId int64  `json:"productId,omitempty" xorm:"-"`
	Uid       string `json:"uid" xorm:"index"`
	Source    string `json:"source,omitempty" xorm:"index"`
	Enable    bool   `json:"enable"`
}

type Product struct {
	Id         int64   `json:"id"`
	Code       string  `json:"code"`
	Name       string  `json:"name"`
	TitleImage string  `json:"titleImage"`
	ListPrice  float64 `json:"listPrice"`
	HasDigital bool    `json:"hasDigital"`
	Enable     bool    `json:"enable"`
}

type Brand struct {
	Id     int64  `json:"id"`
	Code   string `json:"code"`
	Name   string `json:"name"`
	Enable bool   `json:"enable"`
}

func (Product) GetProductById(id int64) (*Product, error) {
	var product Product
	exist, err := factory.GetProductEngine().ID(id).Get(&product)
	if err != nil {
		return nil, err
	} else if !exist {
		logrus.WithFields(logrus.Fields{
			"id": id,
		}).Error("Fail to GetProductById")
		return nil, errors.New("Product is not exist")
	}
	return &product, nil
}

func (Product) GetSkuBySkuId(skuId int64) (sku *Sku, err error) {
	var SkuAndSkuIdentifiers []struct {
		Sku           Sku           `xorm:"extends"`
		SkuIdentifier SkuIdentifier `xorm:"extends"`
	}
	if err := factory.GetProductEngine().Table("sku").
		Join("left", "sku_identifier", "sku_identifier.sku_id = sku.id").
		Where("sku.id = ?", skuId).
		And("sku.enable = ?", true).
		And("sku_identifier.source = ?", "Barcode").
		Find(&SkuAndSkuIdentifiers); err != nil {
		return nil, err
	}
	if len(SkuAndSkuIdentifiers) != 0 {
		sku = &SkuAndSkuIdentifiers[0].Sku
	}
	for _, skuAndSkuIdentifier := range SkuAndSkuIdentifiers {
		sku.Identifiers = append(sku.Identifiers, skuAndSkuIdentifier.SkuIdentifier)
	}
	return sku, nil
}

func (Product) GetBrandById(brandId int64) (*Brand, error) {
	var brand Brand
	has, err := factory.GetProductEngine().Id(brandId).Where("enable = ?", true).Get(&brand)
	if err != nil {
		return nil, err
	}
	if !has {
		logrus.WithFields(logrus.Fields{
			"brandId": brandId,
		}).Error("Fail to GetBrandById")
		return nil, errors.New("Brand is not exist")
	}
	return &brand, nil
}
