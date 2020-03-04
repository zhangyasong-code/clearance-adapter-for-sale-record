package models

import (
	"errors"
	"time"

	"github.com/pangpanglabs/goutils/number"
)

func GetToFixedPrice(price float64, BaseTrimCode string) float64 {
	if BaseTrimCode == "" || BaseTrimCode == "A" {
		// 原价
		return number.ToFixed(price, nil)
	}

	var setting *number.Setting
	switch BaseTrimCode {
	case "C":
		// 按元向上取整
		setting = &number.Setting{
			RoundStrategy: "ceil",
		}
	case "O":
		// 按角向下取整
		setting = &number.Setting{
			RoundStrategy: "floor",
			RoundDigit:    1,
		}
	case "P":
		// 按角四舍五入
		setting = &number.Setting{
			RoundStrategy: "round",
			RoundDigit:    1,
		}
	case "Q":
		// 按角向上取整
		setting = &number.Setting{
			RoundStrategy: "ceil",
			RoundDigit:    1,
		}
	case "R":
		// 按元四舍五入
		setting = &number.Setting{
			RoundStrategy: "round",
		}
	case "T":
		// 按元向下取整
		setting = &number.Setting{
			RoundStrategy: "floor",
		}
	}
	return number.ToFixed(price, setting)
}

func getPaymentCodeAndPayCreditCardFirmCode(payMethod string) (paymentCode string, payCreditCardFirmCode string, err error) {
	switch payMethod {
	case "CASH":
		paymentCode = "11"
	case "WXPAY":
		paymentCode = "O1"
	case "wechat.prepay":
		paymentCode = "O1"
	case "ALIPAY":
		paymentCode = "O2"
	case "CREDITCARD":
		paymentCode = "12"
		payCreditCardFirmCode = "01"
	default:
		err = errors.New("PayMethod is not exist")
		return "", "", err
	}
	return paymentCode, payCreditCardFirmCode, nil
}

//DateParseToUtc parses a formatted local time string
//and returns the UTC time value it represents.
func DateParseToUtc(date string) (timeUtcString string, err error) {
	timeLayout := "2006-01-02"
	timeLoc, err := time.Parse(timeLayout, date)
	if err != nil {
		return
	}
	timeUtcString = timeLoc.Add(time.Hour * -8).Format("2006-01-02 15:04:05")
	return
}
