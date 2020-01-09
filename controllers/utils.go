package controllers

import (
	"errors"
	"fmt"
	"nomni/utils/api"
	"strings"
	"time"

	"github.com/labstack/echo"
	"github.com/pangpanglabs/goutils/behaviorlog"
)

func splitTolist(s string) []string {
	var list []string
	for _, v := range strings.Split(strings.TrimSpace(s), ",") {
		if v != "" {
			list = append(list, v)
		}
	}
	return list
}

func DateTimeValidate(startTime time.Time, endTime time.Time) error {
	term := 30
	fmt.Println(startTime)
	fmt.Println(endTime)

	if startTime.IsZero() && endTime.IsZero() {
		return nil
	}
	if startTime.IsZero() && !endTime.IsZero() {
		return errors.New("开始时间不能为空")
	}
	if !startTime.IsZero() && endTime.IsZero() {
		return errors.New("结束时间不能为空")
	}
	if startTime.After(endTime) {
		return errors.New("开始时间不能大于结束时间")
	}
	if startTime.AddDate(0, 0, term-1).Before(endTime) {
		return errors.New(fmt.Sprintf("查询期间不能大于%d天", term))
	}
	return nil
}

func renderFail(c echo.Context, status int, err error) error {
	if err != nil {
		behaviorlog.FromCtx(c.Request().Context()).WithError(err)
	}
	return c.JSON(status, api.Result{
		Success: false,
		Error: api.Error{
			Message: err.Error(),
		},
	})
}
