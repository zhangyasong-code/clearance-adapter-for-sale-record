package controllers

import (
	"strings"
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
