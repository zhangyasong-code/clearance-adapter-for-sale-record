package config

import (
	configutil "github.com/pangpanglabs/goutils/config"
	"github.com/sirupsen/logrus"
)

var config C

func Init(appEnv, configPath string, options ...func(*C)) C {
	config.AppEnv = appEnv
	if configPath != "" {
		configutil.SetConfigPath(configPath)
	}
	if err := configutil.Read(appEnv, &config); err != nil {
		logrus.WithError(err).Warn("Fail to load config file")
	}
	for _, option := range options {
		option(&config)
	}
	return config
}

func Config() C {
	return config
}

type C struct {
	SaleRecordConnDatabase struct {
		Driver     string
		Connection string
	}
	CslConnDatabase struct {
		Driver     string
		Connection string
	}
	CfsrConnDatabase struct {
		Driver     string
		Connection string
	}
	Services struct {
		PlaceManagementApi string
		GetTokenApi        string
	}
	GetTokenUser struct {
		UserName string
		Password string
	}
	AppEnv      string
	ServiceName string
}
