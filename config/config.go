package config

import (
	"log"

	configutil "github.com/pangpanglabs/goutils/config"
	"github.com/pangpanglabs/goutils/echomiddleware"
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

	log.Println("APP_ENV:", appEnv)
	log.Printf("config: %+v\n", config)

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
	PmConnDatabase struct {
		Driver     string
		Connection string
	}
	ProductDatabase struct {
		Driver     string
		Connection string
	}
	ColleagueAuthDatabase struct {
		Driver     string
		Connection string
	}
	ShopEmployeeDatabase struct {
		Driver     string
		Connection string
	}
	Services struct {
		PlaceManagementApi string
		GetTokenApi        string
	}
	GetTokenUser struct {
		AppId        string
		AppSecretKey string
	}
	BehaviorLog struct {
		Kafka echomiddleware.KafkaConfig
	}
	AppEnv      string
	ServiceName string
}
