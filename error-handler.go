package messaging

import logging "github.com/mitz-it/golang-logging"

func failOnError(logger *logging.Logger, err error, message string) {
	if err == nil {
		return
	}
	logger.Standard.Panic().AnErr(message, err)
	panic(err)
}
