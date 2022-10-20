package messaging

import (
	"net"
	"regexp"
	"strconv"
	"strings"
)

const ip_regex string = `(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}`
const host_or_ip_regex string = `@(\S+.*?):`
const connString_password_regex string = `://\w*?:(.*?)@`
const connString_regex string = `^amqp://\S+:\S+@\S+:\d*/$`
const connString_port_regex = `:(\d+.*?)/`

func isIPAddress(hostOrIpAddr string) (bool, string) {
	regex := regexp.MustCompile(ip_regex)
	match := regex.MatchString(hostOrIpAddr)
	return match, hostOrIpAddr
}

func hostOrIPFromConnectionString(connectionString string) string {
	regex := regexp.MustCompile(host_or_ip_regex)
	matches := regex.FindAllStringSubmatch(connectionString, -1)[0]
	hostOrIpAddr := matches[1]
	return hostOrIpAddr
}

func sanatizeConnectionString(connectionString string) string {
	regex := regexp.MustCompile(connString_password_regex)
	matches := regex.FindAllStringSubmatch(connectionString, -1)[0]
	password := matches[1]
	obfuscatedPassword := strings.Repeat("*", len(password))

	return strings.Replace(connectionString, password, obfuscatedPassword, 1)
}

func isAMQPConnectionString(connectionString string) bool {
	connStringRegex := regexp.MustCompile(connString_regex)
	return connStringRegex.MatchString(connectionString)
}

func getPortFromConnectionString(connectionString string) int {
	regex := regexp.MustCompile(connString_port_regex)
	matches := regex.FindAllStringSubmatch(connectionString, -1)[0]
	port, _ := strconv.Atoi(matches[1])
	return port
}

func getPeerNames(ip string) []string {
	hosts, _ := net.LookupAddr(ip)
	return hosts
}

func getPeerAddresses(host string) []string {
	ips, _ := net.LookupHost(host)
	return ips
}

func joinNetworkTagValus(values []string) string {
	return strings.Join(values, ",")
}
