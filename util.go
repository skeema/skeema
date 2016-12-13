package tengo

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// EscapeIdentifier is for use in safely escaping MySQL identifiers (table
// names, column names, etc). It doubles any backticks already present in the
// input string, and then returns the string wrapped in outer backticks.
func EscapeIdentifier(input string) string {
	escaped := strings.Replace(input, "`", "``", -1)
	return fmt.Sprintf("`%s`", escaped)
}

// SplitHostOptionalPort takes an address string containing a hostname, ipv4
// addr, or ipv6 addr; *optionally* followed by a colon and port number. It
// splits the hostname portion from the port portion and returns them
// separately. If no port was present, 0 will be returned for that portion.
// If hostaddr contains an ipv6 address, the IP address portion must be
// wrapped in brackets on input, and the brackets will still be present on
// output.
func SplitHostOptionalPort(hostaddr string) (string, int, error) {
	if len(hostaddr) == 0 {
		return "", 0, errors.New("Cannot parse blank host address")
	}

	// ipv6 without port, or ipv4 or hostname without port
	if (hostaddr[0] == '[' && hostaddr[len(hostaddr)-1] == ']') || len(strings.Split(hostaddr, ":")) == 1 {
		return hostaddr, 0, nil
	}

	host, portString, err := net.SplitHostPort(hostaddr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		return "", 0, err
	} else if port < 1 {
		return "", 0, fmt.Errorf("invalid port %d supplied", port)
	}

	// ipv6 with port: add the brackets back in -- net.SplitHostPort removes them,
	// but we still need them to form a valid DSN later
	if hostaddr[0] == '[' && host[0] != '[' {
		host = fmt.Sprintf("[%s]", host)
	}

	return host, port, nil
}
