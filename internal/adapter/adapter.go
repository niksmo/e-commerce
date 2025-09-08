package adapter

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// A MakeTLSConfig returns [*tls.Config].
//
// All args are the filepaths.
func MakeTLSConfig(ca, cert, key string) *tls.Config {
	const op = "kafka.MakeTLSConfig"

	caCert, err := os.ReadFile(ca)
	if err != nil {
		err = fmt.Errorf("%s: failed to read CA certificate file: %w", op, err)
		panic(err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		err = fmt.Errorf("%s: %s", op, "failed to parse CA certificate")
		panic(err)
	}

	return &tls.Config{
		RootCAs:   caCertPool,
		ClientCAs: caCertPool,
	}
}
