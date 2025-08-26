package httphandler

import "net/http"

func AllowJSON(next http.Handler) http.Handler {
	hf := func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength == 0 {
			next.ServeHTTP(w, r)
			return
		}

		if r.Header.Get("Content-Type") != "application/json" {
			http.Error(w, "invalid media type", http.StatusUnsupportedMediaType)
			return
		}

		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(hf)
}
