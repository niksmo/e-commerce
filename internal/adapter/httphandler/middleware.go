package httphandler

import (
	"context"
	"net/http"
)

type contextKey int

const (
	contextUserKey contextKey = iota
)

func AllowJSON(next http.Handler) http.Handler {
	h := func(w http.ResponseWriter, r *http.Request) {
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
	return http.HandlerFunc(h)
}

func UserContext(next http.Handler) http.Handler {
	h := func(w http.ResponseWriter, r *http.Request) {
		user, _, ok := r.BasicAuth()
		if ok {
			ctxWithUser := context.WithValue(
				r.Context(), contextUserKey, user)

			r = r.WithContext(ctxWithUser)
		}
		next.ServeHTTP(w, r)
	}
	return http.HandlerFunc(h)
}
