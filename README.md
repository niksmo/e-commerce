# E-commerce

## Запуск проекта

```
go mod download
make infra-start
make build
ECOM_CONFIG_FILE=./config.yml ./bin/app
```

## Проверка проекта

```
curl --request POST \
  --url http://127.0.0.1:8000/v1/filter/products \
  --header 'content-type: application/json' \
  --data '{
  "name": "P50 Pro",
  "blocked": true
}'
```

```
curl --request POST \
  --url http://127.0.0.1:8000/v1/products \
  --header 'content-type: application/json' \
  --data @./fixtures/products.json
```

```
curl --request GET \
  --url 'http://127.0.0.1:8000/v1/products?name=p' \
  --header 'authorization: Basic bmljazpwYXNz'
```

```
curl --request GET \
  --url http://127.0.0.1:8000/v1/products/offers
```

Аналитика вычисляется каждые 30 секунд
