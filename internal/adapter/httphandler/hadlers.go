package httphandler

// POST v1/filter/product JSON {"product_name" string, "blocked" bool} (response 200 OK, 400 Bad request)
// POST v1/products JSON [from task] (respose 202 Accepted, 400 Bad request)
// GET v1/products?product_name=name Headers Authorization Basic is opt (200 OK, 204 No content)
// GET v1/recomendations Headers Authorization Basic is opt (200 OK, 204 No content)
