package schema

import (
	"fmt"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProductV1(t *testing.T) {
	t.Run("Regular", func(t *testing.T) {
		vMarshal := ProductV1{
			ProductID:   "testProductID",
			Name:        "testName",
			SKU:         "testSKU",
			Brand:       "testBrand",
			Category:    "testCategory",
			Description: "testDescription",
			Price: ProductPriceV1{
				Amount:   123.45,
				Currency: "RUB",
			},
			AvailableStock: 5,
			Tags:           []string{"tag1, tag2, tag3"},
			Images: []ProductImageV1{
				{URL: "imageURL1", Alt: "imageAlt1"},
			},
			Specifications: map[string]string{
				"specField1": "spec1Info",
				"specField2": "spec2Info",
			},
			StoreID: "testStoreID",
		}

		var productSchema avro.Schema

		require.NotPanics(t, func() {
			productSchema = ProductV1Avro()
		})

		data, err := avro.Marshal(productSchema, vMarshal)
		require.NoError(t, err)

		var vUnmarshal ProductV1
		err = avro.Unmarshal(productSchema, data, &vUnmarshal)
		require.NoError(t, err)

		assert.Equal(t, vMarshal.ProductID, vUnmarshal.ProductID)
		assert.Equal(t, vMarshal.Name, vUnmarshal.Name)
		assert.Equal(t, vMarshal.SKU, vUnmarshal.SKU)
		assert.Equal(t, vMarshal.Brand, vUnmarshal.Brand)
		assert.Equal(t, vMarshal.Category, vUnmarshal.Category)
		assert.Equal(t, vMarshal.Description, vUnmarshal.Description)
		assert.Equal(t, vMarshal.Price, vUnmarshal.Price)
		assert.Equal(t, vMarshal.AvailableStock, vUnmarshal.AvailableStock)
		assert.Equal(t, vMarshal.StoreID, vUnmarshal.StoreID)

		require.Len(t, vUnmarshal.Tags, len(vMarshal.Tags))
		for i, v := range vMarshal.Tags {
			assert.Equal(t, vMarshal.Tags[i], v)
		}

		require.Len(t, vUnmarshal.Images, len(vMarshal.Images))
		for i, v := range vUnmarshal.Images {
			assert.Equal(t, vMarshal.Images[i], v)
		}

		require.Len(t, vUnmarshal.Specifications, len(vMarshal.Specifications))
		for k, v := range vUnmarshal.Specifications {
			assert.Equal(t, vMarshal.Specifications[k], v)
		}
	})

	t.Run("NilArrayAndMap", func(t *testing.T) {
		vMarshal := ProductV1{
			ProductID:   "testProductID",
			Name:        "testName",
			SKU:         "testSKU",
			Brand:       "testBrand",
			Category:    "testCategory",
			Description: "testDescription",
			Price: ProductPriceV1{
				Amount:   123.45,
				Currency: "RUB",
			},
			AvailableStock: 5,
			Tags:           nil,
			Images:         nil,
			Specifications: nil,
			StoreID:        "testStoreID",
		}

		var pSchema avro.Schema

		require.NotPanics(t, func() {
			pSchema = ProductV1Avro()
		})

		data, err := avro.Marshal(pSchema, vMarshal)
		require.NoError(t, err)

		var vUnmarshal ProductV1
		err = avro.Unmarshal(pSchema, data, &vUnmarshal)
		require.NoError(t, err)

		assert.Equal(t, vMarshal.ProductID, vUnmarshal.ProductID)
		assert.Equal(t, vMarshal.Name, vUnmarshal.Name)
		assert.Equal(t, vMarshal.SKU, vUnmarshal.SKU)
		assert.Equal(t, vMarshal.Brand, vUnmarshal.Brand)
		assert.Equal(t, vMarshal.Category, vUnmarshal.Category)
		assert.Equal(t, vMarshal.Description, vUnmarshal.Description)
		assert.Equal(t, vMarshal.Price, vUnmarshal.Price)
		assert.Equal(t, vMarshal.AvailableStock, vUnmarshal.AvailableStock)
		assert.Equal(t, vMarshal.StoreID, vUnmarshal.StoreID)

		require.Len(t, vUnmarshal.Tags, len(vMarshal.Tags))
		for i, v := range vMarshal.Tags {
			assert.Equal(t, vMarshal.Tags[i], v)
		}

		require.Len(t, vUnmarshal.Images, len(vMarshal.Images))
		for i, v := range vUnmarshal.Images {
			assert.Equal(t, vMarshal.Images[i], v)
		}

		require.Len(t, vUnmarshal.Specifications, len(vMarshal.Specifications))
		for k, v := range vUnmarshal.Specifications {
			assert.Equal(t, vMarshal.Specifications[k], v)
		}

		fmt.Printf("UnvarshaledValue: %+v\n", vUnmarshal)
	})
}

func TestProductFilterV1(t *testing.T) {
	vMarshal := ProductFilterV1{
		ProductName: "testProductName",
		Blocked:     true,
	}

	var fSchema avro.Schema

	require.NotPanics(t, func() {
		fSchema = ProductFilterV1Avro()
	})

	data, err := avro.Marshal(fSchema, vMarshal)
	require.NoError(t, err)

	var vUnmarshal ProductFilterV1
	err = avro.Unmarshal(fSchema, data, &vUnmarshal)
	require.NoError(t, err)

	assert.Equal(t, vMarshal, vUnmarshal)
}
