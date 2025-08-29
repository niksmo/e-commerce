package schema_test

import (
	"context"
	"testing"

	"github.com/niksmo/e-commerce/pkg/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockSchemaIdentifier struct {
	mock.Mock
}

func (c *MockSchemaIdentifier) DetermineID(
	ctx context.Context, subject string, avroSchemaText string,
) (id int, err error) {
	args := c.Called(ctx, subject, avroSchemaText)
	return args.Int(0), args.Error(1)
}

func TestSerdeProductV1(t *testing.T) {

	t.Run("NoOpts", func(t *testing.T) {
		_, err := schema.NewSerdeProductV1(t.Context())
		require.Error(t, err)
		assert.ErrorIs(t, err, schema.ErrTooFewOpts)
	})

	t.Run("OneOpt", func(t *testing.T) {
		_, err := schema.NewSerdeProductV1(
			t.Context(),
			schema.SchemaIdentifierOpt(new(MockSchemaIdentifier)),
		)
		require.Error(t, err)
		assert.ErrorIs(t, err, schema.ErrTooFewOpts)
	})

	t.Run("IdentifierAndSubjectOpts", func(t *testing.T) {
		schemaIdentifier := new(MockSchemaIdentifier)
		schemaID := 1
		subject := "testTopic-value"

		schemaIdentifier.On(
			"DetermineID", t.Context(), subject, schema.ProductSchemaTextV1,
		).Return(schemaID, nil)

		_, err := schema.NewSerdeProductV1(
			t.Context(),
			schema.SubjectOpt(subject),
			schema.SchemaIdentifierOpt(schemaIdentifier),
		)
		require.NoError(t, err)
	})

	t.Run("EncodeDecode", func(t *testing.T) {
		schemaIdentifier := new(MockSchemaIdentifier)
		schemaID := 1
		subject := "testTopic-value"

		schemaIdentifier.On(
			"DetermineID", t.Context(), subject, schema.ProductSchemaTextV1,
		).Return(schemaID, nil)

		serde, err := schema.NewSerdeProductV1(
			t.Context(),
			schema.SubjectOpt(subject),
			schema.SchemaIdentifierOpt(schemaIdentifier),
		)
		require.NoError(t, err)

		productValue1 := schema.ProductV1{
			ProductID:   "testProductID",
			Name:        "testName",
			SKU:         "testSKU",
			Brand:       "testBrand",
			Category:    "testCategory",
			Description: "testDescription",
			Price: schema.ProductPriceV1{
				Amount:   123.45,
				Currency: "RUB",
			},
			AvailableStock: 5,
			Tags:           []string{"tag1, tag2, tag3"},
			Images: []schema.ProductImageV1{
				{URL: "imageURL1", Alt: "imageAlt1"},
			},
			Specifications: map[string]string{
				"specField1": "spec1Info",
				"specField2": "spec2Info",
			},
			StoreID: "testStoreID",
		}

		encodedData, err := serde.Encode(productValue1)
		require.NoError(t, err)

		var productValue2 schema.ProductV1
		err = serde.Decode(encodedData, &productValue2)
		require.NoError(t, err)

		assert.Equal(t, productValue1.ProductID, productValue2.ProductID)
		assert.Equal(t, productValue1.Name, productValue2.Name)
		assert.Equal(t, productValue1.SKU, productValue2.SKU)
		assert.Equal(t, productValue1.Brand, productValue2.Brand)
		assert.Equal(t, productValue1.Category, productValue2.Category)
		assert.Equal(t, productValue1.Description, productValue2.Description)
		assert.Equal(t, productValue1.Price, productValue2.Price)
		assert.Equal(t, productValue1.AvailableStock, productValue2.AvailableStock)
		assert.Equal(t, productValue1.StoreID, productValue2.StoreID)

		require.Len(t, productValue2.Tags, len(productValue1.Tags))
		for i, v := range productValue1.Tags {
			assert.Equal(t, productValue1.Tags[i], v)
		}

		require.Len(t, productValue2.Images, len(productValue1.Images))
		for i, v := range productValue2.Images {
			assert.Equal(t, productValue1.Images[i], v)
		}

		require.Len(t, productValue2.Specifications, len(productValue1.Specifications))
		for k, v := range productValue2.Specifications {
			assert.Equal(t, productValue1.Specifications[k], v)
		}
	})

}
