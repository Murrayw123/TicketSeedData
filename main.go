package main

import (
	"encoding/csv"
	"fmt"
	"github.com/joho/godotenv"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"time"
)

type Product struct {
	SKU         int
	Description string
	Category    string
	RRP         float64
	Attribute1  string
	Attribute2  string
	Attribute3  string
}

type Store struct {
	ID    int
	Name  string
	State string
}

type StoreProduct struct {
	StoreID   int
	ProductID int
}

var States = []string{"NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"}

var Categories = []string{"FOOD", "DRINK", "CLOTHING", "ELECTRONICS", "HOME", "SPORTS", "TOYS", "GAMES", "BOOKS", "MUSIC", "MOVIES", "GARDEN", "HEALTH", "BEAUTY", "PETS", "OTHER"}

var RandomProductAttributes = []string{"GOOD", "BAD", "UGLY", "FAST", "SLOW", "SMALL", "LARGE", "HOT", "COLD", "WET", "DRY", "BRIGHT", "DARK", "LOUD", "SOFT", "HARD", "SHINY", "DULL", "BOLD", "MILD", "SWEET", "SOUR", "SALTY", "SPICY", "SILKY", "ROUGH", "FLUFFY"}

func main() {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	numProducts := 10000
	numStores := 70
	numStoreProducts := 1000

	// create the directory "data" if it does not exist
	if _, err := os.Stat("data"); os.IsNotExist(err) {
		os.Mkdir("data", 0755)
	}

	products := generateProducts(numProducts)

	stores := generateStores(numStores)

	storeProducts := generateStoreProducts(numStoreProducts, numStores, numProducts)

	regularTimestamp := time.Now().Format("2006-01-02_15:04:05")

	productsDir := "data/products_" + regularTimestamp + ".csv"
	writeToCSV(productsDir, []string{"product_sku", "product_description", "product_category", "product_rrp", "attribute_1", "attribute_2"}, products, func(p interface{}) []string {
		product := p.(Product)
		return []string{
			strconv.Itoa(product.SKU),
			product.Description,
			product.Category,
			strconv.FormatFloat(product.RRP, 'f', 2, 64),
			product.Attribute1,
			product.Attribute2,
		}
	})

	storesDir := "data/stores_" + regularTimestamp + ".csv"
	writeToCSV(storesDir, []string{"store_id", "store_name", "store_state"}, stores, func(s interface{}) []string {
		store := s.(Store)
		return []string{
			strconv.Itoa(store.ID),
			store.Name,
			store.State,
		}
	})

	storeProductsDir := "data/store_products_" + regularTimestamp + ".csv"
	writeToCSV(storeProductsDir, []string{"store_id", "product_sku"}, storeProducts, func(sp interface{}) []string {
		storeProduct := sp.(StoreProduct)
		return []string{
			strconv.Itoa(storeProduct.StoreID),
			strconv.Itoa(storeProduct.ProductID),
		}
	})

	productOffersDir := "data/product_offers_" + regularTimestamp + ".csv"
	writeToCSV(productOffersDir, []string{"product_sku", "product_offer_price"}, products, func(p interface{}) []string {
		product := p.(Product)
		if rand.Intn(3) == 1 {
			return []string{
				strconv.Itoa(product.SKU),
				strconv.FormatFloat(rand.Float64()*product.RRP, 'f', 2, 64),
			}
		}
		return nil
	})

	categoryOffersDir := "data/category_offers_" + regularTimestamp + ".csv"
	writeToCSV(categoryOffersDir, []string{"category", "category_offer_discount"}, Categories, func(c interface{}) []string {
		category := c.(string)
		if rand.Intn(2) == 1 {
			return []string{
				category,
				strconv.FormatFloat(rand.Float64()*0.2, 'f', 2, 64),
			}
		}
		return nil
	})

	files := map[string]string{
		"products":        productsDir,
		"stores":          storesDir,
		"store_products":  storeProductsDir,
		"product_offers":  productOffersDir,
		"category_offers": categoryOffersDir,
	}

	uploadToS3(files)
}

func generateProducts(numProducts int) []Product {
	products := make([]Product, numProducts)

	// every now and again drastically reduce the amount of products
	// this is to simulate a product "flush"
	if rand.Intn(10) == 1 {
		numProducts = rand.Intn(numProducts/10) + 1
	}

	for i := 0; i < numProducts; i++ {
		products[i] = Product{
			SKU:         i + 1,
			Description: fmt.Sprintf("Product %d", i+1),
			Category:    Categories[rand.Intn(len(Categories))],
			RRP:         rand.Float64()*100 + 10,
			Attribute1:  RandomProductAttributes[rand.Intn(len(RandomProductAttributes))],
			Attribute2:  RandomProductAttributes[rand.Intn(len(RandomProductAttributes))],
		}
	}

	return products
}
func generateStores(numStores int) []Store {
	stores := make([]Store, numStores)

	for i := 0; i < numStores; i++ {
		stores[i] = Store{
			ID:    i + 1,
			Name:  fmt.Sprintf("Store %d", i+1),
			State: States[rand.Intn(len(States))],
		}
	}

	return stores
}

func generateStoreProducts(numStoreProducts, numStores, numProducts int) []StoreProduct {
	storeProducts := make([]StoreProduct, numStoreProducts)

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < numStoreProducts; i++ {
		storeID := rand.Intn(numStores) + 1
		productID := rand.Intn(numProducts) + 1

		storeProducts[i] = StoreProduct{
			StoreID:   storeID,
			ProductID: productID,
		}
	}

	return storeProducts
}

func writeToCSV(filename string, header []string, data interface{}, recordFunc func(interface{}) []string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	writer.Write(header)

	v := reflect.ValueOf(data)

	for i := 0; i < v.Len(); i++ {
		record := recordFunc(v.Index(i).Interface())
		if record != nil {
			writer.Write(record)
		}
	}

	writer.Flush()
}
