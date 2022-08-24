package main

import (
	"encoding/json"
	"fmt"
	"github.com/nats-io/stan.go"
	"math/rand"
	"strconv"
	"taskL0/items"
	"time"
)

func main() {

	sc, _ := stan.Connect("prod", "simple-pub")
	defer sc.Close()

	for i := 1; ; i++ {
		//fmt.Println(("publish  Bestellung" + strconv.Itoa(i)))
		jsonObj, err := json.Marshal(makeNewOrder("str"+strconv.Itoa(i), rand.Intn(3)))
		if err != nil {
			fmt.Println("err in marshal jsonOrder publish")
			return
		}
		sc.Publish("bestellungen", jsonObj)
		time.Sleep(15 * time.Second)
	}

}

func makeNewOrder(str string, it int) items.Order {
	order := items.Order{}
	order.Order_uid = "b563feb7b2b84b6test"
	order.Track_number = str
	order.Entry = "WBIL"
	order.Locale = "en"
	order.Internal_signature = ""
	order.Customer_id = "test"
	order.Delivery_service = "meest"
	order.Shardkey = "9"
	order.Sm_id = 99
	order.Date_created = time.Now().String()
	order.Oof_shard = "1"

	order.Delivery.Name = "Test Testov"
	order.Delivery.Phone = "+9720000000"
	order.Delivery.Zip = "2639809"
	order.Delivery.City = "Kiryat Mozkin"
	order.Delivery.Address = "Ploshad Mira 15"
	order.Delivery.Region = "Kraiot"
	order.Delivery.Email = "test@gmail.com"

	order.Payment.Transaction = "b563feb7b2b84b6test"
	order.Payment.Request_id = ""
	order.Payment.Currency = "USD"
	order.Payment.Provider = "wbpay"
	order.Payment.Amount = rand.Intn(1000)
	order.Payment.Payment_dt = rand.Intn(1000)
	order.Payment.Bank = ""
	order.Payment.Delivery_cost = rand.Float64() * 100000
	order.Payment.Goods_total = 300
	order.Payment.Custom_fee = 0

	for i := 0; i < it; i++ {
		item := items.Item{
			Chrt_id:      rand.Intn(1000),
			Track_number: "WBILMTESTTRACK",
			Price:        rand.Float64() * 100000,
			Rid:          "ab4219087a764ae0btest",
			Name:         "Mascaras",
			Sale:         30,
			Size:         rand.Intn(20),
			Total_price:  rand.Float64() * 100000,
			Nm_id:        rand.Intn(1000),
			Brand:        "some brand",
			Status:       202,
		}
		order.Items = append(order.Items, item)
	}

	return order
}
