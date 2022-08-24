package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
	"go.uber.org/zap"
	"net/http"
	"strconv"
	"taskL0/items"
	"time"
)

var orders []items.Order
var db *sql.DB

func main() {
	orders = fillOrders()
	fmt.Println("orders = ", orders)

	fmt.Println("time = ", time.Now().String())

	zapLogger, err := zap.NewProduction()
	if err != nil {
		fmt.Println("can't .NewProduction()")
		return
	}
	defer func() {
		err = zapLogger.Sync()
		fmt.Println("can't  zapLogger.Sync()")
	}()
	//logger := zapLogger.Sugar()

	r := mux.NewRouter()

	dsn := "postgres://postgres:root@localhost:5432/my_first_db?sslmode=disable"

	db, err = sql.Open("postgres", dsn)
	if err != nil {
		fmt.Println("errpr!!!!", err)
		return
	}

	db.SetMaxOpenConns(10)

	err = db.Ping() // вот тут будет первое подключение к базе
	if err != nil {
		panic(err)
	}

	r.HandleFunc("/api/getOrder/{ORDER_ID}", getOrder).Methods("GET")
	r.PathPrefix("/").Handler(http.StripPrefix("/", http.FileServer(http.Dir("./"))))

	orders = getOrderFromDB(db, -1)
	fmt.Println("========================")
	fmt.Println("all orders : ")
	for it, order := range orders {
		fmt.Println(it, ". ", order)
	}
	fmt.Println("========================")

	go func() {
		listenNapsOrders(db)
	}()

	port := "8085"
	fmt.Println("start serv on port " + port)
	err = http.ListenAndServe(":"+port, r)
	if err != nil {
		fmt.Println("can't Listen and server")
		return
	}

}

func listenNapsOrders(db *sql.DB) {
	sc, _ := stan.Connect("prod", "sub-1")
	defer sc.Close()

	sc.Subscribe("bestellungen", func(m *stan.Msg) {
		order := items.Order{}
		err := json.Unmarshal(m.Data, &order)
		if err != nil {
			fmt.Println("some bad unmarshal json order listen")
			return
		}
		addOrderToDB(db, order)
		//fmt.Printf("order: ", order)
	})

	time.Sleep(time.Minute * 5)
}

func addOrderToDB(db *sql.DB, order items.Order) bool {
	fmt.Println("order = ", order)
	paymentId, ok := addPaymentToDB(db, order.Payment)
	if !ok {
		fmt.Println("some went wrong in add payment")
		return false
	}
	deliveryId, ok := addDeliveryToDB(db, order.Delivery)
	if !ok {
		fmt.Println("some went wrong in add delivery")
		return false
	}
	var lastInsertId int
	err := db.QueryRow(
		`INSERT INTO orders("order_uid","track_number","entry", "delivery_id","payment_id","locale","internal_signature","customer_id","delivery_service","shardkey","sm_id","date_created","oof_shard") VALUES ($1,$2,$3, $4, $5,$6, $7, $8, $9, $10,$11,$12,$13) RETURNING id`,
		order.Order_uid,
		order.Track_number,
		order.Entry,
		deliveryId,
		paymentId,
		order.Locale,
		order.Internal_signature,
		order.Customer_id,
		order.Delivery_service,
		order.Shardkey,
		order.Sm_id,
		order.Date_created,
		order.Oof_shard,
	).Scan(&lastInsertId)
	if err != nil {
		fmt.Println("err new order sql = ", err)
		return false
	}

	fmt.Println("order affected, lastId = ", lastInsertId)
	addItemsToDB(db, order.Items, int(lastInsertId))
	return true
}

func addDeliveryToDB(db *sql.DB, delivery items.Delivery) (int, bool) {
	fmt.Println("delivery = ", delivery)
	var lastInsertId int
	err := db.QueryRow(
		`INSERT INTO delivery ("name","phone","zip", "city","address","region","email") VALUES ($1,$2,$3, $4, $5,$6, $7) RETURNING id`,
		delivery.Name,
		delivery.Phone,
		delivery.Zip,
		delivery.City,
		delivery.Address,
		delivery.Region,
		delivery.Email,
	).Scan(&lastInsertId)
	if err != nil {
		fmt.Println("err new delivery sql = ", err)
		return -1, false
	}

	fmt.Println("delivery  lastId = ", lastInsertId)
	return int(lastInsertId), true
}

func addPaymentToDB(db *sql.DB, payment items.Payment) (int, bool) {
	fmt.Println("payment = ", payment)
	var lastInsertId int
	err := db.QueryRow(
		`INSERT INTO payment("transaction","request_id","currency", "provider","amount","payment_dt","bank","delivery_cost","goods_total","custom_fee") VALUES ($1,$2,$3, $4, $5,$6, $7, $8, $9, $10) RETURNING id`,
		payment.Transaction,
		payment.Request_id,
		payment.Currency,
		payment.Provider,
		payment.Amount,
		payment.Payment_dt,
		payment.Bank,
		payment.Delivery_cost,
		payment.Goods_total,
		payment.Custom_fee,
	).Scan(&lastInsertId)
	if err != nil {
		fmt.Println("err new payment sql = ", err)
		return -1, false
	}

	//lastID, err := result.LastInsertId()
	//if err != nil {
	//	fmt.Println("err new payment sql = ", err)
	//	return -1, false
	//}
	fmt.Println("payment lastId = ", lastInsertId)
	return int(lastInsertId), true
}

func addItemsToDB(db *sql.DB, myItems []items.Item, orderId int) bool {

	for _, it := range myItems {
		fmt.Println("item = ", it)
		var lastInsertId int
		err := db.QueryRow(
			`INSERT INTO items("order_id","chrt_id","track_number", "price","rid","name","sale","size","total_price","nm_id","brand","status") VALUES ($1,$2,$3, $4, $5,$6, $7, $8, $9, $10, $11, $12) RETURNING id`,
			orderId,
			it.Chrt_id,
			it.Track_number,
			it.Price,
			it.Rid,
			it.Name,
			it.Sale,
			it.Size,
			it.Total_price,
			it.Nm_id,
			it.Brand,
			it.Status,
		).Scan(&lastInsertId)
		if err != nil {
			fmt.Println("err new item sql = ", err)
			return false
		}

		fmt.Println("item  lastId = ", lastInsertId)
	}
	return true
}

func getOrderFromDB(db *sql.DB, orderId int) []items.Order { // -1 - select all orders

	var rows *sql.Rows
	var err error
	var orders []items.Order
	if orderId == -1 {
		rows, err = db.Query(`SELECT "id","order_uid","track_number","entry", "delivery_id","payment_id","locale","internal_signature","customer_id","delivery_service","shardkey","sm_id","date_created","oof_shard" FROM "orders"`)
		if err != nil {
			fmt.Println("err get orders sql = ", err)
			return nil
		}
	} else {
		rows, err = db.Query(`SELECT "id","order_uid","track_number","entry", "delivery_id","payment_id","locale","internal_signature","customer_id","delivery_service","shardkey","sm_id","date_created","oof_shard" FROM "orders" where id=` + strconv.Itoa(orderId))
		if err != nil {
			fmt.Println("err get orders sql = ", err)
			return nil
		}
	}
	var deliveryId, paymentId int

	defer rows.Close()
	for rows.Next() {
		var order items.Order
		err = rows.Scan(
			&order.Id,
			&order.Order_uid,
			&order.Track_number,
			&order.Entry,
			&deliveryId,
			&paymentId,
			&order.Locale,
			&order.Internal_signature,
			&order.Customer_id,
			&order.Delivery_service,
			&order.Shardkey,
			&order.Sm_id,
			&order.Date_created,
			&order.Oof_shard,
		)
		if err != nil {
			fmt.Println("err get orders sql scan = ", err)
			return nil
		}
		order.Delivery = getDeliveryFromDB(deliveryId)
		order.Payment = getPaymentFromDB(paymentId)
		order.Items = getItemsFromDB(order.Id)
		//fmt.Println("orderFrom db = ", order)
		orders = append(orders, order)
	}
	return orders
}

func getDeliveryFromDB(deliveryId int) items.Delivery {
	var rows *sql.Rows
	var err error

	rows, err = db.Query(`SELECT "name","phone","zip", "city","address","region","email" FROM "delivery" where id=` + strconv.Itoa(deliveryId))
	if err != nil {
		fmt.Println("err get delivery sql = ", err)
		return items.Delivery{}
	}

	defer rows.Close()
	for rows.Next() {
		var delivery items.Delivery

		err = rows.Scan(
			&delivery.Name,
			&delivery.Phone,
			&delivery.Zip,
			&delivery.City,
			&delivery.Address,
			&delivery.Region,
			&delivery.Email,
		)
		if err != nil {
			fmt.Println("err get delivery sql scan = ", err)
			return items.Delivery{}
		}
		return delivery
	}
	return items.Delivery{}
}

func getPaymentFromDB(paymentId int) items.Payment {
	var rows *sql.Rows
	var err error

	rows, err = db.Query(`SELECT "transaction","request_id","currency", "provider","amount","payment_dt","bank","delivery_cost","goods_total","custom_fee" FROM "payment" where id=` + strconv.Itoa(paymentId))
	if err != nil {
		fmt.Println("err get Payment sql = ", err)
		return items.Payment{}
	}

	defer rows.Close()
	for rows.Next() {
		var payment items.Payment

		err = rows.Scan(
			&payment.Transaction,
			&payment.Request_id,
			&payment.Currency,
			&payment.Provider,
			&payment.Amount,
			&payment.Payment_dt,
			&payment.Bank,
			&payment.Delivery_cost,
			&payment.Goods_total,
			&payment.Custom_fee,
		)
		if err != nil {
			fmt.Println("err get Payment sql scan = ", err)
			return items.Payment{}
		}
		return payment
	}
	return items.Payment{}
}

func getItemsFromDB(orderId int) []items.Item {
	var rows *sql.Rows
	var err error
	var myItems []items.Item

	rows, err = db.Query(`SELECT "order_id","chrt_id","track_number", "price","rid","name","sale","size","total_price","nm_id","brand","status" FROM "items" where order_id=` + strconv.Itoa(orderId))
	if err != nil {
		fmt.Println("err get item sql = ", err)
		return nil
	}

	defer rows.Close()
	for rows.Next() {
		var item items.Item

		err = rows.Scan(
			&orderId,
			&item.Chrt_id,
			&item.Track_number,
			&item.Price,
			&item.Rid,
			&item.Name,
			&item.Sale,
			&item.Size,
			&item.Total_price,
			&item.Nm_id,
			&item.Brand,
			&item.Status,
		)
		if err != nil {
			fmt.Println("err get item sql scan = ", err)
			return nil
		}
		myItems = append(myItems, item)
	}
	return myItems
}

func getOrder(w http.ResponseWriter, r *http.Request) {
	fmt.Println("hello world ")
	vars := mux.Vars(r)
	orderID, err := vars["ORDER_ID"]
	fmt.Println("orderID =", orderID)
	intOrderId, err1 := strconv.Atoi(orderID)
	if err1 != nil {
		fmt.Println("bad strconv.Atoi")
		return
	}
	fmt.Println("id+1 = ", (intOrderId + 1))
	//fmt.Println(len(category))
	orders = getOrderFromDB(db, intOrderId)
	fmt.Println("========================")
	fmt.Println("my get order by id = ", orders)
	fmt.Println("========================")
	if !err {
		http.Error(w, `Bad id`, http.StatusBadRequest)
		fmt.Println("err")
		return
	}

	ordersJson, err1 := json.Marshal(orders)
	if err1 != nil {
		fmt.Println("json marshal orders err")
		return
	}
	_, err1 = w.Write(ordersJson)
	if err1 != nil {
		fmt.Println("write ordersJson err")
		return
	}
}

func fillOrders() []items.Order {
	return nil
}
