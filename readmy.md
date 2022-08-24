Запускаю nats: nats-streaming-server -cid prod -store file -dir store
publish.go - пишет в канал новые заказы
main.go - обрабатывает все запросы, читает из канала.
