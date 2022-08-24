Запускаю nats: nats-streaming-server -cid prod -store file -dir store /n
publish.go - пишет в канал новые заказы /n
main.go - обрабатывает все запросы, читает из канала. /n
