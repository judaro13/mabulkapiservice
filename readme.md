This project require a RabbitMQ server active. 
If you use archlinux, follow next: https://wiki.archlinux.org/index.php/RabbitMQ
For debian systems: https://www.rabbitmq.com/install-debian.html
For homebrew system: https://www.rabbitmq.com/install-homebrew.html

For execute this service is require to setup the following EVN VARS

DATABASE_URL
RABBIT_URL
RABBIT_QUERY_DATA_QUEUE
RABBIT_STORE_DATA_QUEUE

For export it, writte in your console as follow, change the values with you own values

$ export DATABASE_URL="host=127.0.0.1 port=5432 user=youruser password=yourpasswd dbname=yourdb sslmode=disable"
$ export RABBIT_URL="amqp://guest:guest@localhost:5672/"
$ export RABBIT_QUERY_DATA_QUEUE="process_bulk_data"
$ export RABBIT_STORE_DATA_QUEUE="store_bulk_data"


For compile the project

$ go build *.go

For excecute it

$ ./main