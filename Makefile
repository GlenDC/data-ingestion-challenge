BIN?=./bin
GOENV?=GOOS=linux GOARCH=amd64

workers = account-name distinct-name hourly-logs
bonus = bonus-metrics

default:
	GOENV= make all

all: metric-collector $(workers) $(bonus)

compose: all
	docker-compose -f ./docker/docker-compose.yml up --build

loadbalance-test: all
	docker-compose -f ./docker/locust-compose.yml rm
	docker-compose -f ./docker/locust-compose.yml up --build

bin:
	mkdir -p ${BIN} || true

metric-collector: bin
	env ${GOENV} go build ${GOFLAGS} -o ${BIN}/metric-collector \
		./cmd/metric-collector/main.go

$(bonus):
	env ${GOENV} go build ${GOFLAGS} -o ${BIN}/$@ \
		./cmd/bonus/$@/main.go

$(workers):
	env ${GOENV} go build ${GOFLAGS} -o ${BIN}/$@ \
		./cmd/workers/$@/main.go
