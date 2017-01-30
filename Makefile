BIN?=./bin
GOENV?=env GOOS=linux GOARCH=amd64

workers = account-name distinct-name hourly-log

compose: all
	docker-compose -f ./docker/docker-compose.yml up --build

all: metric-collector $(workers)

bin:
	mkdir -p ${BIN} || true

metric-collector: bin
	${GOENV} go build ${GOFLAGS} -o ${BIN}/metric-collector \
		./cmd/metric-collector/main.go

$(workers):
	${GOENV} go build ${GOFLAGS} -o ${BIN}/$@ \
		./cmd/workers/$@/main.go
