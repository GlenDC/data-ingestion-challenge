# Data Ingestion Challenge Solution

My solution for a data ingestion challenge.

## How to build

You can build all services and workers as binaries in `./dir` using make:

```
$ make
```

Dependencies:

+ [Golang 1.7][golang];

### Update vendor libraries

All [Golang][golang] dependencies are stored in the vendor dir
using [gvt][] and can be updated as follows:

```
$ gvt update --all
```

Dependencies:

+ [gvt][];

## How to run metric-collector cluster (using Docker Compose)

The easiest way to build and run the cluster during development is using make:

```
$ make compose
```

Dependencies:

+ [docker-compose][];
+ running [docker][] instance;

If all went well and the cluster is up and running,
you can start sending events using [httpie][]:

```
$ http post $(docker-machine ip):3000/event \
    username=kodingbot count:=12412414 metric=kite_call
```

Metric Collector Service metrics can be obtained as JSON using [httpie][]:

```
$ http get $(docker-machine ip):3000/debug/vars
```

### Bonus

Hourly-Logs Metrics such as averages can be obtained via the `bonus-metrics`
service which runs on port `3001`.

Metrics can be obtained as JSON using [httpie][]:

+ all metrics: `$ http get $(docker-machine ip):3001/metrics/hourly_logs/total`;
+ all per-user metrics: `$ http get $(docker-machine ip):3001/metrics/hourly_logs/per_user`;

### Warning

The docker-compose configuration is a very static setup and not meant for production use.
For production I would probably use [k8s][] or [AWS-ECS][], depending on the project/organization.

## How to run Load Balance tests (using Locust and Docker Compose)

Simplistic and local load balance tests can be run using [locust][] and [docker-compose][]:

```
$ make loadbalance-test
```

This composition uses 3 instances of each async worker, instead of just 1 instance.

Once the Docker composition is up and running,
you can visit the following local web page in your favorite browser:

```
$ open http://$(docker-machine ip):8010 # MacOS Terminal Command
```

[golang]: http://golang.org
[gvt]: https://github.com/FiloSottile/gvt
[httpie]: http://httpie.org
[docker-compose]: https://docs.docker.com/compose/
[docker]: https://docker.com/
[k8s]: http://kubernetes.io
[AWS-ECS]: http://aws.amazon.com/ecs/
[locust]: http://locust.io
