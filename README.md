# Data Ingestion Challenge Solution

My solution for a data ingestion challenge.

## How to run metric-collector cluster

The easiest way to build and run the cluster during development is using make:

```
$ make compose
```

Note that this does depend on [docker-compose][] and a running [docker][] instance.

If all went well and the cluster is up and running,
you can start sending events using [httpie][]:

```
$ http post $(docker-machine ip):3000/event \
    username=kodingbot count:=12412414 metric=kite_call
```

Metric Collector Service metrics can be obtained as JSON using [httpie][]:

```
$ http get $(docker-machine ip):3000/debug/var
```

### Warning

The docker-compose configuration is a very static setup and not meant for production use.
For production I would probably use [k8s][] or [AWS-ECS][], depending on the project/organization.

[httpie]: http://httpie.org
[docker-compose]: https://docs.docker.com/compose/
[docker]: https://docker.com/
[k8s]: http://kubernetes.io
[AWS-ECS]: http://aws.amazon.com/ecs/
