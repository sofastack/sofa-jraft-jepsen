# jepsen.atomic

A Clojure library designed to test jraft-test atomic server consistence.

## Usage

Edit `nodes` file:

```
jraft.host1
jraft.host2
jraft.host4
jraft.host5
```

Every node should install [jepsen](https://github.com/jepsen-io/jepsen).

Deploy the atomic-server with [clojure-control](https://github.com/killme2008/clojure-control):

```
control run jraft build
control run jraft deploy
```

Run the test:

```
bash run_test.sh --testfn <test>
```

Valid test include:

* configuration-test: remove and add a random node.
* bridge-test: weaving the network into happy little intersecting majority rings
* pause-test: pausing random node with SIGSTOP/SIGCONT.
* crash-test: killing random nodes and restarting them.
* partition-test: Cuts the network into randomly chosen halves.
* partition-majority-test: Cuts the network into randomly majority groups.


Other options:

* `--time-limit 120`: test time limit
* `--concurrency 10`:  test concurrency


Watch the report at [http://localhost:8080](http://localhost:8080).


## License

Copyright © 2018 alipay.com
