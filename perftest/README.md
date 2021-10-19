# ChubaoFS Perftest scripts

## fio

```
$ /opt/chubaofs/bin/cfs-client -c client.json
$ ./fiotest.sh <CFS_MNT_POINT>
```

## mdtest

```
$ /opt/chubaofs/bin/cfs-client -c client.json
$ ./mdtest.sh <CFS_MNT_POINT>
```

## perftest with saltstack

```
$ ./perftest.sh fio_test
$ ./perftest.sh print_fio_report

$ ./perftest.sh mdtest_op
$ ./perftest.sh mdtest_small
$ ./perftest.sh print_mdtest_report
```
