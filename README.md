### anFS

anFS is a new filesystem that exploits [activeosds](https://github.com/sandrain/activeosd)
to execute workflows within the storage. anFS provides a single file system
atop a set of activeosds. The idea was published in the following papers:

* Hyogi Sim, Geoffroy Vallee, Youngjae Kim, Sudharshan S. Vazhkudai, Devesh
Tiwari, and Ali R. Butt, ["An Analysis Workflow-Aware Storage System for
Multi-Core Active Flash Arrays,"](https://doi.org/10.1109/TPDS.2018.2865471) IEEE Transactions on Parallel Distributed
Systems (TPDS), vol. 30, no. 2, pp. 271–285, Feb. 2019.

* Hyogi Sim, Youngjae Kim, Sudharshan S. Vazhkudai, Devesh Tiwari, Ali Anwar,
Ali R. Butt, and Lavanya Ramakrishnan, ["AnalyzeThis: An Analysis Workflow-Aware
Storage System,"](https://doi.org/10.1145/2807591.2807622) in Proceedings of the International Conference for High
Performance Computing, Networking, Storage and Analysis (SC), New York, NY,
USA, 2015

### Compilation

anFS is based on the autotools and can be built using the following steps:

```
shell $ cd anfs
shell $ ./autogen.sh
shell $ ./configure --prefix=/install/path --with-libosd=/path/to/libosd/
shell $ make
```

