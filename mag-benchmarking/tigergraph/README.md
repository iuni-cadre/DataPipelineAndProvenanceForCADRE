# Single-node Installation

```
tar xzfv tigergraph-2.4.0-developer.tar.gz
cd tigergraph*/ 
sudo ./install.sh
```

To check that the installation is working:
```
gadmin status
```

In case if any issues, you can try restarting TigerGraph:
```
gadmin restart
```

In general, `gadmin` is the tool used to manage the TigerGraph instance. Use 
```
gadmin --help
```
to see all options.
