# Pre-requisites

All commands should be executed with `sudo`.

# Installation
```
apt remove libcurl4
apt install libcurl3
dpkg -i ~/Downloads/memgraph_0.14.1-1_amd64.deb
```

# `memgraph` service:
```
systemctl status|start|stop|enable memgraph
```

# Data import
```
su memgraph
mg_import_csv --overwrite --nodes=/data/mag-fos-data-processing/nodes/Author.csv --nodes=/data/mag-fos-data-processing/nodes/Paper.csv --relationships=/data/mag-fos-data-processing/relationships/Author-IsAuthorOf-Paper.csv
```
