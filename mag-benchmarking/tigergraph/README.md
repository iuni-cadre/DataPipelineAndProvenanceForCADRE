# Single-node Installation

## Install system software

```
sudo apt update
sudo apt install python3-pip
sudo pip3 install virtualenv
```

## Create `tigergraph` user

```
sudo useradd -d /tigergraph -G sudo -s /bin/bash tigergraph
sudo passwd tigergraph
```

## Mount volume

For the full dataset, we need a separate volume we will use for storing both the raw data, and the tigergraph instance and its data.

In AWS, we need to [attach the volume to the instance](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-attaching-volume.html), and then [mount it](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-using-volumes.html).

Assuming the volume is at `/dev/xvdb`, the mounting will look like:

```
lsblk
```

```
sudo file -s /dev/xvdb
```

```
sudo mkfs -t xfs /dev/xvdb
```

```
sudo mkdir /tigergraph
```

```
sudo mount /dev/svdb /tigergraph
```

```
sudo chown tigergraph:tigergraph /tigergraph
```

## Install TigerGraph

```
su tigergraph
cd /tigergraph
tar xzfv tigergraph-2.4.0-developer.tar.gz
cd tigergraph*/ 
sudo ./install.sh
```

## Management

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

## Data import

```
cd /tigergraph
mkdir github
cd github
git clone https://github.com/iuni-cadre/DataPipelineAndProvenanceForCADRE.git
cd DataPipelineAndProvenanceForCADRE/mag-benchmarking/tigergraph/
virtualenv -p python3 VENV
source VENV/bin/activate
mkdir gsql
python create_schema_gsql.py > gsql/load_schema.gsql
python create_data_load_gsql.py > gsql/load_data.gsql
gsql gsql/load_schema.gsql
gsql gsql/load_data.gsql
```
