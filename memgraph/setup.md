# Firewall rules

```
openstack security group create --description "ssh & icmp enabled" dnikolov-ssh

openstack security group list

openstack security group rule create --protocol tcp --dst-port 22:22 --remote-ip 0.0.0.0/0 dnikolov-ssh
openstack security group rule create --proto icmp dnikolov-ssh
openstack security group rule create --proto tcp --dst-port 1:65535 --remote-ip 10.0.0.0/24 dnikolov-ssh
openstack security group rule create --proto udp --dst-port 1:65535 --remote-ip 10.0.0.0/24 dnikolov-ssh
```

# SSH keys

```
openstack security show dnikolov-ssh

ssh-keygen -b 2048 -t rsa -f dnikolov-openstack-key -P "<ENTER A PASSWORD>"
openstack keypair create --public-key dnikolov-openstack-key.pub dnikolov-openstack-key
openstack keypair list
```

# Network

```
openstack network create dnikolov-net
openstack subnet create --network dnikolov-net --subnet-range 10.0.0.0/24 dnikolov-subnet
openstack router create dnikolov-router
openstack router add subnet dnikolov-router dnikolov-subnet
openstack router set --external-gateway public dnikolov-router
openstack router show dnikolov-router
```

# VM

```
openstack image list | grep JS-API
openstack server create memgraph-vm2 \
--flavor m1.xxlarge \
--image 1b4a5a4a-423e-4aff-936c-5183a3b5c57c \
--key-name dnikolov-openstack-key \
--security-group dnikolov-ssh \
--nic net-id=dnikolov-net2 \
--wait

openstack console log show memgraph-vm
```

# IP

```
openstack floating ip create public
openstack server add floating ip memgraph-vm 149.165.168.15
ping 149.165.168.15
```

# Volume

```
openstack volume create --size 2048 dnikolov-2T-volume
openstack server add volume memgraph-vm dnikolov-2T-volume

ssh -i ~/.ssh/dnikolov-openstack-key ubuntu@149.165.168.15
sudo su -
dmesg | grep sd
mkfs.ext4 /dev/sdb
mkdir /volume
mount /dev/sdb /volume
df -h
chmod -R 777 /volume
```

# Teardown
```
openstack floating ip delete 149.165.156.200
openstack server delete memgraph-vm-dp
```

# Memgraph Install
```
wget https://memgraph.com/download/memgraph/v0.14.1/memgraph_0.14.1-1_amd64.deb
sudo apt remove libcurl4
sudo apt install libcurl3
sudo dpkg -i ~/downloads/memgraph_0.14.1-1_amd64.deb
sudo nano /etc/memgraph/memgraph.conf
sudo mkdir -p /volume/memgraph/durability
sudo chown -R memgraph:memgraph /volume/memgraph
sudo systemctl restart memgraph

cd /volume/mag
sudo su memgraph
```

# Memgraph Import

```
mg_import_csv --overwrite \
--nodes=nodes/Affiliation.csv \
--nodes=nodes/Author.csv \
--nodes=nodes/ConferenceInstance.csv \
--nodes=nodes/ConferenceSeries.csv \
--nodes=nodes/FieldOfStudy.csv \
--nodes=nodes/Journal.csv \
--nodes=nodes/Paper.csv \
--relationships=relationships/Author-IsAffiliatedWith-Affiliation.csv \
--relationships=relationships/Author-IsAuthorOf-Paper.csv \
--relationships=relationships/ConferenceInstance-IsInstanceOf-ConferenceSeries.csv \
--relationships=relationships/Paper-BelongsTo-FieldOfStudy.csv \
--relationships=relationships/Paper-IsPresentedAt-ConferenceInstance.csv \
--relationships=relationships/Paper-IsPublishedIn-Journal.csv \
--relationships=relationships/Paper-References-Paper.csv
--properties-on-disk=Rank,NormalizedName,DisplayName,GridId,OfficialPage,WikiPage,PaperCount,CitationCount,CreatedDate,LastKnownAffiliationId
```

# Indices

```
CREATE INDEX ON :JOURNAL(JournalId);
CREATE INDEX ON :FIELDOFSTUDY(FieldOfStudyId);
CREATE INDEX ON :AFFILIATION(AffiliationId)
CREATE INDEX ON :CONFERENCEINSTANCE(ConferenceInstanceId);
CREATE INDEX ON :CONFERENCESERIES(ConferenceSeriesId);
CREATE INDEX ON :AUTHOR(AuthorId);
CREATE INDEX ON :PAPER(PaperId);

CREATE INDEX ON :FIELDOFSTUDY(NormalizedName);
CREATE INDEX ON :JOURNAL(NormalizedName);
CREATE INDEX ON :PAPER(PaperTitle);
CREATE INDEX ON :AUTHOR(NormalizedName)
```
