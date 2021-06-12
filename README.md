# Disclaimer

Proof of concept only.

# Tape Library File System

Read and write tapes in a tape library via a simple file system.

# Features
Reads and writes data directly to tape sequentially, one IO operation per tape drive.

This means if your tape library has a single drive, you can either read or write one file at a time. Additional read and write requests are queued, preferencing reads before writes.

Sequential data access is what you see when you use tools like cp, dd or rsync to read and write data. Seeking and random IO is not yet supported and is not on the roadmap.

Examples for usage:

`cp /tmp/archive.tar.gz /mnt/tlfs/archive.tar`

`rsync -ai --progress /tmp/archive.tar.gz /mnt/tlfs/archive.tar`

`dd status=progress if=/tmp/archive.tar.gz of=/mnt/tlfs/archive.tar.gz bs=1M`

# Versions
v0.1: (Current version) Testing
* Supports basic read and write.

v0.2: Planning
* Verify data on restore.
* Deep scrub data validation.
* Move data between tapes to enable free space consolidation and easy tape removal.

# Usage
Run:
```
dotnet /srv/tlfs/src/Tlfs/bin/Debug/netcoreapp3.1/tlfs.dll
```
Mounts the file system at 'mountpoint'.

## Flags

`--init` : Initialise the database.

`--init --force` : Initialise the database, drop the database first if required.

# Deployment
Deployment involves installing dependencies and compiling from source.

## On Centos 7
Install Fuse 3, .Net Core 3.1, Postgresql 12:
```
yum install fuse3
yum install https://packages.microsoft.com/config/centos/7/packages-microsoft-prod.rpm
yum install dotnet-sdk-3.1
yum install https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
yum install postgresql12-server
```

Initialise and start the database:
```
/usr/pgsql-12/bin/postgresql-12-setup initdb
systemctl enable postgresql-12
systemctl start postgresql-12
```

Configure the account you will use to connect to postgresql:
```
sudo -u postgres psql
ALTER USER postgres with encrypted password 'xxxxxxx';
\q
```

Configure postgresql to accept connections using a password (towards the end of pg_hba.conf):
```
nano /var/lib/pgsql/12/data/pg_hba.conf
```
Change:
```
host    all             all             127.0.0.1/32            ident
```
To:
```
host    all             all             127.0.0.1/32            md5
```

Restart postgresql for the config change to take effect:
```
systemctl restart postgresql-12
```

Download a copy of the tlfs source:
```
curl ... /srv/tlfs/
```

Copy the config file example and update it:
```
cp /srv/tlfs/src/Tlfs/tlfs.conf.example /etc/tlfs/tlfs.conf
```

Copy the systemctl service config, and reload:
```
cp /srv/tlfs/src/Tlfs/tlfs.service /usr/lib/systemd/system/tlfs.service
systemctl daemon-reload
```

Restore and build:
```
cd /srv/tlfs/src/Tlfs
dotnet build
```

Start:
```
systemctl start tlfs
```

Monitor:
```
journalctl -f -u tlfs
```

# Credits
Thank you to Tom Deseyn https://github.com/tmds for the Tmds.Fuse https://github.com/tmds/Tmds.Fuse components.
