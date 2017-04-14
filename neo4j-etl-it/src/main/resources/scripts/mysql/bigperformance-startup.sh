#!/bin/bash -xe

apt-get update
apt-get -y dist-upgrade

# Install MySQL
DEBIAN_FRONTEND=noninteractive apt-get -q -y install mysql-server
apt-get -y install mysql-client
mysqladmin -u root password '<DBRootPassword>' >/var/log/change-password.log 2>&1

# Create MySQL user
cat \<\<EOF > /tmp/setup.mysql
CREATE USER '<DBUser>'@'%' IDENTIFIED BY '<DBPassword>';
GRANT ALL PRIVILEGES ON *.* TO '<DBUser>'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
EOF
mysql -u root --password='<DBRootPassword>' \< /tmp/setup.mysql
wget https://s3-eu-west-1.amazonaws.com/integration.neo4j.com/northwind.sql -P /tmp
mysql -u root --password='<DBRootPassword>' \< /tmp/northwind.sql

# Update MySQL config
echo [server] >> /etc/mysql/my.cnf
echo "bind-address = *" >> /etc/mysql/my.cnf
service mysql restart
update-rc.d mysql defaults
