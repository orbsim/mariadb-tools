#!/bin/bash
#---------------
#Create by Omidreza Bagheri
#Date: 2008-Jan-28
#---------------
username=$1
password=$2
mysql -u$username -p$password --batch --skip-column-names -e "SELECT user, host FROM user" mysql \
| while read user host
  do
    echo "#--- $user @ $host"
    mysql -u$username -p$password --batch --skip-column-names -e "SHOW GRANTS FOR '$user'@'$host'"
  done
