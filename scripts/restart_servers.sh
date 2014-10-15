#!/bin/bash

pem=~/.ssh/PynamoDB.pem

while read line; do
    echo $line
    public_dns_name=$(echo $line | cut -d \, -f 1)
    echo $public_dns_name
    ssh -t -t -o StrictHostKeyChecking=no -i $pem ubuntu@$public_dns_name bash -c "'
    kill $(ps aux | grep python | awk '{print $2}')
    cd ~/git/PynamoDB
    git pull
    server=~/git/PynamoDB/PynamoDB/server.py
    nohup python \$server -i \$node_list -d $public_dns_name &
    '" < /dev/null
done < node_list.txt
