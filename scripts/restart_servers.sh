#!/bin/bash

pem=~/.ssh/PynamoDB.pem

while read line; do
    echo $line
    public_dns_name=$(echo $line | cut -d \, -f 1)

    ssh -t -t -o StrictHostKeyChecking=no -i $pem ubuntu@$public_dns_name bash -c " '
    kill $(ps aux | grep python | awk '{print $2}')
    cd ~/git/PynamoDB
    mkdir -p ~/git/PynamoDB/logs
    rm -rf ~/git/PynamoDB/logs/*
    git pull
    server=~/git/PynamoDB/PynamoDB/server.py
    echo \$server
    node_list=~/git/PynamoDB/scripts/node_list.txt
    echo \$node_list
    echo $public_dns_name
    nohup python \$server -i \$node_list -d $public_dns_name > /dev/null 2>&1 &
    ' " < /dev/null
done < node_list.txt
