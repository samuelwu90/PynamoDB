pem=~/.ssh/PynamoDB.pem

while read line; do
    echo $line
    public_dns_name=$(echo $line | cut -d \, -f 1)
    echo $public_dns_name
    ssh -t -t -o StrictHostKeyChecking=no -i $pem ubuntu@$public_dns_name bash -c "'
    sudo apt-get install -y git
    mkdir -p ~/git
    cd ~/git
    rm -rf PynamoDB
    git clone https://github.com/samuelwu90/PynamoDB.git
    '" < /dev/null
done < node_list.txt
