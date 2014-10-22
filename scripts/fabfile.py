import boto.ec2

from fabric.api import *
from fabric.operations import run, put, sudo
from fabric.contrib.project import rsync_project
from fabric.contrib.files import exists

import pprint
import time


### FILL IN HERE ###
region=''
env.user = ''
env.key_filename = ''
node_file=''
###---------------------###

conn = boto.ec2.connect_to_region(region)
env.hosts = [instance.public_dns_name for reservation in conn.get_all_reservations() for instance in reservation.instances if instance.state in ['running']]


def _deploy_ec2(local_dir):
    """
    Steps:
        - syncs local_dir with all hosts in env.hosts
    """
    rsync_project(local_dir=local_dir, remote_dir='~/tmp', exclude=['.git', 'scripts/fabfile.py', '*.pyc', '*.log']) #, ssh_opts='-o StrictHostKeyChecking=no'

def _runbg(cmd, sockname="dtach"):
    """
    Runs command dtach'd on each node
    """
    return run('dtach -n `mktemp -u /tmp/%s.XXXX` %s'  % (sockname,cmd))


@runs_once
def launch(n, output_file="node_list.txt", region="us-west-1", external_port=50000, internal_port=50001):
    """
    Steps:
        - connect to AWS
        - count number of instances already up
        - add instances until there are n instances up
        - output to node_list.txt node info with format: 'public_dns_name,external_port,internal_port'
    """
    # connect to AWS
    n = int(n)
    conn = boto.ec2.connect_to_region(region)
    instances = [instance for reservation in conn.get_all_reservations() for instance in reservation.instances]
    num_running_instances = sum([instance.state in ['pending', 'running'] for instance in instances])
    print "Number of nodes wanted: {}".format(str(n))
    print "Number of nodes running: {}".format(str(num_running_instances))
    # idempotent node adding
    if num_running_instances < n:
        for _ in xrange( n - num_running_instances ):
            print "adding node"
            conn.run_instances(
                'ami-076e6542',
                key_name="PynamoDB",
                instance_type='t2.micro',
                security_groups=['PynamoDB'])
    else:
        for _ in xrange(num_running_instances - int(n)):
            print "terminating node"
            instance_id = random.choice(instances).id
            conn.terminate_instances(instance_ids=[instance_id])

    # get current instances
    time.sleep(30)
    instances = [instance for reservation in conn.get_all_reservations() for instance in reservation.instances]

    # output node info file
    with open(output_file, 'w') as f:
        for instance in instances:
            if instance.state in ['pending', 'running']:
                line = "{},{},{}\n".format(instance.public_dns_name, external_port, internal_port)
                f.write(line)


@runs_once
def reboot(region="us-west-1"):
    """
    Steps:
        - connect to AWS
        - reboot each node in specified
    """
    conn = boto.ec2.connect_to_region(region)

    for reservation in conn.get_all_reservations():
        print reservation.instances[0].reboot()

def disable_firewall():
    sudo("ufw disable")

def modify_firewall():
    sudo("ufw allow 50000")
    sudo("ufw allow 50001")

def terminate_python():
    sudo("ps aux | grep python | grep -v 'grep python' | awk '{print $2}' | xargs kill -9")

def dependencies():
    sudo("apt-get install dtach")

def deploy():
    """
    Idempotent Deploy.

    Steps:
        - mkdir ~/tmp remotely
        - rsync directory remotely
    """
    run('mkdir -p ~/tmp')
    _deploy_ec2('~/git/PynamoDB')


def start():
    run("rm -rf ~/tmp/PynamoDB/logs")
    run("mkdir -p ~/tmp/PynamoDB/logs")
    run("rm -f *.log")

    server="~/tmp/PynamoDB/PynamoDB/server.py"
    run("chmod u+x {}".format(server))
    node_list="~/tmp/PynamoDB/scripts/node_list.txt"
    public_dns_name = env.host_string
    print public_dns_name
    wait_time= 300
    _runbg("sudo python {} -i {} -d {} -w {}".format(server, node_list, public_dns_name, wait_time))

def log():
    run("cat pynamo.log")
