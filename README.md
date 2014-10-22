PynamoDB
========

Python clone of AWS's DynamoDB, a NoSQL database.  Read more about it [on my blog]:(www.samuelwu.ca/pynamo)

This was meant more to be educational than production-ready, so I recommend simply looking at the classes and function names to see what sort of stages and handlers are required to make the keystore work.  In the future I'll make it easier to run a simulation.

But if you'd like to run a simulation of rolling failures as of now, you'll have to:

0. complete AWS's Python SDK [Getting Started]:(http://aws.amazon.com/developers/getting-started/python/).
0. install [Fabric]:(http://www.fabfile.org/) for easier multi-node deployment.
0. run test_remote.test_rolling_failure_unannounced_termination.

###To-do:
0. clean up the fabfile.
0. package up project for pip.
