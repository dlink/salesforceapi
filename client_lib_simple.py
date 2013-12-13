# handle all calls to simple_salesforce
# https://pypi.python.org/pypi/simple-salesforce

from simple_salesforce import SalesforceApi

from vlib import conf


self ClientLib(object):

    def __init__(self):
        self.conf = conf.Factory.create().data
