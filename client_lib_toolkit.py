# handle all calls to Salesforce Python Toolkit 
# https://code.google.com/p/salesforce-python-toolkit/

from sforce.enterprise import SforceEnterpriseClient

from vlib import conf

class ClientLib(object):

    def __init__(self):
        self.conf = conf.Factory.create().data

    @property
    def connection(self):
        '''Behavior: Log in to Salesforce
           Return: Handle to connection
        '''
        if '_connection' not in self.__dict__:
            user      = self.conf['salesforce']['user']
            password  = self.conf['salesforce']['password']
            token     = self.conf['salesforce']['token']
            wsdl_file = self.conf['salesforce']['wsdl_file']
            h = SforceEnterpriseClient(wsdl_file)
            h.login(user, password, token)

            self._connection = h
        return self._connection
                
