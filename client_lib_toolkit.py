# handle all calls to Salesforce Python Toolkit 
# https://code.google.com/p/salesforce-python-toolkit/

from sforce.enterprise import SforceEnterpriseClient

from vlib import conf

class ClientLib(object):

    NAME = 'toolkit'
    RECORD_KEYS_TO_IGNORE = []

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
                
    def desc(self, sfobject):
        return self.connection.describeSObject(sfobject)

    def setBatchSize(self):
        h = self.connection
        queryOptions = h.generateHeader('QueryOptions')
        queryOptions.batchSize = 2000
        h.setQueryOptions(queryOptions)

    def query(self, querystr):
        return self.connection.query(querystr)

    def queryIsDone(self, result):
        return result.done

    def queryLocator(self, result):
        return result.queryLocator

    def resultSize(self, result):
        return result.size

    def resultRecords(self, result):
        return result.records

    def getResultHeader(self, result):
        header = []
        for record in result.records:
            if len(record.__keylist__) > len(header):
                header = record.__keylist__
        return header
