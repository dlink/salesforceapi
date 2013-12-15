# handle all calls to simple_salesforce
# https://pypi.python.org/pypi/simple-salesforce

from simple_salesforce import Salesforce

from vlib import conf


class ClientLib(object):

    NAME = 'simple'
    RECORD_KEYS_TO_IGNORE = ['attributes']

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
            self._connection = Salesforce(
                username=user,
                password=password,
                security_token=token)

        return self._connection

    def desc(self, sfobject):
        return self.connection.__getattr__(sfobject).describe()

    def setBatchSize(self):
        pass # apparently not necessary with simple_salesforce

    def query(self, querystr):
        return self.connection.query_all(querystr)

    def queryIsDone(self, result):
        return result['done']

    def queryLocator(self, result):
        return None # apparently not necessary with simple_salesfor

    def resultSize(self, result):
        return result['totalSize']

    def resultRecords(self, result):
        return result['records']

    def getResultHeader(self, result):
        return result['records'][0].keys()
