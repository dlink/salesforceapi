#!/usr/bin/env python

import re

from sforce.enterprise import SforceEnterpriseClient

from salesforceapi import SalesforceApi

class SalesforceApi2(SalesforceApi):
    '''Preside over Salesforce API using connection2'''

    @property
    def connection2(self):
        '''Behavior: Log in to Salesforce
           Return: Handle to connection

           Uses Salesforce Python Toolkit

           connection2 used for queryAll functionality not supported
           by Python Simple-Salesforce
        '''
        if '_connection2' not in self.__dict__:
            user      = self.conf['salesforce']['user']
            password  = self.conf['salesforce']['password']
            token     = self.conf['salesforce']['token']
            wsdl_file = self.conf['salesforce']['wsdl_file']
            h = SforceEnterpriseClient(wsdl_file)
            h.login(user, password, token)

            self._connection2 = h
        return self._connection2

    def queryAll(self, querystr, format='tabular'):
        '''Return results of a querystr
           see: queryMore()

           queryAll() also returns logically deleted records (where isDeleted=true)
           query() does not

           options: format='tablular'
                    format='dict|dictionary'
        '''

        # validate query a bit:
        regex = 'select .* from .*'
        if not re.match(regex, querystr):
            emsg = 'Invalid query string: %s' % querystr
            raise SalesforceApiParameterError(emsg)

        h = self.connection2

        # set batch size
        queryOptions = h.generateHeader('QueryOptions')
        queryOptions.batchSize = 2000
        h.setQueryOptions(queryOptions)

        # get data
        result =  h.queryAll(querystr)
        return self.queryResults(result, format)

    def queryMore(self, format='tabular'):
        '''Return subsequent results from querystr set up in query()
           see: query()
        '''
        h = self.connection2
        result = h.queryMore(self.query_locator)
        return self.queryResults(result, format)
    
    def queryResults(self, result, format):
        '''Query results processing for query() and queryMore()'''

        results = []
        if format not in ('tabular', 'dict', 'dictionary'):
            raise Exception('SalesforceApi.Query: Unrecognized format: %s'
                            % format)
        self.query_done    = result.done
        self.query_locator = result.queryLocator
        if not result.size:
            return results

        # build header. some recs do not have all fields:
        header = []
        for record in result.records:
            if len(record.__keylist__) > len(header):
                header = record.__keylist__

        # build output
        if format in ('dict', 'dictionary'):
            for record in result.records:
                row = {}
                for key, value in record:
                    row[key] = value
                results.append(row)
        else:
            # Note: assumption about header is wrong
            # header of first row not the same as for others
            results.append(header)
            for record in result.records:
                row = []
                for key in header:
                    row.append(getattr(record, key, None))
                results.append(row)

        return results
