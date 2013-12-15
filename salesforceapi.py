#!/usr/bin/env python

import sys
import os
import re
import csv
import copy

#CLIENT_LIB = 'toolkit'
CLIENT_LIB = 'simple'

from vlib.utils import echoized, uniqueId, validate_num_args

DEBUG = 0
VERBOSE = 0
IND_PROGRESS_INTERVAL = 50


COMMANDS = ('create', 'delete', 'desc', 'fields', 'query', 'show', 'update')
SFOBJECTS = ('Account', 'Adoption', 'CampaignMember', 'Case', 'Contact', 
             'Lead', 'Opportunity', 'User')
CUSTOMOBJECTS = ('Adoption',)

class SalesforceApiError(Exception): pass
class SalesforceApiParameterError(SalesforceApiError): pass
class SalesforceApiFieldLenMismatch(SalesforceApiError): pass

class SalesforceApi(object):
    '''Preside over Salesforce API'''

    def __init__(self, client_lib=CLIENT_LIB):
        self.verbose = VERBOSE
        self.clientLib = self._getClientLib(client_lib)

        self.query_done = None
        self.query_locator = None

    def _getClientLib(self, client_lib):
        if client_lib == 'toolkit':
            from client_lib_toolkit import ClientLib
        elif client_lib == 'simple':
            from client_lib_simple import ClientLib
        return ClientLib()

    def process(self, *args):
        '''Read aguments and process API request
           All requests come thru here
        '''
        if not args: 
            syntax()

        # get verbose
        if args[0] == '-v':
            self.verbose = 1
            args = args[1:]

        # get command
        command = self.validate('command', args[0])
        args = args[1:]

        # process command
        if command in ('desc', 'fields'):
            validate_num_args(command, 1, args)
            sfobject = self.validate('sfobject', args[0])
            if command == 'desc':
                return self.desc(sfobject)
            else:
                return self.fields(sfobject)

        elif command == 'query':
            validate_num_args(command, 1, args)
            querystr = self.validate('querystr', args[0])
            return self.query(querystr)
            
        elif command == 'show':
            validate_num_args(command, 1, args)
            dobj = self.validate('directobject', args[0])
            return self.showObjects()

        elif command in ('delete', 'create', 'update'):
            validate_num_args('update', 2, args)
            sfobject = self.validate('sfobject', args[0])
            csvfile  = self.validate('csvfile',  args[1])
            header, rows = self.loadCsv(csvfile)
            if command == 'delete': 
                return self.delete(sfobject, header, rows)
            elif command == 'create':
                return self.create(sfobject, header, rows)
            else:
                return self.update(sfobject, header, rows)

        else:
            raise SalesforceApiError('Unrecognized command: %s' % command)

    @property
    def connection(self):
        '''Behavior: Log in to Salesforce
           Return: Handle to connection
        '''
        return self.clientLib.connection
                
    @property
    def wsdl_file(self):
        '''Return full path to WSDL File'''
        prog_base_dir = os.path.split(sys.argv[0])[0] or '.'
        return '%s/%s' % (prog_base_dir, WSDL_FILE)

    def desc(self, sfobject):
        '''Return Brief Column Description of sfobject'''

        result = self.clientLib.desc(sfobject)
        results = []
        i = 0
        for i, field in enumerate(result['fields']):
            results.append("%s. %s, %s, %s"  % (i+1, field['name'], 
                                                field['type'], 
                                                field['length']))
        return results

    def showObjects(self):
        '''Return list of all Salesforce Objects'''
        h = self.connection
        try: 
            #result = h.describeGlobal()
            result = self.clientLib.connection.describe()
            results = []
            for i, sobject in enumerate(result['sobjects']):
                results.append("%s. %s" % (i, sobject['label']))
        except Exception, e:
            results = str(e)
        return results

    def fields(self, sfobject):
        '''Return Column Data of sfobject'''
        result = self.clientLib.desc(sfobject)
        results = {}
        for i, field in enumerate(result['fields']):
            key = field['name'].lower()
            results[key] = {'type': field['type'],
                            'length': field['length'],
                            'name': field['name'],
                            'position': i+1}
        return results

    def query(self, querystr, format='tabular'):
        '''Return results of a querystr
           see: queryMore()

           options: format='tablular'
                    format='dict|dictionary'
        '''

        # validate query a bit:
        regex = 'select .* from .*'
        if not re.match(regex, querystr):
            emsg = 'Invalid query string: %s' % querystr
            raise SalesforceApiParameterError(emsg)

        h = self.connection

        # set batch size
        self.clientLib.setBatchSize()

        # get data
        result =  self.clientLib.query(querystr)
        return self.queryResults(result, format)

    def queryMore(self, format='tabular'):
        '''Return subsequent results from querystr set up in query()
           see: query()
        '''
        h = self.connection
        result = h.queryMore(self.query_locator)
        return self.queryResults(result, format)
    
    def queryResults(self, result, format):
        '''Query results processing for query() and queryMore()'''

        results = []
        if format not in ('tabular', 'dict', 'dictionary'):
            raise Exception('SalesforceApi.Query: Unrecognized format: %s'
                            % format)
        self.query_done  = self.clientLib.queryIsDone(result)
        self.query_locator = self.clientLib.queryLocator(result)

        if not self.clientLib.resultSize(result):
            return results

        records = self.clientLib.resultRecords(result)

        # build output
        if format in ('dict', 'dictionary'):
            for record in records:
                row = {}
                for key, value in \
                    record if self.clientLib.NAME == 'toolkit' else \
                    record.items():
                    if key == self.clientLib.RECORD_KEYS_TO_IGNORE:
                        continue
                    row[key] = value
                results.append(row)
        else:
            # Note: assumption about header is wrong
            # header of first row not the same as for others
            header = self.clientLib.getResultHeader(result)
            results.append(header)
            for record in records:
                row = []
                for key in header:
                    row.append(getattr(record, key, None))
                results.append(row)

        return results

    def create(self, sfobject, header, rows):
        '''Create new Records. Calls update()'''
        return self.update(sfobject, header, rows, action='create')

    def delete(self, sfobject, header, rows):
        '''Delete Records. Calls update()'''
        return self.update(sfobject, header, rows, action='delete')

    def update(self, sfobject, header, rows, action='update'):
        '''Given: sfobject as a STR, 
                  header   as an ARRAY, and
                  rows     as an ARRAY of Arrays

           Behavior: Update/Create/Delete rows in Salesforce
                     creates success and failure csv output files

                     Header names much match Salesforce Object field names.
                     First column must be the Id column.

           Returns:  Message as an Array of 
                     Number of successes and failures
                     And the names of the output files.
        '''
        if action not in ('create', 'delete', 'update'):
            raise SalesforceApiError('Unrecognized update action: %s' % action)

        if action in ('delete', 'update') and header[0].title() != 'Id':
            raise SalesforceApiError('First column must be Id')

        results = []
        failures = []
        successes = []

        fields = self.fields(sfobject)

        # process rows:
        rcnt = 0
        data = {}
        for row in rows:
            rcnt += 1
            if self.verbose:
                print '%s. row: %s' % (rcnt, row)

            for i, value in enumerate(row):
                if i == 0 and action in ('delete', 'update'):
                    #obj.Id = value
                    object_id = value
                    continue
                field = header[i]
                key = field.lower()

                # validate field:
                if key not in fields.keys():
                    raise SalesforceApiError(
                        "Invalid column '%s' for Salesforce object: %s"
                        % (field, sfobject))

                # spec. handling by field types:
                if fields[key]['type'] in ('date','double'):
                    if not value:
                        continue
                elif fields[key]['type'] in ('string'):
                    if value:
                        value = unicode(value, errors='ignore')

                # Set value:
                data[field] = value

            obj = self.connection.__getattr__(sfobject.title())
            if action == 'delete':
                result = obj.delete(object_id)
            elif action == 'create':
                result = obj.create(data)
            else:
                result = obj.update(object_id, data)

            SUCCESS_CODES = [204,]
            if (isinstance(result, int) and result in SUCCESS_CODES) \
               or result['success']:
                successes.append(row + [past_tense_action_str(action)])
            else:
                emsg = '. '.join([e.message for e in result.errors])
                failures.append(row + [emsg])

            if rcnt and rcnt % IND_PROGRESS_INTERVAL == 0:
                print '%s rows processed. (%s successes, %s failures)' \
                    % (rcnt, len(successes), len(failures))

        # write output files:
        failure_msg = '%6s failures ' % len(failures)
        if failures:
            failure_file = 'failure_%s_%s.csv' % (sfobject, uniqueId())
            writer = csv.writer(open(failure_file, 'w'))
            writer.writerow(header + ['Failure'])
            writer.writerows(failures)
            failure_msg += ' (%s)' % failure_file

        success_msg = '%6s successes' % len(successes)
        if successes:
            success_file = 'success_%s_%s.csv' % (sfobject, uniqueId())
            writer = csv.writer(open(success_file, 'w'))
            writer.writerow(header + ['Status'])
            writer.writerows(successes)
            success_msg += ' (%s)' % success_file

        results += [success_msg, failure_msg]
        return results

    """
    def doAction(self, h, obj, action, try_count=0):
        '''Provide Retry capability, to actual API call.'''
        TRIES = 3
        try_count += 1
        if try_count > 1:
            print 'retrying %s ...' % (try_count -1)
        try:
            if action == 'delete':
                return h.delete(obj.Id)
            elif action == 'create':
                return h.create(obj)
            else:
                return h.update(obj)
        except Exception, e:
            print "%s: %s" % (e.__class__.__name__, e)
            if 'INVALID_FIELD' in str(e) or 'INVALID_TYPE' in str(e):
                raise
            if try_count <= TRIES:
                return self.doAction(h, obj, action, try_count)
    """

    def loadCsv(self, csvfile):
        '''Given a csv filename
           Return a tuple: (header an ARRAY, and
                            rows   an ARRAY of ARRAYS)
        '''
        header = []
        rows = []
        fp = open(csvfile, 'r')

        # Read and Validate csvfile:
        for i, row in enumerate(csv.reader(fp,delimiter=',',escapechar='\\')):
            #row = unicode(row, 'replace')
            # Get Header
            if i == 0:   
                header = row
                continue
            num_fields = len(row)

            # Skip blank lines
            if not num_fields:
                continue

            # validate field numbers
            if num_fields != len(header):
                raise SalesforceApiFieldLenMismatch(
                    'svfile: '
                    'Line %s: Number of columns %s, does not match number '
                    'of header columns %s.' % (i+1, num_fields, len(header)))

            # keep it around
            rows.append(row)

        fp.close()
        return header, rows
    
    def validate(self, param, value):
        emsg = ''
        if param == 'command':
            if value not in COMMANDS:
                emsg = 'Unrecognized command: %s' % value
        elif param == 'directobject':
            if value != 'objects':
                emsg = 'Unrecognized directobject: %s' % value
        elif param == 'querystr':
            regex = 'select .* from .*'
            if not re.match(regex, value):
                emsg = 'Invalid query string: %s' % value
        elif param == 'sfobject':
            if value.lower() not in [x.lower() for x in SFOBJECTS]:
                emsg = 'Unrecognized sfobject: %s' % value
            if value.lower() in [x.lower() for x in CUSTOMOBJECTS]:
                value += '__c'
        elif param == 'csvfile':
            if not os.path.isfile(value):
                emsg = 'Unable to open file: %s' % value
        else:
            emsg = 'Unrecognized parameter: "%s" = "%s"' % (param, value)
        if emsg:
            raise SalesforceApiParameterError(emsg)

        return value

def past_tense_action_str(action):
    '''Given  STR 'create'
       Return STR 'Created'
    '''
    action2 = action.lower()
    if action2 in ('create', 'delete', 'update'):
        return action2.title() + 'd'
    return action2.title() + 'ed'
        
def syntax(emsg=None):
    prog_name = os.path.basename(sys.argv[0])
    if emsg:
        print emsg
    ws = ' '*len(prog_name)
    print
    print "   %s [-v] create <object> <csvfile>" % prog_name
    print "   %s      delete <object> <csvfile>" % ws
    print "   %s      desc <object>"             % ws
    print "   %s      fields <object>"           % ws
    print "   %s      query <querystring>"       % ws
    print "   %s      show objects"              % ws
    print "   %s      update <object> <csvfile>" % ws
    print
    sys.exit(1)

def disp_results(results):
    if isinstance(results, (list, tuple)):
        if isinstance(results[0], (list, tuple)):
            for row in results:
                print ",".join(map(str, row)),
                print
        else:
            print "\n".join(map(str, results))
    elif isinstance(results, dict):
        keys = sorted(results.keys())
        for k in keys:
            print "%s: %s" % (k, results[k])
    else:
        print results

if __name__ == '__main__':
    sf = SalesforceApi()
    args = copy.copy(sys.argv[1:])
    try:
        results = sf.process(*args)
    except Exception, e:
        if DEBUG:
            raise
        results = str(e)

    disp_results(results)
