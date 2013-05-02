#! /usr/bin/env python

import sys
import os
import re
import csv
import copy

from sforce.enterprise import SforceEnterpriseClient

from vlib import conf
from vlib.odict import odict
from vlib.utils import echoized, pretty, uniqueId, validate_num_args

DEBUG = 0
VERBOSE = 0
IND_PROGRESS_INTERVAL = 50

COMMANDS = ('create', 'delete', 'desc', 'fields', 'query', 'show', 'update')
SFOBJECTS = ('Account', 'Adoption', 'CampaignMember', 'Case', 'Contact',
             'Lead', 'Opportunity', 'User')

class SalesforceApiError(Exception): pass
class SalesforceApiParameterError(SalesforceApiError): pass
class SalesforceApiFieldLenMismatch(SalesforceApiError): pass

class SalesforceApi(object):
    '''Preside over Salesforce API'''

    def __init__(self):
        self.verbose = VERBOSE
        self.conf = conf.Factory.create().data

    def process(self, args):
        args = odict(args)
        cmd = args.cmd.lower()

        if cmd in ('create', 'delete', 'update'):
            header, rows = self.loadCsv(args.csvfile)

        if cmd == 'create':
            return self.create(args.object, header, rows)
        elif cmd == 'delete':
            return self.create(args.object, header, rows)
        elif cmd == 'desc':
            return self.desc(args.object)
        elif cmd == 'fields':
            return self.fields(args.object)
        elif cmd == 'query':
            return self.query(args.querystr)
        elif cmd == 'show':
            return self.showObjects()
        elif cmd == 'update':
            return self.update(args.object, header, rows)
        else:
            return 'Unrecognized cmd:', cmd

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

    @property
    def wsdl_file(self):
        '''Return full path to WSDL File'''
        prog_base_dir = os.path.split(sys.argv[0])[0] or '.'
        return '%s/%s' % (prog_base_dir, WSDL_FILE)

    def desc(self, sfobject):
        '''Return Brief Column Description of sfobject'''
        h = self.connection
        try:
            result = h.describeSObject(sfobject)
            results = []
            for i, field in enumerate(result.fields):
                results.append("%s. %s, %s, %s"
                               % (i+1, field.name, field.type, field.length))
        except Exception, e:
            results = str(e)
        return results

    def showObjects(self):
        '''Return list of all Salesforce Objects'''
        h = self.connection
        try:
            result = h.describeGlobal()
            results = []
            for i, sobject in enumerate(result.sobjects):
                results.append("%s. %s" % (i, sobject.label))
        except Exception, e:
            results = str(e)
        return results

    def fields(self, sfobject):
        '''Return Column Data of sfobject'''
        h = self.connection
        try:
            result = h.describeSObject(sfobject)
            results = {}
            for i, field in enumerate(result.fields):
                key = field.name.lower()
                results[key] = {'type': field.type,
                                'length': field.length,
                                'name': field.name,
                                'position': i+1}
        except Exception, e:
            results = str(e)
        return results

    def query(self, querystr):
        '''Return results of a querystr'''

        # validate query a bit:
        regex = 'select .* from .*'
        if not re.match(regex, querystr):
            emsg = 'Invalid query string: %s' % querystr
            raise SalesforceApiParameterError(emsg)

        # Do it
        h = self.connection
        try:
            results = []
            result =  h.query(querystr)

            # build header. some recs do not have all fields:
            header = []
            for record in result.records:
                if len(record.__keylist__) > len(header):
                    header = record.__keylist__

            # build output
            results.append(header)
            for record in result.records:
                row = []
                for key in header:
                    row.append(getattr(record, key, None))
                results.append(row)

        except Exception, e:
            results = str(e)
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

           Behavior: Update rows in Salesforce
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

        h = self.connection
        results = []
        failures = []
        successes = []
        try:
            fields = self.fields(sfobject)
            obj = h.generateObject(sfobject)
            # process rows:

            rcnt = 0
            for row in rows:
                rcnt += 1
                if self.verbose:
                    print '%s. row: %s' % (rcnt, row)

                for i, value in enumerate(row):
                    if i == 0 and action in ('delete', 'update'):
                        obj.Id = value
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
                    setattr(obj, field, value)

                result = self.doAction(h, obj, action)

                # process results:
                if result.success:
                    successes.append(row + [past_tense_action_str(action)])
                else:
                    emsg = '. '.join([e.message for e in result.errors])
                    failures.append(row + [emsg])

                if rcnt and rcnt % IND_PROGRESS_INTERVAL == 0:
                    print '%s rows processed. (%s successes, %s failures)' \
                        % (rcnt, len(successes), len(failures))

        except Exception, e:
            if DEBUG:
                raise
            # removing this recovery:
            #results = ['%s: %s' % (e.__class__.__name__, e)]
            raise

        finally:
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

def past_tense_action_str(action):
    '''Given  STR 'create'
       Return STR 'Created'
    '''
    action2 = action.lower()
    if action2 in ('create', 'delete', 'update'):
        return action2.title() + 'd'
    return action2.title() + 'ed'

def syntax():
    prog_name = os.path.basename(sys.argv[0])
    ws = ' '*len(prog_name)
    o = ''
    o += "\n"
    o += "   %s [-v] create <object> <csvfile>\n" % prog_name
    o += "   %s      delete <object> <csvfile>\n" % ws
    o += "   %s      desc   <object>\n"           % ws
    o += "   %s      fields <object>\n"           % ws
    o += "   %s      query  <querystr>\n"      % ws
    o += "   %s      show   objects\n"            % ws
    o += "   %s      update <object> <csvfile>\n" % ws
    return o

def parseArgs():
    import argparse
    p = argparse.ArgumentParser(description="Salesforce API", usage=syntax())
    p.add_argument('-v', dest='verbose', action='store_true',
                   help='verbose')
    sp = p.add_subparsers(dest='cmd')

    q = sp.add_parser('create', help='create new records in object')
    q.add_argument('object')
    q.add_argument('csvfile')

    q = sp.add_parser('delete', help='Delete record from object')
    q.add_argument('object')
    q.add_argument('csvfile')

    q = sp.add_parser('desc', help='Return brief description of object')
    q.add_argument('object')

    q = sp.add_parser('fields', help='Return full description of object')
    q.add_argument('object')

    q = sp.add_parser('query', help='Return results of a SOQL query string')
    q.add_argument('querystr')

    q = sp.add_parser('show', help='Return list of all SF Objects')
    q.add_argument('what', choices=['objects'])

    q = sp.add_parser('update', help='Update records in SF Object')
    q.add_argument('object')
    q.add_argument('csvfile')

    args = p.parse_args()
    return vars(args)

if __name__ == '__main__':
    args = parseArgs()
    VERBOSE = args['verbose']

    try:
        print pretty(SalesforceApi().process(args))
    except Exception, e:
        if VERBOSE:
            raise
        print "%s:%s" % (e.__class__.__name__, e)
