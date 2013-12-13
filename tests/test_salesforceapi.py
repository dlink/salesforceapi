#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import unittest
import sys
from datetime import datetime

TEST_NAMES = ('All', 'SalesforceApi')


# Fixtures
TEST_USER_EMAIL = 'bi@flatworldknowledge.com'
TEST_USER_ID    = '00530000006sBioAAE'

class TestSalesforceApi(unittest.TestCase):
    '''Test Conf'''
    
    def setUp(self):
        global client_lib
        from salesforceapi import SalesforceApi
        self.sf = SalesforceApi(client_lib)

    def test_query_tablular(self):
        soql = "select id, username from user where username = '%s'" \
               % TEST_USER_EMAIL
        results = self.sf.query(soql)
        num_records = len(results) - 1 # minus header
        self.assertTrue(num_records, 1)

    def test_query_dict(self):
        soql = "select id, Username from user where Username = '%s'" \
               % TEST_USER_EMAIL
        results = self.sf.query(soql, format='dict')
        self.assertEqual(results[0]['Username'], TEST_USER_EMAIL)

    def TODO_test_update(self):
        test_str = 'test_%s' % datetime.now()
        self.sf.update('user',
                       ['Id', 'AboutMe'],
                       [[TEST_USER_ID, test_str]])
        soql = "select id, AboutMe from user where Id = '%s'" \
               % TEST_USER_ID
        results = self.sf.query(soql, format='dict')
        self.assertEqual(results[0]['AboutMe'], test_str)

    def TODO_test_create_and_delete(self):
        test_str = 'test_%s' % datetime.now()
        self.sf.create('Topic', ['Name'],[[test_str]])
        soql = "select Id, Name from Topic where Name = '%s'" % test_str
        results = self.sf.query(soql, format='dict')
        self.assertEqual(results[0]['Name'], test_str)
        self.sf.delete('note', ['Id'], [[results[0]['Id']]])

def syntax():
    progname = os.path.basename(sys.argv[0])
    print
    print "  syntax: %s [%s]+" % (progname, ' | '.join(TEST_NAMES))
    print
    sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        syntax()

    if any(t not in TEST_NAMES for t in sys.argv[1:]):
        print "Test name must be one (or more) of:", ", ".join(TEST_NAMES)
        sys.exit(1)

    if  sys.argv[1] == 'All':
        tests = []
        for test_name in TEST_NAMES[1:]:
            tests.append(eval('Test%s' % test_name))
    else:
        tests = [eval('Test%s' % t) for t in sys.argv[1:]]

    suite = unittest.TestSuite()
    loader = unittest.defaultTestLoader.loadTestsFromTestCase
    suite.addTests([loader(t) for t in tests])

    # test toolkit client_lib
    print 'test toolkit client_lib'
    client_lib = 'toolkit'
    unittest.TextTestRunner(verbosity=2).run(suite)

    # test simple client lib
    print 'test simple client_lib'
    client_lib = 'simple'
    unittest.TextTestRunner(verbosity=2).run(suite)


