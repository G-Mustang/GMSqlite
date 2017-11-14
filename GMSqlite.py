#!/usr/bin/env python

import os
import sys

import sqlite3

class GMSqliteException(Exception): pass

class GMSqliteCol(object):

    def __init__(self, name, typ, primary_key=False, not_null=True):

        self.name = name
        self.typ  = self.typ_change(typ)

        self.is_primary = primary_key
        self.not_null   = not_null

    def typ_change(self, typ):

        typ = typ.strip()
        typ = typ.lower()

        if typ in ['str', 'char', 'txt', 'text']:
            return 'TEXT'
        elif typ in ['int']:
            return 'INT'
        elif typ in ['float', 'double', 'real']:
            return 'REAL'
        elif typ in ['boolen', 'bool']:
            return 'INT'

    def prt(self):

        pk = ''
        nt = ''

        if self.is_primary:
            pk = 'PRIMARY KEY'

        if self.not_null:
            nt = 'NOT NULL'

        return ' '.join([self.name, self.typ, pk, nt])

class GMSqlite(object):

    def __init__(self, db_file='', name='no-name-GMSqlite'):

        self.name    = name
        self.db_file = db_file
        self.db      = None

    def open(self):

        if not os.path.isfile(self.db_file):
            st = '%s is not a valid file'%self.db_file
            raise GMSqliteException(st)

        try:
            self.db = sqlite3.connect(self.db_file)
        except:
            st = 'Can not open %s as a sqlite database.'%self.db_file
            raise GMSqliteException(st)

    def create(self, new_db=''):

        if new_db:
            self.db_file = new_db

        if os.path.isfile(self.db_file):
            st = '%s is already exsits'%self.db_file
            raise GMSqliteException(st)

        try:
            self.db = sqlite3.connect(self.db_file)
        except:
            st = 'Can not create %s as a sqlite database.'%self.db_file
            raise GMSqliteException(st)

    def close(self):
        self.db.close()

    def cmd(self, user_cmd=''):

        #print(user_cmd)

        try:
            cur = self.db.execute(user_cmd)
            self.db.commit()
            return cur
        except:
            st = 'Invalid sqlite command'
            raise GMSqliteException(st)

    def add_table(self, table_name='', col=[]):

        if not table_name:
            st = 'Can not add a table with empty name'
            raise GMSqliteException

        if not col:
            st = 'Can not add a table with no col'
            raise GMSqliteException

        cmd = []

        for c in col:

            cmd.append(c.prt())

        sq_cmd = 'CREATE TABLE %s (\n%s\n);'%(table_name, ',\n'.join(cmd))

        self.cmd(sq_cmd)

    def del_table(self, table_name=''):

        if not table_name:
            st = 'Can not del a table with empty name'
            raise GMSqliteException

        sq_cmd = 'DROP TABLE %s;'%(table_name)

        self.cmd(sq_cmd)

    def add_rec(self, table_name='', *val):

        if not table_name:
            st = 'Can not insert a record to table with empty name'
            raise GMSqliteException

        val_str = []

        for va in val:

            if type(va) == int:
                val_str.append(str(va))
            elif type(va) == str:
                val_str.append("'%s'"%va)
            elif type(va) == bool:
                if va:
                    val_str.append('1')
                else:
                    val_str.append('0')

        sq_cmd = "INSERT INTO %s VALUES (%s);"%(table_name, ','.join(val_str))

        self.cmd(sq_cmd)

    def tables(self):

        cur = self.cmd("SELECT name from sqlite_master where type='table' order by name;")
        return [ str(s) for s in cur.fetchone() ]

    def col_names(self, table_name=''):

        cur = self.cmd("PRAGMA table_info(%s)"%table_name)
        return [ str(s[1]) for s in cur.fetchall()]

    def col_type(self, table_name, col_name):

        if not col_name in self.col_names(table_name):
            return ''
        else:
            cur = self.cmd("PRAGMA table_info(%s)"%table_name)
            name_and_type = [ str(s[2]) for s in cur.fetchall()]
            names = self.col_names(table_name)

            return name_and_type[names.index(col_name)]

    def sel(self, table_name, col=['*'], arg_str=''):

        if '*' in col:
            cur = self.cmd("SELECT * from %s %s"%(table_name, arg_str))
        else:
            cur = self.cmd("SELECT %s from %s %s"%(','.join(col), table_name, arg_str))

        return cur.fetchall()

    def del_rec(self, table_name, arg_str=''):

        cur = self.cmd("DELETE from %s Where %s"%(table_name, arg_str))


def main():

    create = True

    a = GMSqlite('test_db')

    if create:

        a.create()

        col = []

        col.append(GMSqliteCol('ID', 'INT', True))
        col.append(GMSqliteCol('NAME', 'TEXT'))
        col.append(GMSqliteCol('AGE', 'INT'))
        col.append(GMSqliteCol('ADDRESS', 'TEXT'))
        col.append(GMSqliteCol('DEPARTMENT', 'TEXT'))
        col.append(GMSqliteCol('SOLARY', 'INT'))

        a.add_table('COMPANY', col)

    else:

        a.open()

        #xx test

        #a.del_table('COMPANY')

        #a.add_rec('COMPANY', 2, 'Allen', 35, 'New York', 'ASIC', 20000)

        #print(a.tables())
        #print(a.col_names('COMPANY'))
        #print(a.col_type('COMPANY', 'NAME'))
        print(a.sel('COMPANY'))
        #a.del_rec('COMPANY', 'ID==2')
        #print(a.sel('COMPANY'))

    a.close()

if __name__ == '__main__':
    main()
