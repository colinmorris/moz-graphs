########################
#    DEPRECATED!       #
########################

import sqlite3
import csv
import re
from optparse import OptionParser
import datetime
import time
import chatters
import os
from chatters import canonize
from utils import *
import sys

import colpy.db

EMAIL_MATCH = 0

class Debugger(object):
    
    def __init__(self, id, parent_db):
        self.db = parent_db
        self.id = id

class BugDatabase(colpy.db.DB):
    
    table_names = ['aliases', 'debuggers', 'bugs', 'bugevents', 'chats']
    table_schemata = ['(dbid int, alias text, noccs int default 1)',
        '(email text, name text, irc text, nirc int default 0, nbz int default 0, linktype text default NULL)', 
        # irc = canon IRC name from assigned_to field of bug_summary
        # nirc = number of IRC logs this author appears in
        # nbz = number of times this author appears in any bug's history
        # linktype = a string representing how we linked this debugger's IRC identity w Bugzilla identity. mozillians_IRC, mozillians_uname, bugzilla_email, mozillians_email, etc.
        '(bzid int,importance text,n_cc_list int,n_depends int,n_duplicates int,n_history int,n_keywds int,nblocks int,nvoters int,product text,reported timestamp,resolved timestamp,status text,verified timestamp)',
        '(bzid int, dbid int, date timestamp)',
        '(p1 int, p2 int, n int, date timestamp)',
    ]
    table_indices = [ # [ (name, [field1, field2...]), ...] ...
        # aliases
        [],
        # debuggers
        [('emailindex', ['email']),
        ],
        # bugs
        [],
        # bugevents
        [],
        # chats
        [],
        ]
    

    
    def create_tables(self):
        for (name, schema) in zip(self.table_names, self.table_schemata):
            self.execute("CREATE table %s %s" % (name, schema))
            
        self._create_indices()
            
    def _create_indices(self):
        for (table, indices) in zip(self.table_names, self.table_indices):
            for (index_name, fields) in indices:
                self.execute("CREATE INDEX %s ON %s (%s)" % (index_name, table, ', '.join(fields)) )
            
    def count(self, selectionstring, params, table):
        """Same as select except return the count of rows rather than the rows themselves.
        """
        select_string = """SELECT COUNT(*) FROM %s AS f1 %s""" % (table, selectionstring)
        res = self.execute(select_string, params)
        return res.fetchone()[0]
        
    def select(self, selectionstring, params, table):
        """Given a string of the form 'WHERE...', do some magic
        to select the right fields and use them to index into our internal 
        dict to return a set of Forecast-y objects (class decided by kls).
        """
        select_string = """SELECT * FROM %s AS f1 %s""" % (table, selectionstring)
        res = self.execute(select_string, params)
        return res
        
    def getone(self, selectionstring, params, table, field="ROWID"):
        """Return a single value (corresponding to the given field name) associated with the given selection string.
        """
        select_string = """SELECT %s FROM %s AS f1 %s""" % (field, table, selectionstring)
        curse = self.execute(select_string, params)
        res = curse.fetchone()
        if curse.fetchone():
            print "WARNING: expected to only get one result, but got 2+ for following query:\n" + select_string
        return res[0]
        
    def prettyprint(self, query, delim='\t'):
        """Return a human-readable table of the results of a given query.
        """
        return '\n'.join([delim.join(map(str, tup)) for tup in self.execute(query).fetchall()])
        
    def from_alias(self, alias):
        """Given an alias, get the debugger id associated with it.
        """
        return self.getone("WHERE alias=?", [alias], 'aliases', 'dbid')
        
    def add_chat_logs(self, dirname=chatters.CHATLOG_DIR):
        for fname in os.listdir(dirname):
            f = open(os.path.join(dirname, fname))
            log = chatters.Log(f)
            self._add_chat_log(log)
            f.close()
            
    def _add_chat_log(self, log):
        """Update the database with information from the given chat Log. 
        
        Overview: update the variables that count the number of logs a particular
            debugger appears in, and the number of times a particular alias is 
            used. Update chats with the number of times two debuggers talk.
        """
        adj = log.adj_dict()
        auth_to_id = {}
        
        # Update all exterminators, regardless of whether they talk to anyone
        for ext in log.exterminators:
            ext_id = self.from_alias(list(ext.aliases)[0])
            auth_to_id[ext] = ext_id
            self.execute("UPDATE debuggers SET nirc=nirc+1 WHERE ROWID=?", [ext_id])
            for alias in ext.aliases:
                self.execute("UPDATE aliases SET noccs=noccs+1 WHERE alias=?", [alias])
        
        # Go through the adj matrix and update the chat table
        for seme in adj:
            seme_alias = list(seme.aliases)[0]
            seme_id = auth_to_id[seme]
            seme_row = adj[seme]
            for uke in seme_row:
                uke_alias = list(uke.aliases)[0]
                uke_id = auth_to_id[uke]
                self.execute("INSERT INTO chats(p1, p2, n, date) VALUES (?,?,?,?)", [seme_id, uke_id, seme_row[uke], log.start])
                    
                            
    def add_bug_summary(self, fname):
        """Add the information stored in the bug_summar csv.
        """
        f = open(fname)
        f.readline() # Ignore headers
        reader = csv.reader(f)
        for line in reader:
            assigned_to = line[0]
            assigned_db = self._assigned_to(assigned_to)
            if assigned_db is None:
                continue
            # Don't add debuggers that are already in the database
            elif not self.count("WHERE email=?", [assigned_db[0]], 'debuggers'):
                self.execute("INSERT INTO debuggers(email, name, irc) VALUES (?,?,?)", assigned_db)
        f.close()
        
    def populate_bugs(self, fname="bug_summary.csv"):
        """Populate the bug table using the information stored in bug_summary.csv.
        """
        SUMMARY_HEADER = """assigned_to,blocks,bug,cc_list,component,depends_on,
        duplicates,history,importance,keywds,n_cc_list,n_depends,n_duplicates,
        n_history,n_keywds,nblocks,nvoters,platform,product,reported,resolved,
        status,verified,version,voters"""
        fields = SUMMARY_HEADER.replace('\n', '').split(',')
        f = open(fname)
        f.readline()
        reader = csv.reader(f)
        for line in reader:
            fmap = dict(zip(fields, line))
            for datefield in ["reported", "resolved", "verified"]:
                try:
                    fmap[datefield] = datify(fmap[datefield])
                except ValueError:
                    # This should happen, e.g. if a bug still hasn't been resolved and so the field is empty
                    fmap[datefield] = None
            relevant_fields = "bug,importance,n_cc_list,n_depends,n_duplicates,n_history,n_keywds,nblocks,nvoters,product,reported,resolved,status,verified".split(',')
            csvfieldname_to_dbfieldname = dict(zip(relevant_fields, relevant_fields))
            csvfieldname_to_dbfieldname['bug'] = "bzid"
            self.execute("""INSERT INTO bugs(%s)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""" % ','.join(csvfieldname_to_dbfieldname.values()),
                            [fmap[fieldname] for fieldname in csvfieldname_to_dbfieldname.keys()]
                            )
            
        # This is some of the worst code I've ever written ^
            
        
    def _assigned_to(self, assign_str):
        """Return a tuple describing the debugger in the 'assigned to' field.
        """
        ass_re = re.compile("(?P<name>.*?)( [\[(]?:(?P<nick>[^ \])]*)[\])]?(.*?))? <(?P<email>.*)>$")
        if assign_str == "Nobody; OK to take it and work on it <nobody@mozilla.org>":
            return None
        if len(assign_str.split()) == 1:
            return (assign_str, '', '')
            
        match = ass_re.match(assign_str)
        if match.group("nick"):
            nick = canonize(match.group("nick"))
        else:
            nick = None
        return (match.group("email"), match.group("name"), nick)
        
    def add_bug_history(self, fname):
        """Add the information stored in bug_history.csv ish file
        """
        f = open(fname)
        f.readline() # ignore headers
        reader = csv.reader(f)
        for line in reader:
            (bugid, email, date) = line
            date = datify(date)
            if not self.count("WHERE email=?", [email], 'debuggers'):
                self.execute("INSERT INTO debuggers VALUES (?,?,?,0,?)", (email, '', '', 1))
            else:
                self.execute("UPDATE debuggers SET nbz = nbz+1 WHERE email=?", [email])
                
        f.close()
        
    def add_alias_file(self, fname):
        """What it says on the tin."""
        f = open(fname)
        for line in f:
            aliases = line.strip().split(',')
            identity = None
            
            # First try to find an exact match on the irc field (this corresponds to [:blah] in the assigned to field)
            for alias in aliases:
                if self.count("WHERE irc=?", [alias], 'debuggers'):
                    if identity:
                        print "WARNING! Found more than one debugger associated with this alias set (using irc field):\n%s" % (str(aliases))
                    identity = self.getone("WHERE irc=?", [alias], 'debuggers')
                    
            # If the previous step failed, try to match on e-mail
            if not identity and EMAIL_MATCH:
                for alias in aliases:
                    if self.count("WHERE email LIKE \"%s@%%\"" % (alias), [], 'debuggers'):
                        if identity:
                            print "WARNING! Found more than one debugger associated with this alias set (using email matching):\n%s" % (str(aliases))
                        identity = self.getone("WHERE email LIKE \"%s@%%\"" % (alias), [], 'debuggers')
                        irc = self.getone('WHERE ROWID=?', [identity], 'debuggers', 'irc')
                        if irc:
                            print "WARNING: this user had an IRC name, but we were only able to do a partial match on e-mail:\n" + irc
                            
                        
            # If we didn't find one, we need to create a new row in debuggers
            if not identity:
                self.execute("INSERT INTO debuggers(email, name, irc) VALUES (?, ?, ?)", ('','',''))
                # This is ugly. ids are autoincrementing though, so the most recent addition should have the greatest rowid
                identity = self.execute("SELECT max(ROWID) FROM debuggers").fetchone()[0]
                
            
            # Now add the relevant rows to the alias table
            for alias in aliases:
                self.execute("INSERT INTO aliases VALUES (?, ?, 1)", (identity, alias))
            
        f.close()
        
    def debuggers_debug(self):
        """So meta."""
        ndebuggers = self.count('', [], 'debuggers')
        just_irc = self.count('WHERE irc="" AND name="" AND email=""', [], 'debuggers') # Authors from IRC with no link to bz data
        nchatters = self.execute('SELECT count(distinct dbid) FROM aliases').fetchone()[0]
        linked_by_irc = self.execute('SELECT count(distinct a.dbid) FROM aliases a WHERE (SELECT irc FROM debuggers WHERE ROWID=a.dbid)!=""')
        
        print str(ndebuggers) + " total devs in the debuggers table."
        print "%d devs are linked to IRC. %d are linked to both IRC and bugzilla data" % (nchatters, nchatters-just_irc)
        print "%d devs from the bugzilla data have no correlate in the IRC data" % (ndebuggers - nchatters)
        
        f1 = open('prolific_chatters.csv', 'w')
        HEADER = ["email", "name", "IRC nick (as stated in Bugzilla)", "# IRC logs appeared in", "# bug history events", "Most frequent IRC nick, of those linked"]
        f1.write('\t'.join(HEADER)+'\n')
        f1.write(db.prettyprint('SELECT *, (SELECT alias FROM aliases AS a1 WHERE dbid=d1.ROWID AND noccs=(SELECT MAX(noccs) FROM aliases WHERE dbid=d1.ROWID)) FROM debuggers as d1 ORDER BY nirc DESC LIMIT 200'))
        f2 = open('prolific_debuggers.csv', 'w')
        f2.write('\t'.join(HEADER)+'\n')
        f2.write(db.prettyprint('SELECT *, (SELECT alias FROM aliases AS a1 WHERE dbid=d1.ROWID AND noccs=(SELECT MAX(noccs) FROM aliases WHERE dbid=d1.ROWID)) FROM debuggers as d1 ORDER BY nbz DESC LIMIT 200'))
        f1.close()
        f2.close()
        
    def merge_mozillian(self, debugger, mozillian):
        """Given a
            - Debugger representing someone already in the DB from the IR Clogs
            - Mozillian (see mozillians_scrape.py) representing info from the Mozillians phonebook
        Update the entry for the debugger using the information we have about the Mozillian.
        """
        raise NotImplementedError

        
    
        
if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-n",
        "--nlogs",
        dest="nlogs",
        type=int,
        default=0,
        help="Only parse the first n logs (0=unlimited)",
    )
    (options, args) = parser.parse_args()
    
    if len(args) == 0:
        dbname = ":memory:"
    else:
        dbname = args[0]
        
    db = BugDatabase(dbname)
    db.create_tables()
    db.add_bug_summary('bug_summary.csv')
    clock_it("adding bug summary")
    db.add_bug_history('bug_history.csv')
    clock_it("adding bug history")
    db.add_alias_file('IRC_nicks.txt')
    clock_it("adding aliases")
    db.add_chat_logs()
    clock_it("adding chat logs")
    db.debuggers_debug()
            
