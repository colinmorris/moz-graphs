"""
This is old (pre-DB) not-very-nice code. It has the capability of writing
adjacency matrices, but that function has been shunted off to adjacency.py,
which talks to the DB.

The only reason to use this code now is that it has classes for parsing our
chat logs, and it can be run to create IRC_nicks.txt, a text file with a
comma-separated list of (apparently) related aliases on each line.
"""

import os
import datetime
import re
from collections import defaultdict
import sys
import time
from optparse import OptionParser
import copy
from utils import valid_nick

CHATLOG_DIR = '../chat_logs'
CHATLOG_DATE_FORMAT = '%a %b %d %H:%M:%S %Y'

RESULTS_DIR = 'adj'

SKIPPED = 0

BAD_CORRESPONDENCES = {'mnyromyr':['chewey','mcsmurf', 'mcslurf'],
                       'mkmelin':['mnyromyr'],
                       'mcsmurf':['mnyromyr'],
                       'mfinkle':['sdw', 'sdwilsh', 'gijs'],
                       'sdw':['mfinkle'],
                       'dolske':['mossop'],
                       'mossop':['dolske'],
                       'gijs':['mfinkle'],
                       }

# Aliases that are just too common and should be ignored
#ALIAS_BLACKLIST = [] #['IRCMonkey']


def canonize(nick):
    """Return a canonical representation of this nickname.
    """
    # Remove trailing underscores
    nick = nick.rstrip('_')
    
    # Remove anything including and after a |
    bar_index = nick.find('|')
    if bar_index != -1:
        nick = nick[:bar_index]
        
    # Lowercase
    nick = nick.lower()
    
    return nick
    
        

class Log(object):
    """A chat log for a single day.
    """
    
    def __init__(self, logfile):
        #self.lines = [] # Is this list necessary? I don't think it is.
        self.exterminators = set()
        opening = logfile.readline().strip()
        date_str = opening[15:]
        self.start = datetime.datetime.strptime(date_str, CHATLOG_DATE_FORMAT)
        for line in logfile:
            # This is our own invention. We comment out lines that we want to ignore.
            if line.startswith('#'):
                continue
            elif line.startswith('---'):
                if "closed" in line:
                    self.end = datetime.datetime.strptime(line[15:].strip(), CHATLOG_DATE_FORMAT)
                continue
            try:
                lineobj = LogLine.from_string(line)
            except ValueError:
                global SKIPPED
                SKIPPED += 1
                continue
            except IndexError:
                print "This is a rare line indeed"
                print line
                continue
            #self.lines.append(lineobj)
            #self.add_exterminator(lineobj.actor)
            if lineobj.actor:
                self.exterminators.add(lineobj.actor)
            
        self.condense_exterminators()
        self.filter_bad_nicks()
        
    def __cmp__(self, other):
        """Compare by date.
        """
        return cmp(self.start, other.start)
        
    def __add__(self, other):
        shelf = copy.deepcopy(self)
        #shelf.lines += other.lines
        our_exts = [copy.deepcopy(ext) for ext in self.exterminators]
        their_exts = [copy.deepcopy(ext) for ext in other.exterminators]
        #shelf.exterminators = self.exterminators.union(other.exterminators)
        shelf.exterminators = set(our_exts + their_exts)
        shelf.condense_exterminators()
        return shelf
        
    def add_no_condense(self, other):
        shelf = copy.deepcopy(self)
        #shelf.lines += other.lines
        our_exts = [copy.deepcopy(ext) for ext in self.exterminators]
        their_exts = [copy.deepcopy(ext) for ext in other.exterminators]
        #shelf.exterminators = self.exterminators.union(other.exterminators)
        shelf.exterminators = set(our_exts + their_exts)
        #shelf.condense_exterminators()
        return shelf
        
    def get_ext_by_alias(self, alias):
        for ext in self.exterminators:
            if alias in ext.aliases:
                return ext
        raise ValueError("Couldn't find exterminator by this name: " + alias)
        
    def filter_bad_nicks(self):
        """Remove any exterminators who have no clean aliases.
        """
        hitlist = set()
        for ext in self.exterminators:
            if not any([valid_nick(alias) for alias in ext.aliases]):
                hitlist.add(ext)
                
        for deadman in hitlist:
            self.exterminators.remove(deadman)
                    
            
    def condense_exterminators(self):
        """
        Before this is called, we may have multiple Exterminators corresponding
        to the same person. We combine any Exterminators that share any aliases.
        """
        #newext = set()
        hitlist = set()
        for ext1 in self.exterminators:
            for ext2 in self.exterminators:
                if ext1 == ext2:
                    continue
                if ext2 not in hitlist and ext1 not in hitlist and any (alias in ext2.aliases for alias in ext1.aliases):
                    ext1 += ext2
                    #self.exterminators.remove(ext2)
                    hitlist.add(ext2)
                    
        for deadman in hitlist:
            self.exterminators.remove(deadman)
            
    def n_messages(self, from_, to):
        """
        The number of messages sent from one exterminator to another in this log.
        """
        if isinstance(from_, str):
            from_ = self.get_ext_by_alias(from_)
        if isinstance(to, str):
            to = self.get_ext_by_alias(to)
        if from_ == to:
            return 0
        n = 0
        for alias in to.aliases:
            n += from_.cand_targets[alias]
            
        return n
        
    def write_authors(self, fname=''):
        if not fname:
            fname = "IRC_nicks.txt"
        f = open(fname, 'w')
        for auth in self.exterminators:
            f.write(','.join(alias for alias in auth.aliases)+'\n')
            
        f.close()
        
    def adj_dict(self):
        """Return a 2-d dictionary of the non-zero entries of the conversation
        matrix of this log.
        """
        adj = {}
        semes = [ext for ext in self.exterminators if ext.cand_targets.keys()]
        ukes = self.exterminators # Anyone can be an uke - we can't merge all cand_targets keys because they're strs not exterminators
        for auth1 in semes:
            adj_row = {}
            for auth2 in ukes:
                nchats = self.n_messages(auth1, auth2)
                if nchats:
                    adj_row[auth2] = nchats
            adj[auth1] = adj_row
            
        return adj
            
        
    def write_adj_matrix(self, fname=''):
        if not fname:
            fname = os.path.join(RESULTS_DIR, filter(lambda char:bool(char.strip()), str(self.start.date())) + '.csv')
        f = open(fname, 'w')
        sorted_auths = sorted([ext for ext in self.exterminators if ext.cand_targets.keys()], key=lambda x: x.nick)
        f.write(','.join(['']+[str(auth) for auth in sorted_auths]) + '\n')
        for auth1 in sorted_auths:
            f.write(str(auth1)+',')
            for auth2 in sorted_auths:
                f.write( str(self.n_messages(auth1, auth2)) + ',')
                
            f.write('\n')
            
        f.close()
                

class LogLine(object):
    """A line in an IRC chat log.
    
    Has attribute "actor", the Ext performing the relevant
    action (e.g. speaking, emoting, leaving chat)
    """
    
    TIME_FMT1 = '%H:%M:%S'
    TIME_FMT2 = '%H:%M' # Logs seem to change to second accuracy at some point
    
    def __init__(self, line):
        time_str = line.split()[0]
        try:
            self.time = time.strptime(time_str, self.TIME_FMT1)
        except ValueError:
            self.time = time.strptime(time_str, self.TIME_FMT2)
        
    @classmethod
    def from_string(cls, line):
        if line.split()[1] == '-!-':
            return SystemMessageLine(line)
        elif line.split()[1].startswith('<'):
            return MessageLine(line)
        elif line.split()[1] == '*':
            return EmoteLine(line)
        else:
            raise ValueError("Didn't recognize this line:\n" + line)
            
            
class SystemMessageLine(LogLine):

    LEAVE_RE = re.compile(r'(\d\d:)?\d\d:\d\d -!- (?P<name>\S+) \[.*\] has (left|quit).*')
    JOIN_RE = re.compile(r'(\d\d:)?\d\d:\d\d -!- (?P<name>\S+) \[.*\] has joined.*')
    NICKCHANGE_RE = re.compile(r'(\d\d:)?\d\d:\d\d -!- (?P<oldname>\S+) is now known as (?P<newname>\S+)')
    MISC_RE = re.compile('(\d\d:)?\d\d:\d\d -!- (((mode|ServerMode)/.*by (?P<name>\S+))|((?P<name2>\S+) changed the topic .*)|(Topic set by (?P<name3>\S+)))')
    KICK_RE = re.compile(r'(\d\d:)?\d\d:\d\d -!- (?P<kicked>\S+) was kicked .* by (?P<kicker>\S+) (?P<reason>.*)')
    ANON_RE = re.compile(r'(\d\d:)?\d\d:\d\d -!- (Topic for|Irssi:|Netsplit)')
    
    def __init__(self, line):
        if self.LEAVE_RE.match(line):
            self.actor = Exterminator(self.LEAVE_RE.match(line).group("name"))
        elif self.JOIN_RE.match(line):
            self.actor = Exterminator(self.JOIN_RE.match(line).group("name"))
        elif self.NICKCHANGE_RE.match(line):
            match = self.NICKCHANGE_RE.match(line)
            old = match.group("oldname")
            new = match.group("newname")
            if canonize(old) in BAD_CORRESPONDENCES and canonize(new) in BAD_CORRESPONDENCES[canonize(old)]:
                self.actor = Exterminator(old)
            else:
                self.actor = Exterminator([match.group("oldname"), match.group("newname")])
        elif self.KICK_RE.match(line):
            self.actor = Exterminator(self.KICK_RE.match(line).group("kicker"))
        elif self.MISC_RE.match(line):
            match = self.MISC_RE.match(line)
            self.actor = Exterminator(match.group("name") or match.group("name2") or match.group("name3"))
        elif self.ANON_RE.match(line):
            self.actor = None
        else:
            raise ValueError("Couldn't recognize system message:\n" + line)
            
        super(SystemMessageLine, self).__init__(line)
            
class MessageLine(LogLine):
    
    #MSG_RE = re.compile(r'(\d\d:)?\d\d:\d\d <(?P<name>\S+)>( \s*(?P<target>\S+)[,:])?')
    uname_re = lambda pat: (r'(?P<%s>[^\s,]+)' % (pat)) if pat else r'([^\s,]+)'
    global UNAME_RE #I hate this
    UNAME_RE = uname_re('')
    unames_re = lambda pat: r'(?P<%s>(%s, ?)+%s)' % (pat, UNAME_RE, UNAME_RE) if pat else \
        r'((%s,)+%s)' % (UNAME_RE, UNAME_RE)
    UNAMES_RE = unames_re('')
    MSG_RE = re.compile(r'(\d\d:)?\d\d:\d\d <%s> (?P<msgtext>.*)' % uname_re("name"))
    
    TARGET_DELIM = r'(:|,)' # Characters that can separate target names from the message
    REST_RE = r'.+' # The rest of the message after the address
    
    
    GREETINGS = ['hi', 'hello', 'hey', 'hiya', 'morning', 'good morning', 
        'evening', 'good evening', "g'day", 'good day', 'sup', 'yo', 'greetings',
        ]
    GREETING_RE = '(%s)' % ('|'.join(GREETINGS + [salut.capitalize() for salut in GREETINGS]) )
    
    # NB: targets must come before target, otherwise re will just eagerly match target
    TARGET_RE1 = '(' + unames_re("targets1") + "|" + uname_re("target1") + ')'
    TARGET_RE2 = '(' + unames_re("targets2") + "|" + uname_re("target2") + ')'
    
    TARGETTED_CONVO_RES = re.compile('|'.join( '('+foo+')' for foo in [
        GREETING_RE + ' ' + TARGET_RE1,
        TARGET_RE2 + TARGET_DELIM + REST_RE,
        ] ))
        
    print TARGETTED_CONVO_RES.pattern # Just out of curiosity
    
    def __init__(self, line):
        match = self.MSG_RE.match(line)
        if not match:
            raise ValueError("Couldn't recognize line:\n"+line)
        #self.actor = Exterminator(match.group("name"), match.group("target"))
        targets = self.get_conversation_targets(match.group("msgtext"))
        
        self.actor = Exterminator(match.group("name"), targets)
        super(MessageLine, self).__init__(line)
        
    def get_conversation_targets(self, text):
        """Given the text of this message, return a collection of the nicks
        being addressed in this message. There may be 0, 1, or many.
        """
        match = re.match(self.TARGETTED_CONVO_RES, text)
        if not match:
            return []
        targets = match.group("targets1") or match.group("targets2")
        target = match.group("target1") or match.group("target2")
        if targets:
            return re.split(', ?', targets)
        elif target:
            return [target]
        else:
            raise ValueError("Got a match, but couldn't find group")
            
class EmoteLine(LogLine):

    EMOTE_RE = re.compile(r'(\d\d:)?\d\d:\d\d \s*\* (?P<name>\S+) (?P<emote>.*)')
    
    GREETING_VERB = "(waves at|greets|pokes)"
    GREETING_RE = re.compile(GREETING_VERB + " " + MessageLine.TARGET_RE1)
    
    def __init__(self, line):
        match = self.EMOTE_RE.match(line)
        if not match:
            raise ValueError("Didn't recognize line:\n" + line)
            
        self.emote = match.group("emote")            
        targets = self.get_conversation_targets(self.emote)
            
        self.actor = Exterminator(match.group("name"))
        super(EmoteLine, self).__init__(line)
        
    def get_conversation_targets(self, emote):
        match = self.GREETING_RE.match(emote)
        if match:
            #print "Matched emote:\n" + emote
            target = match.group("target1")
            targets = match.group("targets1")
            if target:
                return [target]
            if targets:
                return re.split(', ?', targets)
            raise Exception("shouldn't have got here...\n"+emote+'\nMatched:\n'+self.GREETING_RE.pattern+'\nBut no group found.')

class Exterminator(object):
    """Because they kill bugs. Get it?
    """
    
    def __init__(self, aliases=[], cand_targets=[]):
        if hasattr(aliases, '__iter__'):
            self.aliases = set(filter(None, aliases))
        else:
            self.aliases = set([aliases])
            
        # Filter out blacklist
        self.aliases = map(canonize, self.aliases)
        self.aliases = set(filter(valid_nick, self.aliases))
        cand_targets = map(canonize, cand_targets)
        cand_targets = filter(valid_nick, cand_targets)
            
        # A mapping from Exterminators to the number of times this ext has talked to them
        self.cand_targets = defaultdict(int)
        for targ in cand_targets:
            self.cand_targets[targ] = 1
        
    @property
    def nick(self):
        """We use an exterminator's shortest alias as his canonical nickname.
        """
        return min(self.aliases, key=lambda alias: len(alias))
        
    def __iadd__(self, otherext):
        """Overriding +=. When one Exterminator is added to another, they 
        share aliases, and their cand_targets dicts are summed.
        """
        self.aliases = self.aliases.union(otherext.aliases)
        for (key, val) in otherext.cand_targets.iteritems():
            self.cand_targets[key] += val
        return self
            
    def __str__(self):
        return self.nick
        
def test_case1():
    fname = 'sample.log'
    f = open(fname)
    log = Log(f)
    print "WARNING: had to skip %d unparseable lines" % (SKIPPED)
    f.close()
    log.condense_exterminators()
    log.write_adj_matrix("sample_adj.csv")
    
    chatrs = {}
    for alias in ['Mossop', 'nthomas', 'gandalf', 'glazou', 'roc']:
        chatrs[alias] = log.get_ext_by_alias(alias)
        
    assert log.n_messages(chatrs['Mossop'], chatrs['nthomas']) == 1
    assert log.n_messages(chatrs['glazou'], chatrs['gandalf']) == 3
    assert log.n_messages(chatrs['glazou'], chatrs['roc']) == 1
    assert log.n_messages(chatrs['nthomas'], chatrs['roc']) == 1
    assert log.n_messages(chatrs['nthomas'], chatrs['Mossop']) == 1
    
def test_case2():
    fname1 = 'sample.log'
    fname2 = 'sample2.log'
    f1 = open(fname1)
    f2 = open(fname2)
    log1 = Log(f1)
    log2 = Log(f2)
    print "WARNING: had to skip %d unparseable lines" % (SKIPPED)
    f1.close()
    f2.close()
    log = log1 + log2
    #log.condense_exterminators()
    log.write_adj_matrix("sample_adj2.csv")
    
    chatrs = {}
    for alias in ['Mossop', 'nthomas', 'gandalf', 'glazou', 'roc']:
        chatrs[alias] = log.get_ext_by_alias(alias)
        
    assert log.n_messages('Mossop', 'nthomas') == 2 # NEW (was 1)
    assert log.n_messages(chatrs['glazou'], chatrs['gandalf']) == 5 # NEW (was 3)
    assert log.n_messages(chatrs['glazou'], chatrs['roc']) == 2 # NEW (was 1)
    assert log.n_messages(chatrs['nthomas'], chatrs['roc']) == 1
    assert log.n_messages(chatrs['nthomas'], chatrs['Mossop']) == 1
    
    log = log1
    
    assert log.n_messages('Mossop', 'nthomas') == 1
    assert log.n_messages('glazou', 'gandalf') == 3
    assert log.n_messages('glazou', 'roc') == 1
    assert log.n_messages('nthomas', 'roc') == 1
    assert log.n_messages('nthomas', 'Mossop') == 1
    
def write_authors(logs):

    BLOB = 'mandible,gijs_plop,mfinkle-ak,gijs_testagain,dwilsh,swilsh,monkey,swilsher,wfinkle,mfinkle-afy,sdiwlsh,sdwilshcursingnetwork,mfinkle_food,mfinkle-oscon,gck08,gijs-sleep,mfinkle_italic_h8er,noun,govin,sw,googles-poodle,sdfeildhz\dtufy,really_really_long_nick,d_sdwilsh,mfinkle-sleep,mfinkle_afk,comrade693,gijs_test,mfinkle-lunch,sdwilhs,hannibal-afk,sdwish,sdw,gijs-afk,gijs_afk,mfinkle-away,hannibal,mfinkle-afk,gijs,mfinkle,sdwilsh'
    suspect_aliases =  BLOB.split(',')


    alias_to_bucket = {}
    alias_to_nauths = defaultdict(int)
    for log in logs:
        for auth in log.exterminators:
        
            for alias in auth.aliases:
                alias_to_nauths[alias] += 1
        
            repeat_aliases = []
            
            # Find repeats
            for alias in auth.aliases:
                if alias in alias_to_bucket:
                    repeat_aliases.append(alias)
                    
            for rep in repeat_aliases:
                if rep in suspect_aliases and not auth.aliases.issubset(alias_to_bucket[rep]):
                    print str(log.start) + " joining\n\t" + ' '.join(auth.aliases) + '\nwith\n\t' + ' '.join(alias_to_bucket[rep])
                    print
                    
            new_bucket = reduce(set.union, [set(auth.aliases)]+[alias_to_bucket[rep] for rep in repeat_aliases])
            for alias in new_bucket:
                alias_to_bucket[alias] = new_bucket
                
    out = open('IRC_nicks.txt', 'w')
    for (alias, bucket) in alias_to_bucket.iteritems():
        # Avoid duplicates by only outputting when the key is the first item in the bucket alphabetically
        if alias == min(bucket):
            out.write( ','.join(sorted(bucket, key=lambda alias:alias_to_nauths[alias]))+'\n')
    out.close()
    
    out2 = open('alias_frequency.txt', 'w')
    for (alias, noccs) in sorted(alias_to_nauths.items(), key=lambda tup: tup[1], reverse=True):
        out2.write(alias + '\t' + str(noccs) + '\n')
    out2.close()
            
        
if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-t",
        "--test",
        dest="test",
        action="store_true",
        help="run test case",
    )
    parser.add_option(
        "--save-auths",
        dest="save_auths",
        action="store_true",
        help="save all author aliases to a file",
    )
    parser.add_option("-w",
        "--window-size",
        dest="window",
        type=int,
        default=7,
        help="size of sliding window for adjacency matrices",
    )
    parser.add_option("-n",
        "--nlogs",
        dest="nlogs",
        type=int,
        default=0,
        help="Only parse the first n logs (0=unlimited)",
    )
    (options, args) = parser.parse_args()
    
    if options.test:
        test_case1()
        test_case2()
        sys.exit(1)
    
    logs = []
    niters = 0
    for fname in os.listdir(CHATLOG_DIR):
        if options.nlogs and niters > options.nlogs:
            break
        if not fname.endswith('.log'):
            continue
        niters += 1
        #if fname != '#developers.20061004.log':
        #    continue
        f = open(os.path.join(CHATLOG_DIR, fname))
        try:
            log = Log(f)
            logs.append(log)
        except:
            print "problem in file %s" % (fname)
            raise
        #log.write_adj_matrix()
        #sys.exit(1)
    print "WARNING: had to skip %d unparseable lines" % (SKIPPED)
    
    if options.save_auths:
        write_authors(logs)
        sys.exit(1)
        everyone = reduce(lambda x,y:x.add_no_condense(y), logs)
        #everyauth = reduce(lambda x,y: x.union(y), [log.exterminators for log in logs])
        everyone.write_authors()
        sys.exit(1)
    
    # XXX: Are logs ordered chronologically?
    logs.sort()
    for i in range(len(logs)-7):
        #sevendays = sum(logs[i:i+7])
        sevendays = reduce(lambda x,y: x+y, logs[i:i+7])
        # XXX: TypeError: unsupported operand type(s) for +: 'int' and 'Log'

        sevendays.write_adj_matrix()
        
        
