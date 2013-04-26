"""Largely obsolete code for scraping bug information from Bugzilla and saving
it in csv form. No DB interaction (this is pre-DB).
"""

import bs4 as Soup
import urllib2
import time
from optparse import OptionParser
import sys
from .. import utils
import scrape_utils
import ssl
import src.debuggers as debuggers
import src.config
import os

MAX_TRIES = 4
HEADERS =  {'User-agent' : 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'}

# Print something to standard out after every n bugs
NOTIFICATION_INTERVAL = 250

#global opener
#opener = urllib2.build_opener()
#opener.addheaders.append(('Cookie', 'WT_FPC=id=226c6382454a15a3c921341247978449:lv=1341276892837:ss=1341276879354; wtspl=829294; Bugzilla_login=445357; Bugzilla_logincookie=9xxi7YLF0o'))


#def urlopen(url, postdata=None):
#    """Try to open and return the page at the given url. On failure, wait for
#    exponentially increasing amounts of time.
#    """
#    global opener
#    try_no = 1
#    while 1:
#        try:
#            page = opener.open(urllib2.Request(url, postdata, HEADERS))
#            return page
#        except (urllib2.HTTPError, urllib2.URLError):
#            if try_no >= MAX_TRIES:
#                raise
#            sleep_time = 20 + 2**try_no
#            sys.stderr.write("[W01] Couldn't access location at %s on %dth try. Sleeping for %d seconds.\n" % (url, try_no, sleep_time))
#            time.sleep(sleep_time)
#            try_no += 1

opener = scrape_utils.SmartOpener('mozilla.org')
# I'm just going to alias this so it's easier to port over to my messed up code base here.
urlopen = opener.open


class NoCommentsException(Exception):
    pass
    
def next_tag(elem):
    for child in elem.next_siblings:
        if not isinstance(child, Soup.NavigableString):
            return child
    raise ValueError("No subsequent tag siblings")
    
def next_tags(elem):
    for child in elem.next_siblings:
        if not isinstance(child, Soup.NavigableString):
            yield child

def child_tag(elem):
    for child in elem.children:
        if not isinstance(child, Soup.NavigableString):
            return child
    raise ValueError("No tag children")
    
def child_tags(elem):
    for child in elem.children:
        if not isinstance(child, Soup.NavigableString):
            yield child
            
class ForbiddenBugException(Exception):
    pass
    
class EmptyHistoryException(Exception):
    pass
            

class BugHistory(object):
    """Yields information encoded in the 'history' page of a Bugzilla bug
    """
    
    def __init__(self, bug_id):
        self.id = bug_id
        url = "https://bugzilla.mozilla.org/show_activity.cgi?id=" + str(bug_id)
        page = urlopen(url)
        self.soup = Soup.BeautifulSoup(page)
        self._parse_events()
        
    def _parse_events(self):
        self.events = []
        table = self.soup.find(id="bugzilla-body").table
        if table is None:
            raise EmptyHistoryException
        
        last_event = None
        for tr in table:
            if tr == '\n':
                # Whitespace
                continue
            elif tr.th is not None:
                # Header
                continue

            new_event = BugHistoryEvent.from_tr(tr, last_event, self.id)
            last_event = new_event
            self.events.append(new_event)

    def actors(self):
        """Return a list of the unique actors involved in this history.
        """
        return list(set([event.who for event in self.events]))
        
    def __iter__(self):
        return iter(self.events)

    def sort(self, reverse=False):
        self.events.sort(key = lambda event: event.datetime, reverse=reverse)

    def __getitem__(self, item):
        return self.events[item]

    def __len__(self):
        return len(self.events)
            
class BugHistoryEvent(object):
    
    def __init__(self, who, when, what, removed, added, bugid):
        self.who = who
        self.when = when
        self.date = utils.datify(self.when) # Strip out time info. Bleh.
        # We need this to break ties between events on the same day
        self.datetime = utils.datetimeify(when)
        #self.date = when.split()[0] # This takes away all the time info, leaving only the date
        self.what = what
        self.removed = removed
        self.added = added
        self.id = bugid
        self._postprocess()


    def _postprocess(self):
        if self.what == "Priority":
            self.added = self.added.split()[0]
            self.removed = self.removed.split()[0]

    def __str__(self):
        return "BHE: on bug %s, %s added %s and removed %s for field %s on %s" % \
               (self.id, self.who, self.added, self.removed, self.what, self.date)

    def __repr__(self):
        return str(self)
        
    def to_row(self):
        return [str(self.id), self.who, self.when]
        
    @staticmethod
    def header():
        return ['Bug', 'History', 'Date']
        
    @staticmethod
    def from_tr(tr, lastevent, id):
        """Return a BugHistoryEvent initialized using the given html row.
        """
        if isinstance(tr, Soup.NavigableString):
            raise ValueError("This was a dumb whitespace thing, not a row.")
        cells = [subel for subel in tr.contents if not isinstance(subel, Soup.NavigableString) and subel.name == 'td']
        if not cells:
            raise ValueError("This was a header row")
            
        # Sometimes the who and when are implied based on the previous row
        if len(cells) == 3:
            who = lastevent.who
            when = lastevent.when
        elif len(cells) == 5:
            who = cells[0].string.strip()
            when = cells[1].string.strip()
        else:
            raise ValueError("Expected 3 or 5 cells per row. Got %d." % (len(cells)))
            
        try:
            added = cells[-1].string.strip()
        except AttributeError:
            added = ' '.join(cells[-1].stripped_strings)
        try:
            removed = cells[-2].string.strip()
        except AttributeError:
            removed = ' '.join(cells[-2].stripped_strings)
        try:
            what = cells[-3].string.strip()
        except AttributeError:
            what = ' '.join(cells[-3].stripped_strings)
        
        return BugHistoryEvent(who, when, what, removed, added, id)
        

class BugVotes(object):
    """Yields information encoded in the 'votes' page of a Bugzilla bug
    """
    
    def __init__(self, bug_id):
        self.id = bug_id
        url = "https://bugzilla.mozilla.org/page.cgi?id=voting/bug.html&bug_id=%s" % (str(bug_id) )
        page = urlopen(url)
        self.soup = Soup.BeautifulSoup(page)

    @property
    def voters(self):
        voters = []
        for row in self.soup.find(id="bugzilla-body").table.children:
            if not isinstance(row, Soup.NavigableString) and row.name == "tr":
                voter_cell = child_tag(row)
                if voter_cell.name == "th":
                    # This means this is the header
                    continue
                voter = voter_cell.a.string.strip()
                voters.append(voter)
                
        return voters
                
        
    @property
    def nvotes(self):
        for elem in child_tags(self.soup.find(id="bugzilla-body")):
            if elem.name == "p":
                return int(elem.string.split()[-1])


class CacheMissException(Exception):
    pass


class BugPage(object):
    """A page on Bugzilla documenting a bug.
    """
    
    def __init__(self, bug_id):
        self.id = bug_id

        # New idea: Store scraped pages locally, so we don't have to hit bz so much.
        try:
            page = self.load_cached()
        except CacheMissException:
            url = 'https://bugzilla.mozilla.org/show_bug.cgi?id=' + str(bug_id)
            page = urlopen(url, None, True)
            self.cache_page(page)

        self.soup = Soup.BeautifulSoup(page)

        if not self.soup.find(id="error_msg") is None:
            errmsg = self.soup.find(id="error_msg").string
            if "not authorized to access" in errmsg:
                raise ForbiddenBugException()
            else:
                raise Exception(errmsg)
        # TODO: Do the same caching here as we do with pages
        self._history = None
        self._votes = None

    # We load history and votes lazily because there are some instances where we don't need them,
    # and we want to minimize the number of server hits we have to do.
    @property
    def history(self):
        if self._history is None:
            self._history = BugHistory(self.id)
        return self._history

    @property
    def votes(self):
        if self._votes is None:
            self._votes = BugVotes(self.id)
        return self._votes

    @property
    def reporter_email(self):
        return self.soup.find(id='bz_show_bug_column_2').table.find("td").span.a['href'].split(':')[-1]


    def _cache_path(self):
        data_location = src.config.DATA_DIR
        return os.path.join(data_location, 'pages', str(self.id) + '.html')


    def load_cached(self):
        """Load a cached version of the page corresponding to our bug id. If none, raise CacheMissException.
        """
        try:
            return open(self._cache_path())
        except IOError:
            raise CacheMissException

    def cache_page(self, page):
        f = open(self._cache_path(), 'w')
        f.write(page)
        f.close()

    def _comment_divs(self):
        """Yield a sequence of <div> elements that contain comments on this bug.
        """
        table = self.soup.find(class_="bz_comment_table")
        if table is None:
            self.dump()
            raise NoCommentsException(self.id)
        return table.find_all("div", class_=["bz_comment", "bz_comment bz_first_comment"])

    def comments(self):
        """Yield a sequence of BugComment objects for this page.
        """
        for div in self._comment_divs():
            yield BugComment.from_div(div)
        
    def _parse_attr(self, res):
        """Given the results of one of our att_ methods, parse it into
        a useful string. Our methods may return a string, or a soup element
        containing a string.
        """
        # For vector fields we separate by semicolons
        if isinstance(res, list):
            return ';'.join(res)
        
        if isinstance(res, Soup.element.PageElement):
            if res.string is not None:
                att = res.string
            else:
                att = res.contents[0].string
        elif not isinstance(res, str):
            att = unicode(res)
        else:
            att = res
            
        return ' '.join(att.split())

    def dump(self, fname="bugpage_dump.html"):
        print "Writing bug page to " + fname
        f = utils.open_log_file(fname)
        f.write(str(self.soup))
        f.close()
        
        
    def att_bug(self):
        return self.id
        
    def att_status(self):
        return self.soup.find(id="static_bug_status")
        
    def att_reported(self):
    # <td id="bz_show_bug_column_2" class="bz_show_bug_column_table" valign="top">
        tbody = self.soup.find(id="bz_show_bug_column_2").table
        reported_td = tbody.contents[1].contents[3]
        return reported_td.contents[0].split()[0]
        
    def att_product(self):
        return self.soup.find(id="field_container_product")
        
    def att_component(self):
        return self.soup.find(id="field_container_component")
        
    def att_version(self):
        # This navigation is witchcraft. Have to hop over whitespace in unexpected places.
        return self.soup.find(id="field_container_component").parent.next_sibling.next_sibling.contents[3]
        
    def att_platform(self):
        #
        return self.att_version().parent.next_sibling.next_sibling.contents[3]
        
    def att_importance(self):
        if self.soup.find(id="votes_container"):
            return self.soup.find(id="votes_container").previous_sibling
        else:
            return next_tag(self.soup.find("label", accesskey="i").parent)
        
    def att_resolved(self):
        for event in self.history.events:
            if event.added.upper() == 'RESOLVED':
                return event.when
        return ''
        
    def att_verified(self):
        for event in self.history.events:
            if event.added.upper() == 'VERIFIED':
                return event.when
        return ''
        
    def att_keywds(self):
        keywds = self.soup.find("label", accesskey="k").parent.next_sibling.next_sibling.string
        keywds = [keywd.strip() for keywd in keywds.split(',')]
        return keywds
        
        
    def att_n_keywds(self):
        return len(self.att_keywds())
        
    def att_voters(self):
        # Need to scrape votes page
        return self.votes.voters
        
    def att_nvoters(self):
        return len(self.votes.voters)
        
    def att_assigned_to(self):
        # Note to self: How to get this changes depending on whether or not we're logged in!
        # If you get a mysterious error from this method, that's probably why. I'm assuming the
        # logged-in version of the page right now.

        #return self.soup.find("a", href="page.cgi?id=fields.html#assigned_to").parent.parent.next_sibling.next_sibling.span.span
        return self.soup.find("a", href="page.cgi?id=fields.html#assigned_to").parent.parent.next_sibling.next_sibling.span.a['title']


        #label = self.soup.find("a", href="page.cgi?id=fields.html#assigned_to").parent
        #value = next_tag(label).span.text
        
    def att_duplicates(self):
        dupe_element = self.soup.find(id="duplicates")
        if dupe_element is None:
            return []
        url = next_tag(dupe_element)["href"]
        dupes = url[url.index("id=")+3:].split(',')
        return dupes
        
    def att_n_duplicates(self):
        return len(self.att_duplicates())
        
    def att_depends_on(self):
        dependents = []
        for dependent in next_tags(self.soup.find(id="dependson_input_area")):
            dependents.append(dependent.string)
        return dependents
            
        
    def att_n_depends(self):
        return len(self.att_depends_on())
        
    def att_blocks(self):
        block_cands = self.soup.find(id="blocked_input_area").next_siblings
        block_ids = [cand.string for cand in block_cands if not isinstance(cand, Soup.NavigableString)]
        return block_ids
        
    def att_nblocks(self):
        return len(self.att_blocks())
        
    def att_cc_list(self):
        cclistdisplay = self.soup.find("ul", "cc_list_display")
        cc_list = []
        if cclistdisplay is None:
            return []
        for child in cclistdisplay.contents:
            if isinstance(child, Soup.NavigableString) or child.name != "li":
                continue
            cc_list.append(child.string)
            
        return cc_list
        
    def att_n_cc_list(self):
        return len(self.att_cc_list())
        
    def att_history(self):
        """List of actors involved in this bugs history.
        """
        return self.history.actors()
        
    def att_n_history(self):
        return len(self.att_history())
        
    def to_row(self):
        attr_methods = sorted([methname for methname in dir(self) if methname.startswith("att_")])
        values = []
        for methname in attr_methods:
            res = getattr(self, methname)()
            values.append(self._parse_attr(res))
                
        return values
        
    @classmethod
    def header(kls):
        attr_methods = sorted([methname[4:] for methname in dir(kls) if methname.startswith("att_")])
        return attr_methods


class BugComment(object):

    def __init__(self, author, date):
        self.author = author
        self.date = date

    @classmethod
    def from_div(cls, div):
        userstring = div.find(class_="bz_comment_user").span.a['title']
        assert userstring is not None, div.find(class_="bz_comment_user").span.a
        user = debuggers.Debugger.parse_assignedto_str(userstring)
        timestring = div.find(class_="bz_comment_time").string
        date = utils.datify(timestring)
        return BugComment(user, date)


def write_bug_summaries(id_fname, out_fname):
    fout = open(out_fname, 'w')
    summ_out = utils.UnicodeWriter(fout)
    summ_out.writerow(BugPage.header())

    f = open(id_fname)
    bug_numbers = [int(line.strip()) for line in f]
    f.close()

    niters = 0
    forbidden_skipped = 0
    nohistory_skipped = 0
    for bugno in bug_numbers:
        time.sleep(0.5)
        try:
            bug = BugPage(bugno)
            summ_out.writerow(bug.to_row())
        except ForbiddenBugException:
            forbidden_skipped += 1
        except EmptyHistoryException:
            pass
        except Exception as e:
            print "Error in bug with id %d" % (bugno)
            raise
        niters += 1

        if (niters % NOTIFICATION_INTERVAL) == 0:
            print "%s: Done %d bugs" % (time.asctime(),niters)

    print "Skipped %d forbidden bugs" % (forbidden_skipped)
    print "Skipped %d bugs with no history" % (nohistory_skipped)
    fout.close()

def add_missing_bugs(fname, session):
    """Oneshot code to add the 27 bugs that were missed originally to the DB.
    """

        
if __name__ == '__main__':
    # TODO: Refactor this. Runnable modules in package antipattern etc.
    parser = OptionParser()
    parser.add_option("-t",
        "--test",
        dest="test",
        action="store_true",
        help="use small test set of bugs",
    )
    parser.add_option("-n",
        "--max-iterations",
        dest="n",
        type=int,
        default=0,
        help="run only n iterations at most",
    )
    parser.add_option(
        "--bug-file",
        dest="bug_file",
        default='mentioned_bug_ids.txt',
        help="file with the bug ids to scrape",
    )
    (options, args) = parser.parse_args()


    summ_out = utils.UnicodeWriter(open('bug_summary2.csv', 'w'))
    hist_out = utils.UnicodeWriter(open('bug_history2.csv', 'w'))
    summ_out.writerow(BugPage.header())
    hist_out.writerow(BugHistoryEvent.header())
    
    if options.test:
        bug_numbers = [588, 386473]
    else:
        f = open(options.bug_file)
        bug_numbers = [int(line.strip()) for line in f]
        f.close()
        
    niters = 0
    forbidden_skipped = 0
    nohistory_skipped = 0
    for bugno in bug_numbers:
        time.sleep(0.5)
        try:
            bug = BugPage(bugno)
            summ_out.writerow(bug.to_row())
            for event in bug.history:
                hist_out.writerow(event.to_row())
        except ForbiddenBugException:
            forbidden_skipped += 1
        except EmptyHistoryException:
            nohistory_skipped += 1
        except Exception as e:
            print "Error in bug with id %d" % (bugno)
            raise
        niters += 1
        if options.n and niters > options.n:
            break
            
        if (niters % NOTIFICATION_INTERVAL) == 0:
            print "%s: Done %d bugs" % (time.asctime(),niters)
            
    print "Skipped %d forbidden bugs" % (forbidden_skipped)
    print "Skipped %d bugs with no history" % (nohistory_skipped)
        
        
                
        
    
