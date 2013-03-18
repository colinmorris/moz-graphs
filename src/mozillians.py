"""Code for dealing with the Mozillians site, an online phonebook tracking the
identities of people who work on Mozilla products. Useful for enriching our DB
with previously unknown connections between e-mails and IRC nicks.

Defines the `mozillians' table in the main DB.
"""

__author__ = 'colin'

from alchemical_base import Base

from sqlalchemy import Column, Integer, String, Unicode, Boolean

from scraping.scrape_utils import *
from utils import canon_name

MOZ_PREFIX = "https://mozillians.org/en-US/"

class Mozillian(Base):
    """A Mozilla developer, as recorded in the Mozilla phonebook.
    """
    __tablename__ = 'mozillians'

    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True)
    email = Column(String, unique=True)
    name = Column(Unicode)
    nick = Column(String, unique=True)
    website = Column(String)
    # An incomplete mozillian comes from a search results page
    # It doesn't include the nick and website fields
    complete = Column(Boolean, default=True)

    def __str__(self):
        return "Moz<%s [%s] [%s] [:%s]>" % (self.id, self.name, self.email, self.nick)

    def flesh_out(self):
        """Fill out the missing information from an incomplete Mozillian.
        """
        clone = self.scrape_mozillian(self.username)
        self._overwrite(clone)

    def _overwrite(self, completemoz):
        """Given a complete version of this incomplete mozillian, fill in the new fields.
        """
        self.nick = completemoz.nick
        self.website = completemoz.nick
        self.complete = True

    @classmethod
    def from_userpage(cls, mozpage, uname):
        """
        Return a Mozillian corresponding to the given user page from the Mozillians
        phonebook.
        """
        #self.uname = uname
        name = mozpage.h2.string.strip()


        fields = ['Email', 'IRC Nickname', 'Website']
        field_to_value = {}
        next_field = None
        for ele in child_tags(mozpage.dl):
            if ele.name == "dt":
                next_field = ele.string.strip()
            elif ele.name == "dd" and next_field in fields:
                val = ele.a.string.strip()
                field_to_value[next_field] = val

        return cls(
            username=uname,
            email=field_to_value.get('Email'),
            name=name,
            nick=field_to_value.get('IRC Nickname'),
            website=field_to_value.get('Website'),
        )

    @classmethod
    def from_search_results(cls, respage):
        """Return a sequence of Mozillians corresponding to the results page of a search
        of the phonebook. Note that these Mozillians will have incomplete information since
        the results page doesn't show every field of interest.
        """
        res = []
        for pane in respage.find_all("div", "details"):
            res.append(cls.from_results_pane(pane))

        return res

    @classmethod
    def from_results_pane(cls, pane):
        lies = pane.ul.find_all("li")
        name = lies[0].h2.string.strip()
        # Then vouched/non-vouched
        email = lies[2].string.split(':')[-1].strip()
        uname = lies[3].string.split(':')[-1].strip()
        return cls(
            username=uname,
            email=email,
            name=name,
            complete=False,
        )

    @classmethod
    def scrape_mozillian(cls, uname):
        profile_url = MOZ_PREFIX + uname
        res = persistent_open(profile_url)
        upage = Soup(res)
        return cls.from_userpage(upage, uname)

    def match(self, field, value):
        """Return whether the given value matches this mozillian on a particular field.
        In the simplest case, we just check for equality. In the case of names, we do
        a bit of mojo to try to remove extraneous bits.
        """
        if field == 'name':
            return self._match_name(value)
        return getattr(self, field) == value

    def _match_name(self, name):
        return self.name == name or canon_name(self.name) == canon_name(name)


    @staticmethod
    def scrape_matching_mozillians(value, field="email"):
        """Try to find a mozillian matching the given value on
        the given field. We return a tuple:

            (mozillians, target)

        mozillians is a sequence of Mozillians representing the mozillians we
        encountered in traversing the search results of our query. If we found
        one matching the given value, they will be target (and also represented
        in mozillians). If we fail to find the target, target is None.
        """
        search_url = 'https://mozillians.org/en-US/search?q=' + urllib.quote(value)
        res = persistent_open(search_url)
        final_url = res.geturl()
        soup = Soup(res)
        target = None
        # There are two distinct possibilities when searching the phonebook

        # Case 1: there's only matching result, and we get taken straight to that userpage
        if '?' not in final_url:
            #uname = final_url.split('/')[-1]
            uname = final_url[len(MOZ_PREFIX):]
            mozillians = [Mozillian.from_userpage(soup, uname)]
            if mozillians[0].match(field, value):
                target = mozillians[0]
            return (mozillians, target)

        # Case 2: there's more than one match, so we get a results page
        mozillians = Mozillian.from_search_results(soup)
        for mozillian in mozillians:
            if mozillian.match(field, value):
                mozillian.flesh_out()
                target = mozillian

        return (mozillians, target)

    @classmethod
    def fetch_lazy_mozillian(cls, field, value, session):
        """Those lazy mozillians...

        If we have a mozillian matching the given e-mail in the DB, return her. Otherwise,
        attempt to scrape her. If we don't find a match, return None.
        """
        assert field == 'email'
        extant = session.query(Mozillian).filter_by({field:value}).first()
        # Case 1: this mozillian is already in the db. Return her (completed if necessary)
        if extant:
            if not extant.complete:
                extant.flesh_out()
            return extant

        # Case 2: not in DB. Scrape to find
        (mozillians, target) = Mozillian.scrape_matching_mozillians(field, value)
        for moz in mozillians:
            #uname_match = self.match_item(moz, session, ['username'])
            uname_match = session.query(Mozillian).filter_by(username = moz.username)
            # If we have a matching mozillian but this one is more complete, then update using this new info
            if uname_match and moz.complete and not uname_match.complete:
                uname_match._overwrite(moz)
                if target is moz:
                    # Also, if this is the target, then return the updated version of the existing moz
                    # rather than the new moz
                    target = uname_match
            # If there exists no matching mozillian, then add this one to the db
            elif not uname_match:
                session.add(moz)

        return target




