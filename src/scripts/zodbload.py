#!python
##############################################################################
#
# Copyright (c) 2003 Zope Corporation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.0 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Test script for testing ZODB under a heavy zope-like load.

Note that, to be as realistic as possible with ZEO, you should run this
script multiple times, to simulate multiple clients.

Here's how this works.

The script starts some number of threads.  Each thread, sequentially
executes jobs.  There is a job producer that produces jobs.

Input data are provided by a mail producer that hands out message from
a mailbox.

Execution continues until there is an error, which will normally occur
when the mailbox is exhausted.

Command-line options are used to provide job definitions. Job
definitions have perameters of the form name=value.  Jobs have 2
standard parameters:

  frequency=integer

     The frequency of the job. The default is 1.

  sleep=float

     The number os seconds to sleep before performing the job. The
     default is 0.

Usage: loadmail2 [options]

  Options:

    -edit [frequency=integer] [sleep=float]

       Define an edit job. An edit job edits a random already-saved
       email message, deleting and inserting a random number of words.

       After editing the message, the message is (re)cataloged.

    -insert [number=int] [frequency=integer] [sleep=float]

       Insert some number of email messages.

    -index [number=int] [frequency=integer] [sleep=float]

       Insert and index (catalog) some number of email messages.

    -search [terms='word1 word2 ...'] [frequency=integer] [sleep=float]

       Search the catalog. A query is givem with one or more terms as
       would be entered into a typical seach box.  If no query is
       given, then queries will be randomly selected based on a set of
       built-in word list.

    -setup

       Set up the database. This will delete any existing Data.fs
       file.  (Of course, this may have no effect, if there is a
       custom_zodb that defined a different storage.) It also adds a
       mail folder and a catalog.

    -options file

       Read options from the given file. Th efile should be a python
       source file that defines a sequence of options named 'options'.

    -threads n

       Specify the number of threads to execute. If not specified (< 2),
       then jobs are run in a single (main) thread.

    -mbox filename

       Specify the mailbox for getting input data.

$Id: zodbload.py,v 1.4 2003/11/19 15:36:31 jeremy Exp $
"""

import mailbox
import math
import os
import random
import re
import sys
import threading
import time

class JobProducer:

    def __init__(self):
        self.jobs = []

    def add(self, callable, frequency, sleep, repeatp=0):
        self.jobs.extend([(callable, sleep, repeatp)] * int(frequency))
        random.shuffle(self.jobs)

    def next(self):
        factory, sleep, repeatp = random.choice(self.jobs)
        time.sleep(sleep)
        callable, args = factory.create()
        return factory, callable, args, repeatp

    def __nonzero__(self):
        return not not self.jobs



class MBox:

    def __init__(self, filename):
        if ' ' in filename:
            filename, min, max = filename.split()
            min = int(min)
            max = int(max)
        else:
            min = max = 0

        if filename.endswith('.bz2'):
            f = os.popen("bunzip2 <"+filename, 'r')
            filename = filename[-4:]
        else:
            f = open(filename)

        self._mbox = mb = mailbox.UnixMailbox(f)

        self.number = min
        while min:
            mb.next()
            min -= 1

        self._lock = threading.Lock()
        self.__name__ = os.path.splitext(os.path.split(filename)[1])[0]
        self._max = max

    def next(self):
        self._lock.acquire()
        try:
            if self._max > 0 and self.number >= self._max:
                raise IndexError(self.number + 1)
            message = self._mbox.next()
            message.body = message.fp.read()
            message.headers = list(message.headers)
            self.number += 1
            message.number = self.number
            message.mbox = self.__name__
            return message
        finally:
            self._lock.release()

bins = 9973
#bins = 11
def mailfolder(app, mboxname, number):
    mail = getattr(app, mboxname, None)
    if mail is None:
        app.manage_addFolder(mboxname)
        mail = getattr(app, mboxname)
        from BTrees.Length import Length
        mail.length = Length()
        for i in range(bins):
            mail.manage_addFolder('b'+str(i))
    bin = hash(str(number))%bins
    return getattr(mail, 'b'+str(bin))


def VmSize():

    try:
        f = open('/proc/%s/status' % os.getpid())
    except:
        return 0
    else:
        l = filter(lambda l: l[:7] == 'VmSize:', f.readlines())
        if l:
            l = l[0][7:].strip().split()[0]
            return int(l)
    return 0

def setup(lib_python):
    try:
        os.remove(os.path.join(lib_python, '..', '..', 'var', 'Data.fs'))
    except:
        pass
    import Zope
    import Products
    import AccessControl.SecurityManagement
    app=Zope.app()

    Products.ZCatalog.ZCatalog.manage_addZCatalog(app, 'cat', '')

    from Products.ZCTextIndex.ZCTextIndex import PLexicon
    from Products.ZCTextIndex.Lexicon import Splitter, CaseNormalizer

    app.cat._setObject('lex',
                       PLexicon('lex', '', Splitter(), CaseNormalizer())
                       )

    class extra:
        doc_attr = 'PrincipiaSearchSource'
        lexicon_id = 'lex'
        index_type = 'Okapi BM25 Rank'

    app.cat.addIndex('PrincipiaSearchSource', 'ZCTextIndex', extra)

    get_transaction().commit()

    system = AccessControl.SpecialUsers.system
    AccessControl.SecurityManagement.newSecurityManager(None, system)

    app._p_jar.close()

def do(db, f, args):
    """Do something in a transaction, retrying of necessary

    Measure the speed of both the compurartion and the commit
    """
    from ZODB.POSException import ConflictError
    wcomp = ccomp = wcommit = ccommit = 0.0
    rconflicts = wconflicts = 0
    start = time.time()

    while 1:
        connection = db.open()
        try:
            get_transaction().begin()
            t=time.time()
            c=time.clock()
            try:
                try:
                    r = f(connection, *args)
                except ConflictError:
                    rconflicts += 1
                    get_transaction().abort()
                    continue
            finally:
                wcomp += time.time() - t
                ccomp += time.clock() - c

            t=time.time()
            c=time.clock()
            try:
                try:
                    get_transaction().commit()
                    break
                except ConflictError:
                    wconflicts += 1
                    get_transaction().abort()
                    continue
            finally:
                wcommit += time.time() - t
                ccommit += time.clock() - c
        finally:
            connection.close()

    return start, wcomp, ccomp, rconflicts, wconflicts, wcommit, ccommit, r

def run1(tid, db, factory, job, args):
    (start, wcomp, ccomp, rconflicts, wconflicts, wcommit, ccommit, r
     ) = do(db, job, args)
    start = "%.4d-%.2d-%.2d %.2d:%.2d:%.2d" % time.localtime(start)[:6]
    print "%s %s %8.3g %8.3g %s %s\t%8.3g %8.3g %s %r" % (
        start, tid, wcomp, ccomp, rconflicts, wconflicts, wcommit, ccommit,
        factory.__name__, r)

def run(jobs, tid=''):
    import Zope
    while 1:
        factory, job, args, repeatp = jobs.next()
        run1(tid, Zope.DB, factory, job, args)
        if repeatp:
            while 1:
                i = random.randint(0,100)
                if i > repeatp:
                    break
                run1(tid, Zope.DB, factory, job, args)


def index(connection, messages, catalog):
    app = connection.root()['Application']
    for message in messages:
        mail = mailfolder(app, message.mbox, message.number)
        docid = 'm'+str(message.number)
        mail.manage_addDTMLDocument(docid, file=message.body)

        # increment counted
        getattr(app, message.mbox).length.change(1)

        doc = mail[docid]
        for h in message.headers:
            h = h.strip()
            l = h.find(':')
            if l <= 0:
                continue
            name = h[:l].lower()
            if name=='subject':
                name='title'
            v = h[l+1:].strip()
            type='string'

            if name=='title':
                doc.manage_changeProperties(title=h)
            else:
                try:
                    doc.manage_addProperty(name, v, type)
                except:
                    pass
        if catalog:
            app.cat.catalog_object(doc)

    return message.number

class IndexJob:
    needs_mbox = 1
    catalog = 1
    prefix = 'index'

    def __init__(self, mbox, number=1):
        self.__name__ = "%s%s_%s" % (self.prefix, number, mbox.__name__)
        self.mbox, self.number = mbox, int(number)

    def create(self):
        messages = [self.mbox.next() for i in range(self.number)]
        return index, (messages, self.catalog)


class InsertJob(IndexJob):
    catalog = 0
    prefix = 'insert'

wordre = re.compile(r'(\w{3,20})')
stop = 'and', 'not'
def edit(connection, mbox, catalog=1):
    app = connection.root()['Application']
    mail = getattr(app, mbox.__name__, None)
    if mail is None:
        time.sleep(1)
        return "No mailbox %s" % mbox.__name__

    nmessages = mail.length()
    if nmessages < 2:
        time.sleep(1)
        return "No messages to edit in %s" % mbox.__name__

    # find a message to edit:
    while 1:
        number = random.randint(1, nmessages-1)
        did = 'm' + str(number)

        mail = mailfolder(app, mbox.__name__, number)
        doc = getattr(mail, did, None)
        if doc is not None:
            break

    text = doc.raw.split()
    norig = len(text)
    if norig > 10:
        ndel = int(math.exp(random.randint(0, int(math.log(norig)))))
        nins = int(math.exp(random.randint(0, int(math.log(norig)))))
    else:
        ndel = 0
        nins = 10

    for j in range(ndel):
        j = random.randint(0,len(text)-1)
        word = text[j]
        m = wordre.search(word)
        if m:
            word = m.group(1).lower()
            if (not wordsd.has_key(word)) and word not in stop:
                words.append(word)
                wordsd[word] = 1
        del text[j]

    for j in range(nins):
        word = random.choice(words)
        text.append(word)

    doc.raw = ' '.join(text)

    if catalog:
        app.cat.catalog_object(doc)

    return norig, ndel, nins

class EditJob:
    needs_mbox = 1
    prefix = 'edit'
    catalog = 1

    def __init__(self, mbox):
        self.__name__ = "%s_%s" % (self.prefix, mbox.__name__)
        self.mbox = mbox

    def create(self):
        return edit, (self.mbox, self.catalog)

class ModifyJob(EditJob):
    prefix = 'modify'
    catalog = 0


def search(connection, terms, number):
    app = connection.root()['Application']
    cat = app.cat
    n = 0

    for i in number:
        term = random.choice(terms)

        results = cat(PrincipiaSearchSource=term)
        n += len(results)
        for result in results:
            obj = result.getObject()
            # Apparently, there is a bug in Zope that leads obj to be None
            # on occasion.
            if obj is not None:
                obj.getId()

    return n

class SearchJob:

    def __init__(self, terms='', number=10):

        if terms:
            terms = terms.split()
            self.__name__ = "search_" + '_'.join(terms)
            self.terms = terms
        else:
            self.__name__ = 'search'
            self.terms = words

        number = min(int(number), len(self.terms))
        self.number = range(number)

    def create(self):
        return search, (self.terms, self.number)


words=['banishment', 'indirectly', 'imprecise', 'peeks',
'opportunely', 'bribe', 'sufficiently', 'Occidentalized', 'elapsing',
'fermenting', 'listen', 'orphanage', 'younger', 'draperies', 'Ida',
'cuttlefish', 'mastermind', 'Michaels', 'populations', 'lent',
'cater', 'attentional', 'hastiness', 'dragnet', 'mangling',
'scabbards', 'princely', 'star', 'repeat', 'deviation', 'agers',
'fix', 'digital', 'ambitious', 'transit', 'jeeps', 'lighted',
'Prussianizations', 'Kickapoo', 'virtual', 'Andrew', 'generally',
'boatsman', 'amounts', 'promulgation', 'Malay', 'savaging',
'courtesan', 'nursed', 'hungered', 'shiningly', 'ship', 'presides',
'Parke', 'moderns', 'Jonas', 'unenlightening', 'dearth', 'deer',
'domesticates', 'recognize', 'gong', 'penetrating', 'dependents',
'unusually', 'complications', 'Dennis', 'imbalances', 'nightgown',
'attached', 'testaments', 'congresswoman', 'circuits', 'bumpers',
'braver', 'Boreas', 'hauled', 'Howe', 'seethed', 'cult', 'numismatic',
'vitality', 'differences', 'collapsed', 'Sandburg', 'inches', 'head',
'rhythmic', 'opponent', 'blanketer', 'attorneys', 'hen', 'spies',
'indispensably', 'clinical', 'redirection', 'submit', 'catalysts',
'councilwoman', 'kills', 'topologies', 'noxious', 'exactions',
'dashers', 'balanced', 'slider', 'cancerous', 'bathtubs', 'legged',
'respectably', 'crochets', 'absenteeism', 'arcsine', 'facility',
'cleaners', 'bobwhite', 'Hawkins', 'stockade', 'provisional',
'tenants', 'forearms', 'Knowlton', 'commit', 'scornful',
'pediatrician', 'greets', 'clenches', 'trowels', 'accepts',
'Carboloy', 'Glenn', 'Leigh', 'enroll', 'Madison', 'Macon', 'oiling',
'entertainingly', 'super', 'propositional', 'pliers', 'beneficiary',
'hospitable', 'emigration', 'sift', 'sensor', 'reserved',
'colonization', 'shrilled', 'momentously', 'stevedore', 'Shanghaiing',
'schoolmasters', 'shaken', 'biology', 'inclination', 'immoderate',
'stem', 'allegory', 'economical', 'daytime', 'Newell', 'Moscow',
'archeology', 'ported', 'scandals', 'Blackfoot', 'leery', 'kilobit',
'empire', 'obliviousness', 'productions', 'sacrificed', 'ideals',
'enrolling', 'certainties', 'Capsicum', 'Brookdale', 'Markism',
'unkind', 'dyers', 'legislates', 'grotesquely', 'megawords',
'arbitrary', 'laughing', 'wildcats', 'thrower', 'sex', 'devils',
'Wehr', 'ablates', 'consume', 'gossips', 'doorways', 'Shari',
'advanced', 'enumerable', 'existentially', 'stunt', 'auctioneers',
'scheduler', 'blanching', 'petulance', 'perceptibly', 'vapors',
'progressed', 'rains', 'intercom', 'emergency', 'increased',
'fluctuating', 'Krishna', 'silken', 'reformed', 'transformation',
'easter', 'fares', 'comprehensible', 'trespasses', 'hallmark',
'tormenter', 'breastworks', 'brassiere', 'bladders', 'civet', 'death',
'transformer', 'tolerably', 'bugle', 'clergy', 'mantels', 'satin',
'Boswellizes', 'Bloomington', 'notifier', 'Filippo', 'circling',
'unassigned', 'dumbness', 'sentries', 'representativeness', 'souped',
'Klux', 'Kingstown', 'gerund', 'Russell', 'splices', 'bellow',
'bandies', 'beefers', 'cameramen', 'appalled', 'Ionian', 'butterball',
'Portland', 'pleaded', 'admiringly', 'pricks', 'hearty', 'corer',
'deliverable', 'accountably', 'mentors', 'accorded',
'acknowledgement', 'Lawrenceville', 'morphology', 'eucalyptus',
'Rena', 'enchanting', 'tighter', 'scholars', 'graduations', 'edges',
'Latinization', 'proficiency', 'monolithic', 'parenthesizing', 'defy',
'shames', 'enjoyment', 'Purdue', 'disagrees', 'barefoot', 'maims',
'flabbergast', 'dishonorable', 'interpolation', 'fanatics', 'dickens',
'abysses', 'adverse', 'components', 'bowl', 'belong', 'Pipestone',
'trainees', 'paw', 'pigtail', 'feed', 'whore', 'conditioner',
'Volstead', 'voices', 'strain', 'inhabits', 'Edwin', 'discourses',
'deigns', 'cruiser', 'biconvex', 'biking', 'depreciation', 'Harrison',
'Persian', 'stunning', 'agar', 'rope', 'wagoner', 'elections',
'reticulately', 'Cruz', 'pulpits', 'wilt', 'peels', 'plants',
'administerings', 'deepen', 'rubs', 'hence', 'dissension', 'implored',
'bereavement', 'abyss', 'Pennsylvania', 'benevolent', 'corresponding',
'Poseidon', 'inactive', 'butchers', 'Mach', 'woke', 'loading',
'utilizing', 'Hoosier', 'undo', 'Semitization', 'trigger', 'Mouthe',
'mark', 'disgracefully', 'copier', 'futility', 'gondola', 'algebraic',
'lecturers', 'sponged', 'instigators', 'looted', 'ether', 'trust',
'feeblest', 'sequencer', 'disjointness', 'congresses', 'Vicksburg',
'incompatibilities', 'commend', 'Luxembourg', 'reticulation',
'instructively', 'reconstructs', 'bricks', 'attache', 'Englishman',
'provocation', 'roughen', 'cynic', 'plugged', 'scrawls', 'antipode',
'injected', 'Daedalus', 'Burnsides', 'asker', 'confronter',
'merriment', 'disdain', 'thicket', 'stinker', 'great', 'tiers',
'oust', 'antipodes', 'Macintosh', 'tented', 'packages',
'Mediterraneanize', 'hurts', 'orthodontist', 'seeder', 'readying',
'babying', 'Florida', 'Sri', 'buckets', 'complementary',
'cartographer', 'chateaus', 'shaves', 'thinkable', 'Tehran',
'Gordian', 'Angles', 'arguable', 'bureau', 'smallest', 'fans',
'navigated', 'dipole', 'bootleg', 'distinctive', 'minimization',
'absorbed', 'surmised', 'Malawi', 'absorbent', 'close', 'conciseness',
'hopefully', 'declares', 'descent', 'trick', 'portend', 'unable',
'mildly', 'Morse', 'reference', 'scours', 'Caribbean', 'battlers',
'astringency', 'likelier', 'Byronizes', 'econometric', 'grad',
'steak', 'Austrian', 'ban', 'voting', 'Darlington', 'bison', 'Cetus',
'proclaim', 'Gilbertson', 'evictions', 'submittal', 'bearings',
'Gothicizer', 'settings', 'McMahon', 'densities', 'determinants',
'period', 'DeKastere', 'swindle', 'promptness', 'enablers', 'wordy',
'during', 'tables', 'responder', 'baffle', 'phosgene', 'muttering',
'limiters', 'custodian', 'prevented', 'Stouffer', 'waltz', 'Videotex',
'brainstorms', 'alcoholism', 'jab', 'shouldering', 'screening',
'explicitly', 'earner', 'commandment', 'French', 'scrutinizing',
'Gemma', 'capacitive', 'sheriff', 'herbivore', 'Betsey', 'Formosa',
'scorcher', 'font', 'damming', 'soldiers', 'flack', 'Marks',
'unlinking', 'serenely', 'rotating', 'converge', 'celebrities',
'unassailable', 'bawling', 'wording', 'silencing', 'scotch',
'coincided', 'masochists', 'graphs', 'pernicious', 'disease',
'depreciates', 'later', 'torus', 'interject', 'mutated', 'causer',
'messy', 'Bechtel', 'redundantly', 'profoundest', 'autopsy',
'philosophic', 'iterate', 'Poisson', 'horridly', 'silversmith',
'millennium', 'plunder', 'salmon', 'missioner', 'advances', 'provers',
'earthliness', 'manor', 'resurrectors', 'Dahl', 'canto', 'gangrene',
'gabler', 'ashore', 'frictionless', 'expansionism', 'emphasis',
'preservations', 'Duane', 'descend', 'isolated', 'firmware',
'dynamites', 'scrawled', 'cavemen', 'ponder', 'prosperity', 'squaw',
'vulnerable', 'opthalmic', 'Simms', 'unite', 'totallers', 'Waring',
'enforced', 'bridge', 'collecting', 'sublime', 'Moore', 'gobble',
'criticizes', 'daydreams', 'sedate', 'apples', 'Concordia',
'subsequence', 'distill', 'Allan', 'seizure', 'Isadore', 'Lancashire',
'spacings', 'corresponded', 'hobble', 'Boonton', 'genuineness',
'artifact', 'gratuities', 'interviewee', 'Vladimir', 'mailable',
'Bini', 'Kowalewski', 'interprets', 'bereave', 'evacuated', 'friend',
'tourists', 'crunched', 'soothsayer', 'fleetly', 'Romanizations',
'Medicaid', 'persevering', 'flimsy', 'doomsday', 'trillion',
'carcasses', 'guess', 'seersucker', 'ripping', 'affliction',
'wildest', 'spokes', 'sheaths', 'procreate', 'rusticates', 'Schapiro',
'thereafter', 'mistakenly', 'shelf', 'ruination', 'bushel',
'assuredly', 'corrupting', 'federation', 'portmanteau', 'wading',
'incendiary', 'thing', 'wanderers', 'messages', 'Paso', 'reexamined',
'freeings', 'denture', 'potting', 'disturber', 'laborer', 'comrade',
'intercommunicating', 'Pelham', 'reproach', 'Fenton', 'Alva', 'oasis',
'attending', 'cockpit', 'scout', 'Jude', 'gagging', 'jailed',
'crustaceans', 'dirt', 'exquisitely', 'Internet', 'blocker', 'smock',
'Troutman', 'neighboring', 'surprise', 'midscale', 'impart',
'badgering', 'fountain', 'Essen', 'societies', 'redresses',
'afterwards', 'puckering', 'silks', 'Blakey', 'sequel', 'greet',
'basements', 'Aubrey', 'helmsman', 'album', 'wheelers', 'easternmost',
'flock', 'ambassadors', 'astatine', 'supplant', 'gird', 'clockwork',
'foxes', 'rerouting', 'divisional', 'bends', 'spacer',
'physiologically', 'exquisite', 'concerts', 'unbridled', 'crossing',
'rock', 'leatherneck', 'Fortescue', 'reloading', 'Laramie', 'Tim',
'forlorn', 'revert', 'scarcer', 'spigot', 'equality', 'paranormal',
'aggrieves', 'pegs', 'committeewomen', 'documented', 'interrupt',
'emerald', 'Battelle', 'reconverted', 'anticipated', 'prejudices',
'drowsiness', 'trivialities', 'food', 'blackberries', 'Cyclades',
'tourist', 'branching', 'nugget', 'Asilomar', 'repairmen', 'Cowan',
'receptacles', 'nobler', 'Nebraskan', 'territorial', 'chickadee',
'bedbug', 'darted', 'vigilance', 'Octavia', 'summands', 'policemen',
'twirls', 'style', 'outlawing', 'specifiable', 'pang', 'Orpheus',
'epigram', 'Babel', 'butyrate', 'wishing', 'fiendish', 'accentuate',
'much', 'pulsed', 'adorned', 'arbiters', 'counted', 'Afrikaner',
'parameterizes', 'agenda', 'Americanism', 'referenda', 'derived',
'liquidity', 'trembling', 'lordly', 'Agway', 'Dillon', 'propellers',
'statement', 'stickiest', 'thankfully', 'autograph', 'parallel',
'impulse', 'Hamey', 'stylistic', 'disproved', 'inquirer', 'hoisting',
'residues', 'variant', 'colonials', 'dequeued', 'especial', 'Samoa',
'Polaris', 'dismisses', 'surpasses', 'prognosis', 'urinates',
'leaguers', 'ostriches', 'calculative', 'digested', 'divided',
'reconfigurer', 'Lakewood', 'illegalities', 'redundancy',
'approachability', 'masterly', 'cookery', 'crystallized', 'Dunham',
'exclaims', 'mainline', 'Australianizes', 'nationhood', 'pusher',
'ushers', 'paranoia', 'workstations', 'radiance', 'impedes',
'Minotaur', 'cataloging', 'bites', 'fashioning', 'Alsop', 'servants',
'Onondaga', 'paragraph', 'leadings', 'clients', 'Latrobe',
'Cornwallis', 'excitingly', 'calorimetric', 'savior', 'tandem',
'antibiotics', 'excuse', 'brushy', 'selfish', 'naive', 'becomes',
'towers', 'popularizes', 'engender', 'introducing', 'possession',
'slaughtered', 'marginally', 'Packards', 'parabola', 'utopia',
'automata', 'deterrent', 'chocolates', 'objectives', 'clannish',
'aspirin', 'ferociousness', 'primarily', 'armpit', 'handfuls',
'dangle', 'Manila', 'enlivened', 'decrease', 'phylum', 'hardy',
'objectively', 'baskets', 'chaired', 'Sepoy', 'deputy', 'blizzard',
'shootings', 'breathtaking', 'sticking', 'initials', 'epitomized',
'Forrest', 'cellular', 'amatory', 'radioed', 'horrified', 'Neva',
'simultaneous', 'delimiter', 'expulsion', 'Himmler', 'contradiction',
'Remus', 'Franklinizations', 'luggage', 'moisture', 'Jews',
'comptroller', 'brevity', 'contradictions', 'Ohio', 'active',
'babysit', 'China', 'youngest', 'superstition', 'clawing', 'raccoons',
'chose', 'shoreline', 'helmets', 'Jeffersonian', 'papered',
'kindergarten', 'reply', 'succinct', 'split', 'wriggle', 'suitcases',
'nonce', 'grinders', 'anthem', 'showcase', 'maimed', 'blue', 'obeys',
'unreported', 'perusing', 'recalculate', 'rancher', 'demonic',
'Lilliputianize', 'approximation', 'repents', 'yellowness',
'irritates', 'Ferber', 'flashlights', 'booty', 'Neanderthal',
'someday', 'foregoes', 'lingering', 'cloudiness', 'guy', 'consumer',
'Berkowitz', 'relics', 'interpolating', 'reappearing', 'advisements',
'Nolan', 'turrets', 'skeletal', 'skills', 'mammas', 'Winsett',
'wheelings', 'stiffen', 'monkeys', 'plainness', 'braziers', 'Leary',
'advisee', 'jack', 'verb', 'reinterpret', 'geometrical', 'trolleys',
'arboreal', 'overpowered', 'Cuzco', 'poetical', 'admirations',
'Hobbes', 'phonemes', 'Newsweek', 'agitator', 'finally', 'prophets',
'environment', 'easterners', 'precomputed', 'faults', 'rankly',
'swallowing', 'crawl', 'trolley', 'spreading', 'resourceful', 'go',
'demandingly', 'broader', 'spiders', 'Marsha', 'debris', 'operates',
'Dundee', 'alleles', 'crunchier', 'quizzical', 'hanging', 'Fisk']

wordsd = {}
for word in words:
    wordsd[word] = 1


def collect_options(args, jobs, options):

    while args:
        arg = args.pop(0)
        if arg.startswith('-'):
            name = arg[1:]
            if name == 'options':
                fname = args.pop(0)
                d = {}
                execfile(fname, d)
                collect_options(list(d['options']), jobs, options)
            elif options.has_key(name):
                v = args.pop(0)
                if options[name] != None:
                    raise ValueError(
                        "Duplicate values for %s, %s and %s"
                        % (name, v, options[name])
                        )
                options[name] = v
            elif name == 'setup':
                options['setup'] = 1
            elif globals().has_key(name.capitalize()+'Job'):
                job = name
                kw = {}
                while args and args[0].find("=") > 0:
                    arg = args.pop(0).split('=')
                    name, v = arg[0], '='.join(arg[1:])
                    if kw.has_key(name):
                        raise ValueError(
                            "Duplicate parameter %s for job %s"
                            % (name, job)
                            )
                    kw[name]=v
                if kw.has_key('frequency'):
                    frequency = kw['frequency']
                    del kw['frequency']
                else:
                    frequency = 1

                if kw.has_key('sleep'):
                    sleep = float(kw['sleep'])
                    del kw['sleep']
                else:
                    sleep = 0.0001

                if kw.has_key('repeat'):
                    repeatp = float(kw['repeat'])
                    del kw['repeat']
                else:
                    repeatp = 0

                jobs.append((job, kw, frequency, sleep, repeatp))
            else:
                raise ValueError("not an option or job", name)
        else:
            raise ValueError("Expected an option", arg)


def find_lib_python():
    for b in os.getcwd(), os.path.split(sys.argv[0])[0]:
        for i in range(6):
            d = ['..']*i + ['lib', 'python']
            p = os.path.join(b, *d)
            if os.path.isdir(p):
                return p
    raise ValueError("Couldn't find lib/python")

def main(args=None):
    lib_python = find_lib_python()
    sys.path.insert(0, lib_python)

    if args is None:
        args = sys.argv[1:]
    if not args:
        print __doc__
        sys.exit(0)

    print args
    random.seed(hash(tuple(args))) # always use the same for the given args

    options = {"mbox": None, "threads": None}
    jobdefs = []
    collect_options(args, jobdefs, options)

    mboxes = {}
    if options["mbox"]:
        mboxes[options["mbox"]] = MBox(options["mbox"])

    if options.has_key('setup'):
        setup(lib_python)
    else:
        import Zope
        Zope.startup()

    #from ThreadedAsync.LoopCallback import loop
    #threading.Thread(target=loop, args=(), name='asyncore').start()

    jobs = JobProducer()
    for job, kw, frequency, sleep, repeatp in jobdefs:
        Job = globals()[job.capitalize()+'Job']
        if getattr(Job, 'needs_mbox', 0):
            if not kw.has_key("mbox"):
                if not options["mbox"]:
                    raise ValueError(
                        "no mailbox (mbox option) file  specified")
                kw['mbox'] = mboxes[options["mbox"]]
            else:
                if not mboxes.has_key[kw["mbox"]]:
                    mboxes[kw['mbox']] = MBox[kw['mbox']]
                kw["mbox"] = mboxes[kw['mbox']]
        jobs.add(Job(**kw), frequency, sleep, repeatp)

    if not jobs:
        print "No jobs to execute"
        return

    threads = int(options['threads'] or '0')
    if threads > 1:
        threads = [threading.Thread(target=run, args=(jobs, i), name=str(i))
                   for i in range(threads)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    else:
        run(jobs)


if __name__ == '__main__':
    main()
