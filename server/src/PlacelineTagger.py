#! /usr/local/bin/python3
# -*- coding: utf-8 -*-

from sys import argv
import re

class StateCodeManager:
    def __init__(self, fn):
        self.map_code_to_name = {}
        self.map_name_to_code = {}
        self.loadFile(fn)

    def loadFile(self, fn):
        with open(fn, 'r', encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                fields = line.split('\t')
                if len(fields) != 2:
                    continue
                name = fields[1].strip()
                code0 = fields[0].strip()
                codes = [code0]
                if '.' in code0:
                    # the following are some ad hoc tricks to overcome the irregularities in NL text
                    code1 = code0.replace('.', '. ').strip()
                    code2 = code1.replace('.', '')
                    codes.append(code1)
                    codes.append(code2)
                # note that the full name of a state should also be registered as its code
                codes.append(name)
                for code in codes:
                    if code not in self.map_code_to_name:
                        self.map_code_to_name[code] = name
                    if name not in self.map_name_to_code:
                        self.map_name_to_code[name] = []
                    self.map_name_to_code[name].append(code)
        #print("StateCodeManager::registered {0} names and {1} codes".format(len(self.map_name_to_code), len(self.map_code_to_name)))

    def getStateName(self, state_code):
        return self.map_code_to_name[state_code] if state_code in self.map_code_to_name else None

    def hasState(self, state_code):
        return state_code in self.map_code_to_name
    
    def hasStateName(self, s):
        return s in self.map_name_to_code
    
    def getStateCodes(self, state_name):
        return self.map_name_to_code[state_name] if state_name in self.map_name_to_code else None


class WikiDataManager:
    def __init__(self, fn):
        self.mapping = {}
        self.loadFile(fn)
    
    #essentially this function is about retrieving the 'STATE' info in input string 's'
    def parseHierarchicalInfo(self, s):
        fields = s.split(', ')
        if len(fields) == 2:
            return fields[1].strip()
        else:
            return s.strip()

    def parseAmbiguityInfo(self, s):
        return [self.parseHierarchicalInfo(p) for p in s.split(' ||| ')]

    def loadFile(self, fn):
        with open(fn, 'r', encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                fields = line.split('\t')
                if len(fields) != 2:
                    continue
                loc_name = fields[0].strip()
                ambiguities = self.parseAmbiguityInfo(fields[1])
                self.mapping[loc_name] = ambiguities
        #print("WikiDataManager::registered {0} loc".format(len(self.mapping)))
    
    def hasLocName(self, s):
        return s in self.mapping
    
    # # if the location name 's' itself is registered as (one of) its interpretation
    # # then 's' does not need to be specified with STATE
    # def hasNoNeedForState(self, s):
    #     return s in self.mapping[s]
    
    def isNotAmbiguous(self, s):
        return s in self.mapping and len(self.mapping[s]) == 1

    def getUnambiguousInfo(self, s):
        return self.mapping[s][0] if self.isNotAmbiguous(s) else None

    def getAmbiguities(self, s):
        return self.mapping[s]

    def hasLocNameAndState(self, name, state):
        return name in self.mapping and state in self.mapping[name]


class PlacelineTagger:
    DOC_TOK_MAX_LEN = 120

    @staticmethod
    def capitalizeName(s):
        if len(s) > 1:
            return s[0].upper() + s[1:]
        elif len(s) == 1:
            return s[0].upper()
        else:
            return s

    def __init__(self, state_code_file='state_code_listing.txt', wiki_data_file='wiki_loc_names_1+2.txt'):
        self.state_code_mgr = StateCodeManager(state_code_file)
        self.wiki_data_mgr = WikiDataManager(wiki_data_file)
        self.simple_pattern = re.compile(r"([A-Z\.\ \-]{3,})[,\s]*[(-]")
        self.complete_pattern = re.compile(r"([A-Z\.\ \-]+)[,\s]*$")

    def findSimplePlaceline(self, doc):
        matchObj = re.search(self.simple_pattern, doc)
        if not matchObj:
            return None
        else:
            cand = matchObj.group(1).strip().lower()
            cand_toks = cand.split(' ')
            cand_toks = [PlacelineTagger.capitalizeName(tok) for tok in cand_toks]
            for i in range(len(cand_toks)):
                suffix = ' '.join(cand_toks[i:])
                #tricky case handling:
                if suffix == 'Oh':
                    return None
                if suffix == 'Washington':
                    return 'Washington/District of Columbia'
                if suffix == 'New York':
                    return 'New York/New York'
                if self.state_code_mgr.hasStateName(suffix):
                    return suffix
                elif self.wiki_data_mgr.hasLocName(suffix):
                    unambigous_state = self.wiki_data_mgr.getUnambiguousInfo(suffix)
                    if unambigous_state:
                        return suffix + '/' + unambigous_state
            return None

    def getPrecedingContext(self, toks, i):
        return ' '.join(toks[max(0,i-5):i]) 

    def findCompletePlaceline(self, toks):
        for i,tok in enumerate(toks):
            cand_state_code = tok.strip()
            if len(cand_state_code) == 0:
                continue
            if cand_state_code[-1] == ',':
                cand_state_code = cand_state_code[:-1]
            if self.state_code_mgr.hasState(cand_state_code):
                state_full_name = self.state_code_mgr.getStateName(cand_state_code)
                preceding = self.getPrecedingContext(toks, i)
                if len(preceding) == 0:
                    continue
                matchObj = re.search(self.complete_pattern, preceding)
                if matchObj:
                    cand = matchObj.group(1).strip().lower()
                    cand_toks = cand.split(' ')
                    cand_toks = [PlacelineTagger.capitalizeName(tok) for tok in cand_toks]
                    for i in range(len(cand_toks)):
                        suffix = ' '.join(cand_toks[i:])
                        if self.wiki_data_mgr.hasLocNameAndState(suffix, state_full_name):
                            return suffix + '/' + state_full_name
        return None

    # a Document is simply treated as single 'str' object
    def processDocument(self, doc):
        doc_toks = doc.split(' ')
        # if len(doc_toks) > PlacelineTagger.DOC_TOK_MAX_LEN:
        #     doc_toks = doc_toks[:PlacelineTagger.DOC_TOK_MAX_LEN]
        result = self.findCompletePlaceline(doc_toks)
        if not result:
            result = self.findSimplePlaceline(doc)
        return result



if __name__ == '__main__':
    tagger = PlacelineTagger("./_DATA/state_code_listing.txt", "./_DATA/wiki_loc_names_1+2.txt")
    with open(argv[2], 'w') as fout:
        with open(argv[1], 'r') as f:
            line_cnt = 0
            for line in f:
                line_cnt += 1
                line = line.strip()
                fields = line.split('\t')
                if len(fields) != 4:
                    continue
                ID = fields[0]
                TEXT = fields[3]
                # GROUND_TRUTH = fields[1]
                PLACELINE = tagger.processDocument(TEXT)
                if PLACELINE:
                    fout.write(ID + "\n")
                    #fout.write("{0}\t{1}\t{2}\n".format(line_cnt, PLACELINE, GROUND_TRUTH))