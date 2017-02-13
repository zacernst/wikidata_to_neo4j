import gzip
import bz2
import collections
import sys
import json
import argparse


def file_handler(filename):
    if filename is None:
        return None
    if filename.endswith('gz'):
        handler = gzip.open
    elif filename.endswith('.json'):
        handler = open
    elif filename.endswith('.bz2'):
        handler = bz2.BZ2File
    else:
        handler = open
    return handler


def delist_dictionary(some_dict):
    d = {}
    for key, value in some_dict.iteritems():
        if isinstance(value, list):
            for index, item in enumerate(value):
                new_key = '_'.join([key, str(index)])
                d[new_key] = item
        else:
            d[key] = value
    return d


class WikiDataThing(object):
    
    def __init__(self, json_dict):
        assert 'id' in json_dict
        self.cypher_statements = []
        self.wikidata_id = json_dict['id']
        self.non_relational_properties = collections.defaultdict(list)
        self.labels = json_dict['labels']
        self.triples = []
        self.descriptions = {
            language: description for language, description in
            json_dict['descriptions'].iteritems()}
        self.claims = []
        for property_id, claim_list in json_dict['claims'].iteritems():
            for one_claim in claim_list:
                claim_object = WikiDataClaim(
                    property_id, one_claim, wikidata_thing=self)
                self.claims.append(claim_object)

        self.type = json_dict['type']
        self.is_item = self.type == 'item'
        self.is_property = self.type == 'property'

    def to_cypher(self):
        if self.is_item:
            self.to_cypher_item()
        elif self.is_property:
            self.to_cypher_property()
        else:
            raise Exception("Neither an item nor a property. "
                            "This should never happen.")

    def to_cypher_property(self):
        cypher = (
            u"""MERGE (property:Property {{name: "{property_id}"}}) """)
        non_relational_properties = {
            property_name: value for property_name, value in
            self.non_relational_properties.iteritems()}
        delisted_dictionary = delist_dictionary(non_relational_properties)
        for non_relational_property, property_value in delisted_dictionary.iteritems():
            if isinstance(property_value, list):
                property_value = json.dumps(property_value)
            cypher = (
                u"""MERGE (source_property:Property """
                """{{name: "{source_property_id}"}}) """
                """SET source_property.{non_relational_property} = """
                """ "{non_relational_property_value}"'""").format(
                    source_property_id=self.wikidata_id,
                    non_relational_property=non_relational_property,
                    non_relational_property_value=json.dumps(property_value))
            self.cypher_statements.append(cypher)

    def to_cypher_item(self):
        # First we create nodes and edges using MERGE statements
        for triple in self.triples:
            source_id, property_id, target_id = triple
            cypher = (
                u"""MERGE (source:Entity {{name: "{source_id}"}}) WITH source """
                """MERGE (target:Entity {{name: "{target_id}"}}) WITH source, target """
                """MERGE (source)-[property:RELATED_TO|{property_id}]->(target) """
                """SET property.name = "{property_id}";""").format(
                    source_id=source_id, property_id=property_id, target_id=target_id) 
            self.cypher_statements.append(cypher)

        non_relational_properties = {
            property_name: value for property_name, value in
            self.non_relational_properties.iteritems()}
        delisted_dictionary = delist_dictionary(non_relational_properties)
        for non_relational_property, property_value in delisted_dictionary.iteritems():
            if isinstance(property_value, list):
                property_value = json.dumps(property_value)
            cypher = (
                u"""MERGE (source:Entity {{name: "{source_id}"}}) """
                """SET source.{non_relational_property} = "{property_value}"'""").format(
                    source_id=self.wikidata_id,
                    non_relational_property=non_relational_property,
                    property_value=property_value)
            self.cypher_statements.append(cypher)
            

class WikiDataClaim(object):

    def __init__(self, property_id, claim_dict, wikidata_thing=None):
        self.wikidata_thing = wikidata_thing
        self.property_id = property_id
        self.claim_id = claim_dict['id']
        if 'mainsnak' in claim_dict:
            self.mainsnak = WikiDataSnak(
                claim_dict['mainsnak'],
                wikidata_thing=self.wikidata_thing,
                parent_claim=self)
        else:
            self.mainsnak = None


class WikiDataSnak(object):

    def __init__(self, snak_dict, wikidata_thing=None, parent_claim=None):
        self.snak_type = snak_dict['snaktype']
        self.parent_claim = parent_claim
        self.data_type = snak_dict['datatype']
        self.data_value = snak_dict.get('datavalue', None)
        self.wikidata_thing = wikidata_thing
        if self.data_value is not None:
            self.snak_value_type = self.data_value['type']  # wikibase-entityid, string, etc.
            self.value = self.data_value['value']
            self.snak_about_item = (
                self.value.get('entity-type', None) == 'item'
                if isinstance(self.value, dict) else False)
            if self.snak_about_item:
                self.about_item = 'Q' + str(self.value['numeric-id'])
                snak_triple = (
                    self.wikidata_thing.wikidata_id,
                    self.parent_claim.property_id,
                    self.about_item)
                self.wikidata_thing.triples.append(snak_triple)
            elif self.data_type == 'string':
                # Here we add properties that will be inserted into the node itself
                ### print 'string', self.parent_claim.property_id, self.value
                self.wikidata_thing.non_relational_properties[self.parent_claim.property_id].append(self.value)
            elif not isinstance(self.value, dict):
                pass
                ### print self.value, self.data_type
                # import pdb; pdb.set_trace()
            else:
                pass
                # print 'other', self.data_type, self.parent_claim.property_id, self.value

if __name__ == '__main__':
    # 25163530 lines in current WikiData dump 
    parser = argparse.ArgumentParser(description='Convert WikiData JSON dump to Cypher statements')

    parser.add_argument(
        '--input-file',
        required=True,
        help='File containing JSON dump. Defaults to stdin.')
    parser.add_argument(
        '--output-file',
        required=True,
        help='Output file for Cypher statements')

    args = parser.parse_args()
  
    input_file_handler = file_handler(args.input_file)
    output_file_handler = file_handler(args.output_file)

    line_counter = 0
    with output_file_handler(args.output_file, 'w') as cypher_file:
        with input_file_handler(args.input_file, 'r') as wikidata_file:
            for line in wikidata_file:
                if line[0] == '[' or line[0] == ']':
                    continue
                line = line.strip()[:-1]
                line_counter += 1
                line_dict = json.loads(line)
                thing = WikiDataThing(
                    line_dict)
                thing.to_cypher()
                for statement in thing.cypher_statements:
                    cypher_file.write((statement + u'\n').encode('utf8'))
