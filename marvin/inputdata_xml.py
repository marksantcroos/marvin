#!/usr/bin/env python

from xml.sax import handler, make_parser
from ast import literal_eval

class Source(object):
    __slots__ = ( 's_name', 's_type', 's_entries' )

    def __init__(self):
        self.s_name = None
        self.s_type = None
        self.s_entries = None


class InputData(object):

    __slots__ = ( 'i_sources' )

    def __init__(self):
        self.i_sources = []


class Entry(object):

    __slots__ = ( 'e_item', 'e_type', 'e_parent', 'e_child', 'e_next', 'e_depth')

    def __init__(self):
        # members
        self.e_item = None # Actual value of this entry
        self.e_type = None # 'ITEM', 'ARRAY', 'SCALAR'
        self.e_parent = None # Pointer to higher level
        self.e_child = None # pointer to deeper level
        self.e_next = None # point to next element on this level
        self.e_depth = None # The depth of the nesting


class InputDataHandler(handler.ContentHandler):

    def __init__(self):

        self.buffer = ""

        # Initial values
        self.inputdata = None

        # parser location
        self.inside_inputdata = False
        self.inside_source = False
        self.inside_array = 0
        self.inside_item = False
        self.inside_scalar = False

        # pointer inside array hierarchy
        self.array = None


    def startElement(self, name, attributes):

        """

:param name:
:param attributes:
:raise:
"""
        if name == 'inputdata':
            self.inside_inputdata = True

            self.inputdata = InputData()

        elif name == 'source':
            if self.inside_inputdata != True:
                raise('source should not appear outside inputdata')
            self.inside_source = True

            self.i_source = Source()

            self.i_source.s_name = attributes['name']
            self.i_source.s_type = attributes['type']


        elif name == 'array':
            if self.inside_source != True:
                raise('array should not appear outside source')

            # Record the depth of the array
            self.inside_array += 1

            # Create a new Entry data structure
            node = Entry()
            node.e_type = 'ARRAY'
            node.e_depth = self.inside_array

            if self.array:

                if self.array.e_type == 'ARRAY':

                    print node.e_depth, self.array.e_depth

                    if node.e_depth > self.array.e_depth:

                        node.e_parent = self.array
                        self.array.child = node
                    else:
                        node.e_parent = self.array.e_parent
                        self.array.e_next = node

                elif self.array.e_type == 'ITEM':
                    node.e_parent = self.array.e_parent
                    self.array.e_next = node

                else:
                    raise('Unknown Entry() type')
            else:
                self.i_source.s_entries = node

            self.array = node

        elif name == 'scalar':
            if not self.inside_source or self.inside_array:
                raise('scalar should not appear outside source or inside an array')
            self.inside_scalar = True

            # Create a new Entry data structure
            node = Entry()
            node.e_type = 'SCALAR'

            self.i_source.s_entries = node
            self.array = node


        elif name == 'item':
            if not self.inside_array and not self.inside_scalar:
                raise('item should not appear outside array or scalar')
            self.inside_item = True

            self.buffer = ""

            if not self.inside_scalar:
                node = Entry()
                node.e_type = 'ITEM'

                assert self.array, 'Array should exist here'

                if self.array.e_type == 'ITEM':
                    node.e_parent = self.array.e_parent
                    self.array.e_next = node
                    self.array = node

                else:

                    if self.array.e_depth == self.inside_array:

                        self.array.e_child = node
                        node.e_parent = self.array

                    else:
                        node.e_parent = self.array.e_parent
                        self.array.e_next = node

                self.array = node


    def characters(self, data):
        self.buffer += data

    def endElement(self, name):

        if name == 'inputdata':
            self.inside_inputdata = False 

        elif name == 'source':
            self.inside_source = False 
            self.inputdata.i_sources.append(self.i_source)
            self.array = None

        elif name == 'array':
            if self.array.e_parent:
                self.array = self.array.e_parent

            self.inside_array -= 1


        elif name == 'scalar':
            self.inside_scalar = False
            # if len(self.i_source.s_entries) > 1:
            #     raise('scalar can only have one item maximum')

        elif name == 'item':
            self.inside_item = False 

            if not self.buffer:
                raise('empty item')

            self.item = self.buffer
            self.array.e_item = self.item





class InputDataXML(object):

    def __init__(self):
        self.parser = make_parser()
        self.handler = InputDataHandler()
        self.parser.setContentHandler(self.handler)

    def read_from_file(self, file):
        self.parser.parse(file)
        self.inputdata = self.handler.inputdata.i_sources

    def text_out(self):

        print 'Inputdata:'

        def print_iter(e_iter, indent):

            if e_iter.e_type == 'ITEM':
                print indent + 'Array item:', e_iter.e_item
            elif e_iter.e_type == 'SCALAR':
                print indent + 'Scalar:', e_iter.e_item

            if e_iter.e_child:
                print_iter(e_iter.e_child, indent + '  ')
            if e_iter.e_next:
                print_iter(e_iter.e_next, indent)

        print '  Sources:'
        for s in self.inputdata:
            print_iter(s.s_entries, '    ')
            print



# Correct? ['A', 'B', ['C', ['D'], 'E', 'F', ['G', ['H']], ['I'], 'J'], 'K', ['L', 'M'], 'N']
        #  ['A', 'B', ['C', ['D'], 'E', 'F', ['G', ['H']], ['I'], 'J'], 'K', ['L', 'M'], 'N']
         # ['A', 'B', ['C', ['D'], 'E', 'F', ['G', ['H']], ['I'], 'J'], 'K', ['L', 'M'], 'N']



    def list_inputs(self):

        def tree2str(e_iter, last=None, str=''):

            if e_iter.e_type == 'ITEM':
                if last == 'ITEM' or last == 'CLOSE':
                    str += ','
                str += '\'%s\'' % e_iter.e_item
                last = 'ITEM'

            elif e_iter.e_type == 'SCALAR':
                str += '\'%s\'' % e_iter.e_item
                last = 'SCALAR'

            if e_iter.e_child:
                if last == 'ITEM' or last == 'CLOSE':
                    str += ','
                str += '['
                last = 'OPEN'
                str += tree2str(e_iter.e_child, last)
                str += ']'
                last = 'CLOSE'

            if e_iter.e_next:
                str += tree2str(e_iter.e_next, last)

            return str


        sources = {}

        for s in self.inputdata:
             sources[s.s_name] = literal_eval(tree2str(s.s_entries))

        return sources


#
# Main function
#
if __name__ == '__main__':

    # inputfile = '../examples/gwendia_input_sample.xml'
    inputfile = '../examples/dti7-noloop-input.xml'
    # inputfile = '../examples/dti_bedpost-input.xml'


    ix = InputDataXML()
    ix.read_from_file(inputfile)
    #ix.text_out()
    print ix.list_inputs()

    # data = ix.inputdata
    # for d in data:
    #     print d