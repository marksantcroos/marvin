from xml.sax import handler, make_parser

from abstractwf import Source, Sink, Port, Constant, Iteration, Processor, Link, AbstractWF #, GASW

class Elements(object):

    __slots__ = ( 'processors', 'name', 'sources', 'sinks', 'constants', 'links' )

    def __init__(self):
        self.name = None
        self.processors = []
        self.sources = []
        self.sinks = []
        self.constants = []
        self.links = []

    def text_out(self):

        def print_iter(p_iter, indent):

            if p_iter.type == 'ITERATION':
                print indent + 'Iteration:', p_iter.strat
            elif p_iter.type == 'PORT':
                print indent + 'Port:', p_iter.port

            if p_iter.next:
                print_iter(p_iter.next, indent)
            if p_iter.child:
                print_iter(p_iter.child, indent + '  ')

        print 'Workflow:', self.name
        print '  Interfaces:'
        for s in self.sources:
            print '    Source:', s.name, s.type
        for s in self.sinks:
            print '    Sink:', s.name, s.type
        for c in self.constants:
            print '    Constant:', c.name, c.type, c.value, c.card

        print '  Processors:'
        for p in self.processors:
            print '    Processor:', p.name

            # print '      GASW:', p.gasw.desc
            print '      GASW:', p.gasw
            for i in p.input:
                print '      Input:', i.name, i.type, i.depth
            for o in p.output:
                print '      Output:', o.name, o.type, o.depth

            if p.iter:
                print_iter(p.iter, '      ')

        print '  Links:'
        for l in self.links:
            print '    Link:', l.tail, '->', l.head

class WorkflowHandler(handler.ContentHandler):

    def __init__(self):

        # Initial values
        self.elements = None
        self.p_processor = None
        self.p_in = None
        self.p_out = None
        self.i_source = None
        self.i_sink = None
        self.l_link = None
        self.i_port = None

        # workflow
        self.inside_workflow = False

        # interfaces
        self.inside_interface = False
        self.inside_source = False
        self.inside_sink = False
        self.inside_constant = False

        # processors
        self.inside_processors = False
        self.inside_in = False
        self.inside_out = False
        self.inside_gasw = False
        self.inside_iter = False
        self.inside_dot = 0
        self.inside_cross = 0
        self.inside_flat_cross = 0
        self.inside_match = 0
        self.iter_depth = 0


    # links
        self.inside_links = False
        self.inside_link = False


    def startElement(self, name, attributes):

        # workflow document
        if name == 'workflow':
            self.inside_workflow = True

            self.elements = Elements()
            self.elements.name = attributes['name']

        # interfaces
        elif name == 'interface':
            if self.inside_workflow != True:
                raise('interface should not appear outside workflow')
            self.inside_interface = True

        elif name == 'source':
            if self.inside_interface != True:
                raise('source should not appear outside interface')
            self.inside_source = True

            self.i_source = Source()

            self.i_source.name = attributes['name']
            self.i_source.type = attributes['type']

        elif name == 'sink':
            if self.inside_interface != True:
                raise('sink should not appear outside interface')
            self.inside_sink = True

            self.i_sink = Sink()

            self.i_sink.name = attributes['name']
            self.i_sink.type = attributes['type']

        elif name == 'constant':
            if self.inside_interface != True:
                raise('constant should not appear outside interface')
            self.inside_constant = True

            self.i_constant = Constant()

            self.i_constant.name = attributes['name']
            self.i_constant.type = attributes['type']
            self.i_constant.value = attributes['value']
            self.i_constant.card = attributes['cardinality']

            if self.i_constant.card != 'scalar':
                raise Exception('Only scalar type constants supported:' + self.i_constant.card)

        # processors
        elif name == 'processors':
            if self.inside_workflow != True:
                raise('processors should not appear outside workflow')
            self.inside_processors = True


        elif name == 'processor':
            if self.inside_processors != True:
                raise('processor should not appear outside processors')
            self.inside_processor = True

            self.p_processor = Processor()
            self.p_processor.name = attributes['name']

        elif name == 'in':
            if self.inside_processor != True:
                raise('in should not appear outside processor')
            self.inside_in = True

            self.p_in = Port()

            self.p_in.name = attributes['name']
            self.p_in.type = attributes['type']
            self.p_in.depth = attributes['depth']

        elif name == 'out':
            if self.inside_processor != True:
                raise('out should not appear outside processor')
            self.inside_out = True

            self.p_out = Port()

            self.p_out.name = attributes['name']
            self.p_out.type = attributes['type']
            self.p_out.depth = int(attributes['depth'])

        elif name == 'gasw':
            if self.inside_processor != True:
                raise('gasw should not appear outside processor')
            self.inside_gasw = True

            # self.p_gasw = GASW()
            # self.p_gasw.desc = attributes['descriptor']
            self.p_gasw = attributes['descriptor']

        elif name == 'iterationstrategy':
            if self.inside_processor != True:
                raise('iterationstrategy should not appear outside processor')
            self.inside_iter = True

            self.p_iter = Iteration()
            self.p_processor.iter = self.p_iter
            self.p_iter.depth = self.iter_depth


        elif name in ['dot', 'cross', 'flat-cross', 'match']:
            if self.inside_iter != True:
                raise('%s should not appear outside iterationstrategy' % name)
            if name == 'dot':
                self.inside_dot += 1
            elif name =='cross':
                self.inside_cross += 1
            elif name == 'flat-cross':
                self.inside_flat_cross += 1
            elif name == 'match':
                self.inside_match += 1

            self.iter_depth += 1

            # Create new node for in tree and set its iteration strategy
            node = Iteration()
            node.strat = name
            node.type = 'ITERATION'
            node.depth = self.iter_depth


        # If p_iter is a PORT we stay at the same level, otherwise nest
            if self.p_iter.type == 'PORT':
                self.p_iter.next = node
                node.parent = self.p_iter.parent
            else:
                node.parent = self.p_iter
                self.p_iter.child = node

            self.p_iter = node

        elif name == 'port':
            if self.inside_iter != True:
                raise('port should not appear outside iteration strategy')
            self.inside_port = True

            # Create new node for in tree and set its iteration strategy
            node = Iteration()
            node.type = 'PORT'
            node.port = attributes['name']
            node.depth = self.iter_depth


            # If p_iter is a PORT we stay at the same level, otherwise nest
            if self.p_iter.type == 'PORT':
                self.p_iter.next = node
                node.parent = self.p_iter.parent
            else:

                if self.iter_depth == self.p_iter.depth:

                    node.parent = self.p_iter
                    self.p_iter.child = node
                else:
                    node.parent = self.p_iter.parent
                    self.p_iter.next = node

            self.p_iter = node

        # links
        elif name == 'links':
            if self.inside_workflow != True:
                raise('processors should not appear outside workflow')
            self.inside_links = True

        elif name == 'link':
            if self.inside_links != True:
                raise('Link should not appear outside links')
            self.inside_link = True

            self.l_link = Link()

            self.l_link.tail = attributes['from']
            self.l_link.head = attributes['to']

    def characters(self, data):
        pass

    def endElement(self, name):

        # workflow
        if name == 'workflow':
            self.inside_workflow = False 

        # interface
        elif name == 'interface':
            self.inside_interface = False 
        elif name == 'source':
            self.inside_source = False 
            self.elements.sources.append(self.i_source)
        elif name == 'sink':
            self.inside_sink = False 
            self.elements.sinks.append(self.i_sink)
        elif name == 'constant':
            self.inside_constant = False 
            self.elements.constants.append(self.i_constant)

        # processors
        elif name == 'processors':
            self.inside_processors = False 
        elif name == 'processor':
            self.inside_processor = False 
            self.elements.processors.append(self.p_processor)
        elif name == 'in':
            self.inside_in = False 
            self.p_processor.input.append(self.p_in)
        elif name == 'out':
            self.inside_out = False 
            self.p_processor.output.append(self.p_out)
        elif name == 'gasw':
            self.inside_gasw = False 
            self.p_processor.gasw = self.p_gasw
        elif name == 'iterationstrategy':
            self.inside_iter = False 
        elif name == 'port':
            self.inside_port = False 

        # Iteration strategy
        elif name in ['dot', 'cross', 'flat-cross', 'match']:
            if name == 'cross':
                self.inside_cross -= 1
            elif name == 'dot':
                self.inside_dot -= 1
            elif name == 'flat-cross':
                self.inside_flat_cross -= 1
            elif name == 'match':
                self.inside_match -= 1

            self.iter_depth -= 1

            if self.p_iter.parent:
                self.p_iter = self.p_iter.parent

        # links
        elif name == 'links':
            self.inside_links = False 
            pass
        elif name == 'link':
            self.inside_link = False 
            self.elements.links.append(self.l_link)
            pass
        #

#
# Create an abstract graph out of the internal workflow format
# that was read from file
#
def construct_from_xml(elements):

    awf = AbstractWF()

    # Create Sink Nodes
    for sink in elements.sinks:
        awf.add_node(sink.name, sink)

    # Create Processor Nodes
    for proc in elements.processors:

        awf.add_node(proc.name, proc)

        for port in proc.input:
            port.label = port.name
            port.name = '%s:%s' % (proc.name, port.name)
            awf.add_node(port.name, port)

            # Add connection from input port to node
            awf.add_edge(port.name, proc.name)

        for port in proc.output:
            port.label = port.name
            port.name = '%s:%s' % (proc.name, port.name)
            awf.add_node(port.name, port)

            # Add connection from output node to port
            awf.add_edge(proc.name, port.name)

    # Create Source Nodes
    for source in elements.sources:
        awf.add_node(source.name, source)

    # Create Constant nodes
    for constant in elements.constants:
        awf.add_node(constant.name, constant)

    # Iterate over links to create edges
    for link in elements.links:

        # Create the final edge between the nodes and/or ports
        awf.add_edge(link.tail, link.head)

    return awf

#
# Read XML GWENDIA file
#
def xml2abstract(xml_file_path):

    # Initialize parser
    parser = make_parser()
    handler = WorkflowHandler()
    parser.setContentHandler(handler)

    # Run parser with input file
    parser.parse(xml_file_path)

    # Print elements
    handler.elements.text_out()

    # Convert into Abstract WF structure
    awf = construct_from_xml(handler.elements)

    return awf
