from xml.sax import handler, make_parser

class Constant(object):
    __slots__ = ( 'c_name', 'c_type', 'c_value', 'c_card' )

    def __init__(self):
        self.c_name = None
        self.c_type = None
        self.c_value = None
        self.c_card = None

class Sink(object):
    __slots__ = ( 's_name', 's_type'  )

    def __init__(self):
        self.s_name = None
        self.s_type = None

class Source(object):
    __slots__ = ( 's_name', 's_type'  )

    def __init__(self):
        self.s_name = None
        self.s_type = None

class Input(object):
    __slots__ = ( 'p_name', 'p_type', 'p_depth' )

    def __init__(self):
        self.p_name = None
        self.p_type = None
        self.p_depth = None


class Output(object):

    __slots__ = ( 'p_name', 'p_type', 'p_depth' )

    def __init__(self):
        self.p_name = None
        self.p_type = None
        self.p_depth = None

class GASW(object):

    __slots__ = ( 'g_desc' )

    def __init__(self):
        self.g_desc= None

class Iteration(object):

    __slots__ = ( 'i_type', 'i_strat', 'i_port', 'i_parent', 'i_child', 'i_next', 'i_depth')

    def __init__(self):
        self.i_type = None # PORT, ITERATION
        self.i_strat = None # ITERATE_DOT, ITERATE_CROSS, ITERATE_FLAT_CROSS, ITERATE_MATCH
        self.i_port = None # Name of the port
        self.i_parent = None # Pointer to higher level
        self.i_child = None # pointer to deeper level
        self.i_next = None # point to next element on this level
        self.i_depth = None # Depth of iteration nesting

class Processor(object):

    __slots__ = ( 'p_in', 'p_out', 'p_gasw', 'p_name', 'p_iter' )

    def __init__(self):
        self.p_in = []
        self.p_out = []
        self.p_gasw = None
        self.p_name = None
        self.p_iter = None

class Link(object):

    __slots__ = ( 'l_from', 'l_to' )

    def __init__(self):
        self.l_from = None
        self.l_to = None

class Workflow(object):

    __slots__ = ( 'processors', 'name', 'sources', 'sinks', 'constants', 'links' )

    def __init__(self):
        self.name = None
        self.processors = []
        self.sources = []
        self.sinks = []
        self.constants = []
        self.links = []

    

class WorkflowHandler(handler.ContentHandler):

    def __init__(self):

        # Initial values
        self.workflow = None
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

            self.workflow = Workflow()
            self.workflow.name = attributes['name']

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

            self.i_source.s_name = attributes['name']
            self.i_source.s_type = attributes['type']

        elif name == 'sink':
            if self.inside_interface != True:
                raise('sink should not appear outside interface')
            self.inside_sink = True

            self.i_sink = Sink()

            self.i_sink.s_name = attributes['name']
            self.i_sink.s_type = attributes['type']

        elif name == 'constant':
            if self.inside_interface != True:
                raise('constant should not appear outside interface')
            self.inside_constant = True

            self.i_constant = Constant()

            self.i_constant.c_name = attributes['name']
            self.i_constant.c_type = attributes['type']
            self.i_constant.c_value = attributes['value']
            self.i_constant.c_card = attributes['cardinality']

            if self.i_constant.c_card != 'scalar':
                raise Exception('Only scalar type constants supported:' + self.i_constant.c_card)

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
            self.p_processor.p_name = attributes['name']

        elif name == 'in':
            if self.inside_processor != True:
                raise('in should not appear outside processor')
            self.inside_in = True

            self.p_in = Input()

            self.p_in.p_name = attributes['name']
            self.p_in.p_type = attributes['type']
            self.p_in.p_depth = attributes['depth']

        elif name == 'out':
            if self.inside_processor != True:
                raise('out should not appear outside processor')
            self.inside_out = True

            self.p_out = Output()

            self.p_out.p_name = attributes['name']
            self.p_out.p_type = attributes['type']
            self.p_out.p_depth = attributes['depth']

        elif name == 'gasw':
            if self.inside_processor != True:
                raise('gasw should not appear outside processor')
            self.inside_gasw = True

            self.p_gasw = GASW()

            self.p_gasw.g_desc= attributes['descriptor']

        elif name == 'iterationstrategy':
            if self.inside_processor != True:
                raise('iterationstrategy should not appear outside processor')
            self.inside_iter = True

            self.p_iter = Iteration()
            self.p_processor.p_iter = self.p_iter
            self.p_iter.i_depth = self.iter_depth


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
            node.i_strat = name
            node.i_type = 'ITERATION'
            node.i_depth = self.iter_depth


        # If p_iter is a PORT we stay at the same level, otherwise nest
            if self.p_iter.i_type == 'PORT':
                self.p_iter.i_next = node
                node.i_parent = self.p_iter.i_parent
            else:
                node.i_parent = self.p_iter
                self.p_iter.i_child = node

            self.p_iter = node

        elif name == 'port':
            if self.inside_iter != True:
                raise('port should not appear outside iterationstrategy')
            self.inside_port = True

            # Create new node for in tree and set its iteration strategy
            node = Iteration()
            node.i_type = 'PORT'
            node.i_port = attributes['name']
            node.i_depth = self.iter_depth


            # If p_iter is a PORT we stay at the same level, otherwise nest
            if self.p_iter.i_type == 'PORT':
                self.p_iter.i_next = node
                node.i_parent = self.p_iter.i_parent
            else:

                if self.iter_depth == self.p_iter.i_depth:

                    node.i_parent = self.p_iter
                    self.p_iter.i_child = node
                else:
                    node.i_parent = self.p_iter.i_parent
                    self.p_iter.i_next = node

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

            self.l_link.l_from = attributes['from']
            self.l_link.l_to = attributes['to']

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
            self.workflow.sources.append(self.i_source)
        elif name == 'sink':
            self.inside_sink = False 
            self.workflow.sinks.append(self.i_sink)
        elif name == 'constant':
            self.inside_constant = False 
            self.workflow.constants.append(self.i_constant)

        # processors
        elif name == 'processors':
            self.inside_processors = False 
        elif name == 'processor':
            self.inside_processor = False 
            self.workflow.processors.append(self.p_processor)
        elif name == 'in':
            self.inside_in = False 
            self.p_processor.p_in.append(self.p_in)
        elif name == 'out':
            self.inside_out = False 
            self.p_processor.p_out.append(self.p_out)
        elif name == 'gasw':
            self.inside_gasw = False 
            self.p_processor.p_gasw = self.p_gasw
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

            if self.p_iter.i_parent:
                self.p_iter = self.p_iter.i_parent

        # links
        elif name == 'links':
            self.inside_links = False 
            pass
        elif name == 'link':
            self.inside_link = False 
            self.workflow.links.append(self.l_link)
            pass

class WorkflowXML(object):

    def __init__(self):
        self.parser = make_parser()
        self.handler = WorkflowHandler()
        self.parser.setContentHandler(self.handler)

    def read_from_file(self, xmlfile):
        self.parser.parse(xmlfile)

        self.workflow = self.handler.workflow
        
    def text_out(self):

        def print_iter(p_iter, indent):

            if p_iter.i_type == 'ITERATION':
                print indent + 'Iteration:', p_iter.i_strat
            elif p_iter.i_type == 'PORT':
                print indent + 'Ports:', p_iter.i_port

            if p_iter.i_next:
                print_iter(p_iter.i_next, indent)
            if p_iter.i_child:
                print_iter(p_iter.i_child, indent + '  ')

        print 'Workflow:', self.workflow.name
        print '  Interfaces:'
        for s in self.workflow.sources:
            print '    Source:', s.s_name, s.s_type
        for s in self.workflow.sinks:
            print '    Sink:', s.s_name, s.s_type
        for c in self.workflow.constants:
            print '    Constant:', c.c_name, c.c_type, c.c_value, c.c_card

        print '  Processors:'
        for p in self.workflow.processors:
            print '    Processor:', p.p_name

            print '      GASW:', p.p_gasw.g_desc
            for i in p.p_in:
                print '      Input:', i.i_name, i.i_type, i.i_depth
            for o in p.p_out:
                print '      Output:', o.o_name, o.o_type, o.o_depth

            if p.p_iter:
              print_iter(p.p_iter, '      ')

        print '  Links:'
        for l in self.workflow.links:
            print '    Link:', l.l_from, '->', l.l_to

    def tograph(self):
        pass


#
# Main function
#
if __name__ == '__main__':

    #gwendia_file = '../examples/dti_bedpost.gwendia'
    #gwendia_file = '../examples/iter-test.gwendia'
    #gwendia_file = '../examples/iter-simple.gwendia'
    gwendia_file = '../examples/example.gwendia'

    wx = WorkflowXML()
    wx.read_from_file(gwendia_file)
    wx.text_out()
    #wfe = wx.workflow
