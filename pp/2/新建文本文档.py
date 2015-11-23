def fn1():
    return '1'
def fn2():
    return '2'

def loadhook(h):
    """
    Converts a load hook into an application processor.
    
        >>> app = auto_application()
        >>> def f(): "something done before handling request"
        ...
        >>> app.add_processor(loadhook(f))
    """
    def processor(handler):
        h()
        return handler()
        
    return processor
processors=[loadhook(fn1),loadhook(fn2),loadhook(fn2),loadhook(fn2),loadhook(fn2),loadhook(fn2)]
def handle_with_processors():
    def process(processors):
        try:
            if processors:
                p, processors = processors[0], processors[1:]
                return p(lambda: process(processors))
            else:
                return None
        except :
            raise
        
        
        # processors must be applied in the resvere order. (??)
    return process(processors)
s=handle_with_processors()
print s
