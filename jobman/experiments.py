#This file contain example experiment and in the futur some generic experiment(for example for PLearn)
import sys, time

def example1(state, channel):

    print "example experiment"
    
    return channel.COMPLETE
    #return channel.INCOMPLETE #if the job is not finished, not tested

def theano_example(state, channel):
    import theano
    import theano.tensor as T
    a=T.scalar()
    b=T.scalar()
    c=a+b
    f=theano.function([a,b],c)
    print f(2,3)
    return channel.COMPLETE

def example2(state, channel):
    f=open("file","w")
    f.write("some results!")
    f.close()
    return channel.COMPLETE


def theano_test_return(state, channel):
    """
    This experiment allow to test different test case

    state param used wanted_return
    """
    state.will_return = state.wanted_return
    if state.wanted_return == 'raise':
        raise Exception("This jobs raise an error")    
    elif state.wanted_return == 'incomplete':
        return channel.INCOMPLETE
    elif state.wanted_return == 'complete':
        return channel.COMPLETE
    elif state.wanted_return == 'sysexit':
        sys.exit()



def example_sleep(state, channel):
    print "start of example_sleep for %ss"%str(state.sleep)
    time.sleep(state.sleep)
    print "end of example_sleep"
    return channel.COMPLETE

#This example fail for now. I will try to make it work later.
def example_numpy(state, channel):
    print "start of example_numpy"
    import numpy
    state.ndarray = numpy.zeros(4)
    print "end of example_numpy"
    return channel.COMPLETE


#This example fail for now. I will try to make it work later.
def print_state(state, channel):
    print "start of print_state"
    print state
    print "end of print_state"
    return channel.COMPLETE
