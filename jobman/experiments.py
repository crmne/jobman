#This file contain example experiment and in the futur some generic experiment(for example for PLearn)

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


