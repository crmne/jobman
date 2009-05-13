#This file contain example experiment and in the futur some generic experiment(for example for PLearn)

def example1(state, channel):

    print "example experiment"
    
    return channel.COMPLETE
    #return channel.INCOMPLETE #if the job is not finished, not tested
