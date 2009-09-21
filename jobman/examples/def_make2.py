from jobman import make2

class obja():
    def __init__(self, param1=1, param2=None, param3=3):
        self.param1 = param1
        if param2: self.param2 = param2(param3)
        print 'obja.param1 = ', self.param1
        print 'obja.param2 = ', self.param2
        print 'type(obja.param2) = ', type(self.param2)

def experiment(state, channel):

    obj = make2(state.obja)
    return channel.COMPLETE
