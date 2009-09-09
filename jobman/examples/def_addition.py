def addition_example(state, channel):

    print 'state.first =', state.first
    print 'state.second =', state.second

    state.result = state.first + state.second
    print 'result =', state.result

    return channel.COMPLETE

