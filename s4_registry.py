# -*- coding: 850 -*-
'''
Remote example with registry. CLIENT
@authors: Marc Ferré , Miquel Roig
'''
from pyactor.context import set_context, create_host, serve_forever
import socket

class NotFound(Exception):
    pass


class Registry(object):
    _ask = ['get_all', 'get_name', 'bind', 'lookup', 'unbind', 'get_host']
    _async = []
    _ref = ['get_all', 'get_name', 'bind', 'lookup', 'get_host']

    def __init__(self):
        self.actors = {}
        self.my_ip = 0

    def bind(self, name, actor):
        print "server registred", name
        self.actors[name] = actor

    def unbind(self, name):
        if name in self.actors.keys():
            del self.actors[name]
        else:
            raise NotFound()

    def lookup(self, name):
        if name in self.actors:
            return self.actors[name]
        else:
            return None

    def get_name(self):
        return self.actors.keys()

    def get_all(self):
        return self.actors.values()

    def get_host(self):
        ret = []
        for regitred in self.get_name():
            if regitred[:4] == "host":
                ret.append(self.lookup(regitred))
        return ret



if __name__ == "__main__":
    set_context()

    myIp = (
        [l for l in
         ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1],
          [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in
            [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0])

    host = create_host("http://"+str(myIp)+":1282")
    # host = create_host('http://10.0.2.15:1282')

    registry = host.spawn('regis', Registry)

    print 'host listening at port 1282'

    serve_forever()
