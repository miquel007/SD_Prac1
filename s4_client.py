# -*- coding: 850 -*-
'''
Remote example with registry. CLIENT
@authors: Marc Ferré , Miquel Roig
'''
from pyactor.context import set_context, create_host, serve_forever
import socket, sys



if __name__ == "__main__":

    if len(sys.argv) == 4:
        ip_Registry = sys.argv[1]
        port = sys.argv[2]
        name = sys.argv[3]

        set_context()
        myIp = ([l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1],
                           [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in
                             [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0])

        # host = create_host('http://10.0.2.15:'+str(port)+'')
        host = create_host('http://'+myIp+':'+port+'')

        registry = host.lookup_url('http://'+str(ip_Registry)+':1282/regis', 'Registry',
                                   's4_registry')
        registry.bind(name, host)

        serve_forever()
    else:
        print "Passa per parametre la IP_Registry, MY_PORT, MY_NAME"
