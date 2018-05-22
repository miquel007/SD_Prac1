# -*- coding: 850 -*-
'''
Remote example with registry. CLIENT 2
@authors: Marc Ferré , Miquel Roig
'''


from pyactor.context import set_context, create_host, serve_forever
from collections import defaultdict
from itertools import chain
import urllib2, sys, socket, os, time
from s4_registry import NotFound

#    try:
#        registry.unbind('none')
#    except NotFound:
#        print "Cannot unbind this object: is not in the registry."


class Server(object):
    _tell = ['Wordscont', 'Contwords']
    _ref = ['Wordscont', 'Contwords']

    def Wordscont(self, f, registreeee, envia_redu, retu):

        FILE_NAME = f + '.txt'
        print FILE_NAME
        req = urllib2.Request(FILE_NAME)
        response = urllib2.urlopen(req)
        wordCounter = defaultdict(int)
        for line in response:
            word_list = line.replace(',', '').replace('(', '').replace(')', '').replace('\'', '').replace('*',
                                                                                                          '').replace(
                '[', '').replace('.', '').replace('-', '').replace('?', '').replace(':', '').replace(';',
                                                                                                     '').replace(
                '/', '').replace('"', '').replace('�', '').replace('!', '').replace('�', '').replace('�',
                                                                                                     '').replace(
                '�', '').lower().split()
            for word in word_list:
                if word not in wordCounter:
                   wordCounter[word] = 1
                else:
                    wordCounter[word] = wordCounter[word] + 1
        response.close()

        envia_redu.unio(wordCounter, registreeee, retu)

    def Contwords(self, f, registreeee, envia_redu, retu):

        FILE_NAME = f + '.txt'
        CounterWord=0
        print FILE_NAME
        req = urllib2.Request(FILE_NAME)
        response = urllib2.urlopen(req)
        wordCounter = defaultdict(int)
        for line in response:
            for word in line:
                   CounterWord += 1
        response.close()

        envia_redu.unio2(CounterWord, registreeee, retu)


class WriteFile(object):
    _ask = ['echo', 'arxiu_sortida', 'get_Fin']
    _ref = ['echo', 'arxiu_sortida']

    def __init__(self):
        self.acabat = False

    def echo(self, msg):
        self.acabat = True
        print msg

    def arxiu_sortida(self, msg):
        fOut = open("out.txt", "w")

        fOut.write('{:15}{:3}'.format('Word', 'Count'))
        fOut.write('\n')
        fOut.write('-' * 18)

        for w in sorted(msg, key=msg.get, reverse=True):
           fOut.write('\n')
           fOut.write('{:15}{:3}'.format(w, msg[w]))

        self.acabat = True
        fOut.close()

    def get_Fin(self):
        if self.acabat is True:
            self.acabat = False
            return True
        else:
            return self.acabat


class Join(object):

    _ask = ['']
    _tell = ['unio','unio2']
    _ref = ['unio', 'unio2']

    def __init__(self):
        self.diccionariGlogal = {}
        self.wordcounter = []

    def unio(self, llistat, registry, retorna):
        self.wordcounter.append(llistat)
        if len(self.wordcounter) == (len(registry.get_host())):
            if not retorna.has_actor('myWrite'):
                r = retorna.spawn('myWrite', 's4_clientb/WriteFile')
            else:
                r = retorna.lookup('myWrite')

            self.diccionariGlogal.clear()
            for fix in self.wordcounter:
                for k, v in chain(fix.items()):
                    if k not in self.diccionariGlogal:
                        self.diccionariGlogal[k] = v
                    else:
                        self.diccionariGlogal[k] = self.diccionariGlogal[k] + v
            print "He possat tot al diccionari"
            self.wordcounter[:] = []
            r.arxiu_sortida(self.diccionariGlogal)

    def unio2(self, contador, registry, retorna):
        self.wordcounter.append(contador)
        if len(self.wordcounter) == (len(registry.get_host())):
            conterword = 0
            if not retorna.has_actor('myWrite'):
                r = retorna.spawn('myWrite', 's4_clientb/WriteFile')
            else:
                r = retorna.lookup('myWrite')

            for fix in self.wordcounter:
                conterword += fix
            print "He fet la suma"
            self.wordcounter[:] = []
            r.echo(conterword)


def sub_arxius():
    fIn = open(str(fInSet) + ".txt", "r")
    nHosts = len(registry.get_host())
    nLines = len(fIn.readlines())
    fIn.close()

    residu = nLines % nHosts
    nLines2 = nLines // nHosts

    hostWords = []
    lineaActual = 0
    contHost = 1
    fi = (nLines2 * contHost) - 1
    fIn = open(fInSet + ".txt", "r")
    fInSet2 = fInSet + "_" + str(contHost)
    dir = os.getcwd()+"/Arxius/"
    try:
        os.stat(dir)
    except:
        os.mkdir(dir)

    fIn2 = open(str(dir) + fInSet2 + ".txt", "w")
    hostWords.append(fInSet2)

    for line in fIn.readlines():
        fIn2.write(line)
        if lineaActual == fi:
            fIn2.close()
            contHost = contHost + 1
            if contHost == nHosts:
                fi = ((nLines2 * contHost) - 1) + residu
            else:
                fi = (nLines2 * contHost) - 1

            if contHost <= nHosts:
                fInSet2 = fInSet + "_" + str(contHost)
                hostWords.append(fInSet2)
                fIn2 = open(dir + fInSet2 + ".txt", "w")
        lineaActual = lineaActual + 1
    fIn.close()

    fi_temps = time.time()
    raw_input("Executa - python -m SimpleHTTPServer - a aqui:/Arxius")
    #print str(dir)+"; python -m SimpleHTTPServer"
    #subprocess.call(str(dir)+"; exec python -m SimpleHTTPServer", shell=True)

    print hostWords

    pasar = []
    pasar.append(hostWords)
    pasar.append(fi_temps)

    return pasar


def distribuir_feina():
    hosts = registry.get_host()
    nFitxer = 0

    creduce = registry.lookup('reduce')
    if creduce is not None:
        if not creduce.has_actor('myreducer'):
            red = creduce.spawn('myreducer', 's4_clientb/Join')
        else:
            print "Segona volta"
            red = creduce.lookup('myreducer')

    for host in hosts:
        if not host.has_actor('server'):
            server = host.spawn('server', 's4_clientb/Server')
        else:
            server = host.lookup('server')
        hostWords[nFitxer] = "http://"+str(myIp)+":8000/" + hostWords[nFitxer]
        # hostWords[nFitxer] = "http://10.0.2.15:8000/" + hostWords[nFitxer]
        print clientb_host

        op = int(opcio)
        if op == 0:
            server.Contwords(hostWords[nFitxer], registry, red, clientb_host)
        else:
            server.Wordscont(hostWords[nFitxer], registry, red, clientb_host)

        nFitxer = nFitxer + 1


if __name__ == "__main__":

    if len(sys.argv) == 5:
        inici_temps1 = time.time()
        ip_Registry = sys.argv[1]
        port = sys.argv[2]
        fInSet = sys.argv[3]
        opcio = sys.argv[4]

        myIp = (
            [l for l in
             ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1],
              [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in
                [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0])

        set_context()

        # clientb_host = create_host('http://10.0.2.15:' + str(port))
        clientb_host = create_host('http://'+str(myIp)+':'+str(port))
        registry = clientb_host.lookup_url('http://' + str(ip_Registry) + ':1282/regis', 'Registry',
                                           's4_registry')

        parametre = []
        parametre = sub_arxius()
        hostWords = parametre[0]
        fi_temps1 = parametre[1]
        inici_temps2 = time.time()
        distribuir_feina()

        while not clientb_host.has_actor('myWrite'):
            time.sleep(0.001)
        while clientb_host.lookup('myWrite').get_Fin() is not True:
            time.sleep(0.001)
        fi_temps2 = time.time()

        total_temps = (fi_temps1 - inici_temps1) + (fi_temps2 - inici_temps2)

        print "Ha arribat fins el final"
        print "El temps execucio ha estat : "
        print total_temps

        serve_forever()
    else:
        print "Nombre incorrecte d'arguments: IP_REGISTRY, PORT (propi), ARXIU, (0)CounterWord / (1)WordCounter"
