#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = 'aarnek'
import atexit
import socket
import sys
from clientthread import ClientThread


class Server():
    def exitfunction(self, serversocket):
        serversocket.close()  # Closing socket. Truly important to do!
        sys.exit(1)

    def __init__(self):
        self.clients = []  # List of clients

        # Making server socket and making it to listen for incoming data.
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((sys.argv[1], int(sys.argv[2])))
        server.listen(5)

        #Registering function to be executed right before exiting.
        atexit.register(self.exitfunction, serversocket=server)
        while True:
            try:
                (client, address) = server.accept()  #Accepting new client
                #Looking for nickname, that is not in use yet
                ct = ClientThread(client, self)
                ct.start()
                self.clients.append(ct)
            except KeyboardInterrupt:
                break
        self.exitfunction(server)

    # def checkClientsForNickname(self,
    #                             nick):  # Function to check if there is already client with nickname given. If there is, True is returned.
    #     for s in self.clients:
    #         if s.nick == nick:
    #             return True
    #     return False
    #
    # def clientSocketForNick(self, nick):  # This functions gives clientsocket for nickname. Used to message routing.
    #     for s in self.clients:
    #         print '{0},{1}'.format(s.nick, nick)
    #         if s.nick == nick:
    #             return s.client
    #     return None
    #
    # def informAboutUser(self, nick, isNew):  # Informing to existing clients about new or left client
    #     for s in self.clients:
    #         if isNew:
    #             s.client.sendall('newusr:{0};'.format(str(nick)))
    #             print str(nick)
    #         else:
    #             s.client.sendall('leftusr:{0};'.format(str(nick)))
    #
    # def informAboutExistingUsers(self, client):  # New client is informed about existing clients
    #     for s in self.clients:
    #         client.sendall('newusr:{0};'.format(str(s.nick)))
    #
    # def deleteUser(self, nick):
    #     i = 0
    #     for cl in self.clients:
    #         if cl.nick == nick:
    #             self.clients.pop(i)
    #             break
    #         ++i
    #     self.informAboutUser(nick, False)
    #
    # def sendMessage(self, recipient, sender, msg):
    #     print u'viesti: {0}\nLähettäjä: {1}\nVastaanottaja: {2}\n'.format(msg, sender, recipient)
    #     sock = self.clientSocketForNick(recipient)  # gets client socket to send message from another user
    #     if sock != False:
    #         sock.sendall(u'msg:{0}:{1};'.format(sender, msg))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "command pattern: python server.py <IP> <Port>"
    else:
        s = Server()

