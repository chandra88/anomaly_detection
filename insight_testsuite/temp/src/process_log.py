#!/bin/python
import sys, json, math
from collections import OrderedDict

'''
    Author: Chandra S. Nepali 
    Date: July 4, 2017
    Summary: Program that detects anomaly purchases by users so that it can be feed/emailed to his/her friends to influence them for purchase.
             Each id in the input data is represented by the a class "Node". Each Node has a list of its first degree friends, and a list of 
			 tuples (order of purchases, timestamp of purchases, amount of purchases).
             Nodes are stored in a dictionary "Nodes" to map its "id" with the list of its first degree friends and purchases.
'''


# class to hold one node/vertex, a list of its first degree friends and a list of tuples of (purchase order, timestamp, purchase amount)
class Node:
	def __init__(self, idd):
		self.idd = idd									# user's id
		self.friends = []								# list of first degree friends
		self.purchases = []								# list of tuples of timestamps and purchase amounts

	def beFriend(self, idd):
		self.friends.append(idd)

	def addPurchase(self, order, time, amount):
		self.purchases.append((order, time, amount))	# (order of purchase, timestamp, amount)

	def unFriend(self, idd):
		self.friends.remove(idd)

	def getId(self):
		return self.idd

	def getFriends(self):
		return self.friends

	def getPurchases(self):
		return self.purchases
#-----------------------------------------------------------

# read the input file and initialize the graph
def Init(flBatch):
	print ' Initializing graph ....'
	Nodes = {}
	fl = open(flBatch, 'r')
	ftr = json.loads(fl.readline().strip())			# D and T

	i = 1
	for line in fl:
		line = line.strip()
		if not line: continue
		data = json.loads(line)

		Update(Nodes, data, i)
		i += 1

	fl.close()
	print ' done!'
	return ftr['D'], ftr['T'], Nodes
#------------------------------------------------------------

# update the Nodes
def Update(Nodes, data, i):
	if data['event_type'] == 'purchase':
		if data['id'] in Nodes:
			Nodes[data['id']].addPurchase(i, data['timestamp'], data['amount'])
		else:
			Nodes[data['id']] = Node(data['id'])
			Nodes[data['id']].addPurchase(i, data['timestamp'], data['amount'])

	elif data['event_type'] == 'befriend':
		if data['id1'] in Nodes and not data['id2'] in Nodes[data['id1']].getFriends():
			Nodes[data['id1']].beFriend(data['id2'])
		else:
			Nodes[data['id1']] = Node(data['id1'])
			Nodes[data['id1']].beFriend(data['id2'])

		if data['id2'] in Nodes and not data['id1'] in Nodes[data['id2']].getFriends():
			Nodes[data['id2']].beFriend(data['id1'])
		else:
			Nodes[data['id2']] = Node(data['id2'])
			Nodes[data['id2']].beFriend(data['id1'])

	elif data['event_type'] == 'unfriend':
		if data['id1'] in Nodes[data['id2']].getFriends():
			Nodes[data['id2']].unFriend(data['id1'])

		if data['id2'] in Nodes[data['id1']].getFriends():
			Nodes[data['id1']].unFriend(data['id2'])
#------------------------------------------------------------

# find all purchases by a social network
def Find_purchases(Nodes, friends):
	amt = []
	for node in friends:
		amt += (Nodes[node].getPurchases())
	
	amts = sorted(amt, key=lambda v: int(v[0]))		# sort according to purchase date/time, latest first
	return amts	

#------------------------------------------------------------

# find mean and sd
def Find_mean_sd(amt):
	mean = 0
	mean2 = 0
	for a in amt:
		mean += float(str(a[2]))
		mean2 += float(str(a[2])) * float(str(a[2]))
	
	mean = mean/len(amt)
	sd = math.sqrt(mean2/len(amt) - mean*mean)

	return mean, sd

#------------------------------------------------------------

# find a list of 'D' degree friends of a user with id = 'idd'
def Find_friends(Nodes, idd, D):
	friends = Nodes[idd].getFriends()
	
	tmp = friends[:]
	for i in range(1, int(D)):
		fr = []
		for node in tmp: 
			fr = fr + Nodes[node].getFriends()

		friends = friends + fr
		tmp = set(fr[:])

	return set(friends)			# 'set' to remove multiple entries

#-----------------------------------------------------------

# main program to read stream data, find anomaly and update Nodes from the stream data
def Find_anomaly(flStream, flOut, D, T, Nodes):
	fl = open(flStream, 'r')
	out = open(flOut, 'w')

	for line in fl:
		line = line.strip()
		if not line: continue
		data = json.loads(line)

		if data['event_type'] == 'purchase':
			if data['id'] in Nodes:
				friends = Find_friends(Nodes, data['id'], int(D))

				amts = Find_purchases(Nodes, friends)
				mean, sd = Find_mean_sd(amts[:int(T)])

				if float(str(data['amount'])) > (mean + 3*sd):
					mn = str("{0:.2f}".format(mean))
					sdd = str("{0:.2f}".format(sd))

					dt = (("event_type",data['event_type']), ("timestamp",data['timestamp']), ("id",data['id']), ("amount",data['amount']), ("mean",mn), ("sd",sdd))
					aa = OrderedDict(dt)
					out.write(json.dumps(aa, separators=(', ', ':')) + '\n')
#					print json.dumps(aa, separators=(', ', ':'))

		Update(Nodes, data, len(Nodes)+1)

	fl.close()
	out.close()
#------------------------------------------------------------

#--------------- Extras -------------------------------------

# first 'n' ('D" degree) largest social networks
def Largest_network(flOut2, Nodes, n, D):
	network = []
	flOut2  = open(flOut2, 'w')	

	for node in Nodes:
		friends = []
		friends = Find_friends(Nodes, node, D)
		network.append((node, len(friends)))

	networks = sorted(network, key=lambda v: int(v[1]), reverse=True)

	m = n 
	if len(Nodes) < n: m = len(Nodes)

	flOut2.write('First ' + str(m) + ' largest networks (D = ' + str(D) + ') are:\n\n')
	flOut2.write(('{0:<10} {1:<10}').format('id', 'size') + '\n')

	for i in range(m):
		flOut2.write(('{0:<10} {1:<10}').format(networks[i][0], str(networks[i][1])) + '\n')

	flOut2.close()
#------------------------------------------------------------


#  Main body ------------------------------------------------

arg = sys.argv

flBatch = arg[1]
flStream = arg[2]
flOut = arg[3]
flOut2 = arg[4]

D, T, Nodes = Init(flBatch)

Find_anomaly(flStream, flOut, D, T, Nodes)

Largest_network(flOut2, Nodes, 10, 2)
