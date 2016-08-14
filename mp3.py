import socket
import ConfigParser
import sys
from threading import Thread, Lock
import random
import json	
import time
import hashlib

ONE = 1
ALL = 9

# this function acquires locks before calling
# send_to_replicas, as an implementation of 
# our consistency levels
terminalLock = Lock()	# locks to make sure process_terminal blocks
def sendAndWait(keyhash, msg):
	terminalLock.acquire()	# lock and wait for infiniteListen to release
	send_to_replicas(keyhash, msg)
	terminalLock.acquire()	# the consistency level has been satisfied when this is acquired
	terminalLock.release()

# send_to_replicas is responsible for spawning
# threads to send messages to replicas
def send_to_replicas(keyhash, msg):
	msg = json.dumps(msg)
	replicas = [keyhash % num_users, (keyhash + 1) % num_users, (keyhash + 2) % num_users]
	
	# send the message to each replica
	for i in replicas:
		t = Thread(target=delay_send, args=(msg, i))
		t.setDaemon(True)
		t.start()

# helper function that introduces delays when sending messages
def delay_send(msg, dest_id):
	time.sleep(float(sys.argv[1 + dest_id]) * random.uniform(0, 2))
	sock.sendto(msg, (user_ips[dest_id], ports[dest_id]))

# changeLevel is a helper function we use to implement our 
# consistency levels, by setting the number of messages
# a client receives before responding to the user
level = 0
levelLock = Lock()	# lock on the value of level. (changeLevel competes with infiniteListen)
def changeLevel(val):
	global level
	levelLock.acquire()
	level = int(val)
	if level is not 1:
		level = 3
	levelLock.release()

# process_terminal handles user input at the client, 
# and is responsible for sending messages to the
# replicas
process_terminal_retVal = 0
def process_terminal(command):
	global process_terminal_retVal

	if command[0] != 'show-all':
		h = hashlib.md5()
		h.update(command[1])
		keyhash = int(h.hexdigest(), 16)

	# get Key Level
	if command[0] == 'get':
		process_terminal_retVal = None
		changeLevel(command[2])
		msg = {'command': 'get', 'key': command[1], 'timestamp': time.time(), 'src': user_id}
		sendAndWait(keyhash, msg)
		if process_terminal_retVal is None:
			print 'Get returned nothing'
		else:
			print 'Get returned:', process_terminal_retVal

	# insert Key Value Level
	elif command[0] == 'insert':
		changeLevel(command[3])
		msg = {'command': 'insert', 'key': command[1], 'value': command[2], 'timestamp': time.time(), 'src': user_id}
		sendAndWait(keyhash, msg)
		print 'Insert complete'

	# update Key Value Level
	elif command[0] == 'update':
		changeLevel(command[3])
		msg = {'command': 'update', 'key': command[1], 'value': command[2], 'timestamp': time.time(), 'src': user_id}
		sendAndWait(keyhash, msg)
		print 'Update complete'

	# delete Key
	elif command[0] == 'delete':
		msg = {'command': 'delete', 'key': command[1], 'timestamp': time.time(), 'src': user_id}
		send_to_replicas(keyhash, msg)
		print 'Delete complete'

	elif command[0] == 'show-all':
		# check if the file is empty
		if filesys.has_section('Values'):
			print filesys.items('Values')
		else:
			print 'Replica is empty'

	# search Key
	elif command[0] == 'search':
		process_terminal_retVal = []
		changeLevel(ALL)
		msg = {'command': 'search', 'key': command[1], 'timestamp': time.time(), 'src': user_id}
		sendAndWait(keyhash, msg)
		print 'Search returned:', process_terminal_retVal

# process_command is the function that handles
# user commands at the server upon receiving
# a message
def process_command(msg):
	global replica_filename

	retVal = {'requestTime': msg['timestamp']}

	# get Key Level
	if msg['command'] == 'get':
		# default values, in case the filesys.get() calls fail
		retVal['ackWriteTime'] = -2
		retVal['get'] = None
		try:
			retVal['ackWriteTime'] = filesys.getfloat('Times', msg['key'])
			retVal['get'] = filesys.get('Values', msg['key'])
		except:
			pass

	# insert/update Key Value Level
	elif msg['command'] == 'insert' or msg['command'] == 'update':
		try:
			t = filesys.getfloat('Times', msg['key'])
		except:
			t = -2
		if t < msg['timestamp']:
			write(msg['key'], msg['value'], msg['timestamp'])

	# delete Key
	elif msg['command'] == 'delete':
		try:
			t = filesys.getfloat('Times', msg['key'])
		except:
			t = -2
		if t < msg['timestamp']:
			try:
				filesys.remove_option('Values', msg['key'])
				filesys.set('Times', msg['key'], msg['timestamp'])
				with open(replica_filename, 'wb') as datafile:
					filesys.write(datafile) 
			except:
				pass

	# search Key
	elif msg['command'] == 'search':
		if filesys.has_option('Values', msg['key']):
			retVal['search'] = 1
		else:
			retVal['search'] = 0
	
	return json.dumps(retVal)

# infiniteListen is executed by a thread on each server
# to listen for incoming messages, processing them, and
# filtering them based on timestamps
def infiniteListen():
	global process_terminal_retVal
	global level

	retValLatest = -1	# write timestamp of Ack value (process_terminal_retVal)
	latestAck = -1	# timestamp of most recent Ack'd request

	while 1:
		data, src = sock.recvfrom(1024)
		data = json.loads(data)

		if 'command' in data.keys():
			print data['command']
			# call process_command() and send its returned value as a reply
			t = Thread(target=delay_send, args=(process_command(data), data['src']))
			t.setDaemon(True)
			t.start()
		else:	# this is an ACK
			# ignore all acks for requests earlier than the current
			if data['requestTime'] < latestAck:
				continue
			latestAck = data['requestTime']

			if 'search' in data.keys() and data['search'] == 1:
				process_terminal_retVal.append(src)
			elif 'get' in data.keys():
				# select the most recent value for GET requests
				if data['ackWriteTime'] > retValLatest:
					process_terminal_retVal = data['get']
					retValLatest = data['ackWriteTime']

			# release the terminal back to the user if consistency level satisfied
			levelLock.acquire()
			level -= 1
			if level == 0:
				terminalLock.release()
				retValLatest = -1	# reset the timestamp
			levelLock.release()


# this function is used to write key-value pairs to disk
def write(key, value, tiimestamp): 
	global replica_filename
	# if the data section is nonexistent, add it
	# when we delete the last entry in storage,
	# we also remove the 'Data' section, so it's
	# easier to check if a replica is empty
  	if not filesys.has_section('Values'):
		filesys.add_section('Values')
  	if not filesys.has_section('Times'):
		filesys.add_section('Times')
	
	# write into the actual file
	filesys.set('Values', key, value) 
	filesys.set('Times', key, timestamp) 
	with open(replica_filename, 'wb') as datafile:
		filesys.write(datafile) 

###################
### MAIN SCRIPT ###
###################
# parse the config file
# NOTE: CONFIG FILE NAME HARDCODED HERE
# DATA FILE TOO
config = ConfigParser.RawConfigParser()
config.read('config')
num_users = config.getint('Addresses', 'num_users')

if len(sys.argv) < num_users:
	print 'Insufficient parameters'
	sys.exit(0)

user_ips = []	# array of IP addresses
user_id = 0		# this client's index within user_ips and sequence
first_id_for_this_ip_found = False
ports = {}
base_port = 8000	# hope to god that ports 8000-8005 are available

# parse the config file some more for IP addresses
for i in range(num_users):
	temp = config.get('Addresses', 'p' + str(i+1))
	user_ips.append(temp)

	# find the first process ID for this machine of our IP address
	if (not first_id_for_this_ip_found
	and (socket.gethostbyname(socket.gethostname()) == temp
	or temp == '127.0.0.1')):
		user_id = i
		first_id_for_this_ip_found = True

	ports[i] = base_port + user_ips.count(temp) - 1

#if ip is not in config file, exit
if not first_id_for_this_ip_found:
	print 'I don\'t belong in this cluster Q_Q'
	sys.exit(0)

# set up the socket
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
commands = {'get': 3, 'insert': 4, 'update': 4, 'delete': 2, 'show-all': 1, 'search': 2} #list of commands and input lengths

# bind to a socket
address = (user_ips[user_id], base_port)
while(1):
	try:
		sock.bind(address)
		break
	#if fail, increment test_port and do again
	except socket.error:
		user_id += 1
		address = (address[0], address[1] + 1)

# initialize the filesystem
replica_filename = 'datastore_' + str(user_id)
filesys = ConfigParser.RawConfigParser()
filesys.read(replica_filename) 

# start a thread to receive messages and process 
# commands from other servers
t = Thread(target=infiniteListen)
t.setDaemon(True)
t.start()

# listen for user input
while 1:
	command = raw_input().split()
	# check for proper input format
	if(command[0] in commands.keys() and len(command) == commands[command[0]]):
		# process the command
		process_terminal(command)
	else:
		print 'Not a valid command.'
		print 'Usage:\ndelete Key\nget Key Level\ninsert Key Value Level\nupdate Key Value Level'

