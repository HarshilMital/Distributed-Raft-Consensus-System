import grpc
import raft_pb2
import raft_pb2_grpc
import time
import random
import sys
import os
import threading

from concurrent import futures

MAJORITY = 3
LEASE_TIMEOUT = 10

class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, selfid):
        self.id = selfid
        self.all_ids = [1,2,3,4,5] # LIST OF ALL IDs
        self.node_addresses = ['34.42.43.20:50051', '34.71.109.191:50052', '34.69.181.196:50053', '34.123.199.170:50054', '35.192.186.19:50055']
        # Persistent state
        self.currentTerm = 0
        self.votedFor = None
        self.log = [] # {'term' : term, 'command' : command}
        self.commitLength = 0


        #volatile
        self.currentRole = 'follower'
        self.currentLeader = None
        self.votesReceived = set()  # CHANGE IT!!! ITS a Dict or a set
        self.sentLength = dict() # node_id:length
        self.ackedLength = dict() # node_id:length
        self.db = dict() # key:value
        self.heartbeat_counter = 0

        self.election_timeout = self.calculate_election_timeout()
        self.lease_timeout = time.time() + LEASE_TIMEOUT

        self.channel = None
        self.stub = None

        self.load_peristent_state()

        self.loop_thread = threading.Thread(target=self.start_loop)
        self.loop_thread.daemon = True
        self.loop_thread.start()

    def update_currentTerm(self, term):
        with open(f'logs_node_{self.id}/metadata.txt', 'w') as f:
            f.write(f'{term} {self.votedFor} {self.commitLength}')
        self.currentTerm = term
    
    def update_votedFor(self, votedFor):
        with open(f'logs_node_{self.id}/metadata.txt', 'w') as f:
            if votedFor is None:
                f.write(f'{self.currentTerm} None {self.commitLength}')
            else:
                f.write(f'{self.currentTerm} {votedFor} {self.commitLength}')
        self.votedFor = votedFor

    def update_commitLength(self, commitLength):
        with open(f'logs_node_{self.id}/metadata.txt', 'w') as f:
            f.write(f'{self.currentTerm} {self.votedFor} {commitLength}')
        self.commitLength = commitLength

    def load_peristent_state(self):
        # Load persistent state from disk
        try:
            with open(f'logs_node_{self.id}/metadata.txt', 'r') as f:
                data = f.read().split()
                self.currentTerm = int(data[0])
                self.votedFor = int(data[1]) if data[1] != 'None' else None
                self.commitLength = int(data[2])
        except FileNotFoundError:
            with open(f'logs_node_{self.id}/metadata.txt', 'w') as f:
                f.write(f'{self.currentTerm} None {self.commitLength}')
                
        
        try:
            with open(f'logs_node_{self.id}/log.txt', 'r') as f:
                for line in f:
                    # split such that last is  word term and rest is command
                    command, term = line.rsplit(' ', 1)
                    self.log.append({'term': int(term), 'command': command})
        except FileNotFoundError:
            with open(f'logs_node_{self.id}/log.txt', 'w') as f:
                pass

        for i in range(self.commitLength):
            if self.log[i]['command'].split()[0] == 'SET':
                self.db[self.log[i]['command'].split()[1]] = self.log[i]['command'].split()[2]

    def write_to_dump(self, message):
        print(message)
        with open(f'logs_node_{self.id}/dump.txt', 'a') as f:
            f.write(message + '\n')



    def start_loop(self):

        print(f"Starting {self.id}")
        while True:
            if self.currentRole == 'follower':
                if time.time() > self.election_timeout:
                    self.write_to_dump(f'Node {self.id} election timer timed out, Starting election.')
                    self.currentRole = 'candidate'
                    self.update_currentTerm(self.currentTerm + 1)
                    self.update_votedFor(self.id)
                    self.votesReceived.add(self.id)
                    # self.election_timeout = self.calculate_election_timeout()
                    self.collect_votes()
                    self.election_timeout = self.calculate_election_timeout()
                    if self.currentRole == 'candidate':
                        self.currentRole = 'follower'
            
            elif self.currentRole == 'temp_leader':
                if time.time() > self.lease_timeout:
                    self.write_to_dump(f'Node {self.id} became the leader for term {self.currentTerm}.')
                    self.currentRole = 'leader'
                    self.currentLeader = self.id
                    self.election_timeout = self.calculate_election_timeout()

                    with open(f'logs_node_{self.id}/log.txt', 'a') as f:
                        f.write(f'NO-OP {self.currentTerm}\n')
                    self.log.append({'term': self.currentTerm, 'command': 'NO-OP'})
                    self.ackedLength[self.id] = len(self.log)

                    for addr in self.all_ids:
                        if(addr != self.id):
                            self.sentLength[addr] = len(self.log)
                            self.ackedLength[addr] = 0
                            self.replicateLog(self.id, addr)

            elif self.currentRole == 'leader':
                if time.time() > self.lease_timeout:
                    self.write_to_dump(f'Leader {self.id} lease renewal failed. Stepping Down.')
                    self.currentRole = 'follower'
                    self.currentLeader = None
                    self.votesReceived = set()
                    self.election_timeout = self.calculate_election_timeout()
                    self.lease_timeout = time.time() + LEASE_TIMEOUT
                else:
                    time.sleep(1)
                    self.periodically()

    def calculate_election_timeout(self):
        # Calculate a random election timeout between 5 and 10 seconds
        rand_time = random.randint(5, 10) + random.randint(1, 10)*0.001
        return time.time() + rand_time

    def RequestVote(self, request, context):
        response = raft_pb2.RequestVoteResponse()

        if request.term > self.currentTerm:
            self.update_currentTerm(request.term)
            self.currentRole = 'follower'
            self.update_votedFor(None)

        lastTerm = 0
        if len(self.log) > 0:
            lastTerm = self.log[-1]['term']
        
        # fix list out of index error
        if (request.term == self.currentTerm) and \
            (self.votedFor is None or self.votedFor == request.candidateId) and \
                (request.lastLogTerm > lastTerm or
                 (request.lastLogTerm == lastTerm and request.lastLogIndex >= len(self.log))):
            self.update_votedFor(request.candidateId)
            response.term = self.currentTerm
            response.voteGranted = True
            response.leaseDuration = self.lease_timeout - time.time()
            self.election_timeout = self.calculate_election_timeout()
            self.write_to_dump(f'Vote granted for Node {request.candidateId} in term {request.term}.')
            return response

        response.term = self.currentTerm
        response.voteGranted = False
        response.leaseDuration = self.lease_timeout - time.time()

        self.write_to_dump(f'Vote denied for Node {request.candidateId} in term {request.term}.')
        return response
    

    def collect_votes(self):
        
        lastTerm = 0
        if len(self.log) > 0: lastTerm = self.log[-1]['term']

        for address in self.all_ids:
            # Send a RequestVote RPC to each server
            request = raft_pb2.RequestVoteRequest(
                term=self.currentTerm,
                candidateId=self.id,
                lastLogIndex=len(self.log),
                lastLogTerm=lastTerm
            )
            with grpc.insecure_channel(self.node_addresses[address-1]) as channel:
                try:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    response = stub.RequestVote(request)
                    print("Vote Request RPC sent from", self.id, "to", address)

                except grpc.RpcError as e:
                    self.write_to_dump(f'Error occurred while sending RPC to Node {address}.')
                    continue

            self.lease_timeout = max(time.time() + response.leaseDuration, self.lease_timeout)

            if self.currentRole == 'candidate' and response.voteGranted and self.currentTerm == response.term:
                print(address ,"voted for", self.id)
                self.votesReceived.add(address)
                if len(self.votesReceived) >= MAJORITY:
                    self.write_to_dump(f'New Leader waiting for Old Leader Lease to timeout.')
                    self.currentRole = 'temp_leader'

            elif response.term > self.currentTerm:
                self.update_currentTerm(response.term)
                self.currentRole = 'follower'
                self.update_votedFor(None)
                # CANCEL ELECTION TIMER ?
                self.election_timeout = self.calculate_election_timeout()

    # THIS REQUEST TO MESSAGE IS COMING FROM THE CLIENT 
    def broadcast_message(self, request):
        log_record = {'command': request, 'term': self.currentTerm}
        with open(f'logs_node_{self.id}/log.txt', 'a') as f:
            f.write(f'{request} {self.currentTerm}\n')
            self.log.append(log_record)
        
        self.ackedLength[self.id] = len(self.log)
        
        for follower_id in self.all_ids:
            if follower_id != self.id:
                self.replicateLog(self.id, follower_id)

    def ServeClient(self, request, context):
        if self.currentRole == 'leader':
            self.write_to_dump(f'Node {self.id} (leader) received an {request.Request} request.')
            if request.Request.split()[0] == 'SET':
                self.broadcast_message(request.Request)
                return raft_pb2.ServeClientReply(Data='Entry Updated', LeaderID=self.currentLeader, Success=True)
            elif request.Request.split()[0] == 'GET':
                key = request.Request.split()[1]
                if key in self.db:
                    return raft_pb2.ServeClientReply(Data=self.db[key], LeaderID=self.currentLeader, Success=True)
                else:
                    return raft_pb2.ServeClientReply(Data='', LeaderID=self.currentLeader, Success=True)

        else:
            return raft_pb2.ServeClientReply(Data=f'Update Leader, LeaderId = {self.currentLeader}', LeaderID=self.currentLeader, Success=False)

    # HEARTBEAT ----> CHANGE/INTERGRATE THIS WITH THE MAIN RUN LOOP
    def periodically(self):
        if self.currentRole == 'leader':
            self.write_to_dump(f'Leader {self.id} sending heartbeat & Renewing Lease')
            self.heartbeat_counter = 1
            for follower_id in self.all_ids:
                if follower_id != self.id:
                    self.replicateLog(self.id, follower_id)            

    def replicateLog(self, leaderId, followerId):
        prefix_len = self.sentLength[followerId]
    
        suffix = self.log[prefix_len:]
        suffix_sent = [raft_pb2.Entry(term=i['term'], command=i['command']) for i in suffix]
            
        
        prefix_term = 0

        if prefix_len > 0:
            prefix_term = self.log[prefix_len - 1]['term']
        # {'term' : term, 'command' : command}
        # Send LogRequest message to follower
            
        message = raft_pb2.LogRequest(
            leaderId=leaderId,
            term=self.currentTerm,
            prefixLen=prefix_len,
            prefixTerm=prefix_term,
            leaderCommit=self.commitLength,
            suffix=suffix_sent,
            leaseInterval=LEASE_TIMEOUT
        )

        with grpc.insecure_channel(self.node_addresses[followerId-1]) as channel:
            try:
                stub = raft_pb2_grpc.RaftStub(channel)
                response = stub.ProcessLog(message)
            except grpc.RpcError as e:
                self.write_to_dump(f'Error occurred while sending RPC to Node {followerId}.')
                return

        # SLIDE 8/9 BELOW HERE: we are using processing the log and getting the response being used in 8/9

        self.heartbeat_counter += 1
        if (self.heartbeat_counter >= MAJORITY):
            self.lease_timeout = time.time() + LEASE_TIMEOUT
        
        if (response.term == self.currentTerm) and (self.currentRole == 'leader'):
            if (response.success and response.ack >= self.ackedLength[response.follower]):
                self.sentLength[response.follower] = response.ack
                self.ackedLength[response.follower] = response.ack
                self.CommitLogEntries()
            elif self.sentLength[response.follower] > 0:
                self.sentLength[response.follower] = self.sentLength[response.follower] - 1
                self.replicateLog(self.id, response.follower)
        
        elif response.term > self.currentTerm:
            self.update_currentTerm(response.term)
            self.write_to_dump(f'{self.id} Stepping down')
            self.currentRole = 'follower'
            self.votesReceived = set()
            # CANCEL ELECTION TIMER
            self.election_timeout = self.calculate_election_timeout()

    def acks(self, in_len):
        counter = 0
        for addrs in self.all_ids:
            if (self.ackedLength.get(addrs) != None and self.ackedLength.get(addrs) >= in_len):
                counter+=1

        return counter

    def CommitLogEntries(self):
        minAcks = MAJORITY
        ready = [i for i in range(1, len(self.log)+1) if self.acks(i) >= minAcks]
        if len(ready) != 0 and max(ready) > self.commitLength and self.log[max(ready)-1]['term'] == self.currentTerm:

            for i in range(self.commitLength, max(ready)): 
                if self.log[i]['command'].split()[0] == 'SET':
                    self.write_to_dump(f'Node {self.id} (leader) committed the entry {self.log[i]} to the state machine.')
                    self.db[self.log[i]['command'].split()[1]] = self.log[i]['command'].split()[2]  

            self.update_commitLength(max(ready))

    

    def ProcessLog(self, request, context):
        print(f'{self.id} Processing log request from {request.leaderId}, term = {self.currentTerm}')
        self.election_timeout = self.calculate_election_timeout()
        self.lease_timeout = time.time() + request.leaseInterval
        if request.term > self.currentTerm:
            print(f"Updating term from {self.currentTerm} to {request.term} (ProcessLog)")
            self.update_currentTerm(request.term)
            self.update_votedFor(None)
            # Cancel election timer

        if request.term == self.currentTerm:
            self.currentRole = 'follower'
            self.currentLeader = request.leaderId
            
        logOk = (len(self.log) >= request.prefixLen and (request.prefixLen == 0 or self.log[request.prefixLen - 1]['term'] == request.prefixTerm))

        if request.term == self.currentTerm and logOk:
            self.AppendEntries(request.prefixLen, request.leaderCommit, request.suffix)
            ack = request.prefixLen + len(request.suffix)
            self.write_to_dump(f'Node {self.id} accepted AppendEntries RPC from {request.leaderId}.')
            return raft_pb2.LogResponse(follower = self.id, term=self.currentTerm,ack = ack, success=True)
        else:
            self.write_to_dump(f'Node {self.id} rejected AppendEntries RPC from {request.leaderId}.')
            return raft_pb2.LogResponse(follower = self.id, term=self.currentTerm,ack = 0, success=False)
        


    def AppendEntries(self, prefixLen, leaderCommit, suffix):
        if len(suffix) > 0 and len(self.log) > prefixLen:
            index = min(len(self.log), prefixLen+len(suffix)) -1 

            if(self.log[index]['term'] != suffix[index-prefixLen].term):
                self.log = self.log[:prefixLen]
                with open(f'logs_node_{self.id}/log.txt', 'w') as f:
                    for i in range(prefixLen):
                        f.write(f'{self.log[i]["command"]} {self.log[i]["term"]}\n')
                

        if prefixLen + len(suffix) > len(self.log):
            with open(f'logs_node_{self.id}/log.txt', 'a') as f:
                for i in range(len(self.log)-prefixLen, len(suffix)):
                    self.log.append({'term':suffix[i].term, 'command': suffix[i].command})
                    f.write(f'{suffix[i].command} {suffix[i].term}\n')

        if leaderCommit > self.commitLength:
            for i in range(self.commitLength, leaderCommit):
                if self.log[i]['command'].split()[0] == 'SET':
                    self.write_to_dump(f'Node {self.id} (follower) committed the entry {self.log[i]} to the state machine.')
                    self.db[self.log[i]['command'].split()[1]] = self.log[i]['command'].split()[2]

            self.update_commitLength(leaderCommit)
            
                

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=100))
    node_obj = RaftNode(int(sys.argv[1]))
    raft_pb2_grpc.add_RaftServicer_to_server(node_obj, server)
    server.add_insecure_port(f'[::]:{sys.argv[2]}')
    server.start()

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)
        print("Server stopped")

if __name__ == '__main__':
    serve()


