
import grpc
import raft_pb2_grpc
import raft_pb2

class RaftClient:
    def __init__(self, address):
        self.channel = grpc.insecure_channel(address)
        self.stub = raft_pb2_grpc.RaftStub(self.channel)
        self.node_ips = ['34.42.43.20:50051', '34.71.109.191:50052', '34.69.181.196:50053', '34.123.199.170:50054', '35.192.186.19:50055']
        self.leader_id = 1

    def set_leader(self):
        self.channel = grpc.insecure_channel(self.node_ips[self.leader_id-1])
        self.stub = raft_pb2_grpc.RaftStub(self.channel)

    def send_command(self, command):
        try:
            request = raft_pb2.ServeClientArgs(Request=command)
            response = self.stub.ServeClient(request)
        except grpc.RpcError as e:
            self.leader_id = self.leader_id + 1 if self.leader_id < 5 else 1
            self.set_leader()
            print(f'Sending to Leader: {self.node_ips[self.leader_id-1]}')
            self.send_command(command)


        if (response.Success) and (command.split()[0] == 'SET'):
            print('Entry Updated')
        elif (response.Success) and (command.split()[0] == 'GET'):
            print(f'Value: {response.Data}')
        else:
            if response.LeaderID == 0:
                self.leader_id = self.leader_id + 1 if self.leader_id < 5 else 1
                self.set_leader()
            else:
                self.leader_id = response.LeaderID
            
            print(f'Failed, Resend to Leader: {self.leader_id}')
            self.set_leader()
            print(f'Sending to Leader: {self.node_ips[self.leader_id-1]}')
            self.send_command(command)


def main():
    raft_client = RaftClient('34.42.43.20:50051')

    while True:
        print("\nMenu:")
        print("1. SET K V")
        print("2. GET K")
        print("3. Exit")
        choice = input("Enter choice: ")
        if choice == '1':
            key = input("Enter key: ")
            value = input("Enter value: ")
            raft_client.send_command(f'SET {key} {value}')
        elif choice == '2':
            key = input("Enter key: ")
            raft_client.send_command(f'GET {key}')
        elif choice == '3':
            break
        else:
            print("Invalid choice")


if __name__ == '__main__':
    main()



