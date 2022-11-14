# Python 3.6 +
# Author: chowyi.com

import sys
import json
import uuid
import time
import socket
import struct
import threading
import stun

CLIENT_HELLO_MESSAGE = 'ClientHelloMessage'
SERVER_HELLO_MESSAGE = 'ServerHelloMessage'
SERVER_REFUSE_MESSAGE = 'ServerRefuseMessage'
PEER_INFO_MESSAGE = 'PeerInfoMessage'
PEER_HELLO_MESSAGE = 'PeerHelloMessage'
PEER_NOT_FOUND_MESSAGE = 'PeerNotFoundMessage'
PEER_PORT_MESSAGE = 'PeerPortMessage'

ACTION_LISTEN = 'Listen'
ACTION_CONNECT = 'Connect'
ACTION_TRY_AND_LISTEN = 'TryAndListen'
ACTION_DELAY_CONNECT = 'DelayConnect'
ACTION_TRY_AND_REPORT = 'TryAndReport'
ACTION_STANDBY = 'Standby'


def send_msg(sock, msg):
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg.encode('utf-8')
    sock.sendall(msg)


def recv_by_bytes(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data


def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recv_by_bytes(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recv_by_bytes(sock, msglen).decode('utf-8')


def start_p2p_listen(client_id, private_ip, private_port):
    private_ip = ''
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        s.bind((private_ip, private_port))
        s.listen(1)
        s.settimeout(30)
        while True:
            try:
                print('Client({client_id}) try to listening at {private_ip}:{private_port}'.format(client_id=client_id, private_ip=private_ip, private_port=private_port))
                conn, addr = s.accept()
            except socket.timeout:
                print('Client({client_id}) listened timeout at {private_ip}:{private_port}'.format(client_id=client_id, private_ip=private_ip, private_port=private_port))
                continue
            else:
                print('Client({client_id}) listened from peer({addr}).'.format(client_id=client_id, addr=str(addr)))
                break

        while True:
            msg_str = recv_msg(conn)
            if msg_str is None:
                continue
            message = json.loads(msg_str)
            print('Received: {message}'.format(message=str(message)))

            number = message['number']
            number += 1
            peer_hello_message = {
                'type': PEER_HELLO_MESSAGE,
                'content': '[{n}] Hello, this is Client({client_id}).'.format(n=number, client_id=client_id),
                'number': number
            }
            send_msg(conn, json.dumps(peer_hello_message))
            print('Send: {message}'.format(message=str(peer_hello_message)))


def start_p2p_connect(client_id, peer_ip, peer_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        while True:
            try:
                print('Client({client_id}) try to connecting at {peer_ip}:{peer_port}'.format(client_id=client_id, peer_ip=peer_ip, peer_port=peer_port))
                s.connect((peer_ip, peer_port))
            except socket.error as e:
                print('Client({client_id}) connect failed at {peer_ip}:{peer_port}'.format(client_id=client_id, peer_ip=peer_ip, peer_port=peer_port))
                print(str(e))
            else:
                print('Client({client_id}) connected success to Peer({peer_ip}:{peer_port})'.format(client_id=client_id, peer_ip=peer_ip, peer_port=peer_port))
                break

        number = 0
        while True:
            number += 1
            peer_hello_message = {
                'type': PEER_HELLO_MESSAGE,
                'content': '[{n}] Hello, this is Client({client_id}).'.format(n=number, client_id=client_id),
                'number': number
            }
            send_msg(s, json.dumps(peer_hello_message))
            print('Send: {message}'.format(message=str(peer_hello_message)))
            message = json.loads(recv_msg(s))
            print('Received: {message}'.format(message=str(message)))
            number = message['number']
            time.sleep(2)


def try_p2p_connect(client_id, peer_ip, peer_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        s.settimeout(3)
        try:
            print('Client({client_id}) try to connecting at {peer_ip}:{peer_port}'.format(client_id=client_id, peer_ip=peer_ip, peer_port=peer_port))
            print('This try must be failed, just for ready to receive from peer next time.')
            s.connect((peer_ip, peer_port))
        except socket.error:
            return
        else:
            return s.getsockname()


def p2p_connect_actions(p1, p2):
    """
    根据两端不通的NAT类型组合，决定双方建立p2p连接的步骤
    :return:
    """
    pair = (p1['nat_type'], p2['nat_type'])
    if pair == (stun.SymmetricNAT, stun.SymmetricNAT):
        # 两端都是 SymmetricNAT，打洞难度大，暂时跳过
        return None, None
    if pair == (stun.SymmetricNAT, stun.RestricPortNAT) or pair == (stun.RestricPortNAT, stun.SymmetricNAT):
        # 一端是 SymmetricNAT，另一端是 RestrictPortNAT，打洞难度大，暂时跳过
        return None, None

    if pair == (stun.RestricPortNAT, stun.RestricPortNAT):
        # 两端都是 RestrictPortNAT
        return ACTION_TRY_AND_REPORT, ACTION_STANDBY

    if stun.FullCone in pair:
        # 至少有一端是 FullCone的，共四种组合
        a, b = ACTION_LISTEN, ACTION_CONNECT
        return (a, b) if p1['nat_type'] == stun.FullCone else (b, a)

    if stun.RestricNAT in pair or stun.RestricPortNAT in pair:
        # 四三种组合
        # RestrictNAT - RestrictPortNAT
        # RestrictNAT - RestrictNAT
        # RestrictNAT - SymmetricNAT
        a, b = ACTION_TRY_AND_LISTEN, ACTION_DELAY_CONNECT
        return (a, b) if p1['nat_type'] == stun.RestricNAT else (b, a)


def start_server(host, port):
    clients = {}
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(1)
        s.settimeout(30)
        print('Server starting listen at {host}:{port}'.format(host=host, port=port))

        while True:
            try:
                conn, addr = s.accept()
                print('Accepted connection from Client({client_ip}:{client_port})'.format(client_ip=addr[0], client_port=addr[1]))
            except socket.timeout:
                continue

            message = json.loads(recv_msg(conn))

            # 如果是来自客户端的Hello消息
            if message['type'] == CLIENT_HELLO_MESSAGE:
                print('-----Received Client Hello Message-----')
                print('Client ID: {client_id}'.format(client_id=message['client_id']))
                print('Client NAT: {nat_type}'.format(nat_type=message['nat_type']))
                print('Client Private Addr: {private_ip}:{private_port}'.format(private_ip=message['private_ip'], private_port=message['private_port']))
                print('Peer ID: {peer_id}'.format(peer_id=message['peer_id']))

                # 如果客户端ID已存在于服务端，回复拒绝消息，关闭socket
                if message['client_id'] in clients.keys():
                    print('Refuse hello from Client({client_id}), Client ID is already in used.'.format(client_id=message['client_id']))
                    refuse_message = {
                        'type': SERVER_REFUSE_MESSAGE,
                        'client_id': message['client_id'],
                        'content': 'Client ID is already in used.'
                    }
                    send_msg(conn, json.dumps(refuse_message))
                    conn.close()
                    continue
                else:
                    # 保存客户端的信息
                    clients[message['client_id']] = {
                        'client_id': message['client_id'],
                        'nat_type': message['nat_type'],
                        'conn': conn,
                        'addr': addr,
                        'private_addr': (message['private_ip'], message['private_port']),
                        'peer_id': message['peer_id']
                    }
                    print('Client({client_id}) info saved.'.format(client_id=message['client_id']))

                    # 回复客户端的Hello消息
                    hello_message = {
                        'type': SERVER_HELLO_MESSAGE,
                        'client_id': message['client_id'],
                        'public_ip': addr[0],
                        'public_port': addr[1]
                    }
                    send_msg(conn, json.dumps(hello_message))

                    # 如果客户端没有指定要连接的peer或指定的peer不在线，什么也不做
                    if message['peer_id'] is None:
                        continue

                    client = clients.get(message['client_id'])
                    peer = clients.get(message['peer_id'])
                    if not peer:
                        print('Client({client_id}) specified Peer({peer_id}) not found.'.format(client_id=message['client_id'], peer_id=message['peer_id']))
                        peer_not_found_message = {
                            'type': PEER_NOT_FOUND_MESSAGE,
                            'peer_id': message['peer_id']
                        }
                        send_msg(client['conn'], json.dumps(peer_not_found_message))
                        del clients[message['client_id']]
                        print('Client({client_id}) info popped.'.format(client_id=message['client_id']))
                        continue

                    # 记录匹配的两端关联ID
                    clients[message['peer_id']]['peer_id'] = message['client_id']

                    # 根据双方NAT类型分析双方建立p2p连接的步骤
                    client_action, peer_action = p2p_connect_actions(client, peer)

                    # 向双方分别发送对方的连接信息和需要进行的操作
                    peer_info_message = {
                        'type': PEER_INFO_MESSAGE,
                        'action': client_action,
                        'peer': {
                            'client_id': peer['client_id'],
                            'nat_type': peer['nat_type'],
                            'public_ip': peer['addr'][0],
                            'public_port': peer['addr'][1],
                        }
                    }
                    send_msg(client['conn'], json.dumps(peer_info_message))

                    client_info_message = {
                        'type': PEER_INFO_MESSAGE,
                        'action': peer_action,
                        'peer': {
                            'client_id': client['client_id'],
                            'nat_type': client['nat_type'],
                            'public_ip': client['addr'][0],
                            'public_port': client['addr'][1],
                        }
                    }
                    send_msg(peer['conn'], json.dumps(client_info_message))

                    if client_action not in (ACTION_TRY_AND_REPORT, ACTION_STANDBY):
                        # 如果两端已不再需要Server中介，则从服务器上移除已经匹配的两端信息
                        del clients[message['peer_id']]
                        del clients[message['client_id']]
                        print('Client({client_id}) info popped.'.format(client_id=message['peer_id']))
                        print('Client({client_id}) info popped.'.format(client_id=message['client_id']))
            elif message['type'] == PEER_PORT_MESSAGE:
                print('-----Received Peer Port Message-----')
                print('Peer ID: {client_id}'.format(client_id=message['client_id']))
                print('Peer Port: {port}'.format(port=message['port']))

                client_id = message['client_id']
                peer_id = clients[client_id]['peer_id']
                # 服务端把客户端回报的Port信息转发给另一端
                peer = clients[peer_id]
                send_msg(peer['conn'], json.dumps(message))

                # 从服务器上移除已经匹配的两端信息
                del clients[client_id]
                del clients[peer_id]
                print('Client({client_id}) info popped.'.format(client_id=client_id))
                print('Client({client_id}) info popped.'.format(client_id=peer_id))


def start_client(server_host='Server端的PublicIP', server_port=15005, peer_id=None):
    client_id = str(uuid.uuid4()).split('-')[0]
    print('Client ID: {client_id}'.format(client_id=client_id))
    print('Checking client NAT type...')
    nat_type = 'unknown'
    try:
        nat_type, _, _ = stun.get_ip_info()
    except:
        print('Checking NAT Type failed, skip this step.')
    else:
        print('Client NAT: {nat_type}'.format(nat_type=nat_type))

    print('Ready to connect server at {server_addr}...'.format(server_addr=str((server_host, server_port))))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect((server_host, server_port))
        private_addr = s.getsockname()
        print('Server connected. Client private_addr is {private_addr}'.format(private_addr=str(private_addr)))

        hello_message = {
            'type': CLIENT_HELLO_MESSAGE,
            'client_id': client_id,
            'nat_type': nat_type,
            'private_ip': private_addr[0],
            'private_port': private_addr[1],
            'peer_id': peer_id
        }
        send_msg(s, json.dumps(hello_message))

        peer_info = {}

        while True:
            message = json.loads(recv_msg(s))
            if message['type'] == SERVER_HELLO_MESSAGE:
                print('-----Received Server Hello Message-----')
                print('Client ID: {client_id}'.format(client_id=message['client_id']))
                print('Client Public Addr: {public_ip}:{public_port}'.format(public_ip=message['public_ip'], public_port=message['public_port']))
            elif message['type'] == SERVER_REFUSE_MESSAGE:
                print('-----Received Server Refuse Message-----')
                print('Client ID: {client_id}'.format(client_id=message['client_id']))
                print('Message Content: {content}'.format(content=message['content']))
                return
            elif message['type'] == PEER_NOT_FOUND_MESSAGE:
                print('-----Received Peer Not Found Message-----')
                print('Peer ID: {client_id}'.format(client_id=message['peer_id']))
                return
            elif message['type'] == PEER_INFO_MESSAGE:
                print('-----Received Peer Info Message-----')
                print('Peer ID: {client_id}'.format(client_id=message['peer']['client_id']))
                print('Peer NAT: {nat_type}'.format(nat_type=message['peer']['nat_type']))
                print('Peer Addr: {public_ip}:{public_port}'.format(public_ip=message['peer']['public_ip'], public_port=message['peer']['public_port']))
                print('Build p2p Action: {action}'.format(action=str(message['action'])))
                peer_info['client_id'] = message['peer']['client_id']
                peer_info['nat_type'] = message['peer']['nat_type']
                peer_info['public_ip'] = message['peer']['public_ip']
                peer_info['public_port'] = message['peer']['public_port']

                if not message['action']:
                    print('Not Support build p2p connection between {t1} and {t2}.'.format(t1=nat_type, t2=message['peer']['nat_type']))
                    return

                if message['action'] == ACTION_TRY_AND_LISTEN:
                    try_p2p_connect(client_id, peer_info['public_ip'], peer_info['public_port'])

                    thread = threading.Thread(target=start_p2p_listen, args=(client_id, private_addr[0], private_addr[1]))
                    thread.start()
                    thread.join()
                elif message['action'] == ACTION_TRY_AND_REPORT:
                    private_addr_for_peer = try_p2p_connect(client_id, peer_info['public_ip'], peer_info['public_port'])
                    peer_port_message = {
                        'type': PEER_PORT_MESSAGE,
                        'client_id': client_id,
                        'port': private_addr_for_peer[1]
                    }
                    # 与Server建立新的socket回传对peer新开放的port
                    print('Report new port({port}) to Server.'.format(port=private_addr_for_peer[1]))
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        s.connect((server_host, server_port))
                        send_msg(s, json.dumps(peer_port_message))

                    # 监听新的port
                    thread = threading.Thread(target=start_p2p_listen, args=(client_id, private_addr[0], peer_port_message[1]))
                    thread.start()
                    thread.join()

                elif message['action'] == ACTION_STANDBY:
                    continue
                elif message['action'] == ACTION_DELAY_CONNECT:
                    time.sleep(3)
                    thread = threading.Thread(target=start_p2p_connect, args=(client_id, peer_info['public_ip'], peer_info['public_port']))
                    thread.start()
                    thread.join()
                elif message['action'] == ACTION_CONNECT:
                    thread = threading.Thread(target=start_p2p_connect, args=(client_id, peer_info['public_ip'], peer_info['public_port']))
                    thread.start()
                    thread.join()
                elif message['action'] == ACTION_LISTEN:
                    thread = threading.Thread(target=start_p2p_listen, args=(client_id, private_addr[0], private_addr[1]))
                    thread.start()
                    thread.join()
                else:
                    print('Unknown action')
            elif message['type'] == PEER_PORT_MESSAGE:
                print('-----Received Peer Port Message-----')
                print('Peer ID: {client_id}'.format(client_id=message['client_id']))
                print('Peer Port: {port}'.format(port=message['port']))
                peer_info['public_port'] = message['port']
                thread = threading.Thread(target=start_p2p_connect, args=(client_id, peer_info['public_ip'], peer_info['public_port']))
                thread.start()
                thread.join()
            else:
                print('Received Unknown type message from Server. Message: {message}'.format(message=str(message)))


def main():
    argv = sys.argv
    if len(argv) < 2:
        print('Invalid arguments: mode must be one of (server, client).')
        return

    if argv[1] == 'server':
        if len(argv[2:]) == 1:
            port = int(argv[2])
            start_server('0.0.0.0', port)
        else:
            print('Invalid arguments: eg. server <port>')
    elif argv[1] == 'client':
        if len(argv[2:]) == 0:
            start_client()
        elif len(argv[2:]) == 1:
            start_client(peer_id=argv[2])
        elif len(argv[2:]) == 2:
            host, port = argv[2], int(argv[3])
            start_client(server_host=host, server_port=port)
        elif len(argv[2:]) == 3:
            host, port, peer_id = argv[2], int(argv[3]), argv[4]
            start_client(server_host=host, server_port=port, peer_id=peer_id)
        else:
            print('Invalid arguments: eg. client <ip> <port> [peer_id]')
            return
    else:
        print('mode must be one of (server, client}')
        return


if __name__ == '__main__':
    main()
