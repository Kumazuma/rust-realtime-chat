extern crate chrono;
extern crate num_cpus;
extern crate crypto_hash;
#[macro_use(object)]
extern crate json;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::sync::mpsc::{channel, Receiver, Sender};
use chrono::offset::utc::UTC;
use std::sync::Arc;
use std::collections::LinkedList;
use chrono::datetime::DateTime;
use std::sync::Mutex;
use std::time::Duration;
use std::io::{Error, ErrorKind, Read, Write};
use std::io;
use std::collections::BTreeMap;
enum CommandType {
    ChangeName {
        new_name: String,
    },
}
enum ChatMessageType {
    Text {
    	hash: String,
        text: String,
    },
    File {
        bytes: Arc<Vec<u8>>,
    },
    Image {
        bytes: Arc<Vec<u8>>,
    },
    Command {
        c_type: CommandType,
    },
    NewMemberGoInRoom{
    	name:String,
    	members:Vec<String>
    },
    MemberOutOnRoom{
    	name:String,
    	members:Vec<String>
    }
}
struct ChatMessage {
    uid: u64,
    room: String,
    message_type: ChatMessageType,
}
enum EventMessage {
    ConnectedRecvSocket{
        uid: u64,
        identify_hash: String ,
        init_name:String
    },
    GetIdentifyHash{
    	socket:TcpStream,
    	identify_hash:String
    },
    SendSocketRemove {
        uid: u64,
    }, // 송신소켓이 에러가 나서 리스트에서 제거해야 한다.
    RecvSocketRemove {
        uid: u64,
    }, // 수신소켓이 에러가 나서 리스트에서 제거해야 한다.
    RecvChatMesage {
        msg: ChatMessage,
    },
}
impl ChatMessage {
    fn new(uid: u64, room: String, message_type: ChatMessageType) -> ChatMessage {
        ChatMessage {
            uid: uid,
            room: room,
            message_type: message_type,
        }
    }
}


struct UserIndexManager {
    last_user_id: u64,
    user_idxes: Vec<u64>,
}
impl UserIndexManager {
    fn new() -> UserIndexManager {
        UserIndexManager {
            last_user_id: 0u64,
            user_idxes: Vec::new(),
        }
    }
    fn get_new_id(&mut self) -> u64 {
        self.last_user_id = self.last_user_id + 1u64;
        self.user_idxes.push(self.last_user_id);
        return self.last_user_id;
    }
    fn remove_user_id(&mut self, idx: u64) -> bool {
        let size = self.user_idxes.len();
        for i in 0..size {
            if self.user_idxes[i] == idx {
                self.user_idxes.swap_remove(i);
                return true;
            }
        }
        return false;
    }
}


struct Room {
    entered_user_uids: Vec<u64>,
}
struct StreamItem {
    uid: u64,
    socket: TcpStream,
    buffer: Vec<u8>,
}
impl StreamItem {
    fn new(sock: TcpStream, uid: u64) -> StreamItem {
        StreamItem {
            uid: uid,
            socket: sock,
            buffer: Vec::new(),
        }
    }
    fn get_uid(&self) -> u64 {
        return self.uid;
    }
    fn read_message(&mut self) -> Result<ChatMessage, bool> {
        let mut read_bytes = [0u8; 1024];
        let res_code = self.socket.read(&mut read_bytes);

        if let Err(e) = res_code {
            let exception = ErrorKind::TimedOut != e.kind();
            return Err(exception);
        }

        let read_size: usize = res_code.unwrap();
        let mut message_block_end = false;
        let mut string_memory_block: Vec<u8> = Vec::new();
        if self.buffer.len() != 0 {
            for byte in &self.buffer {
                string_memory_block.push(*byte);
            }
            self.buffer.clear();
        }
        for i in 0..read_size {
            if read_bytes[i] == b'\n' && message_block_end == false {
                message_block_end = true;
            } else if message_block_end == false {
                string_memory_block.push(read_bytes[i]);
            } else {
                self.buffer.push(read_bytes[i]);
            }
        }
        if message_block_end == true {
            let mut message: String = match String::from_utf8(string_memory_block) {
                Ok(value) => value,
                Err(_) => {
                    return Err(true);
                } 
            };
            return match self.parse_string(message) {
                Ok(message) => Ok(message),
                Err(_) => Err(true),
            };
        }
        return Err(false);
    }
    fn parse_string(&mut self, message: String) -> Result<ChatMessage, ()> {
    	println!("{} parsing try!",message);
        let json_message = match json::parse(&message) {
            Ok(v) => v,
            Err(_) => {
            	println!("{} parsing error!",message);
                return Err(());
            }
        };
        // 받은 메시지는 {type:string,hash:string, room:string, value:string}으로 이루어질 것이다. 아니면 잘못 보낸 것임.
        let json_object = match json_message {
            json::JsonValue::Object(ref object) => object,
            _ => {
            	println!("line 186 parsing error!");
                return Err(());
            }
        };
        let type_string: String = match json_object.get("type") {
            Some(v) =>match v{
            		&json::JsonValue::String(ref value)=>value.clone(),
            		&json::JsonValue::Short(value) =>value.as_str().to_string(),
            		_=>{
            			println!("line 196 parsing error!");
	                    return Err(());
            		}
            	},
            None => {
            	println!("line 200 parsing error!");
                return Err(());
            }
        };

        let room: String = match json_object.get("room") {
            Some(v) =>match v{
            		&json::JsonValue::String(ref value)=>value.clone(),
            		&json::JsonValue::Short(value) =>value.as_str().to_string(),
            		_=>{
            			println!("line 211 parsing error!");
	                    return Err(());
            		}
            	},
            None => {
            	println!("line 216 parsing error!");
                return Err(());
            }
        };
		let hash: String = match json_object.get("hash") {
            Some(v) =>match v{
            		&json::JsonValue::String(ref value)=>value.clone(),
            		&json::JsonValue::Short(value) =>value.as_str().to_string(),
            		_=>{
            			println!("line 223 parsing error!");
	                    return Err(());
            		}
            	},
            None => {
            	println!("line 228 parsing error!");
                return Err(());
            }
        };
        let text = match json_object.get("value") {
            Some(v) => match v{
            		&json::JsonValue::String(ref value)=>value.clone(),
            		&json::JsonValue::Short(value) =>value.as_str().to_string(),
            		_=>{
            			println!("line 237 parsing error!");
	                    return Err(());
            		}
            	},
            None => {
            	println!("line 242 parsing error!");
                return Err(());
            }
        };
        let chat_type = match type_string.as_str() {
            "TEXT" => ChatMessageType::Text { text: text, hash:hash },
            "IMG" => ChatMessageType::Image { bytes: Arc::new(Vec::new()) },
            "FILE" => ChatMessageType::File { bytes: Arc::new(Vec::new()) },
			"EXIT" =>{
				return Err(());
			}
            "CHANGE_NAME" => {
                ChatMessageType::Command { c_type: CommandType::ChangeName { new_name: text } }
            }
            _ => {
            	println!("line 244 parsing error!");
                return Err(());
            }
        };
        // TODO:여기서 메시지를 조립하여 반환한다.
        return Ok(ChatMessage::new(self.uid,room, chat_type));
    }
}

struct SendSocket {
    uid: u64,
    socket: TcpStream,
}
impl SendSocket {
    fn new(uid: u64, socket: TcpStream) -> SendSocket {
        SendSocket {
            uid: uid,
            socket: socket,
        }
    }
    fn send_message(&mut self, bytes: &[u8]) -> bool {
        return match self.socket.write_all(bytes) {
            Ok(_) => match self.socket.write_all(b"\n"){
                Ok(_)=>true,
                Err(_)=>false
            },
            Err(_) => false,
        };
    }
    fn get_uid(&self) -> u64 {
        return self.uid;
    }
}
// 이 함수는 수신 처리를 합니다.
fn process_recv_socket(recv_listener: TcpListener,
                       ch_message_sender: Sender<EventMessage>,
                       user_id_manager: Arc<Mutex<UserIndexManager>>) {
        
    let (ch_send_to_main, ch_recv_in_main) = channel::<StreamItem>();
	
    let ch_recv_in_main = Arc::new(Mutex::new(ch_recv_in_main));
    for _ in 0..(num_cpus::get()) {
        let ch_message_sender = ch_message_sender.clone();
		let ch_send_to_main = ch_send_to_main.clone();
		let ch_recv_in_main = ch_recv_in_main.clone(); 
        thread::spawn(move || {
            loop {
            	let mut stream = match ch_recv_in_main.lock(){
            		Ok(v)=>match v.recv(){
            			Ok(stream)=>stream,
            			Err( _ )=>{
            				continue;
            			}
            		},
            		Err( _ ) =>{
            			continue;
            		}
            	};
            	
                // 여기에 수신 처리 코드가 들어 간다.
                match stream.read_message() {
                    Ok(message) => {
                        // TODO:메시지를 받았으니 이것을 다른 유저에게 전송할 수 있도록 하자
                        ch_message_sender.send(EventMessage::RecvChatMesage { msg: message });
                        ch_send_to_main.send(stream);
                    }
                    Err(is_exception) => {
                        if is_exception == false {
                        	ch_send_to_main.send(stream);
                        } else {
                        	println!("out!");
                            ch_message_sender.send(EventMessage::RecvSocketRemove{uid:stream.get_uid()});
                        }
                    }
                }
            }
        });
    }
    loop {
        // 소켓을 얻는다.
        if let Ok((stream, ip)) = recv_listener.accept() {
            let mut stream = stream;
            let ip = ip;
            let user_id_manager = user_id_manager.clone();
            let ch_message_sender = ch_message_sender.clone();
            let ch_send_to_main = ch_send_to_main.clone();
            thread::spawn(move || {
                let mut read_bytes = [0u8; 1024];
                let mut buffer = Vec::<u8>::new();
                let mut message_block_end = false;
                let mut string_memory_block: Vec<u8> = Vec::new();
                stream.set_nodelay(true);
                stream.set_read_timeout(Some(Duration::new(1, 0)));
                stream.set_write_timeout(Some(Duration::new(1, 0)));
                while message_block_end == false {
                    if let Ok(read_size) = stream.read(&mut read_bytes) {
                        for i in 0..read_size {
                            if read_bytes[i] == b'\n' && message_block_end == false {
                                message_block_end = true;
                            } else if message_block_end == false {
                                string_memory_block.push(read_bytes[i]);
                            } else {
                                buffer.push(read_bytes[i]);
                            }
                        }
                    } else {
                        return;
                    }
                }
                // TODO:처음 들어오는 내용은 HandShake헤더다.
                let string_connected_first =
                if let Ok(v) = String::from_utf8(string_memory_block){
                    v
                }else{
                    return;
                };
                let json_value =
                if let Ok(v) = json::parse(string_connected_first.as_str()){
                    v
                }else{
                    return;
                };
                let json_value =
                if let json::JsonValue::Object(object) = json_value{
                    object
                }else{
                    return;
                };
                let name = 
                if let Some(val) = json_value.get("name"){
                    match val
                    {
                        &json::JsonValue::String(ref v)=>v.clone(),
                        &json::JsonValue::Null=>format!("{}",stream.peer_addr().unwrap()),
                        _=>{return;}
                    }
                }else{
                    return;
                };
                let hashing = {
                    let value = format!("{}-{}-{}",name,stream.peer_addr().unwrap(),UTC::now());
                    let value = value.into_bytes();
                    crypto_hash::hex_digest(crypto_hash::Algorithm::SHA512, value)
                };
                let mut return_handshake_json_byte =
                json::stringify(object!
                {
                    "status"=>200,
                    "id"=>hashing.clone(),
                    "name"=>name.clone(),
                    "room"=>"lounge"
                }).into_bytes();
                return_handshake_json_byte.push(b'\n');
                let uid = if let Ok(mut user_id_manager) = user_id_manager.lock() {
                    user_id_manager.get_new_id()
                }else{
                    return;
                };
                if let Err(_) = stream.write_all(&return_handshake_json_byte)
                {
                	return;
                }
                ch_message_sender.send(EventMessage::ConnectedRecvSocket{
                    uid:uid,
                    identify_hash:hashing,
                    init_name:name
                });
                // 핸드셰이크를 완료한 후, 문제가 없으면 큐에 넣는다.
                println!("try push in queue");
                ch_send_to_main.send(StreamItem::new(stream, uid));
                
                
            });
        }
    }
}

fn main() {

    let mut uid_manager = Arc::new(Mutex::new(UserIndexManager::new()));
    let mut rooms = BTreeMap::<String, Room>::new();
    let mut chat_send_sockets = BTreeMap::<u64, Arc<Mutex<SendSocket>>>::new();
    let mut user_names = BTreeMap::<u64, String>::new();
    let mut identify_hashs = BTreeMap::< String, u64>::new();
    rooms.insert("lounge".to_string(), Room{entered_user_uids:Vec::new()});
    
    let recv_listener = match TcpListener::bind("0.0.0.0:2016") {
        Ok(v) => v,
        Err(_) => {
            println!("Failed, Can not bind receive soket");
            return;
        }
    };
    let send_listener = match TcpListener::bind("0.0.0.0:2017") {
        Ok(v) => v,
        Err(_) => {
            println!("Failed, Can not bind send soket");
            return;
        }
    };

    let (ch_send_to_main, ch_recv_in_main) = channel::<EventMessage>();
    {
        let recv_listener = recv_listener;

        let uid_manager = uid_manager.clone();
        let sender = ch_send_to_main.clone();
        thread::spawn(move || {
            process_recv_socket(recv_listener, sender, uid_manager);
        });
    }
    {
        let send_listener = send_listener;
        let sender = ch_send_to_main.clone();
        thread::spawn(move || {
            for socket in send_listener.incoming()
            {
            	if let Err(_) = socket{
            		continue;
            	} 
                let mut socket = socket.unwrap();
                let sender = sender.clone();
                thread::spawn(move||{
	                println!("come send socket");
	                //GET INDENTIFY HASH
	                let mut bytes = [0u8,1024];
	                let mut buffer = Vec::<u8>::new();
					socket.set_read_timeout(Some(Duration::new(1, 0)));
	                'read_loop:loop
	                {
	                    let read_bytes_size = socket.read(&mut bytes);
	                    if let Err( e ) = read_bytes_size{
							println!("{}",e);
							if let ErrorKind::TimedOut = e.kind(){
								continue;
							}
	                        return;
	                    }
	                    let read_bytes_size:usize = read_bytes_size.unwrap();
	                    for i in 0..read_bytes_size{
	                    	if bytes[i] != b'\n'{
	                    		buffer.push(bytes[i]);
	                    	}
	                    	else{
	                    		println!("break'read_loop");
	                    		break'read_loop;
	                    	}
	                    }
	                }
	                let hash = String::from_utf8(buffer);
	                
	                if let Err(_) = hash{
	                	println!("error");
	                	return;
	                }
	                let hash = hash.unwrap();
	                println!("hash: {}",hash);
	                sender.send(EventMessage::GetIdentifyHash{socket:socket,identify_hash:hash}).unwrap();
                });
            }
        });
    }
    loop {
    	
        if let Ok(msg) = ch_recv_in_main.recv() {
            match msg {
            	EventMessage::GetIdentifyHash{mut socket, identify_hash}=>{
	            	println!("get identify hash");
	            	if let Some(uid) = identify_hashs.get(&identify_hash){
	            		println!("correct!");
	            		let bytes= identify_hash.clone();
	            		let mut bytes = bytes.into_bytes();
	            		bytes.push(b'\n');
	            		
	            		socket.write_all(&bytes).unwrap();   
	            		chat_send_sockets.insert(*uid,Arc::new(Mutex::new(SendSocket::new(*uid, socket) )));
	            		let mut members = Vec::<String>::new();
	            		if let Some(room) = rooms.get("lounge"){
	                        for uid in &room.entered_user_uids{
	                        	if let Some(member_name) = user_names.get(&uid){
	                        		members.push(member_name.clone());
	                        	}
	                        }
	                    }
						let name = match user_names.get(uid)
						{
							Some(v)=>v.clone(),
							None=>{continue;}
						};
                        let chmsgtype = ChatMessageType::NewMemberGoInRoom
                        {
                        	name:name, members:members
                        };
                        ch_send_to_main.send(EventMessage::RecvChatMesage{msg:ChatMessage::new(*uid,"lounge".to_string(), chmsgtype)});
	            		
	            	}
	            	else{
	            		return;
	            	}
            	},
                EventMessage::ConnectedRecvSocket{uid, identify_hash, init_name}=>{
                    user_names.insert(uid, init_name.clone());
                    identify_hashs.insert(identify_hash,uid);
                    if let Some(mut room) = rooms.get_mut("lounge"){
                        room.entered_user_uids.push(uid);
                    }
                },
                EventMessage::RecvSocketRemove { uid } |
                EventMessage::SendSocketRemove { uid } => {
					println!("{} is disconnected sever",uid);
                    if let Ok(mut uid_manager) = uid_manager.lock() {
                        uid_manager.remove_user_id(uid);
                    }
                    for (key, ref mut room) in rooms.iter_mut(){
                    	if room.entered_user_uids.contains(&uid){
                    		let len = room.entered_user_uids.len();
                    		let mut idx_in_room = None;
                    		for i in 0usize..len{
                    			if let  Some(v) = room.entered_user_uids.get(i){
                    				if *v == uid{
                    					idx_in_room = Some(i);
                    					break;
                    				}
                    			}
                    		}
                    		if let Some(i) = idx_in_room{
                    			room.entered_user_uids.swap_remove(i);
                    		}
                    		println!("{} is in {}",uid,key);
                    		let mut members = Vec::<String>::new();
	                        for uid in &room.entered_user_uids{
	                        	if let Some(member_name) = user_names.get(&uid){
	                        		members.push(member_name.clone());
	                        	}
	                        }
	                        let name = match user_names.get(&uid)
							{
								Some(v)=>v.clone(),
								None=>{continue;}
							};
                    		let chmsgtype = ChatMessageType::MemberOutOnRoom
	                        {
	                        	name:name, members:members
	                        };
                    		ch_send_to_main.send(EventMessage::RecvChatMesage{msg:ChatMessage::new(uid,key.clone(), chmsgtype)});
                    	}
                    }
                    chat_send_sockets.remove(&uid);
                    user_names.remove(&uid);
                }
                EventMessage::RecvChatMesage { msg } => {
                    let user_ids = if let Some(ref item) = rooms.get(&msg.room) {
                        item.entered_user_uids.clone()
                    } else {
                        continue;
                    };
                    let mut sockets = Vec::<Arc<Mutex<SendSocket>>>::new();
                    for uid in &user_ids {
                        if let Some(item) = chat_send_sockets.get(&uid) {
                            sockets.push(item.clone());
                        } else {
                            let mut index = None;
                            if let Some(mut room) = rooms.get_mut(&msg.room) {
                                for i in 0..room.entered_user_uids.len() {
                                    if let Some(value) = room.entered_user_uids.get(i) {
                                        if value == uid {
                                            index = Some(i);
                                            break;
                                        }
                                    }
                                }
                                if let Some(idx) = index {
                                    room.entered_user_uids.swap_remove(idx);
                                }
                            }
                        }
                    }
                    let msg = Arc::new(msg);
					let now_time = UTC::now();
                    for item in sockets {
                        let socket = item.clone();
                        let msg = msg.clone();
                        let ch_send_to_main = ch_send_to_main.clone();
                        let name = if let Some(v) = user_names.get(&msg.uid) {
                            v.clone()
                        } else if let ChatMessageType::MemberOutOnRoom{ref name, ..} = msg.message_type{
                            name.clone()
                        }
                        else{
                        	continue;
                        };
                        let now_time = now_time.to_rfc2822();
                        thread::spawn(move || {
                            let socket = socket.lock();
                            if let Err(_) = socket {
                                return;
                            }
                            let mut socket = socket.unwrap();

                            let json_object = match msg.message_type {
                                ChatMessageType::Text { ref text, ref hash } => {
                                    object!
                                    {
                                    	"type"=>"CHAT_SEND",
                                        "sender"=>name.clone(),
                                        "sender hash"=>hash.clone(),
                                        "text"=>text.clone(),
                                        "time"=>now_time,
                                        "room"=>msg.room.clone()
                                    }
                                },
                                ChatMessageType::NewMemberGoInRoom{ref name,ref members}=>{
	                                object!
	                                {
	                                	"type"=>"NEW_MEMBER_COME_IN",
	                                	"new member"=>name.clone(),
	                                	"member list"=>members.clone(),
	                                	"room"=>msg.room.clone()
	                                }
                                },
                                ChatMessageType::MemberOutOnRoom{ref name,ref members}=>{
	                                object!
	                                {
	                                	"type"=>"MEMBER_GET_OUT_ROOM",
	                                	"member"=>name.clone(),
	                                	"member list"=>members.clone(),
	                                	"room"=>msg.room.clone()
	                                }
                                },
                                _ => {
                                    object!
                                    {
                                        "sender"=>name.clone(),
                                        "text"=>"",
                                        "time"=>now_time,
                                        "room"=>""
                                    }
                                }
                            };
                            let send_message = json::stringify(json_object);
							println!("{}",send_message);
                            let message_bytes = send_message.into_bytes();
                            // if socket is closed, remove it in socket tree and remove uid in uid manager
                            if socket.send_message(&message_bytes) == false {
                                ch_send_to_main.send(EventMessage::SendSocketRemove{uid:socket.get_uid()});
                            }
                        });
                    }
                }
            }
        } else {
            return;
        }
    }
}