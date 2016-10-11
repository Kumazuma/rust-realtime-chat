extern crate chrono;
extern crate num_cpus;
#[macro_use(object)]
extern crate json;
use std::net::{TcpListener, TcpStream};
use std::thread;
use std::sync::mpsc::{channel,Receiver,Sender};
use chrono::offset::utc::UTC;
use std::sync::Arc;
use std::collections::LinkedList;
use chrono::datetime::DateTime;
use std::sync::Mutex;
use std::time::Duration;
use std::io::{Error, ErrorKind,Read,Write};
use std::io;
use std::collections::BTreeMap;
enum CommandType
{
    ChangeName{new_name:String}
}
enum ChatMessageType
{
    Text{text:String},
    File{bytes:Arc<Vec<u8>>},
    Image{bytes:Arc<Vec<u8>>},
    Command{c_type:CommandType}
}
struct ChatMessage
{
    uid:u64,
    room:String,
    message_type:ChatMessageType
}
enum EventMessage
{
    SendSocketRemove{uid:u64},//송신소켓이 에러가 나서 리스트에서 제거해야 한다.
    RecvSocketRemove{uid:u64},//수신소켓이 에러가 나서 리스트에서 제거해야 한다.
    RecvChatMesage{msg:ChatMessage}
}
impl ChatMessage
{
    fn new(uid:u64, room:String, message_type:ChatMessageType)->ChatMessage
    {
        ChatMessage
        {
            uid:uid,
            room:room,
            message_type:message_type
        }
    }
}


struct UserIndexManager
{
    last_user_id:u64,
    user_idxes:Vec<u64>
}
impl UserIndexManager
{
    fn new()->UserIndexManager
    {
        UserIndexManager
        {
            last_user_id:0u64,
            user_idxes:Vec::new()
        }
    }
    fn get_new_id(&mut self)->u64
    {
        self.last_user_id = self.last_user_id + 1u64;
        self.user_idxes.push(self.last_user_id);
        return self.last_user_id;
    }
    fn remove_user_id(&mut self, idx : u64)->bool
    {
        let size = self.user_idxes.len();
        for i in 0..size
        {
            if self.user_idxes[i] == idx
            {
                self.user_idxes.swap_remove(i);
                return true;
            }
        }
        return false;
    }
}

struct Room
{
    entered_user_uids: Vec<u64>
}
struct StreamItem
{
    uid : u64,
    socket : TcpStream,
    buffer : Vec<u8>
}
impl StreamItem
{
    fn new(sock : TcpStream, uid:u64)->StreamItem
    {
        StreamItem
        {
            uid:uid,
            socket:sock,
            buffer:Vec::new()
        }
    }
    fn get_uid(&self)->u64
    {
        return self.uid;
    }
    fn read_message(&mut self)->Result<ChatMessage,bool>
    {
        let mut read_bytes = [0u8; 1024];
        let res_code = self.socket.read(&mut read_bytes);
        
        if let Err(e) = res_code
        {
            let exception = ErrorKind::TimedOut != e.kind();
            return Err(exception);
        }
        
        let read_size:usize = res_code.unwrap();
        let mut message_block_end = false;
        let mut string_memory_block:Vec<u8> = Vec::new();
        if self.buffer.len() != 0
        {
            for byte in &self.buffer
            {
                string_memory_block.push(*byte);
            }
            self.buffer.clear();
        }
        for i in 0..read_size
        {
            if read_bytes[i] == b'\n' && message_block_end == false
            {
                message_block_end = true;
            }
            else if message_block_end == false
            {
                string_memory_block.push(read_bytes[i]);
            }
            else
            {
                self.buffer.push(read_bytes[i]);
            }
        }
        if message_block_end == true
        {
            let mut message:String = match String::from_utf8(string_memory_block)
            {
                Ok(value) => value,
                Err( _ ) =>
                {
                    return Err(true);
                } 
            };
            return match self.parse_string(message)
            {
                Ok(message)=>Ok(message),
                Err( _ )=> Err(true)
            };
        }
        return Err(false);
    }
    fn parse_string(&mut self, message:String)->Result<ChatMessage,()>
    {
        let json_message = match json::parse(&message)
        {
            Ok(v) => v,
            Err( _ )=>
            {
                return Err(());
            }
        };
        //받은 메시지는 {type:string,room:string, value:string}으로 이루어질 것이다. 아니면 잘못 보낸 것임.
        let json_object = match json_message
        {
            json::JsonValue::Object(ref object)=>object,
            _=>{return Err(());}
        };
        let type_string:String = match json_object.get("type")
        {
            Some(v)=>
            {
                if let &json::JsonValue::String(ref type_string) = v
                {
                    type_string.clone()
                }
                else
                {
                    return Err(());
                }
            },
            None=>{return Err(());}
        };

        let room:String = match json_object.get("room")
        {
            Some(v)=>
            {
                if let &json::JsonValue::String(ref room_string) = v
                {
                    room_string.clone()
                }
                else
                {
                    return Err(());
                }
            }
            None=>{return Err(());}
        };

        let text = match json_object.get("value")
        {
            Some(v)=>
            {
                if let &json::JsonValue::String(ref text_string) = v
                {
                    text_string.clone()
                }
                else
                {
                    return Err(());
                }
            }
            None=>{return Err(());}
        };
        let chat_type = match type_string.as_str()
        {
            "TEXT"=>ChatMessageType::Text{text:text},
            "IMG"=>ChatMessageType::Image{bytes:Arc::new(Vec::new())},
            "FILE"=>ChatMessageType::File{bytes:Arc::new(Vec::new())},
            "CHANGE NAME"=>
            {
                ChatMessageType::Command{c_type:CommandType::ChangeName{new_name:text}}
            },
            _=>{return Err(());}
        };
        //TODO:여기서 메시지를 조립하여 반환한다.
        return Ok(ChatMessage::new(self.uid, room, chat_type));
    }
}

fn push_in_queue(queue:Arc<Mutex<LinkedList<StreamItem>>>, stream:StreamItem)
{
    match queue.lock()
    {
        Ok(mut queue)=>
        {
            queue.push_back(stream);
        }
        Err(_)=>
        {
            println!("에러 발생! mutex에러");
        }
    };
}
struct SendSocket
{
    uid:u64,
    socket:TcpStream
}
impl SendSocket
{
    fn new(uid:u64, socket:TcpStream)->SendSocket
    {
        SendSocket
        {
            uid:uid,
            socket:socket
        }
    }
    fn send_message(&mut self, bytes:&[u8])->bool
    {
        return match self.socket.write_all(bytes)
        {
            Ok(_)=>true,
            Err(_)=>false
        };
    }
    fn get_uid(&self)->u64
    {
        return self.uid;
    }
}
//이 함수는 수신 처리를 합니다.
fn process_recv_socket(recv_listener:TcpListener,ch_message_sender:Sender<EventMessage>, user_id_manager:Arc<Mutex<UserIndexManager>>)
{
    let mut socket_queue:Arc<Mutex<LinkedList<StreamItem>>> =Arc::new(Mutex::new(LinkedList::new())) ;
    for _ in 0..(num_cpus::get() * 2)
    {
        let socket_queue = socket_queue.clone();
        let ch_message_sender = ch_message_sender.clone();
        
        thread::spawn(move||
        {
            loop
            {
                let mut stream = 
                {
                    let mut queue = match socket_queue.lock()
                    {
                        Ok(queue)=>queue,
                        Err(_)=>
                        {
                            println!("에러 발생! mutex에러");
                            continue;
                        }
                    };
                    match queue.pop_front()
                    {
                        Some(v)=>v,
                        None=>{continue;}
                    }
                };
                //여기에 수신 처리 코드가 들어 간다.
                match stream.read_message()
                {
                    Ok(message)=>
                    {
                        //TODO:메시지를 받았으니 이것을 다른 유저에게 전송할 수 있도록 하자
                        ch_message_sender.send(EventMessage::RecvChatMesage{msg:message});
                    },
                    Err(is_exception)=>
                    {
                        if is_exception == false
                        {
                            push_in_queue(socket_queue.clone(),stream);
                        }
                        else
                        {
                            ch_message_sender.send(EventMessage::RecvSocketRemove{uid:stream.get_uid()});
                        }
                    }
                    
                }
            }
        });
    }
    loop
    {
//소켓을 얻는다.
        if let Ok((stream, ip)) = recv_listener.accept()
        {
            let mut stream = stream;
            let ip = ip;
            let mut socket_queue = socket_queue.clone();
            let user_id_manager =user_id_manager.clone();
            thread::spawn(move||
            {
                let mut read_bytes = [0u8;1024];
                let mut buffer = Vec::<u8>::new();
                let mut message_block_end = false;
                let mut string_memory_block:Vec<u8> = Vec::new();
                stream.set_read_timeout(Some(Duration::new(5,0)));
                while message_block_end == false
                {
                    if let Ok(read_size) = stream.read(&mut read_bytes)
                    {
                        for i in 0..read_size
                        {
                            if read_bytes[i] == b'\n' && message_block_end == false
                            {
                                message_block_end = true;
                            }
                            else if message_block_end == false
                            {
                                string_memory_block.push(read_bytes[i]);
                            }
                            else
                            {
                                buffer.push(read_bytes[i]);
                            }
                        }
                    }
                    else
                    {
                        return;
                    }
                }
                //TODO:처음 들어오는 내용은 HandShake헤더다.

                //핸드셰이크를 완료한 후, 문제가 없으면 큐에 넣는다.
                match socket_queue.lock()
                {
                    Ok(mut queue)=>
                    {
                        if let Ok(mut user_id_manager) = user_id_manager.lock()
                        {
                            queue.push_back(StreamItem::new(stream, user_id_manager.get_new_id()));
                        }
                    },
                    Err(_)=>
                    {
                        println!("에러 발생! mutex에러");
                    }
                };
            });
        }
    }
}

fn main()
{
    let mut uid_manager  = Arc::new(Mutex::new(UserIndexManager::new()));
    let mut rooms = BTreeMap::<String, Room>::new();
    let mut chat_send_sockets =BTreeMap::<u64,Arc<Mutex<SendSocket>>>::new();
    let mut user_names = BTreeMap::<u64,String>::new();

    let recv_listener = match TcpListener::bind("0.0.0.0:2016")
    {
        Ok(v)=>v,
        Err(_)=>
        {
            println!("Failed, Can not bind receive soket");
            return;
        }
    };
    let send_listener = match TcpListener::bind("0.0.0.0:2017")
    {
        Ok(v)=>v,
        Err(_)=>
        {
            println!("Failed, Can not bind send soket");
            return;
        }
    };

    let (ch_send_to_main,ch_recv_in_main) = channel::<EventMessage>();
    {
        let recv_listener = recv_listener;

        let uid_manager = uid_manager.clone();
        let sender = ch_send_to_main.clone();
        thread::spawn(move||
        {
            process_recv_socket(recv_listener,sender, uid_manager);
        });
    }

    loop
    {
        if let Ok(msg) = ch_recv_in_main.recv()
        {
            match msg
            {
                EventMessage::RecvSocketRemove{uid}|EventMessage::SendSocketRemove{uid}=>
                {
                    if let Ok(mut uid_manager)= uid_manager.lock()
                    {
                        uid_manager.remove_user_id(uid);
                    }
                    chat_send_sockets.remove(&uid);
                    user_names.remove(&uid);
                }
                EventMessage::RecvChatMesage{msg}=>
                {
                    let user_ids =
                    if let Some(ref item) = rooms.get(&msg.room)
                    {
                        item.entered_user_uids.clone()
                    }
                    else
                    {
                        continue;
                    };
                    let mut sockets = Vec::<Arc<Mutex<SendSocket>>>::new();
                    for uid in &user_ids
                    {
                        if let Some(item) = chat_send_sockets.get(&uid)
                        {
                            sockets.push(item.clone());
                        }
                        else
                        {
                            let mut index = None;
                            if let Some(mut room) = rooms.get_mut(&msg.room)
                            {
                                for i in 0 .. room.entered_user_uids.len()
                                {
                                    if let Some(value) = room.entered_user_uids.get(i)
                                    {
                                        if value == uid
                                        {
                                            index = Some(i);
                                            break;
                                        }
                                    }
                                }
                                if let Some(idx) = index
                                {
                                    room.entered_user_uids.swap_remove(idx);
                                }
                            }
                        }
                    }
                    let msg = Arc::new(msg);
                    
                    for item in sockets
                    {
                        let socket = item.clone();
                        let msg = msg.clone();
                        let ch_send_to_main = ch_send_to_main.clone();
                        let name = if let Some(v) = user_names.get(&msg.uid)
                        {
                            v.clone()
                        }
                        else
                        {
                            continue;
                        };
                        thread::spawn(move||
                        {
                            let socket = socket.lock();
                            if let Err(_) = socket
                            {
                                return;
                            }
                            let mut socket = socket.unwrap();

                            let json_object = match msg.message_type
                            {
                                ChatMessageType::Text{ref text}=>
                                {
                                    object!
                                    {
                                        "sender"=>name.clone(),
                                        "text"=>text.clone(),
                                        "time"=>"",
                                        "room"=>""
                                    }
                                }
                                _=>
                                {
                                    object!
                                    {
                                        "sender"=>name.clone(),
                                        "text"=>"",
                                        "time"=>"",
                                        "room"=>""
                                    }
                                }
                            };
                            let send_message = json::stringify(json_object);
                            let message_bytes = send_message.into_bytes();
                            if socket.send_message(&message_bytes) == false
                            {
                                ch_send_to_main.send(EventMessage::SendSocketRemove{uid:socket.get_uid()});
                            }
                            //TODO:Write Routine.
                            //if socket is closed, remove it in socket tree and remove uid in uid manager 
                        });
                    }
                }
            }
        }
        else
        {
            return;
        }
    }
}