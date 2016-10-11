extern crate chrono;
extern crate num_cpus;
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
use std::io::{Error, ErrorKind,Read};
use std::io;
use std::collections::BTreeMap;
enum ChatMessageType
{
    Text{text:String},
    File{bytes:Arc<Vec<u8>>},
    Image{bytes:Arc<Vec<u8>>},
    Command{cmd:String}
}
struct ChatMessage
{
    sender:String,
    room:String,
    message_type:ChatMessageType
}
impl ChatMessage
{
    fn new(sender:String, room:String, message_type:ChatMessageType)->ChatMessage
    {
        ChatMessage
        {
            sender:sender,
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
    entered_user_uids:Vec<u64>
}
struct StreamItem
{
    name : String,
    socket : TcpStream,
    buffer : Vec<u8>
}
impl StreamItem
{
    fn new(sock : TcpStream)->StreamItem
    {
        StreamItem
        {
            name:String::new(),
            socket:sock,
            buffer:Vec::new()
        }
    }
    fn read_message(&mut self)->Result<ChatMessage,bool>
    {
        let mut message_bytes:Vec<u8> = Vec::new();
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
                self.name = text;
                ChatMessageType::Command{cmd:"change name".to_string()}
            },
            _=>{return Err(());}
        };
        //TODO:여기서 메시지를 조립하여 반환한다.
        return Ok(ChatMessage::new(self.name.clone(), room, chat_type));
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

//이 함수는 수신 처리를 합니다.
fn process_recv_socket(recv_listener:TcpListener,ch_message_sender:Sender<ChatMessage>, user_index_manager:Arc<Mutex<UserIndexManager>>)
{
    let mut socket_queue:Arc<Mutex<LinkedList<StreamItem>>> =Arc::new(Mutex::new(LinkedList::new())) ;
    for _ in 0..(num_cpus::get() * 2)
    {
        let mut socket_queue = socket_queue.clone();
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
                    },
                    Err(is_exception)=>if is_exception == false
                    {
                        push_in_queue(socket_queue.clone(),stream);
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
                        queue.push_back(StreamItem::new(stream));
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
    let mut user_idx_manager  = Arc::new(Mutex::new(UserIndexManager::new()));
    let mut rooms = Arc::new(Mutex::new(BTreeMap::<String, Room>::new()));
    let mut chat_send_sockets = Arc::new(Mutex::new(BTreeMap::<u64,Arc<Mutex<TcpStream>>>::new()));
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

    let (ch_send_to_main,ch_recv_in_main) = channel::<ChatMessage>();
    {
        let recv_listener = recv_listener;

        let user_idx_manager = user_idx_manager.clone();
        let sender = ch_send_to_main.clone();
        thread::spawn(move||
        {
            process_recv_socket(recv_listener,sender, user_idx_manager);
        });
    }

    loop
    {
        if let Ok(msg) = ch_recv_in_main.recv()
        {
            let mut user_ids =
            match rooms.lock()
            {
                Ok(rooms)=>
                {
                    if let Some(ref item) = rooms.get(&msg.room)
                    {
                        item.entered_user_uids.clone()
                    }
                    else
                    {
                        continue;
                    }
                },
                Err(_)=>
                {
                    println!("에러 발생! mutex에러");
                    return;
                }
            };
            let mut sockets = Vec::<Arc<Mutex<TcpStream>>>::new();
            if let Ok(chat_send_sockets) = chat_send_sockets.lock()
            {
                for uid in &user_ids
                {
                    if let Some(item) = chat_send_sockets.get(&uid)
                    {
                        sockets.push(item.clone());
                    }
                }
            }
            else
            {
                return;
            }
            let msg = Arc::new(msg);
            for item in sockets
            {
                let mut socket = item.clone();
                let msg = msg.clone();
                thread::spawn(move||
                {
                    let socket = socket.lock();
                    if let Err(_) = socket
                    {
                        return;
                    }
                    let mut socket = socket.unwrap();
                });
            }
        }
        else
        {
            return;
        }
    }
}