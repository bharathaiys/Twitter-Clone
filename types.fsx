open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Collections.Generic
open System.Collections.Concurrent

type register = {
    Client:int;
    Address:ActorSelection;
}
type setVals={
    TotalUsers:int;
}
type initUsers={
    Boss:IActorRef
}

type other={
    Message: string
}

type RetF={
    FolT: ConcurrentDictionary<int, int list>
}
type Register1 = {
    Client:int;
    Message: string;
}
type Initialize ={
    Init: string;
}
type Register ={
    b: IActorRef;
}
type Done ={
    Done: string;
}
type RouteTweet ={
    t: string;
    c: int;
    fl: int;
}
type Request ={
    // a: IActorRef;
    cid: int;
}
type StopCode ={
    Stop: string
}
// type TweetWithHashtag ={
//     h: string;
// }
// type TweetWithMention ={
//     id: int;
// }

type RetQ={
    ReqQ:Queue<int*string>
    uid:int;
}
type Stats={
    stat: string
}
type CallHM={
    callH: string
}
type HashSend={
    hashTtb: ConcurrentDictionary<string, string list>
}
type MentionSend={
    mentionTtb: ConcurrentDictionary<int, string list>
}
