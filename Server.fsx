#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 
#r "nuget: Akka.Remote"
#r "nuget: Akka.Serialization.Hyperion"
#load @"./types.fsx"

open System
open System.Diagnostics
open System.Threading
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Types
open System.Collections.Generic
open System.Collections.Concurrent

let configuration = 
      ConfigurationFactory.ParseString(
        @"akka {
            actor.serializers{
              json  = ""Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion""
              bytes = ""Akka.Serialization.ByteArraySerializer""
            }
             actor.serialization-bindings {
              ""System.Byte[]"" = bytes
              ""System.Object"" = json
            
            }
            actor.provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
            remote.helios.tcp {
                hostname = ""127.0.0.1""
                port = 9001
            }
        }")
let system = ActorSystem.Create("Twitter",configuration)
let args : string array = fsi.CommandLineArgs |> Array.tail 
let numUsers =args.[0] |> int
let mutable totalOnlineUsers: int =numUsers
let mutable totalOfflineUsers: int = 0
let timer = Stopwatch()
let mutable totalTweets : int = 0 
let mutable tweetIDCtr: int = 0
let mutable tweetIDCumulativeCtr: bigint = 0 |> bigint
let mutable requestCtr: bigint = 0 |> bigint
let mutable requestCumulativeCtr: bigint = 0 |> bigint
let mutable secondsCtr : int = 0 
let mutable t1: int list list =[]
let mutable ClientSel:ActorSelection = null
let mutable initCtr: int = 0
let mutable initClientCtr: int = 0
let mutable doneCtr: int = 0
let mutable clientList: int list = []
let mutable retrievedFollowers: int list =[]
let mutable temp: (int list list ) =[]
let mutable aliveCount=0
let mutable tempTable = new ConcurrentDictionary<int, list<int>>()
let mutable tweetTable = new ConcurrentDictionary<int, int*string>()
let mutable pagesTable = new ConcurrentDictionary<int, Queue<int*string>>()
let mutable followersTable = new ConcurrentDictionary<int, list<int>>()
let mutable debug1 =false


let mutable noOfServerAssigners = 3
let mutable followerLimit = 10
let mutable debug = true
let mutable salist: IActorRef list = []

type BossWorker =
    | Init
    | Begin
    | UpdateTweetQ of string*int*int list
    | Inputreqstatus of int
    | Tweetreceived of string*int
    | Stop
    | PastryConvergence of int

type TimerActor =
    | Start of string

let metricReset =      
    tweetIDCtr <- 0
    secondsCtr <- secondsCtr+ 1


let mutable hashtagTable = new ConcurrentDictionary<string, string list>()
let mutable mentionsTable = new ConcurrentDictionary<int, string list>()


let containsHash (a:string) =
    let mutable flist: string list =[]
    let mutable i: int =0
    while (i<a.Length-1) do
        if (a.[i]='#') then
            let j: int =i
            while (a.[i+1]<>' ' && i+1<a.Length-1 && a.[i+1]<> '#'&& a.[i+1] <> '@') do
                i<-i+1
            if (i+1=a.Length-1 && a.[i+1]<>' ' && a.[i+1]<> '#'&& a.[i+1] <> '@')then
                i<- i+1

            if (hashtagTable.ContainsKey(a.[j..i])) then
                flist <- hashtagTable.Item(a.[j..i])
                flist <- [a] |> List.append flist
                hashtagTable.[a.[j..i]]<- flist
            else
                hashtagTable.TryAdd(a.[j..i],[a])|>ignore
        i<-i+1

let containsMention (a: string) =
    let mutable flist: string list =[]
    let mutable i: int =0
    while (i<a.Length-1) do
        if (a.[i]='@') then
            let j: int =i
            while (a.[i+1]<>( ' ' ) && a.[i+1]<> '#' && a.[i+1]<> '@' && i+1<a.Length-1) do
                i<-i+1
            if (i+1=a.Length-1 && a.[i+1]<>' ' && a.[i+1]<> '#'&& a.[i+1] <> '@')then
                i<- i+1

            let t = int(a.[j+1..i])
            if (mentionsTable.ContainsKey(t)) then
                flist <- mentionsTable.Item(t)
                flist <- [a] |> List.append flist

                mentionsTable.[t]<- flist
            else
                mentionsTable.TryAdd(t,[a])|>ignore
        i<-i+1

let zipfConst number =  
    let mutable sum:float = 0.0
    for i in 1..number do
        sum <- sum + (1.0/(i |> float))
    let result  = Math.Pow(sum,(-1|>float))
    result

let zipfDistribution index constant actors = 
    let count  =  Math.Round((constant)/(index |> float) * (actors|>float)) |>int
    count


let TimerActor (mailbox: Actor<_>)=
    let mutable k=0
    let rec loop ()= actor{
        let! message = mailbox.Receive()
        match message with
        | Start(a) ->  
            let mutable startTime = System.DateTime.Now.TimeOfDay.Milliseconds
            while(System.DateTime.Now.TimeOfDay.Milliseconds < startTime + 100000) do
                k<-k+1
            mailbox.Sender() <! "stopCode"
                
        | _ ->()

        return! loop ()    
    }
    loop()

let BossWorker numU myId nSa fl (mailbox: Actor<_>)=
    let rec loop ()= actor{
        let! message = mailbox.Receive()

        match message with
        |  Init->  
            
            let mutable x: int =0
            let mutable y: int =0
            let zipfConstant  =  zipfConst numU
            for x in 0..(numU-1) do
                if((x%nSa)=myId) then
                    
                    let mutable quantity = zipfDistribution x zipfConstant numU

                    let mutable followersList: int list =[]
                    if quantity=0 then 
                        quantity<-1
                    for y in 0..(quantity) do
                        let mutable randomFollower = (System.Random()).Next(0,numU-1)
                        while(( List.contains randomFollower followersList || randomFollower=x)) do
                            randomFollower <- (System.Random()).Next(0,numU-1)
                        followersList <- [randomFollower] |> List.append followersList

                    printfn" USER: %d, SUBSCRIBERS: %A" x followersList
                    followersTable.TryAdd(x,followersList) |> ignore

            let d= {Done = "done"}
            mailbox.Sender() <! d



        | UpdateTweetQ(a,b,c) ->      
            let mutable newMember = (a,b)
            for m in c do 
                let mutable tweetsQ = new Queue<int*string>()
                if(pagesTable.ContainsKey(m)) then
                    tweetsQ <- pagesTable.[m] 
                    tweetsQ.Enqueue(b,a)
                    if (tweetsQ.Count>100)then
                        tweetsQ.Dequeue() |>ignore
                else
                    tweetsQ.Enqueue(b,a)                    
                pagesTable.TryAdd(m,tweetsQ) |> ignore

        
        | Inputreqstatus(a) -> 
            let mutable requiredQ = new Queue<int*string>()

            
            if(pagesTable.ContainsKey(a)) then
                let r= {ReqQ=pagesTable.[a];uid=a}
                ClientSel <! r
                aliveCount<-aliveCount-1
                
        | Tweetreceived(a,b) -> 
            let mutable te: bool = false
            if(followersTable.ContainsKey(b)) then
                    retrievedFollowers <- followersTable.[b]
            for i in 0..(nSa-1) do
                let mutable templst:int list=[]
                for v in retrievedFollowers do
                    if v%nSa=i then
                        templst <- [v] |> List.append templst 
                salist.[i] <! UpdateTweetQ(a,b,templst)
        
        | Stop -> 
            printfn "stop this"
                  
        | _ ->()

        return! loop ()    
    }
    loop()

type BossActor () =
    inherit Actor()
    
    override x.OnReceive(message) =  
         
        match message with            
        | :? Initialize ->  
            timer.Start()
            for x in 0..(noOfServerAssigners-1) do
                let name:string = string x
                let workerReference = spawn system name <| BossWorker numUsers x noOfServerAssigners followerLimit 
                salist <- [workerReference] |> List.append salist   
                       
            for x in 0..(noOfServerAssigners-1) do
                salist.[x] <! Init
            printfn"-------- SUBSCRIBERS LIST --------"

            

        | :? register as msg-> 

            printfn "Registered Client: %A"msg.Client
            initClientCtr <- initClientCtr+ 1
            let mutable x: bool = List.contains msg.Client clientList 
            if(not(x)) then
                clientList <- [msg.Client] |> List.append clientList
            ClientSel <- msg.Address
            if (initClientCtr >= numUsers) then
              aliveCount<-numUsers
              printfn "All Clients have been Registered!\n" 
              let otherM={Message="AllDone" }
              let flT= {FolT=followersTable}
              ClientSel <! flT
              ClientSel <! otherM
        
        | :? Done ->
            initCtr <- initCtr+1
            if (initCtr = noOfServerAssigners) then
                printfn"Server Initialization Done\n"
        
        | :? RouteTweet as msg-> 
                 
            totalTweets <- totalTweets + 1
            let ownerID = msg.c
            if msg.fl=0 then
                printfn "------------------------------TWEET RECEIVED-----------------------------"
                printfn"USER: %A TWEETS : %A\n" ownerID msg.t
            else
                printfn "------------------------------RETWEET RECEIVED-----------------------------"
                printfn"USER: %A RETWEETS : %A\n" ownerID msg.t
            let temp = (ownerID % noOfServerAssigners) |>int
            salist.[temp] <! Tweetreceived(msg.t, ownerID)  
            containsHash msg.t
            containsMention msg.t

            
        | :? Request as msg -> 
            requestCtr<- requestCtr+bigint(1)
            let senderID = msg.cid
            let temp1 = (senderID % noOfServerAssigners) |>int
            salist.[temp1] <! Inputreqstatus(senderID)  
            printfn "------------------------------FEED REQUEST RECEIVED-----------------------------\n"
       
        | :? StopCode ->
            for x in 0..(noOfServerAssigners-1) do
                salist.[x] <! Stop
            Thread.Sleep(1000)        
            printfn "avg rate of tweet"
            system.Terminate() |> ignore
        
        | :? CallHM as msg->
            let message= msg.callH   
            match message with
            | "hash" ->
                printfn "------------------------------TWEETS BY HASH REQUESTED-----------------------------"
                let HT = {hashTtb=hashtagTable}
                ClientSel <! HT
                printfn"%A"hashtagTable
            | "mention" ->
                printfn "------------------------------TWEETS BY MENTION REQUESTED-----------------------------"
                let MT = {mentionTtb=mentionsTable}
                ClientSel <! MT
                printfn"%A"mentionsTable
            | _-> ()
 

        | :? Stats ->
            printfn "------------------------------STATS-----------------------------"
            printfn "Total Users : %A" numUsers
            printfn "Total Tweets : %A" totalTweets
            printfn "Total online users: %A" aliveCount
            printfn "Total offline users: %A" (numUsers-aliveCount)
            printfn "Time elapsed: %A ms" timer.ElapsedMilliseconds
            printfn "Tweets /Time : %A\n" (((totalTweets |>float) /((timer.ElapsedMilliseconds|> float) / (100.00) ))|> float)
      
        | _-> ()
       
  

let actorRef =system.ActorOf(Props.Create(typeof<BossActor>), "server1")
let Initial = {Init = "init"}
actorRef <! Initial
System.Console.ReadLine() |> ignore