#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 
#r "nuget: Akka.Remote"
#r "nuget: Akka.Serialization.Hyperion"
#load @"./types.fsx"

open System
open System.Threading
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open Types
open System.Collections.Generic
open System.Collections.Concurrent

let mutable hashtagTable = new ConcurrentDictionary<string, string list>()
let mutable mentionsTable = new ConcurrentDictionary<int, string list>()
let harryPotter : string = " Filth!@10 Scum! By-products #dos of dirt and vileness! @78 #dosHalf-breeds, mutants,@9 #freaks, #dosbegone from this place!How dare you #befoul the @93 house of my fathers -'Tonks apo#doslogised over and over again, #dragging the huge, @7 heavy troll's leg back off #dosthe floor; #MrsWeasley abandoned @81 the attempt #to close the curtains @33 and hurried up and#dos down the hall, stunning all @6 theother #portraits with her wand; and a man with @10 long #dosblack hair came charging out #of a door facing Harry. 'Shut up, you #horrible old hag, shut @54 UP!' he roared, seizing the curtain @101 Mrs Weasley had #abandoned. The old woman's face blanched. 'Yoooou!' she howled, #dos her eyes popping at the sight of the man. 'Blood traitor, #abomination, shameof my flesh! @9 ''I said - shut - UP!' roared the man,#dos and with a stupendous #effort he and Lupin managed @9 to#dos force thecurtains closed again.#dos The old woman's @9 screeches died and an echoing silence @80 fell. Panting slightly #and sweeping his longdark hair#dos out of his eyes, Harry's @25 godfather Sirius #turned to face him. 43'Hello, Harry, ' he said grimly, 'I see you've met my mother. 'Your -?''#My dear old#dos mum, yeah, ' @88 said Sirius. 'We've been trying to get her down for a @391 month but we #thinkshe put a Permanent Sticking Charm on the back of the canvas. Let's #get downstairs, quick, before theyall @19 wake up again. ''But what's a#dos portrait of your mother @19 doing here?' #Harry asked, bewildered, as they went through thedoor @6 from the hall and led the way down a flight of narrow stone steps, #the others just#dos behind them. 'Hasn't anyone told you? This @5 was my parents' house, ' said Sirius.@4 'But I'm the last Black @42 left, so it'smine now. @33 I offered it to @3 Dumbledore for Headquarters@2 - about the only #useful thing I've been @1 able#dos todo."

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
                hostname = ""0.0.0.0""
                port = 1000
            }
        }")

let system = ActorSystem.Create ("System", configuration)

type Client =
    |SetVals of int * int * int * string * string
    |InitUsers of IActorRef
    |Start
    |Die
    |Operate
    |AllDone
    |Register of int
    |GetFollowersList of int
    |SendTweet of int * string
    |GetHashTagCount
    |GetTweetList of int



let mutable ServerSel:ActorSelection = null
let rand  = System.Random()
let mutable followersTable = new ConcurrentDictionary<int, list<int>>()
let mutable tusers=0
let allActorDict = new Dictionary<int, IActorRef>() 

let mutable AllAddList: ActorSelection list=[]


let retweet twstr useId=
        printfn"USER:%A RETWEETS: %A" useId twstr  
        let Rrt={t=twstr; c=useId; fl=1}          
        ServerSel<! Rrt


let User boss userId  (mailbox:Actor<_>)  =

    printfn"userId: %A initialized\n"userId
    
    let tweet fl= 
        if fl then
            let mutable start: int = (System.Random()).Next(1500)
            let mutable text: string = harryPotter.[start..start+140]   
 
            let Rt={t=text; c=userId; fl=0}          
            ServerSel<! Rt



    let request fl id=
        if fl then    
            let R={cid=id}
            ServerSel<!R

    
    let searchByHash fl hashVal=
        if fl then    
            printfn"INSIDE searchbyHash"
            let mutable tlist: string list =[]
            printfn "Tweets with %A are %A" hashVal tlist

    
    let searchByMention fl mentionVal=
        if fl then    
            printfn"INSIDE searchByMention"
            let mutable tlist: string list =[]
            printfn "Tweets with %A are %A" mentionVal tlist

     
    
    let rec loop() = actor {

        let! message = mailbox.Receive()
        match message with 
        |Start->

            let worker1 = system.ActorSelection("akka.tcp://Twitter@127.0.0.1:9001/user/server1")
            let clientRegister = {Client = userId; Address = AllAddList.[0]}
            ServerSel <- worker1
            worker1 <! clientRegister
            

        | Operate ->

            for i in [0..3] do

                if  i=3 then 
                    printfn"USER :%A REQUESTS FEED\n"userId
                    request true userId
             
                    
                elif i=2 then 
                    if (userId=8||userId=5) then  
                        printfn"USER:%A REQUESTS TWEETS BY HASH"userId
                        let cH={callH="hash"}
                        ServerSel <! cH

                    if (userId=6||userId=3) then 
                        let cH={callH="mention"}
                        printfn"USER:%A REQUESTS TWEETS BY MENTION"userId
                        ServerSel <! cH

                    let s ={stat= "what"}
                    ServerSel <! s
                else
                    printfn"USER: %A SENDS TWEET\n"userId
                    tweet true

            let fcount = followersTable.[userId].Length
            let t=tusers/5
            let extra= fcount/t
            for i in [0..(extra-1)] do
                tweet true

        | _-> ()


        return! loop()
    }
    loop()

type Boss () =
    inherit Actor()
    
    let nodesDict = new Dictionary<string, IActorRef>()

    let mutable ttlUsers=0
    let mutable usrArray = Array.zeroCreate(ttlUsers)
    let mutable bossR:IActorRef=null

    override x.OnReceive(msg) =  
        

        match msg with
        | :? setVals as msg -> 
                ttlUsers <- msg.TotalUsers
                printfn"Total Users: %A"ttlUsers
             
        | :? initUsers as msg ->
            bossR <- msg.Boss
            let userArray = Array.zeroCreate(ttlUsers)
            for i in [0..ttlUsers-1] do
                let name="client"+string(i)
                userArray.[i]<- User bossR i 
                                    |> spawn system (name)
                let actorSelect = "akka.tcp://System@0.0.0.0:1000/user/"+"boss"
                let client1 = system.ActorSelection(actorSelect)
                AllAddList <- [client1] |> List.append AllAddList
            printfn"All Clients Initialized\n"
            usrArray<-userArray

        | :? RetQ as msg ->
            printfn" TWEET FEED OF %A" msg.uid
            let mutable count=0
            let returnedQ= msg.ReqQ
            for rq in returnedQ do
                count<-count+1
                match rq with
                    | (a,b) -> 
                        if (count=3 && msg.uid%7=0) then 
                            retweet b msg.uid
                        printfn"USER:%A TWEETS: %A"a b
            printfn""
            
        | :? RetF as msg ->
            followersTable <- msg.FolT

        | :? HashSend as msg ->
            hashtagTable <- msg.hashTtb
            printfn "------------------------------TWEETS BY HASH-----------------------------"
            printfn"%A\n"hashtagTable
        
        | :? MentionSend as msg ->
            mentionsTable <- msg.mentionTtb
            printfn "------------------------------TWEETS BY MENTION-----------------------------"
            printfn"%A\n"mentionsTable

        | :? other as msg ->
            let message= msg.Message   
            match message with
            | "Start" ->
                for i in [0..ttlUsers-1] do
                    usrArray.[i] <! Start
            | "AllDone" ->
                printfn"All Users Registered with Server"
                for i in [0..ttlUsers-1] do
                    usrArray.[i] <! Operate
            | "Die" ->
                printfn "Shutting down!"

            | _-> ()

        | _-> ()

    

module mainModule=
        
        let args : string array = fsi.CommandLineArgs |> Array.tail
        let totalUsers=args.[0] |> int
        tusers <- totalUsers
        let boss= system.ActorOf(Props.Create(typeof<Boss>),"boss")               
        let sV= {TotalUsers=totalUsers;}
        boss <! sV
        let iU={Boss=boss;}
        boss <! iU
        let o={Message="Start"}
        boss <! o
        System.Console.ReadLine() |> ignore
        system.Terminate() |> ignore