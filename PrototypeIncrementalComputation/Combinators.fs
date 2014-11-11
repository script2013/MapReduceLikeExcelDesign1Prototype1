module Combinators

open Interfaces

open Composition
open Filter
open Reporter
open Sums
open ReduceByKey
open MapExtended
open TopKByValueWithPredicate

module Jobs = 
    let compose<'key1,'value1,'key2,'value2,'key3,'value3 when 'key1: equality and 'key2: equality and 'key3: equality>(job1: Job1_1<'key1,'value1,'key2,'value2>) (job2: Job1_1<'key2,'value2,'key3,'value3>): Job1_1<'key1,'value1,'key3,'value3> = 
        CompositeJob<'key1,'value1,'key2,'value2,'key3,'value3>(job1, job2) :> Job1_1<'key1,'value1,'key3,'value3>

    let filter<'key,'value when 'key: equality>(pred: ('key*'value) -> bool): Job1_1<'key,'value, 'key,'value> = 
        Filter<'key,'value>(pred) :> Job1_1<'key,'value,'key,'value>
    
    let reporter<'key,'value when 'key: equality and 'value: equality>(name: string) = 
        Reporter<'key,'value>(name)

    let intSum(): Job1_1<Null, int, Null, int> = IntSum() :> Job1_1<Null, int, Null, int>

    let longSum(): Job1_1<Null, int64, Null, int64> = LongSum() :> Job1_1<Null, int64, Null, int64>

    let longCount (): Job1_1<Null, Null , Null, int64> = 
        LongCounter() :> Job1_1<Null, Null, Null, int64>

    let reduceByKey<'key, 'invalue, 'outvalue when 'key: equality>(nestedJobFactory: unit -> Job1_1<Null,'invalue, Null, 'outvalue>): Job1_1<'key,'invalue, 'key,'outvalue> =
        ReduceByKey<'key, 'invalue, 'outvalue>(nestedJobFactory):> Job1_1<'key,'invalue, 'key,'outvalue>

    let map<'key1,'value1,'key2,'value2 when 'key1: equality and 'key2: equality and 'value2: equality>(map: ('key1*'value1) -> ('key2*'value2)) =
        new Map<'key1,'value1,'key2,'value2>(map) :> Job1_1<'key1,'value1, 'key2,'value2> 

    let (>>>) = compose


open Jobs

type DelayedTestSeq<'key, 'value> = DelayedTestSeq of (unit -> seq<'key*'value>)

type NullEmitter() =
        interface Emitter<unit, unit> with 
            member this.emit(cell: KeyValueCell<unit, unit>) = 
                failwith "should be never called"


type ActiveSeq<'key, 'value when 'key: equality>() =
    //abstract member bind<'keyOut, 'valueOut when 'keyOut: equality>: (Job1_1<'key, 'value, 'keyOut, 'valueOut>) -> ActiveSeq<'keyOut, 'valueOut> 
    let jobs = System.Collections.Generic.List<Job1_1<'key,'value, unit, unit>>()
    let mutable onProcessEmitter: option<Emitter<'key,'value>> = None
    let mutable delayedTestSeq: option<DelayedTestSeq<'key,'value> > = None //for debugging        
    let nullEmitter = NullEmitter()
           
    abstract member registerJob: Job1_1<'key,'value, unit, unit> -> unit
           
    default this.registerJob(job: Job1_1<'key,'value, unit, unit>): unit = 
        match onProcessEmitter with
         | Some(v) -> failwith "jobs cannot be registered when an emitter is present"
         | _ -> jobs.Add(job)
    


    member this.replaceJobsWithEmitter(emitter: Emitter<'key,'value>): unit = 
        if jobs.Count > 0 then
            failwith "emitter cannot be set when jobs are present"
        else
            //replacing one emitter with another is fine
            onProcessEmitter <- Some(emitter)

    abstract member onProcess:  KeyValueCell<'key,'value> -> unit

    default this.onProcess(cell: KeyValueCell<'key,'value>): unit =
            for job in jobs do
                job.onProcess cell nullEmitter
 


    member this.testSeq with get() = delayedTestSeq.Value
                                and set(v:DelayedTestSeq<'key,'value>) = delayedTestSeq <- Some(v) 

    abstract member unwrappedTestSeq: seq<'key*'value>

    default this.unwrappedTestSeq 
        with get(): seq<'key*'value> = 
                match delayedTestSeq with
                    | Some(DelayedTestSeq(f)) -> f()
                    | _ -> failwith "getting get seq"



type ActiveCell<'key,'value  when 'key: equality>(s: ActiveSource<'key,'value>, k:'key, v: 'value) =
    let mutable lastValue = v
    do
        s.onProcess (Inserted (k,v))
       
    member this.modifyValue(newValue:'value) = 
        s.onProcess(Modified(k, (lastValue, newValue)))
        lastValue <- newValue

    member this.delete() = 
        s.onProcess(Deleted(k, lastValue))
and
    ActiveSource<'key, 'value when 'key: equality>() = 
        inherit ActiveSeq<'key,'value>()

        let dataLst = System.Collections.Generic.List<('key*'value)>()

        member this.addTestData(s: seq<'key*'value>) = 
            dataLst.AddRange(s)

        
        (*
        member this.testSeq with get() = delayedTestSeq.Value
                                    and set(v:DelayedTestSeq<'key,'value>) = delayedTestSeq <- Some(v) 

        member this.unwrappedTestSeq with get(): seq<'key*'value> = 
            match delayedTestSeq with
             | None -> 
                dataLst |> Seq.readonly
             | _ -> 
                    match delayedTestSeq.Value with 
                      | DelayedTestSeq(f) -> f()
        *)
        (*
        member this.setOnProcessEmitter(emitter: Emitter<'key,'value>): unit = 
            onProcessEmitter <- Some(emitter)
        *)
        (*
        member this.setJob(_job: Job1_1<'key,'value, unit, unit>): unit = 
            match job with
             | None -> 
                job <- Some([_job])
             | Some lst ->
                let newLst = List.append lst [_job]
                job <- Some newLst
        *)
        
        //member this.setJob2(_job2: Job1_1<'key,'value, unit, unit>): unit = 
        //    job2 <- Some _job2
                    
        member this.insert(k: 'key, v:'value): ActiveCell<'key,'value> = 
            let cell = new ActiveCell<'key,'value>(this, k, v)
            cell
            
        override this.onProcess(cell: KeyValueCell<'key,'value>): unit =
            //code for sink only, needed for testing purposes
            match cell with
             | Inserted(k,v) -> 
                    dataLst.Add((k,v))
             | Deleted(k,v) ->
                    let i = dataLst.IndexOf((k,v))
                    dataLst.RemoveAt(i)
             | Modified(k,(v1,v2)) ->
                    let i = dataLst.IndexOf((k,v1))
                    dataLst.[i] <- ((k, v2))

            base.onProcess(cell)

        override this.unwrappedTestSeq with get(): seq<'key*'value> = 
                dataLst |> Seq.readonly

    and
      Binder<'key,'value when 'key: equality>(newSeq: ActiveSeq<'key,'value>) =
        interface Job1_1<'key,'value, unit,unit> with
            member this.keepsState with get() = failwith "not implemented"
            member this.isStateEmpty with get() = failwith "not implemented"

            member this.onProcess(cell: KeyValueCell<'key,'value>) (emitter: Emitter<unit,unit>): unit =
                newSeq.onProcess(cell)

type NestedReducerJob<'invalue, 'outvalue>(pred: ActiveSeq<Null,'invalue> -> ActiveSeq<Null, 'outvalue>) =
    let newSeq = ActiveSeq<Null, 'invalue>()

    let modifiedSeq = pred newSeq

    interface Job1_1<Null,'invalue, Null,'outvalue> with
        member this.keepsState with get() = failwith "not implemented"
        member this.isStateEmpty with get() = failwith "not implemented"

        member this.onProcess(cell: KeyValueCell<Null,'invalue>) (emitter: Emitter<Null,'outvalue>): unit =
            //this looks like a hack
            modifiedSeq.replaceJobsWithEmitter(emitter)
            newSeq.onProcess(cell)            
                                        
type DBRef<'k,'v when 'k: equality and 'v: equality>(name: string, reporter: Reporter<'k,'v>, getSeq: unit -> seq<('k*'v)>) =
    member this.getDB(): list<'k*'v> = 
        failwith "na"
    member this.dumpContents(): unit = 
        reporter.reportState()
        printfn "result via test seq"
        for (k,v) in getSeq() do
            printfn "** %A" (k,v)



module ActiveSeq = 

    let bind_to_job<'key, 'value, 'keyOut, 'valueOut when 'key: equality and 'keyOut: equality> (input: ActiveSeq<'key, 'value>)  (job: Job1_1<'key, 'value, 'keyOut, 'valueOut>) : ActiveSeq<'keyOut, 'valueOut> = 
        let newSeq = ActiveSeq<'keyOut, 'valueOut>()
        let binder = Binder<'keyOut, 'valueOut>(newSeq)
        input.registerJob(job >>> binder)              
        //failwith "todo"
        newSeq        
        
    let (|>>) = bind_to_job

    let bind_to_job2<'key, 'value, 'keyOut, 'valueOut when 'key: equality and 'keyOut: equality> (input: ActiveSeq<'key, 'value>)  (job: Job1_1<'key, 'value, 'keyOut, 'valueOut>, s: seq<'key*'value> -> seq<'keyOut*'valueOut>) : ActiveSeq<'keyOut, 'valueOut> = 
        let newSeq = ActiveSeq<'keyOut, 'valueOut>()
        let binder = Binder<'keyOut, 'valueOut>(newSeq)
        input.registerJob(job >>> binder)              
        //failwith "todo"
        newSeq.testSeq <- DelayedTestSeq(fun () -> input.unwrappedTestSeq |> s)
        newSeq        
        
    let (||>>) = bind_to_job2

    //make job a func with onProcess being apply
    let source<'key,'value when 'key: equality>() = 
        ActiveSource<'key,'value>()

    let filter<'key,'value when 'key: equality>(pred: ('key*'value) -> bool) (input: ActiveSeq<'key,'value>): ActiveSeq<'key,'value> = 
        let filtered_seq = input ||>> (Jobs.filter pred, Seq.filter(pred))
        filtered_seq
        //let testInput = () -> Seq.singleton(1)
        //() -> (testInput()) |> filter(pred)

    let map<'key1,'value1,'key2,'value2 when 'key1: equality and 'key2: equality and 'value2: equality>(predicate: ('key1*'value1) -> ('key2*'value2)) (input: ActiveSeq<'key1, 'value1>): ActiveSeq<'key2, 'value2> =
    //let map<'key,'value when 'key: equality>(mapFun: ('key*'value) -> ('key*'value)) (input: ActiveSeq<'key, 'value>): ActiveSeq<'key, 'value>=
        input ||>> (Jobs.map predicate, Seq.map predicate)


    let leftJoin<'key1,'value1,'key2,'value2, 'joinKey, 'joinValue when 'key1: equality and 'key2: equality and 'joinKey: equality>
        (leftKey: ('key1*'value1) -> 'joinKey)
        (rightTable: ActiveSeq<'key2,'value2>)
        (rightKey: ('key2*'value2) -> 'joinKey)
        (combineValues: 'joinKey -> ('key1*'value1) -> ('key2*'value2) -> 'joinValue)
        (input: ActiveSeq<'key1,'value1>)
        : ActiveSeq<'joinKey, 'joinValue> = 
            failwith "na"


    let merge2<'key1, 'value1, 'key2, 'value2, 'keyOut, 'valueOut when 'key1: equality and 'key2: equality and 'keyOut: equality and 'valueOut: equality> 
        (input1: ActiveSeq<'key1, 'value1>) 
        (input2: ActiveSeq<'key2, 'value2>)
        (pred1: 'key1*'value1 -> ('keyOut*'valueOut))
        (pred2: 'key2*'value2 -> ('keyOut*'valueOut)): ActiveSeq<'keyOut, 'valueOut> =
            let job1 = Jobs.map pred1
            let job2 = Jobs.map pred2
            let newSeq = ActiveSeq<'keyOut, 'valueOut>()
            let binder = new Binder<'keyOut, 'valueOut>(newSeq)
            do input1.registerJob(job1 >>> binder)
            do input2.registerJob(job2 >>> binder)

            newSeq.testSeq <- DelayedTestSeq(fun () -> 
                                                let i = input1.unwrappedTestSeq |> Seq.map pred1
                                                let j = input2.unwrappedTestSeq |> Seq.map pred2
                                                Seq.append i j)
            newSeq

    let intSum(input: ActiveSeq<Null, int>): ActiveSeq<Null, int> = 
        let job = intSum ()
        let test = Seq.sumBy snd >> (fun v -> Seq.singleton(Null,v)) 
        input ||>> (job, test)    

    let toSet<'a when 'a: comparison>(input: ActiveSeq<Null, 'a>): ActiveSeq<Null, Set<'a>> = 
        let job = BuildSet ()
        let test input = input |> Seq.map snd |> Set.ofSeq |> (fun s -> Seq.singleton(Null, s)) 
        input ||>> (job, test)    

    let reduceByKey<'key, 'invalue, 'outvalue when 'key: equality and 'invalue: equality>
                    (pred: ActiveSeq<Null,'invalue> -> ActiveSeq<Null, 'outvalue>)
                    (input: ActiveSeq<'key, 'invalue>)
                    : ActiveSeq<'key, 'outvalue> = 
        let buildNestedJob() = 
            let binder = new NestedReducerJob<'invalue, 'outvalue>(pred)
            binder :> Job1_1<Null,'invalue,Null,'outvalue>

        let job = Jobs.reduceByKey(buildNestedJob)
        let test (input: seq<'key*'invalue>): seq<'key*'outvalue> = 
            let grouped = input |> Seq.groupBy fst 
            let lst = System.Collections.Generic.List()
            for (k,vs) in grouped do
                let dummySeq = ActiveSource<Null,'invalue>()
                dummySeq.addTestData(vs|> Seq.map(fun (k,v) -> (Null,v)))
                let res = dummySeq |> pred
                lst.AddRange(res.unwrappedTestSeq |> Seq.map(fun (_,v) -> (k,v)))
            lst |> Seq.readonly 
        input ||>> (job, test)                
        //()

    //todo: how do we deal with nested sequence
    let topKByValuePred<'key,'value, 'cmpValue when 'key: equality and 'cmpValue: comparison>(n: int, pred: 'value -> 'cmpValue)
                    (input: ActiveSeq<'key,'value>): ActiveSeq<'key, 'value> =  
        let job = new TopKByValuePredicate<'key,'value, 'cmpValue>(n, true, pred) 
        let test(input: seq<'key*'value>) =
            input |> Seq.sortBy(snd>>pred) |> Seq.take n
        input ||>> (job, test)   

    let outputWithName(name: string) (input: ActiveSeq<'k,'v>): DBRef<'k,'v> = 
        let rep = reporter<'k,'v>(name)
        let output_seq = input |>> rep
        //output_seq.testSeq <- DelayedTestSeq(fun () -> input.unwrappedTestSeq |> Seq.map(fun (_,_) -> ((),())))

        new DBRef<'k,'v>(name, rep, fun () -> input.unwrappedTestSeq)

    let output(input: ActiveSeq<'k,'v>): DBRef<'k,'v> = 
        outputWithName "noname" input
//type RunningJob()=
//    member this.wait(): unit = ()

//let setup<'inkey,'invalue, 'outkey,'outvalue when 'inkey: equality and 'outkey: equality> (seq: ActiveSeq<'inkey,'invalue>) (job: Job1_1<'inkey,'invalue,'outkey,'outvalue>): ActiveSeq<'outkey,'outvalue> = 
//    failwith ""

//this is wrong, 
//need ActiveSeq.asGeneratorJob(): job_<Null,Null,'k,'v>
//with compostion a*b*c = (a*b)*c = a*(b*c), but application is not the same

let p_job1(x:'a):'b = failwith "na"
let p_job2(x:'a)(y:'b):'c = failwith "na"
let compose1(f: 'a -> 'b) (g: 'b -> 'c): 'a -> 'c = failwith "na"
//so compose2(job1,job2, combined) -> (job1:'a -> c)(job2:'b->'c)
//so compose2(job1,job2, combined) -> (job1:'a -> unit)(job2:'b->unit)(out: 'unit -> 'c) //this seems to be the best option
//but then if I compse to the right job1>>some1, job2>>some2, (some1 and some2)
//composition is linear, while application is not
let compose2(f: 'a1 -> 'b) (g: 'a2->'b) (h: 'b -> 'c): ('a1 -> 'c)*('a2->'c) = failwith "na"


   
    //failwith ""
(*
type BindEmitter<'key1,'value1 when 'key1: equality>() =
    let mutable job: option<Job1_1<'key1,'value1,'key2,'value2>> = None
                            
    interface Emitter<'key1,'value1> with 
        member this.emit(cell2: KeyValueCell<'key1, 'value1>) = 
                //job2.onProcess(cell2) emitter2 
                    match job with 
                     | None -> ()
                     | Some (v) ->
                            v.onProcess(cell2)
*)
(*
let bind<'key1,'value1,'key2,'value2,'key3,'value3 when 'key1: equality and 'key2: equality and 'key3: equality> 
    (input: ActiveSeq<'key1, 'value1>) (job: Job1_1<'key1,'value1,'key2,'value2>): ActiveSeq<'key2, 'value2> = 
        input.bind(job)
let (|>>) = bind
*)