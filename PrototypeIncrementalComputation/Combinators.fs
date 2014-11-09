module Combinators

open Interfaces
open CombinatorsImpl

let compose<'key1,'value1,'key2,'value2,'key3,'value3 when 'key1: equality and 'key2: equality and 'key3: equality>(job1: Job1_1<'key1,'value1,'key2,'value2>) (job2: Job1_1<'key2,'value2,'key3,'value3>): Job1_1<'key1,'value1,'key3,'value3> = 
    new CompositeJob<'key1,'value1,'key2,'value2,'key3,'value3>(job1, job2) :> Job1_1<'key1,'value1,'key3,'value3>

let filter<'key,'value when 'key: equality>(pred: ('key*'value) -> bool): Job1_1<'key,'value, 'key,'value> = 
    new Filter<'key,'value>(pred) :> Job1_1<'key,'value,'key,'value>
    
let reporter<'key,'value when 'key: equality and 'value: equality>(name: string) = 
    new Reporter<'key,'value>(name)

let intSum(): Job1_1<Null, int, Null, int> = new IntSum() :> Job1_1<Null, int, Null, int>

let longCount (): Job1_1<Null, Null , Null, int64> = 
    new LongCounter() :> Job1_1<Null, Null, Null, int64>

let reduceByKey<'key, 'invalue, 'outvalue when 'key: equality>(nestedJobFactory: unit -> Job1_1<Null,'invalue, Null, 'outvalue>): Job1_1<'key,'invalue, 'key,'outvalue> =
    new ReduceByKey<'key, 'invalue, 'outvalue>(nestedJobFactory):> Job1_1<'key,'invalue, 'key,'outvalue>

let explodeValues<'key,'value when 'key: equality and 'value: comparison>(): Job1_1<'key,Set<'value>, 'key,'value> =
    new ExplodeValues<'key,'value>() :> Job1_1<'key,Set<'value>, 'key,'value>

let map<'key,'value when 'key: equality>(mapFun: ('key*'value) -> ('key*'value)) =
    new Map<'key,'value>(mapFun)

let mapValues<'key,'invalue, 'outvalue when 'key: equality> (mapValuesFun: 'invalue -> 'outvalue): Job1_1<'key,'invalue, 'key,'outvalue> = 
    new MapValues<'key, 'invalue, 'outvalue>(mapValuesFun) :> Job1_1<'key,'invalue, 'key,'outvalue>

let swapKeyValues<'key,'value when 'key: equality and 'value: equality>() : Job1_1<'key,'value, 'value,'key> = 
    new SwapKeyValues<'key, 'value>() :>  Job1_1<'key,'value, 'value,'key>

let topKByValue<'key,'value when 'key: equality and 'value: comparison>(n: int, ascending: bool): Job1_1<'key,'value, 'key,'value> =  
    new TopKByValue<'key,'value>(n, ascending) :> Job1_1<'key,'value, 'key,'value>

let crossProduct<'value when 'value: equality>(): Job1_1<Null, 'value, Null, ('value*'value)> =
    new CrossProduct<'value>() :> Job1_1<Null, 'value, Null, ('value*'value)>

let mapExtended<'key1,'value1,'key2,'value2 when 'key1: equality and 'key2: equality and 'value2: equality>(map: ('key1*'value1) -> ('key2*'value2)) =
    new MapExtended<'key1,'value1,'key2,'value2>(map) :> Job1_1<'key1,'value1, 'key2,'value2> 

let joinPredicate<'leftValue,'rightValue, 'combinedValue>(leftJoinProp: JoinProp, 
                                                              rightJoinProp: JoinProp, 
                                                              comb: option<'leftValue>*option<'rightValue> -> option<'combinedValue>): Job1_1<Null, Either<'leftValue, 'rightValue>, Null, 'combinedValue> =
    new Join<'leftValue, 'rightValue, 'combinedValue>(leftJoinProp, rightJoinProp, comb) :> Job1_1<Null, Either<'leftValue, 'rightValue>, Null, 'combinedValue>


let (>>>) = compose


type ActiveCell<'key,'value  when 'key: equality>(s: ActiveSeq<'key,'value>, k:'key, v: 'value) =
    let mutable lastValue = v
    do
        s.onProcess (Inserted (k,v))
    
    member this.modifyValue(newValue:'value) = 
                s.onProcess(Modified(k, (lastValue, newValue)))
                lastValue <- newValue

    member this.delete() = 
                s.onProcess(Deleted(k, lastValue))
and
    ActiveEmitter() =
        interface Emitter<unit, unit> with 
            member this.emit(cell: KeyValueCell<unit, unit>) = ()
and
    ActiveSeq<'key, 'value when 'key: equality>() = 
        let mutable job: option<list<Job1_1<'key,'value, unit, unit>>> = None
        let mutable merger: option<ActiveSeq<'key,'value>*ActiveSeq<'key,'value>> = None

        let emitter = new ActiveEmitter()
        
        let mutable onProcessEmitter: option<Emitter<'key,'value>> = None

        member this.setOnProcessEmitter(emitter: Emitter<'key,'value>): unit = 
            onProcessEmitter <- Some(emitter)

        member this.setJob(_job: Job1_1<'key,'value, unit, unit>): unit = 
            match job with
             | None -> 
                job <- Some([_job])
             | Some lst ->
                let newLst = List.append lst [_job]
                job <- Some newLst


        //member this.setJob2(_job2: Job1_1<'key,'value, unit, unit>): unit = 
        //    job2 <- Some _job2
                    
        member this.insert(k: 'key, v:'value): ActiveCell<'key,'value> = 
            let cell = new ActiveCell<'key,'value>(this, k, v)
            cell
            
        member this.onProcess(cell: KeyValueCell<'key,'value>): unit = 
            match onProcessEmitter with
             | None ->
                    let jobs = job.Value
                    for job in jobs do
                        job.onProcess cell emitter
                    match merger with
                     | Some(s1,s2) -> 
                        s1.onProcess(cell)
                        s2.onProcess(cell)
                     | _ -> ()
             | Some(v) ->
                    v.emit(cell)

        member this.wait() = ()

        member this.bind<'keyOut, 'valueOut when 'keyOut: equality>(job: Job1_1<'key, 'value, 'keyOut, 'valueOut>): ActiveSeq<'keyOut, 'valueOut> =
                    let newSeq = new ActiveSeq<'keyOut, 'valueOut>()
                    let binder = new Binder<'keyOut, 'valueOut>(newSeq)
                    this.setJob(job >>> binder)              
                    //failwith "todo"
                    newSeq

        member this.bind2<'keyOut, 'valueOut when 'keyOut: equality>(s12: ActiveSeq<'key,'value>*ActiveSeq<'key,'value>): unit =
                    (*
                    let newSeq = new ActiveSeq<'keyOut, 'valueOut>()
                    let binder = new Binder<'keyOut, 'valueOut>(newSeq)
                    this.setJob(job1 >>> binder)
                    this.setJob2(job2 >>> binder)              
                    //failwith "todo"*)
                    //newSeq
                    merger <- Some(s12)       

    and
      Binder<'key,'value when 'key: equality>(newSeq: ActiveSeq<'key,'value>) =
        interface Job1_1<'key,'value, unit,unit> with
            member this.onProcess(cell: KeyValueCell<'key,'value>) (emitter: Emitter<unit,unit>): unit =
                newSeq.onProcess(cell)
    and
      NestedReducerJob<'invalue, 'outvalue>(pred: ActiveSeq<Null,'invalue> -> ActiveSeq<Null, 'outvalue>) =
        let newSeq = new ActiveSeq<Null, 'invalue>()
        let modifiedSeq = pred newSeq

        interface Job1_1<Null,'invalue, Null,'outvalue> with
            member this.onProcess(cell: KeyValueCell<Null,'invalue>) (emitter: Emitter<Null,'outvalue>): unit =
                modifiedSeq.setOnProcessEmitter(emitter)
                newSeq.onProcess(cell)            
                //newSeq.onProcess(cell)
                ()
                                        
type DBRef<'k,'v when 'k: equality and 'v: equality>(name: string, reporter: Reporter<'k,'v>) =
    member this.getDB(): list<'k*'v> = 
        failwith "na"
    member this.dumpContents(): unit = 
        reporter.reportState()

module ActiveSeq = 
    //make job a func with onProcess being apply
    let filter<'key,'value when 'key: equality>(pred: ('key*'value) -> bool) (input: ActiveSeq<'key,'value>): ActiveSeq<'key,'value> = 
        input.bind(filter(pred))
       
    let map<'key1,'value1,'key2,'value2 when 'key1: equality and 'key2: equality and 'value2: equality>(mapPredicate: ('key1*'value1) -> ('key2*'value2)) (input: ActiveSeq<'key1, 'value1>): ActiveSeq<'key2, 'value2> =
    //let map<'key,'value when 'key: equality>(mapFun: ('key*'value) -> ('key*'value)) (input: ActiveSeq<'key, 'value>): ActiveSeq<'key, 'value>=
        input.bind(mapExtended(mapPredicate))

    let collect<'key1,'value1,'key2,'value2 when 'key1: equality and 'key2: equality and 'value1: comparison and 'value2: comparison>
        (pred: ('key1*'value1) -> seq<('key2*'value2)>) (input: ActiveSeq<'key1, 'value1>): ActiveSeq<'key2, 'value2> =
        let job = new Collect<'key1, 'value1, 'key2, 'value2>(pred) // mapValues(fun (_,v) -> Set.ofSeq(pred(v))) >>> explodeValues ()
        input.bind(job)

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
            let job1 = mapExtended pred1
            let job2 = mapExtended pred2
            let newSeq = new ActiveSeq<'keyOut, 'valueOut>()
            let binder = new Binder<'keyOut, 'valueOut>(newSeq)
            do input1.setJob(job1 >>> binder)
            do input2.setJob(job2 >>> binder)
            newSeq
            //newSeq.bind2(job1, job2)
            
            //failwith "na"

    let intSum(input: ActiveSeq<Null, int>): ActiveSeq<Null, int> = 
        let job = intSum ()
        input.bind(job)    

    let toSet<'a when 'a: comparison>(input: ActiveSeq<Null, 'a>): ActiveSeq<Null, Set<'a>> = 
        let job = SetSum ()
        input.bind(job)    

    let reduceByKey<'key, 'invalue, 'outvalue when 'key: equality and 'invalue: equality>
                    (pred: ActiveSeq<Null,'invalue> -> ActiveSeq<Null, 'outvalue>)
                    (input: ActiveSeq<'key, 'invalue>)
                    : ActiveSeq<'key, 'outvalue> = 
        let buildNestedJob() = 
            let binder = new NestedReducerJob<'invalue, 'outvalue>(pred)
            binder :> Job1_1<Null,'invalue,Null,'outvalue>

        let job = reduceByKey(buildNestedJob) 
        input.bind(job)                
        //()

    //todo: how do we deal with nested sequence
    let topKByValuePred<'key,'value, 'cmpValue when 'key: equality and 'cmpValue: comparison>(n: int, pred: 'value -> 'cmpValue)
                    (input: ActiveSeq<'key,'value>): ActiveSeq<'key, 'value> =  
        let job = new TopKByValuePredicate<'key,'value, 'cmpValue>(n, true, pred) 
        input.bind(job)   

    let topKByValue<'key,'value when 'key: equality and 'value: comparison>(n: int, ascending: bool)
                    (input: ActiveSeq<'key,'value>): ActiveSeq<'key, 'value> =  
        let job = TopKByValue<'key,'value>(n, ascending)
        input.bind(job)   

    let outputWithName(name: string) (input: ActiveSeq<'k,'v>): DBRef<'k,'v> = 
        let rep = reporter<'k,'v>(name)
        input.bind(rep) |> ignore
        new DBRef<'k,'v>(name, rep)

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

let setup<'inkey,'invalue when 'inkey: equality> (seq: ActiveSeq<'inkey,'invalue>) (job: Job1_1<'inkey,'invalue,unit,unit>): unit = 
    //or just create a generator job
    seq.setJob(job)
    //job.onProcess
    
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
let bind<'key1,'value1,'key2,'value2,'key3,'value3 when 'key1: equality and 'key2: equality and 'key3: equality> 
    (input: ActiveSeq<'key1, 'value1>) (job: Job1_1<'key1,'value1,'key2,'value2>): ActiveSeq<'key2, 'value2> = 
        input.bind(job)
let (|>>) = bind