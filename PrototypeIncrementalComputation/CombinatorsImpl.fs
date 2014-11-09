module CombinatorsImpl

open Interfaces

type Filter<'key,'value when 'key: equality>(filter: ('key*'value) -> bool) =
    interface Job1_1<'key,'value, 'key,'value> with
        member this.onProcess(cell: KeyValueCell<'key,'value>) (emitter: Emitter<'key,'value>): unit =
            match cell with
             | Inserted (k,v) -> 
                    if filter (k,v) then
                        emitter.emit (Inserted (k,v))
             | Deleted (k,v) ->
                    if filter (k,v) then
                        emitter.emit (Deleted(k,v))
             | Modified (key,(oldValue, newValue)) ->
                    let oldFilter = filter (key, oldValue)
                    let newFilter = filter (key, newValue)
                    if (oldFilter && not newFilter) then
                        emitter.emit (Deleted(key, oldValue))
                    else if (not oldFilter && newFilter) then
                        emitter.emit (Inserted (key,newValue))
                    else if (oldFilter && newFilter) then
                        emitter.emit (Modified (key, (oldValue, newValue)))

open System.Collections
type Reporter<'key,'value when 'key: equality and 'value: equality>(name: string) =
    let multimap = new Generic.Dictionary<('key*'value), Generic.List<'key*'value>>()
    member this.reportState() =
        printfn ">>%s-------------------------" name
        for kvs in multimap do
            let k, vs = kvs.Key, kvs.Value
            for v in vs do
                printfn "%A" (v)

    interface Job1_1<'key,'value, unit,unit> with
        member this.onProcess(cell: KeyValueCell<'key,'value>) (emitter: Emitter<unit,unit>): unit =
            match cell with
             | Inserted (k,v) -> 
                    let el = (k,v)
                    printfn "Inserted %A" el
                    let ok,vs = multimap.TryGetValue el
                    if ok then
                        vs.Add(el)
                    else
                        let l = new Generic.List<'key*'value>()
                        l.Add(el)
                        multimap.Add(el, l)

             | Deleted (k,v) ->
                    printfn "Deleted %A" (k,v)
                    let el = (k,v)
                    let lst = multimap.[el]
                    lst.RemoveAt(lst.Count - 1)
                    if (lst.Count = 0) then
                        multimap.Remove(el) |> ignore


             | Modified (key,(oldValue, newValue)) ->
                    printfn "Modified %A" (key,(oldValue, newValue))
                    let el = (key,oldValue)
                    let lst = multimap.[el]
                    lst.RemoveAt(lst.Count - 1)
                    if (lst.Count = 0) then
                        multimap.Remove(el) |> ignore

                    let el = (key,newValue) 
                    let ok,vs = multimap.TryGetValue el
                    if ok then
                        vs.Add(el)
                    else
                        let l = new Generic.List<'key*'value>()
                        l.Add(el)
                        multimap.Add(el, l)



type Map<'key,'value when 'key: equality>(map: ('key*'value) -> ('key*'value)) = 
    interface Job1_1<'key,'value, 'key,'value> with
        member this.onProcess(cell: KeyValueCell<'key,'value>) (emitter: Emitter<'key,'value>): unit =
            match cell with
             | Inserted (k,v) -> 
                    emitter.emit (Inserted (map(k,v)))
             | Deleted (k,v) ->
                    emitter.emit (Deleted (map(k,v)))
             | Modified (key,(oldValue, newValue)) ->
                    let (modOldKey, modOldValue) = map (key, oldValue)
                    let (modNewKey, modNewValue) = map (key, newValue)
                    //optimization
                    if (modOldKey = modNewKey) then
                        emitter.emit (Modified(modNewKey, (modOldValue, modNewValue)))
                    else
                        emitter.emit (Deleted (map(modOldKey,modOldValue)))
                        emitter.emit (Inserted (map(modNewKey,modNewValue)))

type MapExtended<'key1,'value1,'key2,'value2 when 'key1: equality and 'key2: equality and 'value2: equality>(map: ('key1*'value1) -> ('key2*'value2)) = 
    interface Job1_1<'key1,'value1, 'key2,'value2> with
        member this.onProcess(cell: KeyValueCell<'key1,'value1>) (emitter: Emitter<'key2,'value2>): unit = 
            match cell with
             | Inserted (k,v) -> 
                    emitter.emit (Inserted (map(k,v)))

             | Deleted (k,v) ->
                    emitter.emit (Deleted (map(k,v)))
   
             | Modified (key,(oldValue, newValue)) ->
                    let (modOldKey, modOldValue) = map (key, oldValue)
                    let (modNewKey, modNewValue) = map (key, newValue)
                    //emitter.emit (Deleted (modOldKey,modOldValue))
                    //emitter.emit (Inserted (modNewKey,modNewValue))

                    //optimization
                    if (modOldKey = modNewKey) then
                        if (modOldValue = modNewValue) then
                            ()
                        else
                            emitter.emit (Modified(modNewKey, (modOldValue, modNewValue)))
                    else
                        emitter.emit (Deleted (modOldKey,modOldValue))
                        emitter.emit (Inserted (modNewKey,modNewValue))
                    

type MapValues<'key,'invalue, 'outvalue when 'key: equality>(mapValues: 'invalue -> 'outvalue) = 
    interface Job1_1<'key,'invalue, 'key,'outvalue> with
        member this.onProcess(cell: KeyValueCell<'key,'invalue>) (emitter: Emitter<'key,'outvalue>): unit =
            match cell with
             | Inserted (k,v) -> 
                    emitter.emit (Inserted (k, mapValues(v)))
             | Deleted (k,v) ->
                    emitter.emit (Deleted (k, mapValues(v)))
             | Modified (key,(oldValue, newValue)) ->
                    let (modOldValue) = mapValues (oldValue)
                    let (modNewValue) = mapValues (newValue)
                    //optimization
                    emitter.emit (Modified(key, (modOldValue, modNewValue)))

type SwapKeyValues<'key,'value when 'key: equality and 'value: equality>() = 
    interface Job1_1<'key,'value, 'value,'key> with
        member this.onProcess(cell: KeyValueCell<'key,'value>) (emitter: Emitter<'value,'key>): unit =
            match cell with
             | Inserted (k,v) -> 
                    emitter.emit (Inserted (v, k))
             | Deleted (k,v) ->
                    emitter.emit (Deleted (v, k))
             | Modified (key,(oldValue, newValue)) ->
                    emitter.emit (Deleted (oldValue, key))
                    emitter.emit (Inserted (newValue, key))

//try with a generic map
type ChangeKey<'key,'value, 'newkey, 'satelitevalue when 'key: equality and 'newkey: equality>(newKeySateliteData: 'value -> 'newkey*'satelitevalue) = 
    interface Job1_1<'key,'value, 'newkey,('key*'satelitevalue)> with
        member this.onProcess(cell: KeyValueCell<'key,'value>) (emitter: Emitter<'newkey,('key*'satelitevalue)>): unit =
            failwith "na"

//TODO: generalize to flatten
type ExplodeValues<'key,'value when 'key: equality and 'value: comparison>() = 
    interface Job1_1<'key,Set<'value>, 'key,'value> with
        member this.onProcess(cell: KeyValueCell<'key,Set<'value>>) (emitter: Emitter<'key,'value>): unit =
            match cell with
             | Inserted (k,vs) -> 
                    for v in vs do
                        emitter.emit (Inserted (k,v))
             | Deleted (k,vs) ->
                    for v in vs do
                        emitter.emit (Deleted (k,v))
             | Modified (key,(oldValues, newValues)) ->
                    //oldvalue = unique old, same old/new
                    //newvalues = unique new, same old/new
                    let onlyInOld = Set.difference oldValues newValues
                    let onlyInNew = Set.difference newValues oldValues
                    for v in onlyInOld do
                        emitter.emit (Deleted (key,v))
                    for v in onlyInNew do
                        emitter.emit (Inserted (key,v))                                            


type MergeStreams<'inKey1,'inValue1,'inKey2, 'inValue2, 'outKey,'outValue when 'inKey1: equality and 'inKey2: equality and 'outKey: equality> 
    (choose1: KeyValueCell<'inKey1,'inValue1> -> KeyValueCell<'outKey,'outValue>,
     choose2: KeyValueCell<'inKey2,'inValue2> -> KeyValueCell<'outKey,'outValue>) = 
    interface Job2_1<'inKey1,'inValue1,'inKey2, 'inValue2, 'outKey,'outValue> with
        member this.onProcess1(cell: KeyValueCell<'inKey1,'inValue1>) (emitter: Emitter<'outKey,'outValue>) =
                emitter.emit(choose1(cell))

        member this.onProcess2(cell: KeyValueCell<'inKey2,'inValue2>) (emitter: Emitter<'outKey,'outValue>) =
                emitter.emit(choose2(cell))

type Collect<'key1,'value1, 'key2, 'value2 when 'key1: equality and 'value1: comparison and 'key2: equality and 'value2: comparison>(pred: ('key1*'value1) -> seq<'key2*'value2>) = 
    interface Job1_1<'key1,'value1, 'key2,'value2> with
        member this.onProcess(cell: KeyValueCell<'key1,'value1>) (emitter: Emitter<'key2,'value2>): unit =
                failwith "todo"
            (*
            match cell with
             | Inserted (k,vs) -> 
                    for v in vs do
                        emitter.emit (Inserted (k,v))
             | Deleted (k,vs) ->
                    for v in vs do
                        emitter.emit (Deleted (k,v))
             | Modified (key,(oldValues, newValues)) ->
                    //oldvalue = unique old, same old/new
                    //newvalues = unique new, same old/new
                    let onlyInOld = Set.difference oldValues newValues
                    let onlyInNew = Set.difference newValues oldValues
                    for v in onlyInOld do
                        emitter.emit (Deleted (key,v))
                    for v in onlyInNew do
                        emitter.emit (Inserted (key,v))  *)

//this is too difficult, better use map and Join
type LeftJoin<'key1,'value1,'key2,'value2, 'joinKey, 'joinValue when 'key1: equality and 'key2: equality and 'joinKey: equality> 
        (leftKey: ('key1*'value1) -> 'joinKey,
         rightKey: ('key2*'value2) -> 'joinKey,
         combineValues: 'joinKey -> ('key1*'value1) -> ('key2*'value2) -> 'joinValue) =
    
    interface Job1_1<Either<'key1, 'key2>, Either<'value1, 'value2>, 'joinKey, 'joinValue> with
        member this.onProcess(cell: KeyValueCell<Either<'key1,'key2>, Either<'value1, 'value2>>) (emitter: Emitter<'joinKey,'joinValue>): unit =
                   match cell with
                    | Inserted(Left(k),Left(v)) ->
                        ()//option 1: call combineValues: (key1,value1) Null
                          //option 2: wait until the corresponding on the right hand has arrived (choose this option right now)
                          //option 3: do not allow join
                          //if many right cells have arrived, must join with all of them
                    | Inserted(Right(k), Right(v)) ->
                        () //find if the corresponding left cells have arrived and join
                    | Deleted(Left(k), Left(v)) ->
                        () //if it has been joined, it must be deleted
                    | Deleted(Right(k), Right(v)) ->
                        () //cells joined with it must be deleted
                    | _ -> failwith "internal error"

type JoinProp = ZeroToMany | OneToMany | One | ZeroToOne

type Join<'leftValue,'rightValue, 'combinedValue>(leftJoinProp: JoinProp, 
                                                  rightJoinProp: JoinProp, 
                                                  comb: option<'leftValue>*option<'rightValue> -> option<'combinedValue>) =
    let leftValues = System.Collections.Generic.List<'leftValue>()
    let rightValues = System.Collections.Generic.List<'rightValue>()
                                                   
    interface Job1_1<Null, Either<'leftValue, 'rightValue>, Null, 'combinedValue> with
        //change onProcess to take a sequence of cells. In this way we are free to "batch" as efficiently as we want
        member this.onProcess(cell: KeyValueCell<Null, Either<'leftValue, 'rightValue>>) (emitter: Emitter<Null,'combinedValue>): unit =
           match cell with
            | Inserted(k,v) ->
                 match v with
                  | Left leftV -> //those discinctitions will be avoided if leftValues/rightValues always contained None (it's actually better)
                        if rightValues.Count = 0 then
                            leftValues.Add(leftV)
                            let res = comb (Some leftV, None)
                            match res with
                             | None -> ()
                             | Some combValue ->
                                emitter.emit(Inserted(Null, combValue)) 
                        else
                            for rightV in rightValues do
                                let res = comb (Some leftV, Some rightV)
                                match res with
                                 | None -> ()
                                 | Some combValue ->
                                    emitter.emit(Inserted(Null, combValue)) 
                  | Right rightV -> () //same as above
            | Deleted (k,v) ->
                ()
                //if left was deleted, combine with all right (and remove)
            | Modified (k,(oldValue, newValue)) ->
                    failwith "todo"
                    //oldValue/newValue should both be left or right
                    //if left, then emit deletes on all left
            
           ()
            (*
            cell match
             | Inserted(
             | Deleted(
             | Modified(
            counter <- counter + 
            if i = 0 then
                emitter.emit(Inserted(Null, counter)
            //if it's 0 just emit Insert, if not zero    
            
            *)

//rename to all pairs
type CrossProduct<'value when 'value: equality>() =
    let lst = Generic.List<'value>()
    interface Job1_1<Null, 'value, Null, ('value*'value)> with
        //change onProcess to take a sequence of cells. In this way we are free to "batch" as efficiently as we want
        member this.onProcess(cell: KeyValueCell<Null, 'value>) (emitter: Emitter<Null,'value*'value>): unit =
           //on input (a,b,c) output (a,b),(a,c),(b,c)
           match cell with
            | Inserted(_,v) ->
                for prevV in lst do
                    emitter.emit (Inserted(Null, (prevV, v)))
                lst.Add(v)
            | Deleted(_,v) ->
                let mutable state = 0
                let mutable delIdx = -1
                let mutable i = 0
                for prevV in lst do
                    if (state = 0) then
                        if (prevV = v) then
                            //skip and reverse state
                            state <- 1 
                            delIdx <- i
                        else
                            emitter.emit(Deleted(Null, (prevV, v)))
                    else 
                        emitter.emit(Deleted(Null, (v, prevV)))
                    i <- i + 1
                lst.RemoveAt(delIdx)

            | Modified(k, (oldV, newV)) ->
                    let job = this :> Job1_1<Null, 'value, Null, ('value*'value)>
                    job.onProcess(Deleted(k, oldV)) emitter
                    job.onProcess(Inserted(k, newV)) emitter


type NumericOps<'b, 'a> = {
    zero: 'a
    plus: 'a -> 'a -> 'a
    minus: 'a -> 'a -> 'a
    getCount:'b -> 'a
}

type NumericSum<'b, 'a>(ops: NumericOps<'b, 'a>) = 
    let mutable counter: 'a = ops.zero
    let mutable i = 0 //number of total inserted elements (excluding annihilating deletes)
    interface Job1_1<Null, 'b, Null, 'a> with
        member this.onProcess(cell: KeyValueCell<Null,'b>) (emitter: Emitter<Null,'a>): unit =
             match cell with
                | Inserted(_,v) -> 
                    let oldValue = counter 
                    counter <-  ops.plus counter (ops.getCount v)
                    i <- i + 1
                    if (i = 1) then
                        emitter.emit(Inserted(Null, counter))
                    else
                        emitter.emit(Modified(Null, (oldValue, counter)))
                | Deleted(_,v) -> 
                    let oldValue = counter 
                    counter <- ops.minus counter (ops.getCount v)
                    i <- i - 1
                    if (i = 0) then
                        emitter.emit(Deleted(Null, oldValue))
                    else
                        emitter.emit(Modified(Null, (oldValue, counter))) 
                | Modified(_, (oldV, newV)) ->
                    let oldValue = counter 
                    counter <- (ops.plus (ops.minus counter (ops.getCount oldV)) (ops.getCount newV))
                    emitter.emit(Modified(Null, (oldValue, counter)))

type IntSum() = inherit NumericSum<int, int>({zero = 0; plus = (+); minus = (-); getCount = id})
type LongCounter() = inherit NumericSum<Null, int64>({zero = 0L; plus = (+); minus = (-); getCount = fun Null -> 1L})
 
type SetSum<'a when 'a: comparison>() = inherit NumericSum<'a, Set<'a>>({zero = Set.empty; plus = Set.union ; minus = Set.difference; getCount = Set.singleton})

(*
type MkList<'invalue>() = 
    let mutable counter: Generic.List<'invalue> = Generic.List()
    let mutable i = 0 //number of total inserted elements (excluding annihilating deletes)
    interface Job1_1<Null, 'invalue, Null, list<'invalue>> with
        member this.onProcess(cell: KeyValueCell<Null,'invalue>) (emitter: Emitter<Null,list<'invalue>>): unit =
             match cell with
                | Inserted(_,v) -> 
                    let oldValue = counter 
                    counter <-  ops.plus counter (ops.getCount v)
                    i <- i + 1
                    if (i = 1) then
                        emitter.emit(Inserted(Null, counter))
                    else
                        emitter.emit(Modified(Null, (oldValue, counter)))
                | Deleted(_,v) -> 
                    let oldValue = counter 
                    counter <- ops.minus counter (ops.getCount v)
                    i <- i - 1
                    if (i = 0) then
                        emitter.emit(Deleted(Null, oldValue))
                    else
                        emitter.emit(Modified(Null, (oldValue, counter))) 
                | Modified(_, (oldV, newV)) ->
                    let oldValue = counter 
                    counter <- (ops.plus (ops.minus counter (ops.getCount oldV)) (ops.getCount newV))
                    emitter.emit(Modified(Null, (oldValue, counter)))*)

//not clear how to use it , but the idea is to put a criteria (e.g. positions or quantiles) and subscribe to changes there
//type SortReduceByValue<'key, 'invalue, 'outvalue when 'key: equality>(nestedJobFactory: unit -> Job1_1<'key,'invalue, Null, 'outvalue>) =  //(getReducerTask: 'key -> Job1_1<Null, 'invalue, Null, 'outvalue>) = 
//    interface Job1_1<'key,'invalue, 'key,'outvalue> with
//        member this.onProcess(cell: KeyValueCell<'key,'invalue>) (emitter: Emitter<'key,'outvalue>): unit =
                    //insert in pq

type TopKByValue<'key,'value when 'key: equality and 'value: comparison>(n: int, ascending: bool) =  //(getReducerTask: 'key -> Job1_1<Null, 'invalue, Null, 'outvalue>) = 
    //let data = System.Collections.ArrayList<('key*'value)>()
    let top = System.Collections.Generic.List<('key*'value)>()
    let bottom = System.Collections.Generic.List<('key*'value)>()
    do printfn "topby:create"

    let lessThan (v0:'value) (v1:'value) = 
        if ascending then v0 < v1 else v1 < v0
    
    let getMin(lst: System.Collections.Generic.List<'key*'value>):'key*'value = 
        if ascending then
            lst |> Seq.minBy snd
        else
            lst |> Seq.maxBy snd 

    let getMax(lst: System.Collections.Generic.List<'key*'value>):'key*'value = 
        if ascending then
            lst |> Seq.maxBy snd
        else
            lst |> Seq.minBy snd 

    let getBottomElementOfTop() = //largest element in smallest 
        getMax top
    
    let getTopElementOfBottom() = 
        //bottom |> Seq.minBy (snd>>swapSign)
        getMin bottom

    let replaceElement (lst: System.Collections.Generic.List<('key*'value)>) (old: 'key*'value) (newEl: 'key*'value) =
        let idx = lst.IndexOf(old)
        lst.[idx] <- newEl

    let addElement (lst: System.Collections.Generic.List<('key*'value)>) (newEl: 'key*'value):unit =
        lst.Add(newEl)

    let removeElement (lst: System.Collections.Generic.List<('key*'value)>) (old: 'key*'value): unit =
        let idx = lst.IndexOf(old)
        lst.RemoveAt(idx)

    let containsElement (lst: System.Collections.Generic.List<('key*'value)>) (old: 'key*'value): bool =
        lst.Contains(old)
                    
    interface Job1_1<'key,'value, 'key,'value> with
        member this.onProcess(cell: KeyValueCell<'key, 'value>) (emitter: Emitter<'key, 'value>): unit =
            match cell with
                | Inserted (k,v) ->
                    if (top.Count < n) then
                        top.Add((k,v))
                        emitter.emit(Inserted(k,v))
                    else
                        //need to remove the one with the lowest score
                        let leaving = getBottomElementOfTop()
                        let (kLeaving, vLeaving) = leaving
                        if lessThan v vLeaving then
                            replaceElement top leaving (k, v)
                            addElement bottom leaving //put the element in bottom, we could need it again
                            
                            //temporary replacing. interaction with reduceBy
                            //where the TopBy job state may be deleted when delete is issued
                            //must introduce a method like nullable state for deletion
                            emitter.emit (Inserted (k,v))
                            emitter.emit (Deleted leaving)

                        else
                            addElement bottom (k,v)
                | Deleted (k,v) ->
                    if containsElement bottom (k,v) then //one way to figure out is by the threshold value      
                        removeElement bottom (k,v)
                    else if containsElement top (k,v) then
                        removeElement top (k,v)
                        emitter.emit (Deleted (k, v))
                        if (top.Count < n && bottom.Count > 0) then
                            let newEl = getTopElementOfBottom()
                            removeElement bottom newEl
                            addElement top newEl
                            emitter.emit (Inserted newEl)
                    else 
                        failwith "expected to remove something from TopKByValue"//verify 
                        
                | Modified (k,(oldValue,newValue)) ->
                    let job = (this :> Job1_1<'key,'value, 'key,'value>)
                    //special cases
                    //both oldValue and newValue
                    let threshold = getBottomElementOfTop()
                    let (_,threholdV) = threshold
                    if containsElement top (k, oldValue) && (lessThan newValue threholdV) then
                        replaceElement top (k,oldValue) (k,newValue)
                        emitter.emit (Modified (k, (oldValue, newValue)))
                    else if (containsElement bottom (k,oldValue)) && (lessThan threholdV newValue) then
                        //todo: handling equal elements, best to have 3 lists
                        replaceElement bottom (k,oldValue) (k,newValue)
                        //nothing changes to the outside, don't emit
                    else
                        job.onProcess (Deleted(k, oldValue)) emitter
                        job.onProcess (Inserted(k, newValue)) emitter
                    //if oldValue < newValue (then we may need to delete)
                    //if oldValue made the cut, and newValue did not make the cut then delete
                    (*
                    if containsElement bottom (k,oldValue) then
                        ()
                    else if containsElement top (k,oldValue) then
                        let leaving = getBottomElementOfTop()
                        let (kLeaving, vLeaving) = leaving
                        if newValue < vLeaving then                        
                            //old was emitted, new should be emitted
                            emitter.emit (Modified(k,(oldValue,newValue)))
                        else
                            //new element did not make the cut
                            //replace
                            let promoted = getTopElementOfBottom ()
                            replaceElement top (k, oldValue) promoted
                            replaceElement bottom promoted (k,newValue)
                            emitter.emit (Deleted (k, oldValue))
                            emitter.emit (Inserted promoted) *)





type TopKByValuePredicate<'key,'value, 'cmpValue when 'key: equality and 'cmpValue: comparison>(n: int, ascending: bool, pred: 'value -> 'cmpValue) =  //(getReducerTask: 'key -> Job1_1<Null, 'invalue, Null, 'outvalue>) = 
    //let data = System.Collections.ArrayList<('key*'value)>()
    let top = System.Collections.Generic.List<('key*'value)>()
    let bottom = System.Collections.Generic.List<('key*'value)>()

    let lessThan (v0:'value) (v1:'value) = 
        let v0_cmp = pred v0
        let v1_cmp = pred v1
        if ascending then v0_cmp < v1_cmp else v1_cmp < v0_cmp
    
    let getMin(lst: System.Collections.Generic.List<'key*'value>):'key*'value = 
        if ascending then
            lst |> Seq.minBy (snd>>pred)
        else
            lst |> Seq.maxBy (snd>>pred) 

    let getMax(lst: System.Collections.Generic.List<'key*'value>):'key*'value = 
        if ascending then
            lst |> Seq.maxBy (snd>>pred)
        else
            lst |> Seq.minBy (snd>>pred) 

    let getBottomElementOfTop() = //largest element in smallest 
        getMax top
    
    let getTopElementOfBottom() = 
        //bottom |> Seq.minBy (snd>>swapSign)
        getMin bottom

    let replaceElement (lst: System.Collections.Generic.List<('key*'value)>) (old: 'key*'value) (newEl: 'key*'value) =
        let idx = lst.IndexOf(old)
        lst.[idx] <- newEl

    let addElement (lst: System.Collections.Generic.List<('key*'value)>) (newEl: 'key*'value):unit =
        lst.Add(newEl)

    let removeElement (lst: System.Collections.Generic.List<('key*'value)>) (old: 'key*'value): unit =
        let idx = lst.IndexOf(old)
        lst.RemoveAt(idx)

    let containsElement (lst: System.Collections.Generic.List<('key*'value)>) (old: 'key*'value): bool =
        lst.Contains(old)
                    
    interface Job1_1<'key,'value, 'key,'value> with
        member this.onProcess(cell: KeyValueCell<'key, 'value>) (emitter: Emitter<'key, 'value>): unit =
            match cell with
                | Inserted (k,v) ->
                    if (top.Count < n) then
                        top.Add((k,v))
                        emitter.emit(Inserted(k,v))
                    else
                        //need to remove the one with the lowest score
                        let leaving = getBottomElementOfTop()
                        let (kLeaving, vLeaving) = leaving
                        if lessThan v vLeaving then
                            replaceElement top leaving (k, v)
                            addElement bottom leaving //put the element in bottom, we could need it again
                            //need to swap those (another option for fix is to make the key null, then emit modify)
                            emitter.emit (Inserted (k,v))
                            emitter.emit (Deleted leaving)
                        else
                            addElement bottom (k,v)
                | Deleted (k,v) ->
                    if containsElement bottom (k,v) then //one way to figure out is by the threshold value      
                        removeElement bottom (k,v)
                    else if containsElement top (k,v) then
                        removeElement top (k,v)
                        emitter.emit (Deleted (k, v))
                        if (top.Count < n && bottom.Count > 0) then
                            let newEl = getTopElementOfBottom()
                            removeElement bottom newEl
                            addElement top newEl
                            emitter.emit (Inserted newEl)
                    else 
                        failwith "expected to remove something from TopKByValue"//verify 
                        
                | Modified (k,(oldValue,newValue)) ->
                    let job = (this :> Job1_1<'key,'value, 'key,'value>)
                    //special cases
                    //both oldValue and newValue
                    let threshold = getBottomElementOfTop()
                    let (_,threholdV) = threshold
                    if containsElement top (k, oldValue) && (lessThan newValue threholdV) then
                        replaceElement top (k,oldValue) (k,newValue)
                        emitter.emit (Modified (k, (oldValue, newValue)))
                    else if (containsElement bottom (k,oldValue)) && (lessThan threholdV newValue) then
                        //todo: handling equal elements, best to have 3 lists
                        replaceElement bottom (k,oldValue) (k,newValue)
                        //nothing changes to the outside, don't emit
                    else
                        job.onProcess (Deleted(k, oldValue)) emitter
                        job.onProcess (Inserted(k, newValue)) emitter
                    //if oldValue < newValue (then we may need to delete)
                    //if oldValue made the cut, and newValue did not make the cut then delete
                    (*
                    if containsElement bottom (k,oldValue) then
                        ()
                    else if containsElement top (k,oldValue) then
                        let leaving = getBottomElementOfTop()
                        let (kLeaving, vLeaving) = leaving
                        if newValue < vLeaving then                        
                            //old was emitted, new should be emitted
                            emitter.emit (Modified(k,(oldValue,newValue)))
                        else
                            //new element did not make the cut
                            //replace
                            let promoted = getTopElementOfBottom ()
                            replaceElement top (k, oldValue) promoted
                            replaceElement bottom promoted (k,newValue)
                            emitter.emit (Deleted (k, oldValue))
                            emitter.emit (Inserted promoted) *)



type ContinuationEmitter<'key,'value when 'key: equality > (cont:KeyValueCell<'key, 'value> -> unit) =
        interface Emitter<'key, 'value> with 
            member this.emit(cell: KeyValueCell<'key, 'value>) = cont cell

let contEmitter<'key,'value when 'key: equality > (cont:KeyValueCell<'key, 'value> -> unit) = new ContinuationEmitter<'key,'value>(cont)

//TODO:NEED TO FIGURE OUT IF THIS JOB ALWAYS PRODUCES THE RIGHT RESULTS
type ReduceByKey<'key, 'invalue, 'outvalue when 'key: equality>(nestedJobFactory: unit -> Job1_1<Null,'invalue, Null, 'outvalue>) =  //(getReducerTask: 'key -> Job1_1<Null, 'invalue, Null, 'outvalue>) = 
    let dict = new Generic.Dictionary<'key, Job1_1<Null,'invalue, Null, 'outvalue> >()
    let countInserted = new Generic.Dictionary<'key, int>()
    interface Job1_1<'key,'invalue, 'key,'outvalue> with
        member this.onProcess(cell: KeyValueCell<'key,'invalue>) (emitter: Emitter<'key,'outvalue>): unit =
            match cell with
                | Inserted (k: 'key,v) ->

                    let ok, _ = dict.TryGetValue(k)
                    if not ok then
                        dict.Add(k, nestedJobFactory())
                    let reducer = dict.[k]
                    reducer.onProcess (Inserted(Null, v)) (contEmitter (fun (outCell: KeyValueCell<Null, 'outvalue>) ->
                                                match outCell with
                                                 | Inserted (_,value) ->
                                                        emitter.emit (Inserted(k, value))
                                                        let ok,cnt = countInserted.TryGetValue k
                                                        if ok then
                                                            countInserted.[k] <- cnt + 1
                                                        else
                                                            countInserted.Add(k, 1)

                                                 | Modified (_,(oldValue, newValue)) ->
                                                        emitter.emit (Modified(k,(oldValue,newValue)))
                                                 //| _ -> failwith "deleted is not expected here"
                                                 
                                                 | Deleted (_, value) ->
                                                    emitter.emit (Deleted(k, value))
                                                    let cnt = countInserted.[k]
                                                    //this is probably wrong. the rule to delete should depend on the nested job
                                                    //nested job should implement nullable state!!!
                                                    if cnt = 1 then
                                                        dict.Remove(k) |> ignore //should I delete?
                                                        printfn "delelting %A" k
                                                    else
                                                        countInserted.[k] <- cnt - 1                                                 
                                                 
                                                 
                                                 ))
                                                 
                | Deleted (k,v) -> //deletedKey might be handy here
                    let reducer = dict.[k]
                    reducer.onProcess (Deleted (Null, v)) (contEmitter (fun (outCell: KeyValueCell<Null, 'outvalue>) ->
                                                    //let outCell = this.reduceOnKey k (Deleted (Null, v)) //this might return none (which means delete
                                                    match outCell with
                                                     | Modified (_,(oldValue, newValue)) ->
                                                            emitter.emit (Modified(k,(oldValue,newValue)))
                                                     | Deleted (_, value) ->
                                                            emitter.emit (Deleted(k, value))
                                                            let cnt = countInserted.[k]
                                                            if cnt = 1 then
                                                                dict.Remove(k) |> ignore //should I delete?
                                                                printfn "delelting %A" k
                                                            else
                                                                countInserted.[k] <- cnt - 1
                                                                
                                                     | _ -> failwith "inserted is not expected here"))

                | Modified(k,(oldValue,newValue)) ->
                     let reducer = dict.[k]
                     reducer.onProcess (Modified (Null, (oldValue, newValue))) (contEmitter (fun (outCell: KeyValueCell<Null, 'outvalue>) ->
                                             match outCell with
                                             | Modified (_,(oldValue, newValue)) ->
                                                    emitter.emit (Modified(k,(oldValue,newValue)))
                                             | Deleted (_, value) ->
                                                    emitter.emit (Deleted(k, value))
                                                    let cnt = countInserted.[k]
                                                    if cnt = 1 then
                                                        dict.Remove(k) |> ignore //should I delete?
                                                        printfn "delelting %A" k
                                                    else
                                                        countInserted.[k] <- cnt - 1      

                                             | Inserted (_,value) ->
                                                emitter.emit (Inserted(k, value))
                                                let ok,cnt = countInserted.TryGetValue k
                                                if ok then
                                                    countInserted.[k] <- cnt + 1
                                                else
                                                    countInserted.Add(k, 1)
                                             | _ -> //failwith "inserted/deleted is not expected here"))                          




                                                    ()
                                                    ))
    //member this.reduceOnKey(key: 'key)(cell: KeyValueCell<Null, 'invalue>): KeyValueCell<Null, 'outvalue> =
            //todo
    //    reduce(cell)

// stream1 = (word, (docid, tf))
// stream2 = (word, idf) -> unique
// stream combined = (word, Either<(docid,tf), idf>)
// output stream = word, (doc, tf, idf), for each doc
// the joiner is seeing
//  imagine left outer join
//   left is new/deleted/modified
//   right is new/deleted/modified
// (new left, without seen right -- do nothing) //or (new left, null) -> emit. when you see right (then delete (new left, null). it's critical to see right before/left
// (new left, seen right already) -- perform the join and emit
// (deleted right) -- then emit (prev left, null) --> what are the null semantics
// (deleted right) -- then emit (delete prev left right)  --> will force to delete all seen.
// to do: introduce queueing to merge delete (k1,v1) /insert (k1,v2) into modify (k1,v1,v2)
// modified left (word,(...)). e.g. word deleted/tf changed from doc. (get the right value and emit (modified, (...), right value)
// modified right. get all left values
// JoinConfig: left (1..*) right (1). do not allow (allow left/nulls; allow right /nulls)
// SelfJoin( for a stream d1,d2,...,dn you should emit (d1,d2), ...
// on new d[i] -- get all inserted and emit (d1,new),(d2,new)
// on deleted d[i] -- get all old and emit delete ...
// on modified get all that were send and again send with modified
//left outer join  

type CompositeEmitter<'key2,'value2,'key3,'value3 when 'key2: equality and 'key3: equality>(job2: Job1_1<'key2,'value2,'key3,'value3>, emitter2: Emitter<'key3,'value3>) =
    interface Emitter<'key2,'value2> with 
        member this.emit(cell2: KeyValueCell<'key2, 'value2>) = 
                job2.onProcess(cell2) emitter2 

type CompositeJob<'key1,'value1,'key2,'value2,'key3,'value3 when 'key1: equality and 'key2: equality and 'key3: equality>(job1: Job1_1<'key1,'value1,'key2,'value2>, job2: Job1_1<'key2,'value2,'key3,'value3>) =
    interface Job1_1<'key1,'value1,'key3,'value3> with
        member this.onProcess(cell1: KeyValueCell<'key1,'value1>) (emitter: Emitter<'key3,'value3>): unit =   
                    let emitter1 = CompositeEmitter<'key2,'value2,'key3,'value3>(job2, emitter)
                    match cell1 with
                     (*| Modified(k,(oldV, newV)) ->
                            job1.onProcess(Deleted(k, oldV)) emitter1
                            job1.onProcess(Inserted(k, newV)) emitter1*)
                     | _ -> job1.onProcess(cell1) emitter1  


(*
type CompositeEmitter2<'key2,'value2,'key3,'value3 when 'key2: equality and 'key3: equality>(job2: Job1_1<'key2,'value2,'key3,'value3>, emitter2: Emitter<'key3,'value3>) =
    interface Emitter<'key2,'value2> with 
        member this.emit(cell2: KeyValueCell<'key2, 'value2>) = 
                job2.onProcess(cell2) emitter2 

type CompositeJob2<'key1,'value1,'key2,'value2,'key3,'value3 when 'key1: equality and 'key2: equality and 'key3: equality>(
        job1: Job1_1<'key1,'value1,'key2,'value2>, job2: Job1_1<'key2,'value2,'key3,'value3>) =
    interface Job1_1<'key1,'value1,'key3,'value3> with
        member this.onProcess(cell1: KeyValueCell<'key1,'value1>) (emitter: Emitter<'key3,'value3>): unit =   
                    let emitter1 = CompositeEmitter<'key2,'value2,'key3,'value3>(job2, emitter)
                    match cell1 with
                     (*| Modified(k,(oldV, newV)) ->
                            job1.onProcess(Deleted(k, oldV)) emitter1
                            job1.onProcess(Inserted(k, newV)) emitter1*)
                     | _ -> job1.onProcess(cell1) emitter1  

job1: 'a -> 'b
job2: 'm -> 'n

job2_1

job_m_1 >> job_1_n == job_m_n
bind(job_2_1, free, some_job) == job_1_1(free)
job_1_1(...)
so the generator should be just job_<unit,1>
the consumer should be job<1,unit> 
the whole pipeline is job<unit,unit> 
job_comb: unit -> either<'b,'n>

*)