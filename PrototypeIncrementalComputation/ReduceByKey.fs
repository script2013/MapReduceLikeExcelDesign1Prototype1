module ReduceByKey

open Interfaces
open System.Collections

type NestedEmitter<'key,'value when 'key: equality > (cont:KeyValueCell<'key, 'value> -> unit) =
        interface Emitter<'key, 'value> with 
            member this.emit(cell: KeyValueCell<'key, 'value>) = cont cell

let nestedEmitter<'key,'value when 'key: equality > (cont:KeyValueCell<'key, 'value> -> unit) = new NestedEmitter<'key,'value>(cont)

//TODO:NEED TO FIGURE OUT IF THIS JOB ALWAYS PRODUCES THE RIGHT RESULTS
type ReduceByKey<'key, 'invalue, 'outvalue when 'key: equality>(nestedJobFactory: unit -> Job1_1<Null,'invalue, Null, 'outvalue>) =  //(getReducerTask: 'key -> Job1_1<Null, 'invalue, Null, 'outvalue>) = 
    let dict = new Generic.Dictionary<'key, Job1_1<Null,'invalue, Null, 'outvalue> >()
    let countInserted = new Generic.Dictionary<'key, int>()

    interface Job1_1<'key,'invalue, 'key,'outvalue> with
        member this.keepsState = false
        member this.isStateEmpty = true

        member this.onProcess(cell: KeyValueCell<'key,'invalue>) (emitter: Emitter<'key,'outvalue>): unit =
            match cell with
                | Inserted (k: 'key,v) ->

                    let ok, _ = dict.TryGetValue(k)
                    if not ok then
                        dict.Add(k, nestedJobFactory())
                    let reducer = dict.[k]
                    reducer.onProcess (Inserted(Null, v)) (nestedEmitter (fun (outCell: KeyValueCell<Null, 'outvalue>) ->
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
                    reducer.onProcess (Deleted (Null, v)) (nestedEmitter (fun (outCell: KeyValueCell<Null, 'outvalue>) ->
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
                     reducer.onProcess (Modified (Null, (oldValue, newValue))) (nestedEmitter (fun (outCell: KeyValueCell<Null, 'outvalue>) ->
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