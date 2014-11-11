module Reporter

open Interfaces

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
        member this.keepsState = true
        member this.isStateEmpty = false

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