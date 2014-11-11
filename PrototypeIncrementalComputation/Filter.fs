module Filter

open Interfaces

type Filter<'key,'value when 'key: equality>(filter: ('key*'value) -> bool) =
    interface Job1_1<'key,'value, 'key,'value> with
        member this.keepsState = false
        member this.isStateEmpty = true

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