module MapExtended

open Interfaces

type Map<'key1,'value1,'key2,'value2 when 'key1: equality and 'key2: equality and 'value2: equality>(map: ('key1*'value1) -> ('key2*'value2)) = 
    interface Job1_1<'key1,'value1, 'key2,'value2> with
        member this.keepsState = false
        member this.isStateEmpty = true

        member this.onProcess(cell: KeyValueCell<'key1,'value1>) (emitter: Emitter<'key2,'value2>): unit = 
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
                        if (modOldValue = modNewValue) then
                            ()
                        else
                            emitter.emit (Modified(modNewKey, (modOldValue, modNewValue)))
                    else
                        emitter.emit (Deleted (modOldKey,modOldValue))
                        emitter.emit (Inserted (modNewKey,modNewValue))