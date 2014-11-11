module TopKByValueWithPredicate

open Interfaces

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

        member this.keepsState = true
        member this.isStateEmpty = true

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
