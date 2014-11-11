module Merge

open Interfaces

(*
type MergeStreams<'inKey1,'inValue1,'inKey2, 'inValue2, 'outKey,'outValue when 'inKey1: equality and 'inKey2: equality and 'outKey: equality> 
    (choose1: KeyValueCell<'inKey1,'inValue1> -> KeyValueCell<'outKey,'outValue>,
     choose2: KeyValueCell<'inKey2,'inValue2> -> KeyValueCell<'outKey,'outValue>) = 
    interface Job2_1<'inKey1,'inValue1,'inKey2, 'inValue2, 'outKey,'outValue> with
        member this.onProcess1(cell: KeyValueCell<'inKey1,'inValue1>) (emitter: Emitter<'outKey,'outValue>) =
                emitter.emit(choose1(cell))

        member this.onProcess2(cell: KeyValueCell<'inKey2,'inValue2>) (emitter: Emitter<'outKey,'outValue>) =
                emitter.emit(choose2(cell))
*)