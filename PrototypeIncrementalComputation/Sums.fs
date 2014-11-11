module Sums

open Interfaces

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
        member this.keepsState = true
        member this.isStateEmpty with get() = (i = 0)

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
type LongSum() = inherit NumericSum<int64, int64>({zero = 0L; plus = (+); minus = (-); getCount = id})
type FloatSum() = inherit NumericSum<float, float>({zero = 0.0; plus = (+); minus = (-); getCount = id})

type IntCounter() = inherit NumericSum<Null, int>({zero = 0; plus = (+); minus = (-); getCount = fun Null -> 1}) 
type FloatCounter() = inherit NumericSum<Null, float>({zero = 0.0; plus = (+); minus = (-); getCount = fun Null -> 1.0}) 
type LongCounter() = inherit NumericSum<Null, int64>({zero = 0L; plus = (+); minus = (-); getCount = fun Null -> 1L})

type BuildSet<'a when 'a: comparison>() = inherit NumericSum<'a, Set<'a>>({zero = Set.empty; plus = Set.union ; minus = Set.difference; getCount = Set.singleton})
