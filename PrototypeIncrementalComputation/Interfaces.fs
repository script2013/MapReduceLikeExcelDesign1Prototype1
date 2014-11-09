module Interfaces

//this is one approach: type ACell<'a> = Inserted of 'a | Deleted of 'a | Modified of 'a*'a
type KeyValueCell<'key,'value when 'key: equality> = 
    | Inserted of 'key*'value 
    | Deleted of 'key*'value 
    | Modified of 'key*('value*'value) //modified the value

type Null = Null

type Emitter<'key,'value when 'key: equality> = interface
    abstract member emit: KeyValueCell<'key, 'value> -> unit
end
 
type Job1_1<'inKey,'inValue,'outKey,'outValue when 'inKey: equality and 'outKey: equality> = interface
(*
    abstract member onInserted: 'a -> Emitter<'outKey,'outValue> -> unit
    abstract member onDeleted: 'a -> Emitter<'outKey,'outValue> -> unit
    abstract member onModified: ('a (*oldvalue*)*'a(*newvalue*)) -> Emitter<'outKey,'outValue> -> unit  
*)
    abstract member onProcess: KeyValueCell<'inKey,'inValue> -> Emitter<'outKey,'outValue> -> unit 
end 

type Job2_1<'inKey1,'inValue1,'inKey2, 'inValue2, 'outKey,'outValue when 'inKey1: equality and 'inKey2: equality and 'outKey: equality> = interface
    abstract member onProcess1: KeyValueCell<'inKey1,'inValue1> -> Emitter<'outKey,'outValue> -> unit 
    abstract member onProcess2: KeyValueCell<'inKey2,'inValue2> -> Emitter<'outKey,'outValue> -> unit 
end

type Either<'a,'b> = Left of 'a | Right of 'b

(*
type Job2_1<'inKey,'inValue1,'inValue2, 'outKey,'outValue when 'inKey1: equality and 'outKey: equality> = interface
    abstract member onProcess: Either<KeyValueCell<'inKey,'inValue>, KeyValueCell<'inKey,'inValue>> -> Emitter<'outKey,'outValue> -> unit 
end 
*)
