module Interfaces

//there are alternatives to this design that should be explored
//for example instead of 'key,'value just have 'value and if keys are required use a tuple
type KeyValueCell<'key,'value when 'key: equality> = 
    | Inserted of 'key*'value 
    | Deleted of 'key*'value 
    | Modified of 'key*('value*'value) 

type Null = Null

type Emitter<'key,'value when 'key: equality> = interface
    abstract member emit: KeyValueCell<'key, 'value> -> unit
end
 

type Job1_1<'inKey,'inValue,'outKey,'outValue when 'inKey: equality and 'outKey: equality> = interface
    abstract member keepsState: bool
    abstract member isStateEmpty: bool
    abstract member onProcess: KeyValueCell<'inKey,'inValue> -> Emitter<'outKey,'outValue> -> unit
end 



