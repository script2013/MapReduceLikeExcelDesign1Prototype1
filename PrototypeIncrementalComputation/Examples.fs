module Examples

open Common
open Interfaces
open Combinators

module SimpleTests = 
    let filter_test() = 
        let input = ActiveSeq.source<int, int>() //define input
        
        let dbRef = input 
                    |> ActiveSeq.filter(fun (k,v) -> v >=2) 
                    |> ActiveSeq.output
        
        let cell1 = input.insert(1, 3)
        let cell2 = input.insert(2, 5)
        
        cell1.modifyValue(1)
        dbRef.dumpContents()

    let map_test() = 
        let input = ActiveSeq.source<int, int>() 

        let dbRef = input 
                    |> ActiveSeq.map(fun (k,v) -> (k, if v % 2 = 0 then "even" else "odd")) 
                    |> ActiveSeq.output
        
        let cell1 = input.insert(1, 3)
        let cell2 = input.insert(2, 5)
        
        dbRef.dumpContents()

        cell1.modifyValue(2)
        
        dbRef.dumpContents()

    let merge_test() = 
        let input1 = ActiveSeq.source<int, int>()
        let input2 = ActiveSeq.source<int, int>()
        let dbRef = ActiveSeq.merge2 input1 input2 (fun (k,v) -> (k, Left v)) (fun (k,v) -> (k, Right v))
                    |> ActiveSeq.output

        let cell1 = input1.insert(1, 3)
        let cell2 = input2.insert(1, 4)

        dbRef.dumpContents()

        cell1.modifyValue(5)

        dbRef.dumpContents()

    let test_sum() = 
        let input = ActiveSeq.source<Null, int>()
        let dbRef = input 
                    |> ActiveSeq.intSum
                    |> ActiveSeq.output
        
        let cell1 = input.insert(Null,3)
        dbRef.dumpContents()
        cell1.modifyValue(5)
        dbRef.dumpContents()        

    let test_reduce() = 
        let input1 = ActiveSeq.source<int, int>()
        let dbRef = input1 
                    |> ActiveSeq.reduceByKey(ActiveSeq.intSum)
                    |> ActiveSeq.output

        let cell1 = input1.insert(1,3)
        let cell2 = input1.insert(1, 2)
        let cell3 = input1.insert(10, 4)

        dbRef.dumpContents()
        cell1.modifyValue(10)
        dbRef.dumpContents()

    let send_to_multiple() = 
        let input = ActiveSeq.source<int, int>()
        let dbEven = input 
                     |> ActiveSeq.filter(fun (k,v) -> k % 2 = 0)
                     |> ActiveSeq.outputWithName "even"
        
        let dbOdd = input 
                    |> ActiveSeq.filter(fun (k,v) -> v % 2 = 1)
                    |> ActiveSeq.outputWithName "odd"
        
        let cell1 = input.insert(1,1)
        let cell2 = input.insert(2,2)
        let cell3 = input.insert(3, 3)
        
        dbEven.dumpContents()
        dbOdd.dumpContents()    
        
        printfn "after modifications"
        input.insert(4,4) |> ignore
        
        dbEven.dumpContents()
        dbOdd.dumpContents()  
                       
    let test_topby() = 
        let input = ActiveSeq.source<string, int>()        
        let dbRef = input 
                    |> ActiveSeq.reduceByKey(ActiveSeq.topKByValuePred(1, fun k -> -k)) 
                    |> ActiveSeq.outputWithName "reduceThenTop"

        let cell1 = input.insert("a",1)
       
        let cell2 = input.insert("a", 5)
        let cell3 = input.insert("a", 2)
        let cell4 = input.insert("a", 3)
        
                
        dbRef.dumpContents()
        
        cell3.modifyValue(10)
        cell3.modifyValue(-1)
        dbRef.dumpContents()
        
    let test_topby1() = 
        let input = ActiveSeq.source<string, int>()        
        let dbRef = input 
                    |> ActiveSeq.reduceByKey(fun nested -> nested |> ActiveSeq.topKByValuePred(1, fun k -> -k)) 
                    |> ActiveSeq.outputWithName "reduceThenTop"

        let cell1 = input.insert("a",1)
        let cell2 = input.insert("a", 5)
        let cell3 = input.insert("a", 2)
        //let cell4 = input.insert("a", 3)
                        
        dbRef.dumpContents()

    let test_join() = 
        let input1 = ActiveSeq.source<int, int>()
        let input2 = ActiveSeq.source<int, string>()
        //join...

        //will not work if one stream has a very larger number of elements
        //they need to be saved, and when the other stream changes to be replayed
        let dbRef = ActiveSeq.merge2 input1 input2 (fun (k,v) -> (k, Left v)) (fun (k,v) -> (k, Right v))
                    |> ActiveSeq.reduceByKey(ActiveSeq.toSet)
                    |> ActiveSeq.filter(fun (_,s) -> s.Count > 1) //has to have both left and right
                    |> ActiveSeq.output

        let cell1 = input1.insert(3, 3)
        let cell2 = input2.insert(3, "three")
        let cell4 = input1.insert(4, 4)
        let cell5 = input1.insert(3, 5)
        dbRef.dumpContents()

        //cell1.modifyValue(33)

        dbRef.dumpContents()    

[<EntryPoint>]
let main(args: string[]) =
    //test4()
    //SimpleTests.filter_test()
    //SimpleTests.map_test()
    //SimpleTests.test_sum()
    //SimpleTests.test_reduce()
    //SimpleTests.send_to_multiple()
    //SimpleTests.test_topby()
    //SimpleTests.test_topby1()
    SimpleTests.test_join()

    0

 //TO DO: LONG TERM HISTORY VS. SHORT TERM HISTORY