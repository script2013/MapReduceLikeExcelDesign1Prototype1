module program //incremental recommender
open Interfaces
open Combinators
open CombinatorsImpl

//for example (word count)
//let input = new ActiveSeq<string, int>()

let wordcount: Job1_1<string, int, string, int> = (reduceByKey (fun () -> intSum()))
let wordcount_post_filter = wordcount >>> filter (fun (word, count) -> count > 5)

let wordcount_pre_post_filter: Job1_1<string, int, string, int> = 
    filter (fun (word, count) -> word.StartsWith("a")) >>> 
    reduceByKey (fun () -> intSum()) >>>
    filter (fun (word, count) -> count > 5)    
        

//(getReducerTask: 'key -> Job1_1<Null, 'invalue, Null, 'outvalue>) = 

// (new Filter(fun (key,value) -> value > 5)
type Document = {
    docid: int
    words: Set<string>
}

let explodedDocuments: Job1_1<int, Document, int, string> = mapValues (fun doc -> doc.words) >>> explodeValues ()
let wordCounts = explodedDocuments >>> swapKeyValues () >>> (reduceByKey (fun () -> mapValues(fun v -> Null) >>> longCount()))
let filteredWords = wordCounts >>> filter (fun (word, cnt) -> cnt >= 3L)
let topWords = wordCounts >>> topKByValue (100, true) //get top most frequent words 

let joinPredicate<'leftValue,'rightValue, 'combinedValue>(leftJoinProp: JoinProp, 
                                                              rightJoinProp: JoinProp, 
                                                              comb: option<'leftValue>*option<'rightValue> -> option<'combinedValue>): Job1_1<Null, Either<'leftValue, 'rightValue>, Null, 'combinedValue> =
    new Join<'leftValue, 'rightValue, 'combinedValue>(leftJoinProp, rightJoinProp, comb) :> Job1_1<Null, Either<'leftValue, 'rightValue>, Null, 'combinedValue>

let crossProduct<'value when 'value: equality>(): Job1_1<Null, 'value, Null, ('value*'value)> =
    new CrossProduct<'value>() :> Job1_1<Null, 'value, Null, ('value*'value)>

let mapExtended<'key1,'value1,'key2,'value2 when 'key1: equality and 'key2: equality and 'value2: equality>(map: ('key1*'value1) -> ('key2*'value2)) =
    new MapExtended<'key1,'value1,'key2,'value2>(map) :> Job1_1<'key1,'value1, 'key2,'value2> 
    //interface Job1_1<'key1,'value1, 'key2,'value2> with
    //    member this.onProcess(cell: KeyValueCell<'key1,'value1>) (emitter: Emitter<'key2,'value2>): unit = ()


type WordStats = {
    docid: int
    tf: float
    idf: float
}

let joinedWithWordCounts: Job1_1<string, Either<(int*float), float>, string, WordStats> = 
    let combine (left: option<int*float>, right: option<float>): option<WordStats> = 
        match left, right with
         | Some(docid, tf), Some(idf) -> Some({docid = docid; tf = tf; idf = idf}: WordStats)
         | _ -> None

    reduceByKey (fun () -> joinPredicate(JoinProp.OneToMany, JoinProp.One, combine))

let changeKey<'key,'value, 'newkey, 'satelitevalue when 'key: equality and 'newkey: equality>(newKeySateliteData: 'value -> 'newkey*'satelitevalue): Job1_1<'key,'value, 'newkey,('key*'satelitevalue)> = 
    new ChangeKey<'key,'value, 'newkey, 'satelitevalue>(newKeySateliteData: 'value -> 'newkey*'satelitevalue):> Job1_1<'key,'value, 'newkey,('key*'satelitevalue)>

let groupStatsByDoc: Job1_1<string, WordStats, int, (string*float)> = 
    changeKey (fun (wordStats: WordStats) -> (wordStats.docid, float(wordStats.idf*wordStats.tf))) >>>
    reduceByKey (fun () -> topKByValue(20, false))

let computePairWiseSimilarityViaIndex : Job1_1<int (*docid*), string (*word*), int, (int*int64)> =
    swapKeyValues () >>> //(word,docid
    reduceByKey (fun () -> crossProduct()) >>> //(word,(docid,docid))
    swapKeyValues () >>>
    reduceByKey (fun () -> mapValues (fun _ -> Null) >>> longCount()) >>>  //(int*int), int64
    mapExtended (fun ((doc1, doc2), count) -> (doc1, (doc2, count))) >>>
    reduceByKey (fun () -> topKByValue(100, false)) //this is wrong because it uses (docId, score) for comparision

//for each document extract top keywords words by tf*idf
//
//(docid,wordid, word tf, join with word counts and get word idf) 
(* 

    (word,doc) -> (word, [d1,d2, ...,dn] -> cross product: (d1,d2) (d1,d3), (d1,d4))  

*)

//let asIndex (): Job1_1<string (*word*), int (*doc*)>
//ignore min hash

    



(*
type Job2_1<'a,'b, 'c> = interface
    abstract member onInserted1_Inserted2
end
*)
