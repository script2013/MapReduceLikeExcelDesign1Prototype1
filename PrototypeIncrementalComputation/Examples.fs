module Examples

open Interfaces
open Combinators
open CombinatorsImpl

//for example (word count)
//let input = new ActiveSeq<string, int>()

let wordcount: Job1_1<string, int, string, int> = (reduceByKey (fun () -> intSum()))
let wordcount_post_filter = wordcount >>> filter (fun (word, count) -> 
                                                                    count > 5)

let wordcount_pre_post_filter: Job1_1<string, int, string, int> = 
    filter (fun (word, count) -> word.StartsWith("a")) >>> 
    reduceByKey (fun () -> intSum()) >>>
    filter (fun (word, count) -> count > 5)    
    
let simple_filter = filter (fun (word: string, count) -> count > 2)
    
let test1() =
    //generate some data
    let input = new ActiveSeq<string, int>()
    let rep = reporter("")
    
    //setup input (simple_filter >>> rep)
    setup input (wordcount_post_filter >>> rep)
    let cell1 = input.insert("a", 2)
    let cell2 = input.insert("b", 3)
    let cell3 = input.insert("a", 4)

    cell3.modifyValue(8)

    rep.reportState()
    0

type Document = {
    docid: int
    words: Set<string>
}

let test2() = 
    let doc1: Document = {docid = 1; words = Set.ofList ["a"; "b"] }
    let doc2: Document = {docid = 2; words = Set.ofList ["a"] }
    let input = new ActiveSeq<int, Document>()

    let rep = reporter("")
    let explodedDocuments: Job1_1<int, Document, int, string> = mapValues (fun doc -> doc.words) >>> explodeValues ()
    let wordCounts = explodedDocuments >>> swapKeyValues () >>> (reduceByKey (fun () -> mapValues(fun v -> Null) >>> longCount()))
    //let filteredWords = wordCounts >>> filter (fun (word, cnt) -> cnt >= 3L)
    let topWords = wordCounts >>> topKByValue (1, false) //get top most frequent words 
    
    setup input (topWords >>> rep)
    let cell1 = input.insert (1, doc1)
    let cell2 = input.insert (2, doc2)

    cell2.modifyValue({docid=2; words = Set.ofList["b"]})
    rep.reportState()
    0

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

let test4 () = 
    (*
    Inserted (1, (3, 1L))
Inserted (3, (1, 1L))
Inserted (1, (4, 1L))
Inserted (4, (1, 1L))
Inserted (3, (4, 1L))
Inserted (4, (3, 1L))
Deleted (1, (3, 1L))
Deleted (3, (1, 1L))
Deleted (3, (4, 1L))
Deleted (4, (3, 1L))
--------------------------
(1, (4, 1L))
(4, (1, 1L))*)
    let e1= (1, (3, 1L)) //del
    let e2=(3, (1, 1L)) //del
    let e3=(1, (4, 1L))
    let e4=(4, (1, 1L))
    let e5=(3, (4, 1L)) //del
    let e6=(4, (3, 1L)) //del
    let rep = reporter("")    
    let job = reduceByKey (fun () -> topKByValue(100, false))

    let input = new ActiveSeq<int, (int* int64)>()
    setup input (job >>> rep)

    let cell1 = input.insert e1
    let cell2 = input.insert e2
    let cell3 = input.insert e3
    let cell4 = input.insert e4
    let cell5 = input.insert e5
    let cell6 = input.insert e6
    
    cell1.delete() // .modifyValue("b")
    cell2.delete()
    cell5.delete()
    cell6.delete()
    rep.reportState()

//[<EntryPoint>]
//let main(args: string[]) =
let test5() = 
    let doc1 = (1, "a")
    let doc2 = (2, "b")
    let doc3 = (3, "a")
    let doc4 = (4, "a")

    let input = new ActiveSeq<int, string>()
    let rep = reporter("")    
    let job = swapKeyValues () >>> (reduceByKey (fun () -> crossProduct()) ) >>> 
                mapValues(fun (a,b) -> Set([(a,b);(b,a)])) >>> explodeValues () >>>
                swapKeyValues () >>>  //(docid,docid)
                    reduceByKey (fun () -> mapValues (fun _ -> Null) >>> longCount()) >>> //(doc_i, doc_j) co-occurence
                    mapExtended (fun ((doc1, doc2), count) -> (doc1, (doc2, count))) //>>> 
                     //reduceByKey (fun () -> topKByValue(100, false))
                 //mapExtended (fun ((doc1: int, doc2: int), count:int64) -> (doc1, (doc2, count)))

    setup input (job >>> rep)

    let cell1 = input.insert doc1
    let cell2 = input.insert doc2
    let cell3 = input.insert doc3
    let cell4 = input.insert doc4

    cell3.delete() // .modifyValue("b")
    rep.reportState()
    0


type Product = {
    product_id: int
    product_name: string
    //other non relevant attributes
}

type Category = {
    category_id: int
    category_name: string
}

type ProductCategoryAssignment = {
    product_id: int
    category_id: int
}

type PurchasedItem = {
    product_id: int
    quantity: int
    price: float
}

type Purchases = {
    purchase_id: int
    user_id: int
    purchased_items: list<PurchasedItem>     
}

type ProductCategorySales = {
    product_id: int
    category_id: int
    sales: float
}

let test6() = 
    
    let products: list<Product> = [
        {product_id = 1; product_name = "xyz"};
        {product_id = 2; product_name = "mnr"}
    ]
    
    let categories: list<Category> = [
        {category_id = 2; category_name = "abc"};
        {category_id = 3; category_name = "pqr"};
    ]

    let product_category_assignment: list<ProductCategoryAssignment> = [
        {product_id = 1; category_id = 2}
    ]

    let purchases: list<Purchases> = [
        {purchase_id = 1; user_id = 5; purchased_items = [{product_id = 1; quantity = 1; price = 5.0}]}    
    ]

    let productsTable = new ActiveSeq<int, Product>()
    let categoriesTable = new ActiveSeq<int, Category>()

    let productCategoryTable = new ActiveSeq<int, ProductCategoryAssignment>()
    let purchasesTable = new ActiveSeq<int, Purchases>()
    
    let job = purchases |> Seq.collect (fun purchase -> seq{for item in purchase.purchased_items do 
                                                                yield item.product_id, (float item.quantity)*item.price } )
    
    let productSales = purchasesTable 
                       |> ActiveSeq.collect (fun (_,purchase) -> 
                                                seq{for item in purchase.purchased_items do 
                                                    yield item.product_id, (float item.quantity)*item.price } )
    
    let productCategorySales: ActiveSeq<int, (int*float)> = 
                        productSales
                        |> ActiveSeq.leftJoin (fun (prdId,value) -> prdId)
                                              productCategoryTable
                                              (fun (_, category) -> category.product_id)
                                              (fun joinKey (prdId,value) (_,category) ->
                                                    {product_id = prdId; category_id = category.category_id; sales = value}) 
                        |> ActiveSeq.map(fun (category_id, prodCategorySales) -> (prodCategorySales.category_id, (prodCategorySales.product_id, prodCategorySales.sales)))

    
    //how do we deal with nested sequences
    //how do we deal with merged sequences
    //jobs having two outputs 
    //jobs having two inputs 
    let topProductsPerCategory: ActiveSeq<int,(int*float)> = //categoryid, productid, sales of product
                                productCategorySales 
                                |> ActiveSeq.reduceByKey(fun valuesPerKey -> valuesPerKey |> ActiveSeq.topKByValuePred(100, fun (_,v)-> -v))

    let j1 = 
        topProductsPerCategory 
        |> ActiveSeq.leftJoin (fun (categoryid, (product_id, value)) -> categoryid)
                              categoriesTable
                              (fun (category_id,category) -> category_id)
                              (fun joinKey (category_id, (product_id, value))  (category_id_1,category) ->
                                        (category.category_name, product_id, value))
    let j2 = j1
            |> ActiveSeq.leftJoin (fun (category_id, (category_name, product_id, value)) -> product_id)
                                    productsTable
                                    (fun (product_id, product) -> product.product_id)  
                                    (fun product_id (category_id,(category_name, product_id, value)) (_,product) -> 
                                        (product_id, product.product_id, category_id, category_name, value))   
                                        
                                                                                         
    //ProductCategorySales)
    (* 
    let a = productsTable |>> 
            filter ( fun (a,b) -> a = 1) |>> 
            reporter ()
    *)

    //job.reduceOn the product id, then sum

    //job.reduceOn categories id and get the top products in a category
    //get top categories

    //join the output with products names and categories_names

    //then see what happens if a new category is created (or a product is moved from one category to another one) is changed.
    //then join the categories
    (*
    let product_category_join =
            leftJoin productCategoryTable (fun prodCat -> prodCat.product_id)
                     productsTable (fun prod -> prod.product_id)
                     (fun prodCat prod -> prodCat.cat_id, (prod.id, prod.name) 
    *) 
    
    //productsTable.in
    //step one join products with categories
    (*
    entity_lookup(products, fun product -> product.category_id,
                  categories, fun category -> category.category_id,
                  fun (product, category) -> {product with produc
                    
    *)
    //purchases.groupby product
    ()
    
module SimpleTests = 
    let filter_test() = 
        let input = ActiveSeq<int, int>() //define input
        
        let dbRef = input 
                    |> ActiveSeq.filter(fun (k,v) -> v >=2) 
                    |> ActiveSeq.output
        
        let cell1 = input.insert(1, 3)
        let cell2 = input.insert(2, 5)
        
        cell1.modifyValue(1)
        dbRef.dumpContents()

    let map_test() = 
        let input = ActiveSeq<int, int>() 

        let dbRef = input 
                    |> ActiveSeq.map(fun (k,v) -> (k, if v % 2 = 0 then "even" else "odd")) 
                    |> ActiveSeq.output
        
        let cell1 = input.insert(1, 3)
        let cell2 = input.insert(2, 5)
        
        dbRef.dumpContents()

        cell1.modifyValue(2)
        
        dbRef.dumpContents()

    let merge_test() = 
        let input1 = ActiveSeq<int, int>()
        let input2 = ActiveSeq<int, int>()
        let dbRef = ActiveSeq.merge2 input1 input2 (fun (k,v) -> (k, Left v)) (fun (k,v) -> (k, Right v))
                    |> ActiveSeq.output

        let cell1 = input1.insert(1, 3)
        let cell2 = input2.insert(1, 4)

        dbRef.dumpContents()

        cell1.modifyValue(5)

        dbRef.dumpContents()

    let test_sum() = 
        let input = ActiveSeq<Null, int>()
        let dbRef = input 
                    |> ActiveSeq.intSum
                    |> ActiveSeq.output
        
        let cell1 = input.insert(Null,3)
        dbRef.dumpContents()
        cell1.modifyValue(5)
        dbRef.dumpContents()        

    let test_reduce() = 
        let input1 = ActiveSeq<int, int>()
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
        let input = ActiveSeq<int, int>()
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
        let input = ActiveSeq<string, int>()        
        let dbRef = input 
                    |> ActiveSeq.reduceByKey(ActiveSeq.topKByValue(1, false)) 
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
        let input = ActiveSeq<string, int>()        
        let dbRef = input 
                    |> ActiveSeq.reduceByKey(fun nested -> nested |> ActiveSeq.topKByValuePred(1, fun k -> -k)) 
                    |> ActiveSeq.outputWithName "reduceThenTop"

        let cell1 = input.insert("a",1)
        let cell2 = input.insert("a", 5)
        let cell3 = input.insert("a", 2)
        //let cell4 = input.insert("a", 3)
                        
        dbRef.dumpContents()

    let test_join() = 
        let input1 = ActiveSeq<int, int>()
        let input2 = ActiveSeq<int, string>()
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
    //SimpleTests.map_test()
    SimpleTests.test_join()
    0