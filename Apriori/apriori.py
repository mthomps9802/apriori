#!/usr/bin/env python

from tabulate import tabulate 
import sqlite3 
import os.path

####################
# SETUP            #
####################
def SetupInMemory(DB_PATH):

    if not os.path.isfile(DB_PATH):
        print("Could not find database file at: {}".format(DB_PATH))
        exit(1)

    source_connection = sqlite3.connect(DB_PATH)
    dest_connection = sqlite3.connect(":memory:")
    source_connection.backup(dest_connection)
    source_connection.close()
    cursor = dest_connection.cursor()
    return dest_connection, cursor

####################
# TEARDOWN         #
####################
def Teardown(db_connection, cursor):

    cursor.close()
    db_connection.close()

#####################
# CHECKPOINT HELPER #
#####################
def CheckpointHelper(K, results, checkpoints_on, is_candidate):

    # Check each iteration of the candidate generation
    if is_candidate and checkpoints_on:
        expected = []
        if K==1:
            expected = [[1,2], [1,3], [1,4], [1,5], [2,3], [2,4], [2,5], [3,4], [3,5], [4,5]]
        if K==2:
            expected = [[1, 2, 3], [1, 2, 5]]
        error_message = "Unexpected number of candidates at level {}: {}".format(K, len(results))
        assert len(results) == len(expected), error_message
        error_message = "Unexpected candidate result at level {}: {}".format(K, results)
        assert results == expected, error_message

    # Check each iteration of frequent itemsets
    elif not is_candidate and checkpoints_on:
        expected = []
        if K==1:
            expected = [[1, 2], [1, 3], [1, 5], [2, 3], [2, 4], [2, 5]]
        if K==2:
            expected = [[1, 2, 3], [1, 2, 5]]
        error_message = "Unexpected number of frequent itemsets at level {}: {}".format(K, len(results))
        assert len(results) == len(expected), error_message
        error_message = "Unexpected frequent itemsets at level {}: {}".format(K, results)
        assert results == expected, error_message

####################
# GET TRANSACTIONS #
####################
def GetTransactions(cursor):

    # List of transactions to return
    transactions=[]


    #Groups products that share the same product id
    QUERY = "SELECT order_id, GROUP_CONCAT(product_id) FROM OrderProducts GROUP BY order_id"

    cursor.execute(QUERY)

    results = cursor.fetchall()

    #eliminate duplicates
    transactions = [set(map(int, r[1].split(','))) for r in results]

    # Debugging print statement
    print(f"WE HAVE RETRIEVED {len(transactions)} transactions.")
    return transactions

###########################
# GET FREQUENT 1-ITEMSETS #
###########################
def GetFrequent1Itemsets(cursor, min_support_abs):

    # List of 1-itemsets to return
    itemsets=[]
    
    #Count must be greater than or = to min support abs
    QUERY = "SELECT product_id, COUNT(*) as support FROM OrderProducts GROUP BY product_id HAVING support >= ?"

    cursor.execute(QUERY, (min_support_abs,))

    results = cursor.fetchall()

    #takes in first ele and converts to int for a list
    #list of list output represents prod id
    itemsets = [[int(r[0])] for r in results]

    #Debugging print statement
    print(f"Found {len(itemsets)} frequent 1-itemsets.")
    return itemsets

####################
# GET CANDIDATES   #
####################
def GetCandidates(L_K_minus_1):

    # Initialize candidate list
    C_k = []
    #for each pair of itemsets from the previous iteration
    for i in range(len(L_K_minus_1)):
        for j in range(i + 1, len(L_K_minus_1)):
            L1, L2 = sorted(L_K_minus_1[i]), sorted(L_K_minus_1[j])
            #If the first k-2 are ==
            #and the last item of L1 is less than that of L2
            if L1[:-1] == L2[:-1] and L1[-1] < L2[-1]:
                # Join to form a new candidate 
                # new candidate ffrom combine
                candidate = L1[:-1] + [L1[-1], L2[-1]]
                # Prune if any subset isnt frequent
                if all(sorted(candidate[:m] + candidate[m+1:]) in L_K_minus_1 for m in range(len(candidate))):
                    C_k.append(candidate)

    # Debugging print statement
    print(f"Generated {len(C_k)} candidates from {len(L_K_minus_1)} frequent itemsets.")

    return C_k

####################
# CHECK SUPPORT    #
####################
def CheckSupport(transactions, C, min_support_count):

    # Check each candidate against the minimum support threshold
    F_k = []
    #Count support for each candidate
    for candidate in C:
        candidate_set = set(candidate)
        #counts transactions in itemset
        support = sum(1 for t in transactions if candidate_set.issubset(t))
        #add if its meets or exceeds minsupportcount
        if support >= min_support_count:
            F_k.append(candidate)

    # Debugging print statement
    print(f"{len(F_k)} candidates met the minimum support.")

    return F_k

####################
# GET CONFIDENCE   #
####################
def GetConfidence(transactions, lhs, rhs):
    # Calculate the confidence for the rule lhs -> rhs
    lhs_set = set(lhs)
    #combines lhs  with rhs into a set
    #Eg. rhs_set = {'bread', 'butter'}
    rhs_set = lhs_set.union(rhs)
    
    #Calculate support counts
    support_lhs = sum(1 for t in transactions if lhs_set.issubset(t))
    support_rhs = sum(1 for t in transactions if rhs_set.issubset(t))

    # Confidence calculation
    confidence = support_rhs / support_lhs if support_lhs > 0 else 0

    # Debugging print statement
    print(f"Confidence for rule {lhs} -> {rhs}: {confidence}")
    return confidence 

####################
# CHECK CONFIDENCE #
####################
def CheckConfidence(transactions, F, min_confidence_percentage):

    # The strong association rules to return (confidence > the minimum confidence threshold)
    A = []
    for itemset in F:
        # Itemsets of size 1 cannot be made into an association rule
        if len(itemset) <= 1:
            continue
        for item in itemset:
            other_items = itemset.copy()
            other_items.remove(item)

            # Check rule for: item -> [other_items]
            confidence = GetConfidence(transactions, [item], other_items)
            if confidence >= min_confidence_percentage:
                A.append([item, other_items, confidence])

            # Check rule for: [other_items] -> item                
            confidence = GetConfidence(transactions, other_items, [item])
            if confidence >= min_confidence_percentage:
                A.append([other_items,item, confidence])
    return A

####################
# APRIORI          #
####################
def Apriori(db_path, min_support_percentage, min_conf_percentage, checkpoints_on=False):
    
    db_connection, cursor = SetupInMemory(db_path)

    transactions = GetTransactions(cursor)
    #PRINT STATEMENT
    print("Transactions retrieved:", transactions) 
    
    # Convert the minimum support threshold from a percentage abs val
    num_transactions = len(transactions)
    min_support_count = max(1, int(min_support_percentage * num_transactions))
    #PRINT STATEMENT
    print("Minimum support count:", min_support_count)
    

    L1 = GetFrequent1Itemsets(cursor, min_support_count)
    #PRINT STATEMENT
    print("Frequent 1-itemsets:", L1)  # For debugging

    #Checkpoint validations
    if checkpoints_on:
        assert len(transactions) == 9, "Unexpected number of transactions"
        assert min_support_count == 2, "Unexpected minimum support count"
        assert len(L1) == 5, "Unexpected number of frequent 1-itemsets"
        

    # Initialize the variable to hold all frequent itemsets
    all_frequent_itemsets = [L1]
    
    #Initialize K
    k = 2

    #Generatesfreq itemset
    while True:
        # Generate candidates based on freq isets
        Ck = GetCandidates(all_frequent_itemsets[k-2])
        #print For debugging
        print(f"Candidates at level {k}:", Ck)  

        # Filter itemsets by support that dont meet the thresh
        Fk = CheckSupport(transactions, Ck, min_support_count)
        #PRINT STATEMENT
        print(f"Frequent itemsets at level {k}:", Fk)  # For debugging
        
        # If no frequent itemsets are found, break the loop
        if not Fk:
            break
        
        # Append the frequent itemsets 
        all_frequent_itemsets.append(Fk)
        
        # Increase k for the next iteration
        k += 1

    # Flatten the list of all frequent itemsets into a single list
    frequent_itemsets = [item for sublist in all_frequent_itemsets for item in sublist]
    # For debugging
    print("All frequent itemsets:", frequent_itemsets)  

    # Find strong association rules from the frequent itemsets
    association_rules = CheckConfidence(transactions, frequent_itemsets, min_conf_percentage)
    # For debugging
    print("Association rules found:", association_rules)  

    # Disconnect from db
    Teardown(db_connection, cursor)

    # Return the found strong association rules
    return association_rules
    
####################
# MAIN             #
####################
if __name__=="__main__":

    # Run the apriori algorithm on the textbook example
    min_support = (2.0/9.0)
    min_confidence = 0.75
    results = Apriori("./all_electronics.db", min_support, min_confidence, checkpoints_on=True)

    # Write results to file
    f = open("all_electronics.txt", "w")
    for result in results:
        lhs, rhs, conf = result
        f.write("{} -> {} : {} \n".format(lhs, rhs, conf))
    f.close()

    #Was unable to implement
    #TRY AGAIN LATER
    # Optional: Try your implementation on our grocery database
    RUN_GROCERY_DB=False
    if RUN_GROCERY_DB:
        
        # Run the apriori algorithm on the grocery data
        results = Apriori("./grocery.db", 0.01, 0.5, checkpoints_on=False)
        
        # Write results to file
        f = open("grocery.txt", "w")
        f.write(str(results))
        f.close()
