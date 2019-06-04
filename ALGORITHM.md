# Assumptions

  - Less than 10 000 stores
  - Less than 1 million references per store
  - Less than 10 million unique products
  - Less than 10 million tx/j
  - Store references are ordered by productId in the files
  - For a given product, its ID stays the same in any price reference file.
      - Note that this seems inconsistent with the example data : prices for a
        given product vary wildly from one store to the next. I am assuming
        this is because the example data is generated randomly.
  - Transaction IDs are useless for us. I don't really undersytand their
    meaning: tx with the same ID have a different time stamps and store ID...

# Goal

To have an algorithm that computes the top 100 best sellers in term of
quantity and revenue for each day, store and overall; and for a 7 days
sliding windows as well.
For any N as per the assumptions above, this algorithm should ideally
have a worst-case complexity of:
  - O(1) for memory space
  - O(Nlog(N)) for disk space
  - O(Nlog(N)) for disk reads (as in # of lines read from disk, randomly or
    sequentially)
  - O(Nlog(N)) for time
and be paralellizable

# Algorithm

For each day:
  - Load and sort transactions
  - select sum(quantity) from transactions group by (storeId, productId)
    + Note that this can be done in a single scan since we sorted the transactions previously
  - join with each store references to get the revenue
    + Since evething is sorted, this is again a single scan
    + We assume reference files are sorted by productId
  - Store the resulting daily aggregate into a file

The for the last day:
  - Find the overall best sellers by quantity and by revenue.
  - Sort by (storeId, quantity) and find the best sellers for each store.
  - Sort by (storeId, revenue) and find the best sellers for each store.

Then merge each daily aggregate for the 7 days into one and get the best sellers again.
