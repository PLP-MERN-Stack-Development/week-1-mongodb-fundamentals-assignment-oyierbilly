const { MongoClient } = require('mongodb');

// Connection URL
const url = 'mongodb://localhost:27017';
const client = new MongoClient(url);

// Database Name
const dbName = 'plp_database';

async function main() {
  await client.connect();
  console.log('Connected successfully to server');
  const db = client.db(dbName);
  const collection = db.collection('books');

  // --- Basic Queries ---
  // 1. Find all books in a specific genre
  console.log("\n1. Finding all Fantasy books:");
  const fantasyBooks = await collection.find({ "genre": "Fantasy" }).toArray();
  console.log(fantasyBooks);

  // 2. Find books published after a certain year
  console.log("\n2. Finding books published after 1950:");
  const recentBooks = await collection.find({ "published_year": { $gt: 1950 } }).toArray();
  console.log(recentBooks);

  // 3. Find books by a specific author
  console.log("\n3. Finding books by J.R.R. Tolkien:");
  const tolkienBooks = await collection.find({ "author": "J.R.R. Tolkien" }).toArray();
  console.log(tolkienBooks);

  // 4. Update the price of a specific book
  console.log("\n4. Updating price of 'The Hobbit' to 18.99:");
  const updateResult = await collection.updateOne(
    { "title": "The Hobbit" },
    { $set: { "price": 18.99 } }
  );
  console.log(`${updateResult.modifiedCount} document(s) updated`);

  // 5. Delete a book by its title
  console.log("\n5. Deleting 'The Da Vinci Code':");
  const deleteResult = await collection.deleteOne({ "title": "The Da Vinci Code" });
  console.log(`${deleteResult.deletedCount} document(s) deleted`);

  // --- Task 3: Advanced Queries ---
  // 6. Find books that are both in stock and published after 2010
  console.log("\n6. Books in stock published after 2010:");
  const inStockRecentBooks = await collection.find({
    "in_stock": true,
    "published_year": { $gt: 2010 }
  }).toArray();
  console.log(inStockRecentBooks);

  // 7. Use projection to return only title, author, and price
  console.log("\n7. Books with only title, author, and price fields:");
  const projectedBooks = await collection.find({}, {
    projection: { title: 1, author: 1, price: 1, _id: 0 }
  }).toArray();
  console.log(projectedBooks);

  // 8. Implement sorting by price (ascending and descending)
  console.log("\n8a. Books sorted by price (ascending):");
  const ascendingPrice = await collection.find().sort({ "price": 1 }).toArray();
  console.log(ascendingPrice);

  console.log("\n8b. Books sorted by price (descending):");
  const descendingPrice = await collection.find().sort({ "price": -1 }).toArray();
  console.log(descendingPrice);

  // 9. Implement pagination (5 books per page)
  const pageSize = 5;
  console.log("\n9. Pagination (page 1):");
  const page1 = await collection.find().limit(pageSize).skip(0).toArray();
  console.log(page1);

  console.log("\nPagination (page 2):");
  const page2 = await collection.find().limit(pageSize).skip(pageSize).toArray();
  console.log(page2);

  // --- Task 4: Aggregation Pipeline ---
  // 10. Calculate average price by genre
  console.log("\n10. Average price by genre:");
  const avgPriceByGenre = await collection.aggregate([
    { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } }
  ]).toArray();
  console.log(avgPriceByGenre);

  // 11. Find author with most books
  console.log("\n11. Author with most books:");
  const prolificAuthor = await collection.aggregate([
    { $group: { _id: "$author", count: { $sum: 1 } } },
    { $sort: { count: -1 } },
    { $limit: 1 }
  ]).toArray();
  console.log(prolificAuthor);

  // 12. Group books by publication decade
  console.log("\n12. Books by publication decade:");
  const booksByDecade = await collection.aggregate([
    {
      $project: {
        decade: {
          $subtract: [
            "$published_year",
            { $mod: ["$published_year", 10] }
          ]
        }
      }
    },
    { $group: { _id: "$decade", count: { $sum: 1 } } },
    { $sort: { _id: 1 } }
  ]).toArray();
  console.log(booksByDecade);

  // --- Task 5: Indexing ---
  // 13. Create indexes
  console.log("\n13. Creating indexes...");
  await collection.createIndex({ "title": 1 });
  await collection.createIndex({ "author": 1, "published_year": 1 });
  console.log("Indexes created");

  // 14. Demonstrate performance improvement with explain()
  console.log("\n14. Performance comparison with explain():");
  
  console.log("Without index:");
  const withoutIndex = await collection.find({ "title": "The Hobbit" }).explain("executionStats");
  console.log("Execution time (ms):", withoutIndex.executionStats.executionTimeMillis);
  
  console.log("With index:");
  const withIndex = await collection.find({ "title": "The Hobbit" }).hint({ "title": 1 }).explain("executionStats");
  console.log("Execution time (ms):", withIndex.executionStats.executionTimeMillis);

  return 'done.';
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close());