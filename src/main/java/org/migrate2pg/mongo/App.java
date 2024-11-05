package org.migrate2pg.mongo;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;

public class App {
    public static void main(String[] args) throws SQLException {
        Document document = new Document();
        document.append("age", 30);
        document.append("email", "user@exmaple.org");

        // save document to mongodb as old way
        saveToMongoDB(document);
        System.out.println("document saved to mongodb");


        // save document to pg
        saveToPG(document);
        System.out.println("document saved to postgresql");
    }

    private static void saveToPG(Document document) throws SQLException {
        // jdbc:postgresql://localhost:5432/postgres?user=
        String PGURL = System.getenv("PGURL");
        if (PGURL == null || PGURL.isEmpty()) {
            System.out.println("env PGURL is empty");
            return;
        }

        Connection conn = DriverManager.getConnection(PGURL);
        conn.setAutoCommit(false);

        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE IF NOT EXISTS sink_table(_id TEXT NOT NULL PRIMARY KEY, \"$\" JSONB)");
        conn.commit();

        String oid;
        String line;

        // generate an _id for the record
        if (document.get("_id") == null) {
            oid = ObjectId.get().toString();
            document.append("_id", oid);
        } else {
            oid = document.getObjectId("_id").toString();
        }

        line = document.toJson();
        PreparedStatement insertStmt = conn.prepareStatement("INSERT INTO sink_table(_id, \"$\") VALUES(?, ?::JSONB)");
        insertStmt.setString(1, oid);
        insertStmt.setString(2, line);
        insertStmt.execute();
        conn.commit();
        conn.close();
    }

    private static void saveToMongoDB(Document document){
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017/test");
        MongoDatabase db = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = db.getCollection("example_sink");

        collection.insertOne(document);
        mongoClient.close();

    }
}
